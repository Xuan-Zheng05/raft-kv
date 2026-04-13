package raft

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Xuan-Zheng05/raft-kv/network"
)

var seedFlag = flag.Uint64("seed", 0, "seed for the simulated network; 0 picks a random one")

// pollEvery is how often the harness checks who the leader is. It's small
// next to the 150 ms minimum election timeout, so a new leader gets noticed
// within a fraction of one timeout.
const pollEvery = 10 * time.Millisecond

// attemptWithin is how long one waits for a submitted command to be applied
// before submitting it again. Twice the longest election timeout is plenty
// for a live leader to get a command through a few lost messages, and if the
// leader has lost its job, it's enough time for a new one to be elected.
const attemptWithin = 2 * electionTimeoutMax

// snapshotEvery is how often the test application takes a snapshot, in log
// entries. It's small so that any test with more than a handful of commands
// compacts its logs, and a follower that misses a few dozen entries has to be
// sent a snapshot.
const snapshotEvery = 10

// cluster is a group of Raft servers talking over a simulated network, all
// inside one test process.
type cluster struct {
	t       *testing.T
	net     *network.Network
	rafts   []*Raft
	alive   []bool
	storage []*MemStorage // what each server has saved; outlives the server itself
	// leaderOf remembers who was leader in each term we've seen, so two
	// leaders in the same term get caught even if we saw them at different
	// moments.
	leaderOf map[int]int
	// plan is where randomized tests get their choices. It's seeded like the
	// network, so rerunning with -seed replays the same plan.
	plan *rand.Rand

	// The collect goroutines write these, so they're guarded by mu.
	mu          sync.Mutex
	life        []int         // counts each server's restarts, so collect can ignore a dead instance
	appliedUpTo []int         // the last index each server's current instance has applied
	snapshots   []int         // how many snapshots each server has been handed on its apply channel
	committed   map[int]any   // the command at each index, as applied by whichever server got there first
	applyErr    error         // the first time a server applied something it shouldn't have
	stop        chan struct{} // closed when the test ends, so the collect goroutines exit
}

// newCluster starts n servers. If the test fails, it prints the network's
// seed so the same fault plan can be rerun with -seed.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	seed := *seedFlag
	if seed == 0 {
		seed = rand.Uint64()
	}
	c := &cluster{
		t:           t,
		net:         network.New(seed),
		rafts:       make([]*Raft, n),
		alive:       make([]bool, n),
		storage:     make([]*MemStorage, n),
		leaderOf:    make(map[int]int),
		plan:        rand.New(rand.NewPCG(seed, 0)),
		life:        make([]int, n),
		appliedUpTo: make([]int, n),
		snapshots:   make([]int, n),
		committed:   make(map[int]any),
		stop:        make(chan struct{}),
	}
	for i := range n {
		c.start(i)
	}
	t.Cleanup(func() {
		for i, rf := range c.rafts {
			if c.alive[i] {
				rf.Kill()
			}
		}
		close(c.stop)
		c.mu.Lock()
		if c.applyErr != nil {
			t.Error(c.applyErr)
		}
		c.mu.Unlock()
		if t.Failed() {
			t.Logf("network seed %d (rerun with -seed=%d)", seed, seed)
		}
	})
	return c
}

// start boots a fresh instance of server i; after a crash, that's a
// restart. Make has to come before Serve: Raft needs its Endpoint to be
// built, and only a built Raft can handle calls.
func (c *cluster) start(i int) {
	// The new instance gets a copy of what the old one saved, so goroutines
	// the old one left behind can't write into the new one's storage.
	storage := &MemStorage{}
	if old := c.storage[i]; old != nil {
		storage.Save(old.Load())
	}
	c.storage[i] = storage

	// A restarted server applies its log again from the start, so collect
	// starts over for it too.
	c.mu.Lock()
	c.life[i]++
	life := c.life[i]
	c.appliedUpTo[i] = 0
	c.mu.Unlock()

	ep := c.net.Connect(i)
	applyCh := make(chan ApplyMsg)
	c.rafts[i] = Make(ep, i, len(c.rafts), storage, applyCh)
	ep.Serve(c.rafts[i])
	c.alive[i] = true
	go c.collect(i, life, c.rafts[i], applyCh)
}

// crash kills server i. Both halves matter: the network stops delivering to
// and from it, and Kill stops its goroutines. Without the network half, a
// killed server would still answer RPCs.
func (c *cluster) crash(i int) {
	c.net.Crash(i)
	c.rafts[i].Kill()
	c.alive[i] = false
}

// collect plays the application for one instance of server i. It applies
// what Raft hands it, checks everything on the way in, and takes a snapshot
// every snapshotEvery entries. Each server has to apply indexes in order with
// no gaps, and no two servers may ever apply different commands at the same
// index. Once server i restarts, this instance is dead and collect stops.
func (c *cluster) collect(i, life int, rf *Raft, applyCh <-chan ApplyMsg) {
	var applied []any // what this instance has applied; applied[k] is the command at index k+1
	for {
		var msg ApplyMsg
		select {
		case <-c.stop:
			return
		case msg = <-applyCh:
		}

		c.mu.Lock()
		if c.life[i] != life {
			c.mu.Unlock()
			return
		}
		if c.applyErr != nil {
			c.mu.Unlock()
			continue
		}
		if msg.IsSnapshot {
			applied, c.applyErr = c.checkSnapshot(i, msg)
		} else {
			c.applyErr = c.checkCommand(i, msg)
			applied = append(applied, msg.Command)
		}
		snapshotNow := c.applyErr == nil && !msg.IsSnapshot && msg.Index%snapshotEvery == 0
		c.mu.Unlock()

		// Raft takes its own lock in Snapshot, so ours is let go first.
		if snapshotNow {
			rf.Snapshot(msg.Index, gobEncode(applied))
		}
	}
}

// checkCommand records a command server i applied, or says what's wrong
// with it. Call it with c.mu held.
func (c *cluster) checkCommand(i int, msg ApplyMsg) error {
	if want := c.appliedUpTo[i] + 1; msg.Index != want {
		return fmt.Errorf("server %d applied index %d, but the next one should have been %d", i, msg.Index, want)
	}
	if prev, seen := c.committed[msg.Index]; seen && prev != msg.Command {
		return fmt.Errorf("server %d applied %v at index %d, but another server applied %v there", i, msg.Command, msg.Index, prev)
	}
	c.appliedUpTo[i] = msg.Index
	c.committed[msg.Index] = msg.Command
	return nil
}

// checkSnapshot records a snapshot server i was handed, or says what's
// wrong with it, and returns the commands it holds. A snapshot has to move
// the server forward, and every command in it has to match what was
// committed at its index. Call it with c.mu held.
func (c *cluster) checkSnapshot(i int, msg ApplyMsg) ([]any, error) {
	if msg.Index <= c.appliedUpTo[i] {
		return nil, fmt.Errorf("server %d was handed a snapshot up to index %d, but it had already applied up to %d", i, msg.Index, c.appliedUpTo[i])
	}
	var applied []any
	gobDecode(msg.Snapshot, &applied)
	if len(applied) != msg.Index {
		return nil, fmt.Errorf("server %d was handed a snapshot up to index %d that holds %d commands", i, msg.Index, len(applied))
	}
	for k, cmd := range applied {
		if prev, seen := c.committed[k+1]; seen && prev != cmd {
			return nil, fmt.Errorf("server %d was handed a snapshot with %v at index %d, but another server applied %v there", i, cmd, k+1, prev)
		}
		c.committed[k+1] = cmd
	}
	c.appliedUpTo[i] = msg.Index
	c.snapshots[i]++
	return applied, nil
}

// appliedAt reports how many servers have applied index, and which command
// is there. It fails the test if collect has found a problem.
func (c *cluster) appliedAt(index int) (count int, cmd any) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != nil {
		c.t.Fatal(c.applyErr)
	}
	for _, upTo := range c.appliedUpTo {
		if upTo >= index {
			count++
		}
	}
	return count, c.committed[index]
}

// one submits cmd to the current leader and waits until at least want
// servers have applied it, and returns the index it landed at. If cmd doesn't
// get applied in time, say because that leader lost its job first, one
// submits it again.
func (c *cluster) one(cmd any, want int) int {
	c.t.Helper()
	within := reelectionWithin + attemptWithin // time for a new leader to take over, then one full attempt
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		leader := c.leader()
		if leader < 0 {
			time.Sleep(pollEvery)
			continue
		}
		index, _, ok := c.rafts[leader].Start(cmd)
		if !ok {
			continue
		}

		attemptEnd := time.Now().Add(attemptWithin)
		for time.Now().Before(attemptEnd) {
			count, got := c.appliedAt(index)
			if got != nil && got != cmd {
				break // a different command won this index, so submit cmd again
			}
			if count >= want {
				return index
			}
			time.Sleep(pollEvery)
		}
	}
	c.t.Fatalf("command %v wasn't applied by %d servers within %v", cmd, want, within)
	return 0
}

// others returns every server id from 0 to n-1 except the ones given.
func others(n int, except ...int) []int {
	var ids []int
	for i := range n {
		if !slices.Contains(except, i) {
			ids = append(ids, i)
		}
	}
	return ids
}

// leader returns the live server that thinks it's the leader with the
// highest term, or -1 if there's none. A leader cut off from the others may
// still think it's in charge, but its term will be lower.
func (c *cluster) leader() int {
	c.t.Helper()
	leader, best := -1, -1
	ids, terms := c.leaders()
	for k, id := range ids {
		if terms[k] > best {
			leader, best = id, terms[k]
		}
	}
	return leader
}

// leaders returns the live servers that think they're the leader right now,
// with their terms. It also checks Raft's core safety rule every time: no
// term ever has two leaders.
func (c *cluster) leaders() (ids, terms []int) {
	c.t.Helper()
	for i, rf := range c.rafts {
		if !c.alive[i] {
			continue
		}
		t, isLeader := rf.State()
		if !isLeader {
			continue
		}
		if prev, seen := c.leaderOf[t]; seen && prev != i {
			c.t.Fatalf("servers %d and %d were both leader in term %d", prev, i, t)
		}
		c.leaderOf[t] = i
		ids, terms = append(ids, i), append(terms, t)
	}
	return ids, terms
}

// waitForLeader waits up to within for exactly one live server to think it's
// the leader, and returns that server and its term.
func (c *cluster) waitForLeader(within time.Duration) (id, term int) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for {
		ids, terms := c.leaders()
		if len(ids) == 1 {
			return ids[0], terms[0]
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("no single leader after %v; servers that think they're leader: %v", within, ids)
		}
		time.Sleep(pollEvery)
	}
}

// waitForStableLeader waits for a leader like waitForLeader, then waits twice
// the longest election timeout and checks that every live server is still in
// that leader's term. A follower that stops hearing from the leader times out
// within one election timeout and starts a new term, so this catches a leader
// that gets elected but can't hold on.
func (c *cluster) waitForStableLeader(within time.Duration) (id, term int) {
	c.t.Helper()
	id, term = c.waitForLeader(within)
	time.Sleep(2 * electionTimeoutMax)
	for i, rf := range c.rafts {
		if !c.alive[i] {
			continue
		}
		if now, _ := rf.State(); now != term {
			c.t.Fatalf("server %d is in term %d, but leader %d was elected in term %d and nothing has failed since", i, now, id, term)
		}
	}
	return id, term
}

// gobEncode and gobDecode turn the test application's state into snapshot
// bytes and back. They panic on failure, since that would be a bug in the
// test itself.
func gobEncode(v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic(fmt.Sprintf("can't encode %T: %v", v, err))
	}
	return buf.Bytes()
}

func gobDecode(data []byte, v any) {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		panic(fmt.Sprintf("can't decode into %T: %v", v, err))
	}
}
