package raft

import (
	"flag"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Xuan-Zheng05/raft-kv/network"
)

var seedFlag = flag.Uint64("seed", 0, "seed for the simulated network; 0 picks a random one")

// pollEvery is how often the harness checks who the leader is. It's small
// next to the 150 ms minimum election timeout, so a new leader gets noticed
// within a fraction of one timeout.
const pollEvery = 10 * time.Millisecond

// cluster is a group of Raft servers talking over a simulated network, all
// inside one test process.
type cluster struct {
	t     *testing.T
	net   *network.Network
	rafts []*Raft
	alive []bool
	// leaderOf remembers who was leader in each term we've seen, so two
	// leaders in the same term get caught even if we saw them at different
	// moments.
	leaderOf map[int]int
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
		t:        t,
		net:      network.New(seed),
		rafts:    make([]*Raft, n),
		alive:    make([]bool, n),
		leaderOf: make(map[int]int),
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
		if t.Failed() {
			t.Logf("network seed %d (rerun with -seed=%d)", seed, seed)
		}
	})
	return c
}

// start boots a fresh instance of server i; after a crash, calling it again
// is a restart. Make has to come before Serve: Raft needs its Endpoint to be
// built, and only a built Raft can handle calls.
func (c *cluster) start(i int) {
	ep := c.net.Connect(i)
	c.rafts[i] = Make(ep, i, len(c.rafts))
	ep.Serve(c.rafts[i])
	c.alive[i] = true
}

// crash kills server i. Both halves matter: the network stops delivering to
// and from it, and Kill stops its goroutines. Without the network half, a
// killed server would still answer RPCs.
func (c *cluster) crash(i int) {
	c.net.Crash(i)
	c.rafts[i].Kill()
	c.alive[i] = false
}

// waitForLeader waits up to within for exactly one live server to think it's
// the leader, and returns that server and its term. While it polls, it also
// checks Raft's core safety rule: no term ever has two leaders.
func (c *cluster) waitForLeader(within time.Duration) (id, term int) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for {
		var ids, terms []int
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
