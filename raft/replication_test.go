package raft

import (
	"testing"

	"github.com/Xuan-Zheng05/raft-kv/network"
)

// With one message in ten dropped, every server in a 5-server cluster still
// applies every command, in the same order. Fifty commands keep the test
// running long enough that each follower misses dozens of messages, so
// resending and catching up both get exercised.
func TestAgreeUnreliable(t *testing.T) {
	t.Parallel()
	const n = 5
	c := newCluster(t, n)
	c.net.SetDropRate(0.1)
	for cmd := range 50 {
		c.one(cmd, n)
	}
}

// A follower misses ten entries, then comes back under a different leader.
// The new leader starts out assuming the follower's log matches its own, so
// it has to step back through all ten missing entries before the follower
// accepts anything.
func TestFollowerCatchesUp(t *testing.T) {
	t.Parallel()
	const n = 5
	c := newCluster(t, n)
	c.one(1, n)
	leader, _ := c.waitForLeader(firstElectionWithin)
	behind := (leader + 1) % n

	c.net.Partition([]int{behind}, others(n, behind))
	for cmd := 2; cmd <= 11; cmd++ {
		c.one(cmd, n-1)
	}

	// Cut the leader off too, so the follower comes back to a leader that
	// never talked to it.
	c.net.Partition([]int{behind}, []int{leader}, others(n, behind, leader))
	c.one(12, n-2)

	c.net.Heal()
	c.one(13, n)
}

// A leader cut off with a minority keeps taking commands, but it can't commit
// them. After the partition heals, it and its follower throw those entries
// away in favour of what the majority committed meanwhile. If a stale entry
// had been applied anywhere, collect would see two servers apply different
// commands at the same index.
func TestStaleLeaderEntriesReplaced(t *testing.T) {
	t.Parallel()
	const n = 5
	c := newCluster(t, n)
	c.one(1, n)
	old, _ := c.waitForLeader(firstElectionWithin)

	minority := []int{old, (old + 1) % n}
	c.net.Partition(minority, others(n, minority...))
	for cmd := 100; cmd < 103; cmd++ {
		c.rafts[old].Start(cmd) // accepted, but it can never reach a majority
	}
	for cmd := 2; cmd <= 4; cmd++ {
		c.one(cmd, n-len(minority))
	}

	c.net.Heal()
	c.one(5, n)
}

// A follower that turns entries down says where the leader should try next,
// so a leader can catch it up in a round or two instead of stepping back one
// entry per round. The server runs alone and is fed AppendEntries directly:
// first entries from term 1 at indexes 1-3 and term 2 at indexes 4-5, then
// two leaders whose logs don't fit it.
func TestRejectionSaysWhereToResume(t *testing.T) {
	t.Parallel()
	rf := Make(network.New(1).Connect(0), 0, 3, &MemStorage{}, make(chan ApplyMsg))
	defer rf.Kill()
	appendEntries := func(args *AppendEntriesArgs) AppendEntriesReply {
		var reply AppendEntriesReply
		rf.AppendEntries(args, &reply)
		return reply
	}
	entry := func(term int) LogEntry { return LogEntry{Term: term, Command: term} }
	appendEntries(&AppendEntriesArgs{Term: 1, Entries: []LogEntry{entry(1), entry(1), entry(1)}})
	appendEntries(&AppendEntriesArgs{Term: 2, PrevLogIndex: 3, PrevLogTerm: 1, Entries: []LogEntry{entry(2), entry(2)}})

	// A leader whose entries go after index 10: this log only reaches 5.
	if got := appendEntries(&AppendEntriesArgs{Term: 3, PrevLogIndex: 10, PrevLogTerm: 3}); got.Success || got.ConflictTerm != 0 || got.ConflictIndex != 6 {
		t.Fatalf("log too short: got %+v, want a refusal pointing just past index 5", got)
	}
	// A leader with term 3 at index 5, where this log has term 2: the leader
	// should skip back to where term 2 starts here, index 4.
	if got := appendEntries(&AppendEntriesArgs{Term: 3, PrevLogIndex: 5, PrevLogTerm: 3}); got.Success || got.ConflictTerm != 2 || got.ConflictIndex != 4 {
		t.Fatalf("term mismatch: got %+v, want a refusal naming term 2 starting at index 4", got)
	}
}
