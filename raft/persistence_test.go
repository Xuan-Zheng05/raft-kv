package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/Xuan-Zheng05/raft-kv/network"
)

// Each case makes one change that has to be saved, restarts the server
// straight away, and checks the change is still there. The randomized test
// only catches a missing save by luck, since in a busy cluster the next save
// writes the same state moments later. Here nothing else saves in between.
//
// The server runs alone: the other two servers in its cluster never connect,
// so nothing it sends gets an answer. Every step happens well inside the
// 150 ms before its first election timeout, except where a case waits for
// that election on purpose.
func TestSavedStateSurvivesRestart(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		// change makes the change and returns the term it happened in.
		change func(t *testing.T, rf *Raft) int
		check  func(t *testing.T, rf *Raft, term int)
	}{
		{
			name: "a granted vote",
			change: func(t *testing.T, rf *Raft) int {
				var reply RequestVoteReply
				rf.RequestVote(&RequestVoteArgs{Term: 5, CandidateID: 1}, &reply)
				if !reply.VoteGranted {
					t.Fatal("server refused the first vote of term 5")
				}
				return 5
			},
			check: func(t *testing.T, rf *Raft, term int) {
				var reply RequestVoteReply
				rf.RequestVote(&RequestVoteArgs{Term: term, CandidateID: 2}, &reply)
				if reply.VoteGranted {
					t.Fatalf("after restarting, server voted a second time in term %d", term)
				}
			},
		},
		{
			name: "its own vote when it starts an election",
			change: func(t *testing.T, rf *Raft) int {
				for {
					if term, _ := rf.State(); term > 0 {
						return term
					}
					time.Sleep(pollEvery)
				}
			},
			check: func(t *testing.T, rf *Raft, term int) {
				var reply RequestVoteReply
				rf.RequestVote(&RequestVoteArgs{Term: term, CandidateID: 1}, &reply)
				if reply.VoteGranted {
					t.Fatalf("after restarting, server voted for someone else in term %d, when it had voted for itself", term)
				}
			},
		},
		{
			name: "a newer term it heard about",
			change: func(t *testing.T, rf *Raft) int {
				// A leader of term 7 whose entries don't fit this log. The server
				// moves to term 7 and turns the entries down, so nothing else
				// gets saved.
				var reply AppendEntriesReply
				rf.AppendEntries(&AppendEntriesArgs{Term: 7, PrevLogIndex: 5, PrevLogTerm: 7}, &reply)
				return 7
			},
			check: func(t *testing.T, rf *Raft, term int) {
				if now, _ := rf.State(); now < term {
					t.Fatalf("after restarting, server is back in term %d; it had moved to term %d", now, term)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			net := network.New(1)
			storage := &MemStorage{}
			rf := Make(net.Connect(0), 0, 3, storage, make(chan ApplyMsg))
			term := tc.change(t, rf)

			net.Crash(0)
			rf.Kill()
			restarted := &MemStorage{}
			restarted.Save(storage.Load())
			rf = Make(net.Connect(0), 0, 3, restarted, make(chan ApplyMsg))
			defer rf.Kill()
			tc.check(t, rf, term)
		})
	}
}

// Every server crashes and comes back. Afterwards they still agree on what
// was committed before, and they carry on. Without saved state they'd come
// back with empty logs and commit new commands at old indexes, which collect
// would catch.
func TestRestartAll(t *testing.T) {
	t.Parallel()
	const n = 3
	c := newCluster(t, n)
	for cmd := 1; cmd <= 3; cmd++ {
		c.one(cmd, n)
	}
	for i := range n {
		c.crash(i)
	}
	for i := range n {
		c.start(i)
	}
	c.one(4, n)
}

// Servers crash and restart at random moments while the cluster also gets
// split up at random, with messages dropped and delayed so they arrive out
// of order. Commands keep going to every live server that thinks it's the
// leader, stale leaders included, so logs pile up entries that disagree. Each
// round lasts up to two election timeouts, so sometimes a new leader takes
// over and sometimes nobody gets the chance. At the end every server comes
// back, the network heals, and they all have to agree. All along, collect
// checks that no two servers ever apply different commands at the same
// index, which is exactly what a lost committed entry would look like, and
// leaders checks that no term ever has two leaders.
//
// It's thirty rounds in all, spread over three clusters that run side by
// side, so the test takes a third of the time one long run would.
func TestAgreeThroughRandomFailures(t *testing.T) {
	t.Parallel()
	for run := range 3 {
		t.Run(fmt.Sprintf("run %d", run), func(t *testing.T) {
			t.Parallel()
			const n = 5
			c := newCluster(t, n)
			c.net.SetDropRate(0.1)
			// Up to two heartbeats of delay each way, so a request can arrive
			// after the one the leader sent next.
			c.net.SetDelay(0, 2*heartbeatInterval)

			cmd := 0
			for range 10 {
				if i := c.plan.IntN(n); c.alive[i] {
					c.crash(i)
				} else {
					c.start(i)
				}

				groups := make([][]int, 1+c.plan.IntN(3))
				for i := range n {
					g := c.plan.IntN(len(groups))
					groups[g] = append(groups[g], i)
				}
				c.net.Partition(groups...)

				ids, _ := c.leaders()
				for _, i := range ids {
					if _, _, isLeader := c.rafts[i].Start(cmd); isLeader {
						cmd++
					}
				}
				time.Sleep(time.Duration(c.plan.Int64N(int64(2 * electionTimeoutMax))))
			}

			for i := range n {
				if !c.alive[i] {
					c.start(i)
				}
			}
			c.net.Heal()
			c.net.SetDropRate(0)
			c.net.SetDelay(0, 0)
			c.one(cmd, n)
		})
	}
}
