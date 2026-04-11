package raft

import (
	"fmt"
	"testing"
	"time"
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

// The cluster gets split up at random, over and over, with messages dropped
// and delayed so they arrive out of order. Commands keep going to every
// server that thinks it's the leader, stale leaders included, so logs pile up
// entries that disagree. Each split lasts up to two election timeouts, so
// sometimes a new leader takes over and sometimes nobody gets the chance. At
// the end the network heals and every server has to agree. All along,
// collect checks that no two servers ever apply different commands at the
// same index.
//
// It's thirty splits in all, spread over three clusters that run side by
// side, so the test takes a third of the time one long run would.
func TestAgreeThroughRandomPartitions(t *testing.T) {
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
				groups := make([][]int, 1+c.plan.IntN(3))
				for i := range n {
					g := c.plan.IntN(len(groups))
					groups[g] = append(groups[g], i)
				}
				c.net.Partition(groups...)
				for _, rf := range c.rafts {
					if _, _, isLeader := rf.Start(cmd); isLeader {
						cmd++
					}
				}
				time.Sleep(time.Duration(c.plan.Int64N(int64(2 * electionTimeoutMax))))
			}

			c.net.Heal()
			c.net.SetDropRate(0)
			c.net.SetDelay(0, 0)
			c.one(cmd, n)
		})
	}
}
