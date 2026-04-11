package raft

import "testing"

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
