package raft

import "testing"

// Under a long run of writes, every server's log stays short, because the
// application takes a snapshot every snapshotEvery entries and Raft drops
// what the snapshot covers.
//
// Each round submits snapshotEvery commands at once and then waits for one
// more to be applied everywhere. By then the latest snapshot is at most
// snapshotEvery entries back, and the application may be about to take the
// next one, so a log should never hold more than twice snapshotEvery.
//
// Ten rounds write over a hundred entries, far more than that limit, so a log
// that never gets trimmed fails quickly.
func TestLogStaysSmall(t *testing.T) {
	t.Parallel()
	const n = 3
	c := newCluster(t, n)
	cmd := 0
	for range 10 {
		if leader := c.leader(); leader >= 0 {
			for range snapshotEvery {
				c.rafts[leader].Start(cmd)
				cmd++
			}
		}
		c.one(cmd, n)
		cmd++

		for i, rf := range c.rafts {
			rf.mu.Lock()
			length := len(rf.log)
			rf.mu.Unlock()
			if length > 2*snapshotEvery {
				t.Fatalf("server %d's log holds %d entries after %d commands", i, length, cmd)
			}
		}
	}
}

// A follower is cut off while the others commit enough for the leader to
// compact its log past everything the follower has, more than once. When
// the follower comes back, the entries it needs are gone, so the leader has
// to send its snapshot instead.
func TestFollowerCatchesUpFromSnapshot(t *testing.T) {
	t.Parallel()
	const n = 3
	c := newCluster(t, n)
	c.one(0, n)
	leader, _ := c.waitForLeader(firstElectionWithin)
	behind := (leader + 1) % n

	c.net.Partition([]int{behind}, others(n, behind))
	for cmd := 1; cmd <= 3*snapshotEvery; cmd++ {
		c.one(cmd, n-1)
	}

	c.net.Heal()
	c.one(-1, n)
	c.mu.Lock()
	got := c.snapshots[behind]
	c.mu.Unlock()
	if got == 0 {
		t.Fatalf("server %d caught up without being sent a snapshot", behind)
	}
}
