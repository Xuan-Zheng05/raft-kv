package raft

import (
	"fmt"
	"testing"
	"time"
)

// A cluster should elect exactly one leader within about a second, and when
// that leader is killed, a new one should take over within a few seconds.
const (
	firstElectionWithin = time.Second
	reelectionWithin    = 3 * time.Second
)

// 3 and 5 servers need different majorities (2 and 3), so both get tested.
var clusterSizes = []int{3, 5}

func TestInitialElection(t *testing.T) {
	t.Parallel()
	for _, n := range clusterSizes {
		t.Run(fmt.Sprintf("%d servers", n), func(t *testing.T) {
			t.Parallel()
			c := newCluster(t, n)
			c.waitForStableLeader(firstElectionWithin)
		})
	}
}

func TestReElection(t *testing.T) {
	t.Parallel()
	for _, n := range clusterSizes {
		t.Run(fmt.Sprintf("%d servers", n), func(t *testing.T) {
			t.Parallel()
			c := newCluster(t, n)
			old, _ := c.waitForStableLeader(firstElectionWithin)
			c.crash(old)
			c.waitForStableLeader(reelectionWithin)
		})
	}
}
