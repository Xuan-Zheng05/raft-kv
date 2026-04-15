package kv_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Xuan-Zheng05/raft-kv/raft"
)

func TestPutGetAppend(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 3)
	ck := c.client()

	if got := ck.Get("x"); got != "" {
		t.Fatalf("Get of a key nobody wrote = %q, want empty", got)
	}
	ck.Put("x", "a")
	ck.Append("x", "b")
	if got := ck.Get("x"); got != "ab" {
		t.Fatalf("after Put a, Append b: Get = %q, want %q", got, "ab")
	}
	ck.Put("x", "c")
	if got := ck.Get("x"); got != "c" {
		t.Fatalf("after Put c: Get = %q, want %q", got, "c")
	}
}

// Several clients keep reading and writing while the leader keeps getting
// cut off from everyone else, so requests are constantly in flight when
// leadership changes. Each client is the only writer of its own key, so it
// always knows exactly what a Get should return.
func TestAcrossLeaderChanges(t *testing.T) {
	t.Parallel()
	const n = 5
	c := newCluster(t, n)

	var wg sync.WaitGroup
	for k := range 5 {
		ck := c.client()
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, want := fmt.Sprintf("key%d", k), ""
			for j := range 20 {
				marker := fmt.Sprintf("[%d]", j)
				ck.Append(key, marker)
				want += marker
				if got := ck.Get(key); got != want {
					t.Errorf("client %d: Get(%q) = %q, want %q", k, key, got, want)
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for {
		select {
		case <-done:
			return
		default:
		}
		// Cut the leader off for up to two election timeouts, long enough for
		// the others to elect someone new, then let it back in.
		if leader := c.leader(); leader >= 0 {
			c.net.Partition([]int{leader}, others(n, leader))
		}
		time.Sleep(time.Duration(c.plan.Int64N(int64(2 * raft.ElectionTimeoutMax))))
		c.net.Heal()
		time.Sleep(raft.ElectionTimeoutMax)
	}
}

// Clients append to one shared key over a network that drops one message in
// ten, while servers crash and restart at random. Lost replies make clients
// retry requests that already went through, and restarts mean some of those
// retries reach servers that came back from a snapshot. At the end every
// marker must appear in the value exactly once, and each client's markers in
// the order it appended them.
//
// A server only gets crashed if a majority stays up. With fewer than that
// alive, nothing can commit until enough of them come back, which makes the
// test slower without testing anything new. The work is spread over three
// clusters that run side by side, so the test takes a third of the time.
func TestRetriedAppendAppliedOnce(t *testing.T) {
	t.Parallel()
	for run := range 3 {
		t.Run(fmt.Sprintf("run %d", run), func(t *testing.T) {
			t.Parallel()
			const n, clients, appends = 5, 5, 7
			c := newCluster(t, n)
			c.net.SetDropRate(0.1)

			var wg sync.WaitGroup
			for k := range clients {
				ck := c.client()
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := range appends {
						ck.Append("log", fmt.Sprintf("[%d.%d]", k, j))
					}
				}()
			}

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
		chaos:
			for {
				select {
				case <-done:
					break chaos
				default:
				}
				alive := 0
				for _, up := range c.alive {
					if up {
						alive++
					}
				}
				if i := c.plan.IntN(n); !c.alive[i] {
					c.start(i)
				} else if alive-1 > n/2 {
					c.crash(i)
				}
				time.Sleep(time.Duration(c.plan.Int64N(int64(2 * raft.ElectionTimeoutMax))))
			}

			for i := range n {
				if !c.alive[i] {
					c.start(i)
				}
			}
			c.net.SetDropRate(0)
			value := c.client().Get("log")
			for k := range clients {
				last := -1
				for j := range appends {
					marker := fmt.Sprintf("[%d.%d]", k, j)
					if count := strings.Count(value, marker); count != 1 {
						t.Fatalf("marker %s appears %d times in %q", marker, count, value)
					}
					at := strings.Index(value, marker)
					if at < last {
						t.Fatalf("client %d's markers are out of order in %q", k, value)
					}
					last = at
				}
			}
		})
	}
}

// After enough writes for every server to take a snapshot, the whole cluster
// crashes and comes back. Everything written before must still be there, and
// the cluster must keep working.
func TestStateSurvivesRestart(t *testing.T) {
	t.Parallel()
	const n = 3
	c := newCluster(t, n)
	ck := c.client()
	for k := range 3 * snapshotEvery {
		ck.Put(fmt.Sprintf("key%d", k), fmt.Sprintf("value%d", k))
	}

	for i := range n {
		c.crash(i)
	}
	for i := range n {
		c.start(i)
	}

	ck = c.client()
	for k := range 3 * snapshotEvery {
		key, want := fmt.Sprintf("key%d", k), fmt.Sprintf("value%d", k)
		if got := ck.Get(key); got != want {
			t.Fatalf("after restarting everyone, Get(%q) = %q, want %q", key, got, want)
		}
	}
	ck.Append("key0", "+")
	if got := ck.Get("key0"); got != "value0+" {
		t.Fatalf("after restarting everyone, Append then Get = %q, want %q", got, "value0+")
	}
}

// others returns every server id from 0 to n-1 except the one given.
func others(n, except int) []int {
	var ids []int
	for i := range n {
		if i != except {
			ids = append(ids, i)
		}
	}
	return ids
}
