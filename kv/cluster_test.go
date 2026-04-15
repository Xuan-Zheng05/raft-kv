package kv_test

import (
	"flag"
	"math/rand/v2"
	"testing"

	"github.com/Xuan-Zheng05/raft-kv/client"
	"github.com/Xuan-Zheng05/raft-kv/kv"
	"github.com/Xuan-Zheng05/raft-kv/network"
	"github.com/Xuan-Zheng05/raft-kv/raft"
)

var seedFlag = flag.Uint64("seed", 0, "seed for the simulated network; 0 picks a random one")

// snapshotEvery is how often each test server takes a snapshot, in log
// entries. It's small so that every test compacts its logs, and a server
// that restarts comes back from a snapshot.
const snapshotEvery = 10

// cluster is a group of key-value servers, each on its own Raft server, and
// the clients that talk to them, all over one simulated network.
type cluster struct {
	t       *testing.T
	net     *network.Network
	servers []*kv.Server
	storage []*raft.MemStorage // what each server has saved; outlives the server itself
	alive   []bool
	ids     []int // the servers' network ids, which is what clients need
	// clientIDs are the clients' network ids, which come after the servers'.
	// A partition has to list them too, or they're cut off from everyone.
	clientIDs []int
	// plan is where randomized tests get their choices. It's seeded like the
	// network, so rerunning with -seed replays the same plan.
	plan *rand.Rand
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
		t:       t,
		net:     network.New(seed),
		servers: make([]*kv.Server, n),
		storage: make([]*raft.MemStorage, n),
		alive:   make([]bool, n),
		plan:    rand.New(rand.NewPCG(seed, 0)),
	}
	for i := range n {
		c.ids = append(c.ids, i)
		c.start(i)
	}
	t.Cleanup(func() {
		for i, srv := range c.servers {
			if c.alive[i] {
				srv.Kill()
			}
		}
		if t.Failed() {
			t.Logf("network seed %d (rerun with -seed=%d)", seed, seed)
		}
	})
	return c
}

// start boots a fresh instance of server i; after a crash, that's a restart.
// The new instance gets a copy of what the old one saved, so goroutines the
// old one left behind can't write into the new one's storage.
func (c *cluster) start(i int) {
	storage := &raft.MemStorage{}
	if old := c.storage[i]; old != nil {
		storage.Save(old.Load())
	}
	c.storage[i] = storage

	ep := c.net.Connect(i)
	c.servers[i] = kv.StartServer(ep, i, len(c.servers), storage, snapshotEvery)
	ep.Serve(c.servers[i], c.servers[i].Raft())
	c.alive[i] = true
}

// crash kills server i, both on the network and in its own goroutines.
func (c *cluster) crash(i int) {
	c.net.Crash(i)
	c.servers[i].Kill()
	c.alive[i] = false
}

// client connects a new client. Each one handles one request at a time, so a
// test that wants several requests in flight uses several clients.
func (c *cluster) client() *client.Clerk {
	id := len(c.servers) + len(c.clientIDs)
	c.clientIDs = append(c.clientIDs, id)
	return client.MakeClerk(c.net.Connect(id), c.ids)
}

// leader returns the live server that thinks it's the leader with the
// highest term, or -1 if there's none.
func (c *cluster) leader() int {
	leader, best := -1, -1
	for i, srv := range c.servers {
		if !c.alive[i] {
			continue
		}
		if term, isLeader := srv.Raft().State(); isLeader && term > best {
			leader, best = i, term
		}
	}
	return leader
}
