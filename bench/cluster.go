package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/Xuan-Zheng05/raft-kv/client"
	"github.com/Xuan-Zheng05/raft-kv/network"
)

// settings are the flags throughput and failover share.
type settings struct {
	servers       int
	clients       int
	keys          int
	valueSize     int
	snapshotEvery int
	serverProcs   int // GOMAXPROCS for each server process; 0 leaves Go's default
}

func (s *settings) register(fs *flag.FlagSet) {
	fs.IntVar(&s.servers, "servers", 0, "how many servers to run")
	fs.IntVar(&s.clients, "clients", 0, "how many clients to run, each sending its next request as soon as the last one returns")
	fs.IntVar(&s.keys, "keys", 0, "how many different keys the clients use")
	fs.IntVar(&s.valueSize, "value-size", 0, "how many bytes each Put writes")
	fs.IntVar(&s.snapshotEvery, "snapshot-every", 0, "how often each server takes a snapshot, in log entries")
	fs.IntVar(&s.serverProcs, "server-procs", 0, "GOMAXPROCS for each server process; 0 leaves Go's default")
}

func (s *settings) check() {
	if s.servers <= 0 || s.clients <= 0 || s.keys <= 0 || s.valueSize <= 0 || s.snapshotEvery <= 0 {
		fail("-servers, -clients, -keys, -value-size and -snapshot-every all have to be given, and positive")
	}
}

// describe says how a result was measured, for printing next to it.
func (s *settings) describe() string {
	procs := "default"
	if s.serverProcs > 0 {
		procs = fmt.Sprint(s.serverProcs)
	}
	return fmt.Sprintf("%d servers, %d clients, %d keys, %d-byte values, snapshot every %d entries, server GOMAXPROCS %s; %s %s/%s, %d CPUs",
		s.servers, s.clients, s.keys, s.valueSize, s.snapshotEvery, procs, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
}

// cluster is a set of server processes on this machine.
type cluster struct {
	settings settings
	addrs    []string
	dir      string
	procs    []*exec.Cmd
	exited   []chan struct{} // closed when each process exits
}

// startCluster starts every server, each in its own process with its own
// file in a fresh temporary directory.
func startCluster(s settings) *cluster {
	dir, err := os.MkdirTemp("", "raft-kv-bench-*")
	if err != nil {
		fail("can't make a directory for the servers' files: %v", err)
	}
	c := &cluster{
		settings: s,
		dir:      dir,
		procs:    make([]*exec.Cmd, s.servers),
		exited:   make([]chan struct{}, s.servers),
	}
	// Ask the system for free ports by listening on port 0, then let go of
	// them for the servers to take.
	for range s.servers {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fail("can't find a free port: %v", err)
		}
		c.addrs = append(c.addrs, ln.Addr().String())
		ln.Close()
	}
	for i := range s.servers {
		c.start(i)
	}
	return c
}

// start runs server i. After a kill, this is a restart from the same file.
func (c *cluster) start(i int) {
	cmd := exec.Command(os.Args[0], "server",
		"-id", fmt.Sprint(i),
		"-addrs", strings.Join(c.addrs, ","),
		"-file", filepath.Join(c.dir, fmt.Sprint(i)),
		"-snapshot-every", fmt.Sprint(c.settings.snapshotEvery))
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if c.settings.serverProcs > 0 {
		cmd.Env = append(os.Environ(), fmt.Sprintf("GOMAXPROCS=%d", c.settings.serverProcs))
	}
	if err := cmd.Start(); err != nil {
		fail("can't start server %d: %v", i, err)
	}
	exited := make(chan struct{})
	go func() {
		cmd.Wait()
		close(exited)
	}()
	c.procs[i], c.exited[i] = cmd, exited
}

// kill crashes server i the hard way, with SIGKILL, and waits for it to be
// gone.
func (c *cluster) kill(i int) {
	c.procs[i].Process.Kill()
	<-c.exited[i]
}

// stop kills every server and deletes their files.
func (c *cluster) stop() {
	for i := range c.procs {
		c.kill(i)
	}
	os.RemoveAll(c.dir)
}

// client connects a new client to the cluster. Server i is at addrs[i], so
// its id on the network is i.
func (c *cluster) client() *client.Clerk {
	servers := make([]int, len(c.addrs))
	for i := range servers {
		servers[i] = i
	}
	return client.MakeClerk(network.DialTCP(c.addrs), servers)
}

// workload is one client's stream of requests: half Puts and half Gets, on
// keys picked at random.
type workload struct {
	rng   *rand.Rand
	keys  int
	value string
}

func newWorkload(s settings, seed uint64) *workload {
	return &workload{rng: rand.New(rand.NewPCG(seed, 0)), keys: s.keys, value: strings.Repeat("x", s.valueSize)}
}

// next sends one request and reports whether it was a Put and how long it
// took.
func (w *workload) next(ck *client.Clerk) (isPut bool, took time.Duration) {
	key := fmt.Sprintf("key%d", w.rng.IntN(w.keys))
	isPut = w.rng.IntN(2) == 0
	start := time.Now()
	if isPut {
		ck.Put(key, w.value)
	} else {
		ck.Get(key)
	}
	return isPut, time.Since(start)
}

// percentile returns the p-th percentile of durations by the nearest-rank
// method: the smallest value with at least p percent of all the values at or
// below it.
func percentile(durations []time.Duration, p int) time.Duration {
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	rank := (p*len(sorted) + 99) / 100 // p percent of the count, rounded up
	return sorted[max(rank, 1)-1]
}
