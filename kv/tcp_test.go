package kv_test

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/Xuan-Zheng05/raft-kv/client"
	"github.com/Xuan-Zheng05/raft-kv/kv"
	"github.com/Xuan-Zheng05/raft-kv/network"
	"github.com/Xuan-Zheng05/raft-kv/raft"
)

// The same store on real TCP connections and real files: three servers in
// this one test, each with its own listener and its own file. Writes go
// through, a crashed leader gets replaced, and after every server restarts
// from its file, everything written is still there.
func TestOverTCPAndFiles(t *testing.T) {
	t.Parallel()
	const n = 3
	dir := t.TempDir()
	addrs := make([]string, n)
	listeners := make([]net.Listener, n)
	for i := range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i], addrs[i] = ln, ln.Addr().String()
	}

	servers := make([]*kv.Server, n)
	tcp := make([]*network.TCPServer, n)
	alive := make([]bool, n)
	start := func(i int, ln net.Listener) {
		storage := raft.NewFileStorage(filepath.Join(dir, fmt.Sprint(i)))
		servers[i] = kv.StartServer(network.DialTCP(addrs), i, n, storage, snapshotEvery)
		var err error
		if tcp[i], err = network.ServeTCP(ln, servers[i], servers[i].Raft()); err != nil {
			t.Fatal(err)
		}
		alive[i] = true
	}
	restart := func(i int) {
		ln, err := net.Listen("tcp", addrs[i])
		if err != nil {
			t.Fatal(err)
		}
		start(i, ln)
	}
	crash := func(i int) {
		tcp[i].Close()
		servers[i].Kill()
		alive[i] = false
	}
	for i := range n {
		start(i, listeners[i])
	}
	t.Cleanup(func() {
		for i := range n {
			if alive[i] {
				crash(i)
			}
		}
	})

	ck := client.MakeClerk(network.DialTCP(addrs), []int{0, 1, 2})
	put := func(k int) { ck.Put(fmt.Sprintf("key%d", k), fmt.Sprintf("value%d", k)) }
	for k := range 2 * snapshotEvery {
		put(k)
	}

	crash(ck.Leader())
	for k := 2 * snapshotEvery; k < 3*snapshotEvery; k++ {
		put(k)
	}

	for i := range n {
		if alive[i] {
			crash(i)
		}
	}
	for i := range n {
		restart(i)
	}
	ck = client.MakeClerk(network.DialTCP(addrs), []int{0, 1, 2})
	for k := range 3 * snapshotEvery {
		key, want := fmt.Sprintf("key%d", k), fmt.Sprintf("value%d", k)
		if got := ck.Get(key); got != want {
			t.Fatalf("after restarting everyone from their files, Get(%q) = %q, want %q", key, got, want)
		}
	}
}
