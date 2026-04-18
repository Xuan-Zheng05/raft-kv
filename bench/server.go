package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Xuan-Zheng05/raft-kv/kv"
	"github.com/Xuan-Zheng05/raft-kv/network"
	"github.com/Xuan-Zheng05/raft-kv/raft"
)

// serverMain runs one key-value server until it's told to stop or killed.
// The other subcommands start these; there's no reason to run one by hand.
func serverMain(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	id := fs.Int("id", -1, "this server's position in -addrs")
	addrs := fs.String("addrs", "", "every server's host:port, comma-separated, in id order")
	file := fs.String("file", "", "the file this server keeps its saved state in")
	snapshotEvery := fs.Int("snapshot-every", 0, "take a snapshot every this many log entries")
	fs.Parse(args)
	list := strings.Split(*addrs, ",")
	if *addrs == "" || *id < 0 || *id >= len(list) || *file == "" || *snapshotEvery <= 0 {
		fail("server needs -id, -addrs, -file and a positive -snapshot-every")
	}

	ln, err := net.Listen("tcp", list[*id])
	if err != nil {
		fail("server %d can't listen on %s: %v", *id, list[*id], err)
	}
	srv := kv.StartServer(network.DialTCP(list), *id, len(list), raft.NewFileStorage(*file), *snapshotEvery)
	if _, err := network.ServeTCP(ln, srv, srv.Raft()); err != nil {
		fail("server %d can't serve: %v", *id, err)
	}

	// Benchmarks crash servers with SIGKILL, which doesn't come through
	// here. This is only for stopping a server politely.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	srv.Kill()
}
