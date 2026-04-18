// Command bench runs the key-value store as real processes on this machine,
// talking over TCP and saving to real files, and measures it.
//
//	go run ./bench throughput -servers 5 -clients 256 -duration 10s -keys 1000 -value-size 100 -snapshot-every 1000 -server-procs 1
//	go run ./bench failover   -servers 5 -clients 256 -trials 40 -keys 1000 -value-size 100 -snapshot-every 1000 -server-procs 1
//
// Both start the servers themselves, each as its own process running this
// same program's server subcommand, with its state in a temporary directory
// that's removed at the end. Every setting is a flag with no default to fall
// back on, except -server-procs, which can be left at Go's own default; the
// result names it either way. So whatever a result says about how it was
// measured is exactly what was run.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "server":
		serverMain(os.Args[2:])
	case "throughput":
		throughputMain(os.Args[2:])
	case "failover":
		failoverMain(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench server|throughput|failover [flags]  (run a subcommand with -h for its flags)")
	os.Exit(2)
}

// fail prints why the benchmark can't go on and exits.
func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "bench: "+format+"\n", args...)
	os.Exit(1)
}
