package main

import (
	"flag"
	"fmt"
	"sync"
	"time"
)

// throughputMain measures how many requests per second the cluster finishes
// with closed-loop clients, and how long Puts and Gets take.
func throughputMain(args []string) {
	fs := flag.NewFlagSet("throughput", flag.ExitOnError)
	var s settings
	s.register(fs)
	duration := fs.Duration("duration", 0, "how long to measure for")
	fs.Parse(args)
	s.check()
	if *duration <= 0 {
		fail("throughput needs a positive -duration")
	}

	c := startCluster(s)
	defer c.stop()
	// The first request waits for the servers to start and elect a leader,
	// so measuring starts only once it has gone through.
	c.client().Put("warmup", "")

	var mu sync.Mutex
	var puts, gets []time.Duration
	end := time.Now().Add(*duration)
	for k := range s.clients {
		ck := c.client()
		w := newWorkload(s, uint64(k))
		go func() {
			for {
				isPut, took := w.next(ck)
				// Only requests that finished inside the window count.
				if time.Now().After(end) {
					return
				}
				mu.Lock()
				if isPut {
					puts = append(puts, took)
				} else {
					gets = append(gets, took)
				}
				mu.Unlock()
			}
		}()
	}
	// Don't wait for requests still in flight at the end: they wouldn't
	// count anyway, and if the cluster has stopped making progress they never
	// finish. The clients are left running until the program exits.
	time.Sleep(time.Until(end))
	mu.Lock()
	defer mu.Unlock()

	total := len(puts) + len(gets)
	fmt.Printf("throughput: %s\n", s.describe())
	fmt.Printf("  %d requests in %v: %.0f requests/s\n", total, *duration, float64(total)/duration.Seconds())
	fmt.Printf("  Put latency: %s\n", latencies(puts))
	fmt.Printf("  Get latency: %s\n", latencies(gets))
}

// latencies describes a set of request times by their median and 99th
// percentile.
func latencies(durations []time.Duration) string {
	if len(durations) == 0 {
		return "none finished"
	}
	return fmt.Sprintf("median %v, p99 %v (%d requests)", percentile(durations, 50), percentile(durations, 99), len(durations))
}
