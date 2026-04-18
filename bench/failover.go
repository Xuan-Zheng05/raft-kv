package main

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// failoverMain measures how long the cluster takes to accept writes again
// after its leader dies, while clients keep it busy. Each trial kills the
// leader with SIGKILL and times how long until a Put that started right
// after the kill goes through under a new leader. That's what a client
// sees: it includes the client giving up on the dead server, not just the
// election.
func failoverMain(args []string) {
	fs := flag.NewFlagSet("failover", flag.ExitOnError)
	var s settings
	s.register(fs)
	trials := fs.Int("trials", 0, "how many times to kill the leader")
	fs.Parse(args)
	s.check()
	if *trials <= 0 {
		fail("failover needs a positive -trials")
	}

	c := startCluster(s)
	defer c.stop()
	c.client().Put("warmup", "")

	// Background load that runs through every trial.
	var stop atomic.Bool
	var wg sync.WaitGroup
	for k := range s.clients {
		ck := c.client()
		w := newWorkload(s, uint64(k))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				w.next(ck)
			}
		}()
	}

	probe := c.client()
	var times []time.Duration
	for trial := range *trials {
		// A Put that goes through tells us who the leader is right now.
		probe.Put("probe", fmt.Sprint(trial))
		leader := probe.Leader()

		killed := time.Now()
		c.kill(leader)
		probe.Put("probe", fmt.Sprint(trial))
		times = append(times, time.Since(killed))

		// Bring the dead server back, so every trial starts with the whole
		// cluster.
		c.start(leader)
	}
	stop.Store(true)
	wg.Wait()

	var each []string
	for _, t := range times {
		each = append(each, t.Round(time.Millisecond).String())
	}
	fmt.Printf("failover: %s\n", s.describe())
	fmt.Printf("  %d trials: median %v, worst %v\n", len(times), percentile(times, 50).Round(time.Millisecond), percentile(times, 100).Round(time.Millisecond))
	fmt.Printf("  each: %s\n", strings.Join(each, " "))
}
