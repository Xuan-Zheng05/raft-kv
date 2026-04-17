package kv_test

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Xuan-Zheng05/raft-kv/kv"
	"github.com/Xuan-Zheng05/raft-kv/raft"
)

var runsFlag = flag.Int("runs", 3, "how many runs TestChaos does side by side")

// kvInput is what Porcupine records as an operation's input. Its output is
// the value a Get read, or "" for a Put or Append.
type kvInput struct {
	Kind  kv.Kind
	Key   string
	Value string
}

// kvModel is a single, unreplicated key-value store, written out for
// Porcupine. A history is linearizable if its operations can be put in some
// order that respects real time (an operation that finished before another
// started comes first) and that this model accepts step by step. Keys don't
// affect each other, so each key's operations get checked on their own,
// which keeps the search small.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(kvInput).Key
			byKey[key] = append(byKey[key], op)
		}
		var parts [][]porcupine.Operation
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		value, in := state.(string), input.(kvInput)
		switch in.Kind {
		case kv.Get:
			return output.(string) == value, value
		case kv.Put:
			return true, in.Value
		case kv.Append:
			return true, value + in.Value
		default:
			panic(fmt.Sprintf("unknown op kind %q", in.Kind))
		}
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		if in.Kind == kv.Get {
			return fmt.Sprintf("Get(%q) → %q", in.Key, output)
		}
		return fmt.Sprintf("%s(%q, %q)", in.Kind, in.Key, in.Value)
	},
	DescribeState: func(state any) string { return fmt.Sprintf("%q", state) },
}

// TestChaos is the end-to-end check. In each run, clients read and write a
// few shared keys while the cluster gets split up, servers crash and restart,
// and the network drops and reorders messages. Every operation gets recorded
// with when it started and when it finished, and afterwards Porcupine checks
// the whole history: the store must have behaved like a single copy that
// each operation hit at one instant between its start and its finish.
//
// It does -runs runs side by side: three in the normal suite, like the other
// randomized tests, and as many as asked for when counting, for example
//
//	go test -race -run TestChaos ./kv/ -runs=300
//
// Every run uses its own seed, printed if the run fails. A run that fails
// also writes Porcupine's picture of the history to a file and says where.
func TestChaos(t *testing.T) {
	t.Parallel()
	for run := range *runsFlag {
		t.Run(fmt.Sprintf("run %d", run), func(t *testing.T) {
			t.Parallel()
			chaosRun(t)
		})
	}
}

func chaosRun(t *testing.T) {
	const n, clients = 5, 5
	// Fewer keys than clients, so every key is shared and operations on it
	// overlap.
	keys := []string{"a", "b", "c"}
	c := newCluster(t, n)
	c.net.SetDropRate(0.1)
	// Up to a fifth of the longest election timeout each way. That's longer
	// than the gap between a leader's heartbeats, so messages arrive out of
	// order, while a round trip still fits inside the shortest election
	// timeout, so the cluster can keep a leader.
	c.net.SetDelay(0, raft.ElectionTimeoutMax/5)

	start := time.Now()
	var mu sync.Mutex
	var history []porcupine.Operation
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for k := range clients {
		ck := c.client()
		// Each client draws its operations from its own generator, seeded
		// from the plan, so the seed decides the workload as well as the
		// faults.
		choose := rand.New(rand.NewPCG(c.plan.Uint64(), uint64(k)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			kinds := []kv.Kind{kv.Get, kv.Put, kv.Append}
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				in := kvInput{Kind: kinds[choose.IntN(len(kinds))], Key: keys[choose.IntN(len(keys))]}
				if in.Kind != kv.Get {
					in.Value = fmt.Sprintf("<%d.%d>", k, j) // unique, so the history shows exactly which write a Get saw
				}
				var out string
				call := time.Since(start).Nanoseconds()
				switch in.Kind {
				case kv.Get:
					out = ck.Get(in.Key)
				case kv.Put:
					ck.Put(in.Key, in.Value)
				case kv.Append:
					ck.Append(in.Key, in.Value)
				}
				ret := time.Since(start).Nanoseconds()

				mu.Lock()
				history = append(history, porcupine.Operation{ClientId: k, Input: in, Call: call, Output: out, Return: ret})
				mu.Unlock()
			}
		}()
	}

	// Five cycles, each a fault and then a heal. The fault splits servers
	// and clients into random groups, so some clients end up stuck with a
	// leader that's been cut off from the rest, and crashes one server. The
	// heal reconnects everyone and restarts that server. The heal is where
	// most of the interesting moments happen: a cut-off leader finds out it's
	// been replaced, a server that fell behind gets a snapshot, and clients
	// retry requests whose replies got lost. Drops and delays stay on the
	// whole time. Each phase lasts one to two election timeouts, long enough
	// for whichever side can make progress to elect a leader.
	phase := func() {
		time.Sleep(raft.ElectionTimeoutMax + time.Duration(c.plan.Int64N(int64(raft.ElectionTimeoutMax))))
	}
	for range 5 {
		groups := make([][]int, 1+c.plan.IntN(3))
		for _, id := range append(slices.Clone(c.ids), c.clientIDs...) {
			g := c.plan.IntN(len(groups))
			groups[g] = append(groups[g], id)
		}
		c.net.Partition(groups...)
		down := c.plan.IntN(n)
		c.crash(down)
		phase()

		c.net.Heal()
		c.start(down)
		phase()
	}

	// Calm the network down so each client can finish the operation it's in
	// the middle of, then stop them.
	c.net.SetDropRate(0)
	c.net.SetDelay(0, 0)
	close(stop)
	wg.Wait()

	result, info := porcupine.CheckOperationsVerbose(kvModel, history, 0) // 0: no time limit, so the answer is always Ok or Illegal
	t.Logf("%d operations, %s", len(history), result)
	if result != porcupine.Ok {
		file, err := os.CreateTemp("", "raft-kv-chaos-*.html")
		if err != nil {
			t.Fatalf("history of %d operations isn't linearizable, and the picture of it couldn't be saved: %v", len(history), err)
		}
		defer file.Close()
		if err := porcupine.Visualize(kvModel, info, file); err != nil {
			t.Fatalf("history of %d operations isn't linearizable, and the picture of it couldn't be saved: %v", len(history), err)
		}
		t.Fatalf("history of %d operations isn't linearizable; open %s to see it", len(history), file.Name())
	}
}
