package network

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// Unless a test sets a delay, a call that gets through comes back within
// microseconds. So deliverWait is a generous deadline for calls we expect to
// succeed, and lostWait is how long we wait before deciding a call was lost.
const (
	deliverWait = time.Second
	lostWait    = 50 * time.Millisecond
)

type Args struct {
	X     int
	Items []int
}

type Reply struct {
	Y int
}

// Echo is the little service every test server runs. It counts how many
// times its handlers ran, and Hold lets a test pause a handler halfway.
type Echo struct {
	ran     atomic.Int32
	started chan struct{} // Hold pings this as soon as it starts
	release chan struct{} // Hold waits here until the test closes it
}

func (s *Echo) Double(args *Args, reply *Reply) error {
	s.ran.Add(1)
	reply.Y = 2 * args.X
	return nil
}

func (s *Echo) Clobber(args *Args, reply *Reply) error {
	s.ran.Add(1)
	args.Items[0] = -1
	return nil
}

func (s *Echo) Hold(args *Args, reply *Reply) error {
	s.ran.Add(1)
	s.started <- struct{}{}
	<-s.release
	reply.Y = args.X
	return nil
}

type lowercaseArgs struct{ x int }

// Leaky's handler takes an argument with a lowercase field, which gob would
// silently drop.
type Leaky struct{}

func (*Leaky) Handle(args *lowercaseArgs, reply *Reply) error { return nil }

// setup connects servers 0 through size-1, each running its own Echo.
func setup(size int) (*Network, []*Endpoint, []*Echo) {
	n := New(1)
	eps := make([]*Endpoint, size)
	echos := make([]*Echo, size)
	for i := range size {
		echos[i] = &Echo{started: make(chan struct{}, 1), release: make(chan struct{})}
		eps[i] = n.Connect(i)
		eps[i].Serve(echos[i])
	}
	return n, eps, echos
}

// call makes one call with a fresh reply and the given deadline.
func call(e *Endpoint, to int, method string, args *Args, wait time.Duration) (Reply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var reply Reply
	err := e.Call(ctx, to, method, args, &reply)
	return reply, err
}

func TestCallReturnsReply(t *testing.T) {
	t.Parallel()
	_, eps, _ := setup(2)
	reply, err := call(eps[0], 1, "Echo.Double", &Args{X: 21}, deliverWait)
	if err != nil || reply.Y != 42 {
		t.Fatalf("got (%+v, %v), want ({Y:42}, nil)", reply, err)
	}
}

func TestHandlerGetsACopyOfArgs(t *testing.T) {
	t.Parallel()
	_, eps, _ := setup(2)
	args := &Args{Items: []int{7}}
	if _, err := call(eps[0], 1, "Echo.Clobber", args, deliverWait); err != nil {
		t.Fatal(err)
	}
	if args.Items[0] != 7 {
		t.Fatalf("handler's write reached the caller's args: Items = %v", args.Items)
	}
}

func TestPartition(t *testing.T) {
	t.Parallel()
	n, eps, echos := setup(3)
	n.Partition([]int{0, 1}, []int{2})
	if _, err := call(eps[0], 1, "Echo.Double", &Args{}, deliverWait); err != nil {
		t.Fatalf("0 -> 1 in the same group: %v", err)
	}
	if _, err := call(eps[0], 2, "Echo.Double", &Args{}, lostWait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("0 -> 2 across the partition: got %v, want deadline exceeded", err)
	}
	if echos[2].ran.Load() != 0 {
		t.Fatal("handler on 2 ran although 2 was partitioned away")
	}
	n.Heal()
	if _, err := call(eps[0], 2, "Echo.Double", &Args{}, deliverWait); err != nil {
		t.Fatalf("0 -> 2 after Heal: %v", err)
	}
}

// If the link is cut while a handler is running, the handler's work still
// happens but the caller never hears back. A client that retries has to cope
// with exactly this.
func TestReplyLostAfterHandlerRan(t *testing.T) {
	t.Parallel()
	cuts := []struct {
		name string
		cut  func(n *Network)
	}{
		{"partition", func(n *Network) { n.Partition([]int{0}, []int{1}) }},
		{"crash", func(n *Network) { n.Crash(1) }},
	}
	for _, tc := range cuts {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n, eps, echos := setup(2)
			errc := make(chan error, 1)
			go func() {
				_, err := call(eps[0], 1, "Echo.Hold", &Args{}, lostWait)
				errc <- err
			}()
			<-echos[1].started
			tc.cut(n)
			close(echos[1].release)
			if err := <-errc; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v, want deadline exceeded", err)
			}
		})
	}
}

func TestCrashAndRestart(t *testing.T) {
	t.Parallel()
	n, eps, echos := setup(2)
	n.Crash(1)
	if _, err := call(eps[0], 1, "Echo.Double", &Args{}, lostWait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("call to crashed server: got %v, want deadline exceeded", err)
	}
	if echos[1].ran.Load() != 0 {
		t.Fatal("crashed server ran a handler")
	}
	if _, err := call(eps[1], 0, "Echo.Double", &Args{}, deliverWait); !errors.Is(err, ErrCrashed) {
		t.Fatalf("call from crashed server: got %v, want ErrCrashed", err)
	}

	n.Connect(1).Serve(&Echo{})
	if reply, err := call(eps[0], 1, "Echo.Double", &Args{X: 2}, deliverWait); err != nil || reply.Y != 4 {
		t.Fatalf("call to restarted server: got (%+v, %v), want ({Y:4}, nil)", reply, err)
	}
	if _, err := call(eps[1], 0, "Echo.Double", &Args{}, deliverWait); !errors.Is(err, ErrCrashed) {
		t.Fatalf("old endpoint after restart: got %v, want ErrCrashed", err)
	}
}

func TestDelay(t *testing.T) {
	t.Parallel()
	n, eps, _ := setup(2)
	const d = 20 * time.Millisecond
	n.SetDelay(d, d)
	start := time.Now()
	if _, err := call(eps[0], 1, "Echo.Double", &Args{}, deliverWait); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 2*d {
		t.Fatalf("call took %v; the request and the reply should each take %v", elapsed, d)
	}
}

// Two networks with the same seed should make the same random choices. With
// 100 choices at a 0.5 drop rate, seeing only one outcome has probability
// 2^-99, so the second check won't flake.
func TestSameSeedSameChoices(t *testing.T) {
	t.Parallel()
	seed := rand.Uint64()
	choices := func() []bool {
		n := New(seed)
		n.SetDropRate(0.5)
		a, b := n.Connect(0), n.Connect(1)
		delivered := make([]bool, 100)
		for i := range delivered {
			delivered[i] = n.transmit(a.inst, b.inst)
		}
		return delivered
	}
	first, second := choices(), choices()
	if !slices.Equal(first, second) {
		t.Fatalf("seed %d: two networks with the same seed made different choices", seed)
	}
	if !slices.Contains(first, true) || !slices.Contains(first, false) {
		t.Fatalf("seed %d: drop rate 0.5 gave %v", seed, first)
	}
}

func TestMisusePanics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		misuse func(ctx context.Context, n *Network, e *Endpoint)
	}{
		{"call without deadline", func(ctx context.Context, n *Network, e *Endpoint) {
			e.Call(context.Background(), 1, "Echo.Double", &Args{}, &Reply{})
		}},
		{"unknown method", func(ctx context.Context, n *Network, e *Endpoint) {
			e.Call(ctx, 1, "Echo.Triple", &Args{}, &Reply{})
		}},
		{"wrong args type", func(ctx context.Context, n *Network, e *Endpoint) {
			e.Call(ctx, 1, "Echo.Double", &Reply{}, &Reply{})
		}},
		{"reply not zero", func(ctx context.Context, n *Network, e *Endpoint) {
			e.Call(ctx, 1, "Echo.Double", &Args{}, &Reply{Y: 1})
		}},
		{"unexported field", func(ctx context.Context, n *Network, e *Endpoint) {
			n.Connect(2).Serve(&Leaky{})
		}},
		{"service not a pointer", func(ctx context.Context, n *Network, e *Endpoint) {
			n.Connect(2).Serve(Reply{})
		}},
		{"service without handlers", func(ctx context.Context, n *Network, e *Endpoint) {
			n.Connect(2).Serve(&Reply{})
		}},
		{"connect a running server", func(ctx context.Context, n *Network, e *Endpoint) {
			n.Connect(0)
		}},
		{"serve twice", func(ctx context.Context, n *Network, e *Endpoint) {
			e.Serve(&Echo{})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n, eps, _ := setup(2)
			ctx, cancel := context.WithTimeout(context.Background(), deliverWait)
			defer cancel()
			defer func() {
				if recover() == nil {
					t.Fatal("did not panic")
				}
			}()
			tc.misuse(ctx, n, eps[0])
		})
	}
}
