package network

import (
	"context"
	"errors"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// serveEcho starts an Echo on a free local port and returns it with its
// server. The server is closed when the test ends.
func serveEcho(t *testing.T, addr string) (*Echo, *TCPServer) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	echo := &Echo{started: make(chan struct{}, 1), release: make(chan struct{})}
	srv, err := ServeTCP(ln, echo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	return echo, srv
}

func tcpCall(t *TCP, method string, args *Args, wait context.Context) (Reply, error) {
	var reply Reply
	err := t.Call(wait, 0, method, args, &reply)
	return reply, err
}

func TestTCPCallReturnsReply(t *testing.T) {
	t.Parallel()
	_, srv := serveEcho(t, "127.0.0.1:0")
	tcp := DialTCP([]string{srv.ln.Addr().String()})
	ctx, cancel := context.WithTimeout(context.Background(), deliverWait)
	defer cancel()
	if reply, err := tcpCall(tcp, "Echo.Double", &Args{X: 21}, ctx); err != nil || reply.Y != 42 {
		t.Fatalf("got (%+v, %v), want ({Y:42}, nil)", reply, err)
	}
}

// A call to a server that's gone fails instead of hanging, and once the
// server comes back at the same address, calls work again without anyone
// reconnecting by hand.
func TestTCPReconnectsAfterServerRestarts(t *testing.T) {
	t.Parallel()
	_, srv := serveEcho(t, "127.0.0.1:0")
	addr := srv.ln.Addr().String()
	tcp := DialTCP([]string{addr})
	ctx, cancel := context.WithTimeout(context.Background(), deliverWait)
	defer cancel()
	if _, err := tcpCall(tcp, "Echo.Double", &Args{X: 1}, ctx); err != nil {
		t.Fatal(err)
	}

	srv.Close()
	if _, err := tcpCall(tcp, "Echo.Double", &Args{X: 1}, ctx); err == nil {
		t.Fatal("call to a closed server succeeded")
	}

	serveEcho(t, addr)
	if reply, err := tcpCall(tcp, "Echo.Double", &Args{X: 2}, ctx); err != nil || reply.Y != 4 {
		t.Fatalf("call after the server came back: got (%+v, %v), want ({Y:4}, nil)", reply, err)
	}
}

// countAccepts is a listener that counts the connections it hands out.
type countAccepts struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countAccepts) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return conn, err
}

// Calls to a server share one connection. A call that finished keeps its
// connection open even after its caller cancels the context, which every
// caller does right after.
func TestTCPCallsShareAConnection(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &countAccepts{Listener: ln}
	srv, err := ServeTCP(l, &Echo{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	tcp := DialTCP([]string{ln.Addr().String()})

	for x := range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), deliverWait)
		reply, err := tcpCall(tcp, "Echo.Double", &Args{X: x}, ctx)
		cancel()
		if err != nil || reply.Y != 2*x {
			t.Fatalf("call %d: got (%+v, %v), want ({Y:%d}, nil)", x, reply, err, 2*x)
		}
		time.Sleep(lostWait) // time for a wrongly closed connection to be gone
	}
	if n := l.accepted.Load(); n != 1 {
		t.Fatalf("two calls in a row used %d connections, want 1", n)
	}
}

// parkFirst is a listener that accepts its first connection and then never
// reads from it, like a peer whose connection has stopped moving. Every
// connection after that is handed on as usual.
type parkFirst struct {
	net.Listener
	parked chan net.Conn
}

func (l *parkFirst) Accept() (net.Conn, error) {
	if len(l.parked) == 0 {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.parked <- conn
	}
	return l.Listener.Accept()
}

// A connection can stop moving without breaking: the other end stops reading,
// or TCP is waiting out a long retransmission backoff after losing packets.
// A call that's stuck sending on it still returns at its deadline, and the
// next call goes through on a new connection instead of queueing behind the
// stuck one.
func TestTCPStuckConnectionIsReplaced(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &parkFirst{Listener: ln, parked: make(chan net.Conn, 1)}
	srv, err := ServeTCP(l, &Echo{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		srv.Close()
		if len(l.parked) > 0 {
			(<-l.parked).Close()
		}
	})
	tcp := DialTCP([]string{ln.Addr().String()})

	// Each of these encodes to 9 bytes, so this is about 18 MB: more than the
	// two ends' socket buffers can hold between them (at most 4 MB each on
	// macOS by default), so the send can't finish while nobody reads.
	items := make([]int, 2<<20)
	for i := range items {
		items[i] = math.MaxInt
	}
	ctx, cancel := context.WithTimeout(context.Background(), lostWait)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := tcpCall(tcp, "Echo.Double", &Args{Items: items}, ctx)
		errc <- err
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stuck call: got %v, want deadline exceeded", err)
		}
	case <-time.After(lostWait + deliverWait):
		t.Fatal("a call stuck sending didn't return after its deadline")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), deliverWait)
	defer cancel2()
	if reply, err := tcpCall(tcp, "Echo.Double", &Args{X: 2}, ctx2); err != nil || reply.Y != 4 {
		t.Fatalf("call after the stuck one: got (%+v, %v), want ({Y:4}, nil)", reply, err)
	}
}

// When the caller's deadline passes, Call returns, and a reply that arrives
// afterwards doesn't land in the caller's variable.
func TestTCPLateReplyIsDropped(t *testing.T) {
	t.Parallel()
	echo, srv := serveEcho(t, "127.0.0.1:0")
	tcp := DialTCP([]string{srv.ln.Addr().String()})

	ctx, cancel := context.WithTimeout(context.Background(), lostWait)
	defer cancel()
	var reply Reply
	errc := make(chan error, 1)
	go func() { errc <- tcp.Call(ctx, 0, "Echo.Hold", &Args{X: 7}, &reply) }()
	<-echo.started
	if err := <-errc; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}

	close(echo.release)
	// By the time a second call finishes, the held call has had every
	// chance to answer.
	ctx2, cancel2 := context.WithTimeout(context.Background(), deliverWait)
	defer cancel2()
	if _, err := tcpCall(tcp, "Echo.Double", &Args{}, ctx2); err != nil {
		t.Fatal(err)
	}
	if reply != (Reply{}) {
		t.Fatalf("a late reply was written into the caller's variable: %+v", reply)
	}
}
