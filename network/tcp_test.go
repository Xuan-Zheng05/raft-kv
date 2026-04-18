package network

import (
	"context"
	"errors"
	"net"
	"testing"
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
	// A second call on the same connection finishes after the late reply
	// has come back.
	ctx2, cancel2 := context.WithTimeout(context.Background(), deliverWait)
	defer cancel2()
	if _, err := tcpCall(tcp, "Echo.Double", &Args{}, ctx2); err != nil {
		t.Fatal(err)
	}
	if reply != (Reply{}) {
		t.Fatalf("a late reply was written into the caller's variable: %+v", reply)
	}
}
