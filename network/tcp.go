package network

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"reflect"
	"sync"
)

// TCP sends calls to servers running as real processes, over TCP, using the
// standard library's net/rpc. It's what the benchmarks use; tests use the
// simulated Network. Handlers have the same shape either way, so a Raft or
// key-value server doesn't know which one it's on.
type TCP struct {
	peers []*peer
}

// peer is one server we talk to, with the connection to it if we have one.
type peer struct {
	addr   string
	mu     sync.Mutex
	client *rpc.Client // nil until the first call, and again after the connection breaks
}

// DialTCP returns a TCP that reaches server i at addrs[i]. It doesn't connect
// to anything yet: each server gets connected on the first call to it, and
// reconnected on the call after its connection breaks, so servers can start
// in any order and come back after a crash.
func DialTCP(addrs []string) *TCP {
	t := &TCP{}
	for _, addr := range addrs {
		t.peers = append(t.peers, &peer{addr: addr})
	}
	return t
}

// Call sends a request to server to and waits for the reply, with the same
// rules as Endpoint.Call: ctx has to have a deadline, and reply has to point
// to a zero value. It returns the handler's error if there was one, or the
// network's if the call couldn't be made or ctx ran out first.
func (t *TCP) Call(ctx context.Context, to int, method string, args, reply any) error {
	if _, ok := ctx.Deadline(); !ok {
		panic("network: Call needs a context with a deadline; a lost message never returns otherwise")
	}
	p := t.peers[to]
	client, err := p.connect(ctx)
	if err != nil {
		return err
	}

	// Decode into a fresh value and copy it into reply only if the call
	// works out. A reply that turns up after we've given up then can't
	// write into the caller's variable behind its back.
	fresh := reflect.New(reflect.TypeOf(reply).Elem())
	call := client.Go(method, args, fresh.Interface(), make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			// A ServerError is the handler's own error, and the connection is
			// fine. Anything else means the connection broke, so drop it and
			// let the next call dial again.
			var handlerErr rpc.ServerError
			if !errors.As(call.Error, &handlerErr) {
				p.forget(client)
			}
			return call.Error
		}
		reflect.ValueOf(reply).Elem().Set(fresh.Elem())
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// connect returns the connection to this peer, dialing it first if there
// isn't one. Only calls to the same peer wait on each other here.
func (p *peer) connect(ctx context.Context) (*rpc.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, err
	}
	p.client = rpc.NewClient(conn)
	return p.client, nil
}

// forget drops a broken connection, unless someone has already replaced it.
func (p *peer) forget(client *rpc.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == client {
		p.client.Close()
		p.client = nil
	}
}

// TCPServer answers calls that arrive on a listener.
type TCPServer struct {
	ln     net.Listener
	mu     sync.Mutex
	conns  map[net.Conn]bool
	closed bool
}

// ServeTCP registers each service with net/rpc and answers calls arriving on
// ln, until Close. A service's methods shaped like
// func(args *Args, reply *Reply) error become handlers named "Type.Method",
// the same as on the simulated network. The caller makes the listener, so it
// can learn the address before the services exist.
func ServeTCP(ln net.Listener, services ...any) (*TCPServer, error) {
	server := rpc.NewServer()
	for _, svc := range services {
		if err := server.Register(svc); err != nil {
			return nil, err
		}
	}
	s := &TCPServer{ln: ln, conns: make(map[net.Conn]bool)}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // the listener was closed
			}
			if !s.track(conn) {
				conn.Close()
				return
			}
			go func() {
				server.ServeConn(conn)
				s.untrack(conn)
			}()
		}
	}()
	return s, nil
}

// Close stops taking calls and drops every open connection, which is what
// clients see when the server's process dies.
func (s *TCPServer) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.ln.Close()
	for conn := range s.conns {
		conn.Close()
	}
}

func (s *TCPServer) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = true
	return true
}

func (s *TCPServer) untrack(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
}
