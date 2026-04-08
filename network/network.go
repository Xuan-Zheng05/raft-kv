// Package network fakes a network inside one process, so we can test Raft and
// the key-value service against the kinds of failures a real network causes.
//
// Servers register handlers shaped like net/rpc handlers, and callers make
// request/reply calls to them. The test decides how badly the network
// behaves: it can drop messages, delay them, split servers into partitions,
// and crash servers.
//
// Every message, whether a request or a reply, is dropped with the configured
// probability. If it survives, it waits a random delay. Even then it's thrown
// away if, by the time it arrives, either side has crashed or the two sides
// have been partitioned. Each message gets its own delay, so a later message
// can overtake an earlier one; reordering comes for free.
//
// Arguments and replies are copied through encoding/gob, the way a real
// network would serialize them, so the caller and the handler never share
// memory.
//
// Nobody is told when a message is lost. The caller just waits until its
// context deadline runs out, the same as on a real network. Mistakes that
// would otherwise fail quietly, like calling a method that doesn't exist or
// using a struct field gob can't see, panic instead so you find out right
// away.
package network

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"
)

// ErrCrashed is what Call returns when the server that owns the Endpoint has
// crashed. A dead server can't send anything.
var ErrCrashed = errors.New("network: endpoint's server has crashed")

var errorType = reflect.TypeFor[error]()

// Network connects servers and clients, each known by a small integer id.
type Network struct {
	mu        sync.Mutex
	rng       *rand.Rand
	dropRate  float64
	minDelay  time.Duration
	maxDelay  time.Duration
	group     map[int]int       // which partition group each id is in; nil means no partition
	instances map[int]*instance // the current life of each id
}

// instance is one life of a server: it starts at Connect and ends at Crash.
// A restart creates a fresh instance, which is how we keep leftovers from the
// old life away from the new one.
type instance struct {
	id       int
	crashed  bool
	handlers map[string]*handler // filled in by Serve; nil until then
}

// handler is one method a server can be called on.
type handler struct {
	fn        reflect.Value // the method, already tied to its service, so we can call it directly
	argType   reflect.Type
	replyType reflect.Type
}

// Endpoint is one server instance's (or client's) handle on the network: you
// send calls through it and register handlers on it. After a crash, Connect
// hands out a new Endpoint, and the old one stays dead for good.
type Endpoint struct {
	net  *Network
	inst *instance
}

// New returns a network where everyone can reach everyone, and nothing is
// dropped or delayed. All of its random choices come from seed, so two
// networks with the same seed make the same choices in the same order. Which
// message ends up with which choice still depends on how Go schedules
// goroutines, so a seed doesn't replay a run exactly.
func New(seed uint64) *Network {
	return &Network{
		rng:       rand.New(rand.NewPCG(seed, seed)),
		instances: make(map[int]*instance),
	}
}

// Connect starts a new instance of server id and returns its Endpoint. The
// instance can send calls right away, but it won't receive any until it
// calls Serve. That gap is on purpose: a Raft server needs its Endpoint before
// it's fully built. Calling Connect again for the same id is how you restart
// a server, and it's only allowed after Crash.
func (n *Network) Connect(id int) *Endpoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	if old := n.instances[id]; old != nil && !old.crashed {
		panic(fmt.Sprintf("network: Connect(%d): server is running; Crash it first", id))
	}
	inst := &instance{id: id}
	n.instances[id] = inst
	return &Endpoint{net: n, inst: inst}
}

// Crash kills the current instance of server id. After this, nothing reaches
// it and nothing leaves it, not even replies from handlers that were halfway
// through, and its Endpoint returns ErrCrashed. The network can't stop the
// server's own goroutines; whoever owns the server has to do that.
func (n *Network) Crash(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	inst := n.instances[id]
	if inst == nil {
		panic(fmt.Sprintf("network: Crash(%d): no such server", id))
	}
	inst.crashed = true
}

// SetDropRate sets the chance, from 0 to 1, that any single message is
// dropped.
func (n *Network) SetDropRate(p float64) {
	if p < 0 || p > 1 {
		panic(fmt.Sprintf("network: drop rate %v is not a probability", p))
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// SetDelay makes every message wait a random time between lo and hi,
// inclusive, before it arrives.
func (n *Network) SetDelay(lo, hi time.Duration) {
	if lo < 0 || hi < lo {
		panic(fmt.Sprintf("network: invalid delay range [%v, %v]", lo, hi))
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minDelay, n.maxDelay = lo, hi
}

// Partition splits the servers into groups. Two ids can talk only if they're
// in the same group, and an id you don't list can't talk to anyone. Call Heal
// to undo it.
func (n *Network) Partition(groups ...[]int) {
	group := make(map[int]int)
	for i, g := range groups {
		for _, id := range g {
			if _, dup := group[id]; dup {
				panic(fmt.Sprintf("network: id %d is in more than one partition group", id))
			}
			group[id] = i
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = group
}

// Heal reconnects everyone.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = nil
}

// Serve registers handlers on this Endpoint's instance. Each service has to be
// a pointer, and every method on it shaped like
//
//	func (s *T) Name(args *Args, reply *Reply) error
//
// becomes a handler called "T.Name". Methods with any other shape are
// skipped, so your Raft type can still have Kill, Start and so on. Calls that
// show up before Serve are lost, as if the server hadn't started listening.
func (e *Endpoint) Serve(services ...any) {
	if len(services) == 0 {
		panic("network: Serve needs at least one service")
	}
	handlers := make(map[string]*handler)
	for _, svc := range services {
		register(handlers, svc)
	}
	n := e.net
	n.mu.Lock()
	defer n.mu.Unlock()
	if e.inst.handlers != nil {
		panic(fmt.Sprintf("network: server %d is already serving", e.inst.id))
	}
	e.inst.handlers = handlers
}

// Call sends a request to server to and waits for the reply. method names the
// handler as "Type.Name", for example "Raft.RequestVote". args and reply have
// to match the handler's parameter types, and reply has to point to a fresh
// zero value.
//
// You get nil once the reply has been filled in, the handler's own error if
// it returned one, ErrCrashed if your own server has crashed, or ctx's error
// if the request or the reply got lost. A timeout doesn't mean the handler
// didn't run: it may have run, and only the reply was lost.
func (e *Endpoint) Call(ctx context.Context, to int, method string, args, reply any) error {
	if _, ok := ctx.Deadline(); !ok {
		panic("network: Call needs a context with a deadline; a lost message never returns otherwise")
	}
	if v := reflect.ValueOf(reply); v.Kind() != reflect.Pointer || !v.Elem().IsZero() {
		panic(fmt.Sprintf("network: Call(%s): reply must point to a zero value, because gob leaves fields that are zero on the wire untouched", method))
	}
	n := e.net
	dst, err := n.resolve(e, to, method, args, reply)
	if err != nil {
		return err
	}
	req := encode(args)

	type result struct {
		reply []byte
		err   error
	}
	// The message travels on its own goroutine, so Call can give up when the
	// deadline passes. That goroutine keeps going even after Call gives up,
	// which is why the handler can still run after a timeout.
	done := make(chan result, 1) // room for one, so the goroutine can always leave its result and exit
	go func() {
		if !n.transmit(e.inst, dst) {
			return
		}
		h := n.handlerFor(dst, method)
		if h == nil {
			return // the server hasn't called Serve yet
		}
		rep, err := h.invoke(req)
		if !n.transmit(dst, e.inst) {
			return
		}
		done <- result{rep, err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			return r.err
		}
		decode(r.reply, reply)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// resolve does the checks that can happen before anything is sent: is the
// caller still alive, and if the other server is already serving, does the
// call match one of its handlers? It returns the other server's current
// instance, or nil if that id has never connected.
func (n *Network) resolve(e *Endpoint, to int, method string, args, reply any) (*instance, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e.inst.crashed {
		return nil, ErrCrashed
	}
	dst := n.instances[to]
	if dst == nil || dst.handlers == nil {
		return dst, nil
	}
	h, ok := dst.handlers[method]
	if !ok {
		panic(fmt.Sprintf("network: server %d has no handler %q", to, method))
	}
	if reflect.TypeOf(args) != h.argType || reflect.TypeOf(reply) != h.replyType {
		panic(fmt.Sprintf("network: %s takes (%v, %v), called with (%T, %T)", method, h.argType, h.replyType, args, reply))
	}
	return dst, nil
}

// transmit carries one message from src to dst and reports whether it made
// it. The message may be dropped up front. Otherwise it waits out its delay,
// and it's still lost if by then either end has crashed or the two have been
// partitioned.
func (n *Network) transmit(src, dst *instance) bool {
	n.mu.Lock()
	drop := n.dropRate > 0 && n.rng.Float64() < n.dropRate
	delay := n.minDelay + time.Duration(n.rng.Int64N(int64(n.maxDelay-n.minDelay)+1))
	n.mu.Unlock() // never sleep while holding the lock, or every other message would wait too
	if drop {
		return false
	}
	time.Sleep(delay)

	n.mu.Lock()
	defer n.mu.Unlock()
	return dst != nil && !src.crashed && !dst.crashed && n.connected(src.id, dst.id)
}

// connected says whether a and b can reach each other right now. Call it with
// n.mu held.
func (n *Network) connected(a, b int) bool {
	if n.group == nil {
		return true
	}
	ga, okA := n.group[a]
	gb, okB := n.group[b]
	return okA && okB && ga == gb
}

// handlerFor looks the handler up when the request arrives rather than when
// it was sent, because the server may have started serving in between.
func (n *Network) handlerFor(inst *instance, method string) *handler {
	n.mu.Lock()
	defer n.mu.Unlock()
	return inst.handlers[method]
}

// invoke runs the handler on its own decoded copy of the request and hands
// back the reply, already encoded for the trip back.
func (h *handler) invoke(req []byte) ([]byte, error) {
	args := reflect.New(h.argType.Elem())
	decode(req, args.Interface())
	reply := reflect.New(h.replyType.Elem())
	out := h.fn.Call([]reflect.Value{args, reply})
	if err, _ := out[0].Interface().(error); err != nil {
		return nil, err
	}
	return encode(reply.Interface()), nil
}

// register finds every handler on svc and adds it to handlers. Serve's comment
// says what counts as a handler.
func register(handlers map[string]*handler, svc any) {
	v := reflect.ValueOf(svc)
	if v.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("network: service %T must be a pointer", svc))
	}
	service := v.Elem().Type().Name()
	before := len(handlers)
	for i := range v.NumMethod() {
		fn := v.Method(i)
		ft := fn.Type()
		if ft.NumIn() != 2 || ft.NumOut() != 1 || ft.Out(0) != errorType ||
			ft.In(0).Kind() != reflect.Pointer || ft.In(1).Kind() != reflect.Pointer {
			continue // not a handler (think Raft's Kill), so skip it
		}
		seen := make(map[reflect.Type]bool)
		mustBeExported(ft.In(0), seen)
		mustBeExported(ft.In(1), seen)
		name := service + "." + v.Type().Method(i).Name
		if _, dup := handlers[name]; dup {
			panic(fmt.Sprintf("network: handler %s registered twice", name))
		}
		handlers[name] = &handler{fn: fn, argType: ft.In(0), replyType: ft.In(1)}
	}
	if len(handlers) == before {
		panic(fmt.Sprintf("network: service %T has no methods of the form func(*Args, *Reply) error", svc))
	}
}

// mustBeExported panics if t has a struct field gob can't see. Gob skips
// unexported fields (names starting with a lowercase letter) without a word,
// so a field like term would quietly arrive as 0. Better to fail loudly here.
func mustBeExported(t reflect.Type, seen map[reflect.Type]bool) {
	if seen[t] {
		return
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		mustBeExported(t.Elem(), seen)
	case reflect.Map:
		mustBeExported(t.Key(), seen)
		mustBeExported(t.Elem(), seen)
	case reflect.Struct:
		for i := range t.NumField() {
			f := t.Field(i)
			if !f.IsExported() {
				panic(fmt.Sprintf("network: field %v.%s is unexported, so gob would silently drop it", t, f.Name))
			}
			mustBeExported(f.Type, seen)
		}
	}
}

// encode and decode panic when they fail, because a value that can't go over
// the wire is a bug in the caller, not something the network did.
func encode(v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic(fmt.Sprintf("network: cannot encode %T: %v", v, err))
	}
	return buf.Bytes()
}

func decode(data []byte, v any) {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		panic(fmt.Sprintf("network: cannot decode into %T: %v", v, err))
	}
}
