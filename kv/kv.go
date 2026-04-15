// Package kv is a replicated key-value store built on Raft. Each server runs
// on top of its own Raft server: client requests go into the Raft log, and
// every server applies them to its map in log order, so all the maps stay
// the same. Clients talk to it through package client.
package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/Xuan-Zheng05/raft-kv/raft"
)

// Kind says what an Op does.
type Kind string

const (
	Get    Kind = "Get"    // read Key
	Put    Kind = "Put"    // set Key to Value
	Append Kind = "Append" // add Value to the end of Key's value
)

// Op is one client request. It's both what clients send and what goes into
// the Raft log. ClientID and Seq together name the request: a client numbers
// its requests one after another and sends a retry with the same Seq, which
// is how a server tells a retry apart from a new request.
type Op struct {
	Kind     Kind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

// Err says how a request went.
type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader" // this server isn't the leader, or stopped being it before the request committed
	ErrTimeout     Err = "ErrTimeout"     // the request didn't commit in time; maybe this leader has been cut off
)

// Reply is the answer to an Op. Value is only set for a Get.
type Reply struct {
	Err   Err
	Value string
}

// Ops travel through the Raft log inside a field of type any, and gob can
// only send a value there if its concrete type has been registered.
func init() {
	gob.Register(Op{})
}

// Server is one key-value server.
type Server struct {
	mu            sync.Mutex
	rf            *raft.Raft
	snapshotEvery int
	data          map[string]string
	// lastSeq is, for each client, the Seq of the last Put or Append of
	// theirs that's been applied. Anything at or below it is a retry.
	lastSeq map[int64]int64
	// waiting holds, for each log index, the request handler waiting to hear
	// what got applied there.
	waiting map[int]chan applied
	done    chan struct{} // closed by Kill
}

// applied is what the apply loop tells a waiting handler.
type applied struct {
	op    Op
	value string // what a Get read
}

// state is what a snapshot holds: the map, and what's needed to keep spotting
// retries after a server starts over from the snapshot.
type state struct {
	Data    map[string]string
	LastSeq map[int64]int64
}

// StartServer starts a key-value server on top of a new Raft server.
// Servers are numbered 0 to n-1, this one is me, and peers is how its Raft
// reaches the others. storage is where its Raft keeps what has to survive a
// crash. The server takes a snapshot every snapshotEvery log entries; how
// often is a trade between a shorter log and the cost of each snapshot, so
// it's up to the caller.
//
// The caller has to make both the server and its Raft reachable: register
// the server and Raft() as services wherever requests arrive.
func StartServer(peers raft.Transport, me, n int, storage raft.Storage, snapshotEvery int) *Server {
	if snapshotEvery <= 0 {
		panic(fmt.Sprintf("kv: snapshotEvery must be positive, got %d", snapshotEvery))
	}
	applyCh := make(chan raft.ApplyMsg)
	kv := &Server{
		snapshotEvery: snapshotEvery,
		data:          make(map[string]string),
		lastSeq:       make(map[int64]int64),
		waiting:       make(map[int]chan applied),
		done:          make(chan struct{}),
	}
	kv.rf = raft.Make(peers, me, n, storage, applyCh)
	go kv.applyLoop(applyCh)
	return kv
}

// Raft returns the Raft server underneath.
func (kv *Server) Raft() *raft.Raft {
	return kv.rf
}

// Kill stops the server and its Raft for good.
func (kv *Server) Kill() {
	kv.rf.Kill()
	close(kv.done)
}

// Do is the handler clients call. It puts op in the Raft log and waits for it
// to come out the other side, then answers with what happened.
//
// Gets go through the log too. A leader that's been cut off from the others
// doesn't know it yet, and answering from its own map could hand back a value
// that's already been overwritten.
func (kv *Server) Do(op *Op, reply *Reply) error {
	// Hold the lock across Start, so the apply loop can't get to this index
	// before we've said we're waiting for it.
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return nil
	}
	// Room for one, so the apply loop never has to wait on us. If an old
	// handler was waiting on this index, our op replaced its op in the log,
	// so it'll time out and its client will try again.
	result := make(chan applied, 1)
	kv.waiting[index] = result
	kv.mu.Unlock()

	select {
	case got := <-result:
		if got.op.ClientID != op.ClientID || got.op.Seq != op.Seq {
			// Someone else's request ended up at our index, which means we
			// lost leadership and our entry was overwritten before it
			// committed.
			reply.Err = ErrWrongLeader
			return nil
		}
		reply.Err, reply.Value = OK, got.value
	case <-time.After(raft.ElectionTimeoutMax):
		kv.mu.Lock()
		if kv.waiting[index] == result {
			delete(kv.waiting, index)
		}
		kv.mu.Unlock()
		reply.Err = ErrTimeout
	}
	return nil
}

// applyLoop takes what Raft has committed, in log order, and applies it.
// Every server applies the same log in the same order, so they all end up
// with the same map and make the same calls about which requests are
// retries.
func (kv *Server) applyLoop(applyCh <-chan raft.ApplyMsg) {
	for {
		var msg raft.ApplyMsg
		select {
		case <-kv.done:
			return
		case msg = <-applyCh:
		}

		kv.mu.Lock()
		if msg.IsSnapshot {
			kv.restore(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		value := kv.apply(op)
		if result, ok := kv.waiting[msg.Index]; ok {
			result <- applied{op: op, value: value}
			delete(kv.waiting, msg.Index)
		}
		var snapshot []byte
		if msg.Index%kv.snapshotEvery == 0 {
			snapshot = kv.snapshot()
		}
		kv.mu.Unlock()

		if snapshot != nil {
			kv.rf.Snapshot(msg.Index, snapshot)
		}
	}
}

// apply runs op against the map and returns what a Get reads. A Put or
// Append whose Seq the client has already had applied is a retry whose first
// try got through, so it's skipped. A repeated Get just reads again, which is
// harmless. The caller must hold kv.mu.
func (kv *Server) apply(op Op) string {
	switch op.Kind {
	case Get:
		return kv.data[op.Key]
	case Put, Append:
		if op.Seq <= kv.lastSeq[op.ClientID] {
			return ""
		}
		kv.lastSeq[op.ClientID] = op.Seq
		if op.Kind == Put {
			kv.data[op.Key] = op.Value
		} else {
			kv.data[op.Key] += op.Value
		}
		return ""
	default:
		panic(fmt.Sprintf("kv: unknown op kind %q", op.Kind))
	}
}

// snapshot encodes the server's state for Raft to keep. The caller must hold
// kv.mu.
func (kv *Server) snapshot() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state{Data: kv.data, LastSeq: kv.lastSeq}); err != nil {
		panic(fmt.Sprintf("kv: can't encode a snapshot: %v", err))
	}
	return buf.Bytes()
}

// restore replaces the server's state with a snapshot. The caller must hold
// kv.mu.
func (kv *Server) restore(snapshot []byte) {
	var s state
	if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&s); err != nil {
		panic(fmt.Sprintf("kv: can't read a snapshot: %v", err))
	}
	// gob sends an empty map as nothing at all, so it can come back nil.
	kv.data, kv.lastSeq = s.Data, s.LastSeq
	if kv.data == nil {
		kv.data = make(map[string]string)
	}
	if kv.lastSeq == nil {
		kv.lastSeq = make(map[int64]int64)
	}
}
