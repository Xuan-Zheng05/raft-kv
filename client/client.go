// Package client is how programs talk to the key-value store in package kv.
package client

import (
	"context"
	"math/rand/v2"

	"github.com/Xuan-Zheng05/raft-kv/kv"
	"github.com/Xuan-Zheng05/raft-kv/raft"
)

// Clerk sends requests to the key-value servers, finds whichever one is the
// leader, and retries until each request goes through. It handles one request
// at a time, so it isn't safe to use from several goroutines at once; use one
// Clerk per goroutine instead.
type Clerk struct {
	net     raft.Transport
	servers []int // servers[i] is Raft server i's id on the network
	leader  int   // which server we think the leader is
	id      int64 // picked at random, so two clients won't share one
	seq     int64 // the Seq of the last request we started
}

// MakeClerk returns a Clerk that reaches the servers through net.
// servers[i] has to be the network id of Raft server i, since that's how
// servers name the leader when they point a client to it.
func MakeClerk(net raft.Transport, servers []int) *Clerk {
	return &Clerk{net: net, servers: servers, id: rand.Int64()}
}

// Leader returns which server answered this Clerk's last request, as an
// index into the servers it was made with. As far as the Clerk knows, that
// server is the leader.
func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get returns key's value, or "" if it's never been set.
func (ck *Clerk) Get(key string) string {
	return ck.do(kv.Op{Kind: kv.Get, Key: key})
}

// Put sets key's value.
func (ck *Clerk) Put(key, value string) {
	ck.do(kv.Op{Kind: kv.Put, Key: key, Value: value})
}

// Append adds value to the end of key's value.
func (ck *Clerk) Append(key, value string) {
	ck.do(kv.Op{Kind: kv.Append, Key: key, Value: value})
}

// do sends op until some server applies it, and returns what a Get read.
// Every try uses the same Seq: if an earlier try went through and only its
// reply got lost, the servers recognize the retry and don't apply it again.
func (ck *Clerk) do(op kv.Op) string {
	ck.seq++
	op.ClientID, op.Seq = ck.id, ck.seq
	for {
		// A server that can't get a request committed within the longest
		// election timeout has most likely lost its leadership.
		ctx, cancel := context.WithTimeout(context.Background(), raft.ElectionTimeoutMax)
		var reply kv.Reply
		err := ck.net.Call(ctx, ck.servers[ck.leader], "Server.Do", &op, &reply)
		cancel()
		if err == nil && reply.Err == kv.OK {
			return reply.Value
		}
		// A server that isn't the leader usually knows who is. Going straight
		// there saves trying servers we may not be able to reach, each of
		// which costs a full timeout.
		if err == nil && reply.Err == kv.ErrWrongLeader && reply.Leader >= 0 && reply.Leader != ck.leader {
			ck.leader = reply.Leader
			continue
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}
