// Package raft is a Raft consensus server, built from Section 5 and Figure 2
// of the Raft paper. It handles leader election.
package raft

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

// Election timeouts are picked at random from this range, as Section 5.2 of
// the paper suggests. The randomness is what keeps servers from all timing
// out at once and splitting the vote over and over.
const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

// heartbeatInterval is how often a leader sends heartbeats. Five of them fit
// inside the shortest election timeout, so a follower only gives up on a live
// leader if five heartbeats in a row get lost. Even on a network that drops
// one message in ten, that's a 1 in 100,000 chance.
const heartbeatInterval = electionTimeoutMin / 5

// noVote is what votedFor holds when this server hasn't voted in the current
// term yet.
const noVote = -1

// Transport is everything Raft needs from the network: send a request to
// another server and wait for its reply. The simulated network's Endpoint is
// one; anything with this Call method works.
type Transport interface {
	Call(ctx context.Context, to int, method string, args, reply any) error
}

type role int

const (
	follower role = iota
	candidate
	leader
)

// LogEntry is one command in the log, tagged with the term in which the
// leader received it.
type LogEntry struct {
	Term    int
	Command any
}

// Raft is one server in a Raft cluster.
type Raft struct {
	mu    sync.Mutex
	peers Transport
	me    int // this server's id
	n     int // how many servers are in the cluster, counting this one

	// Persistent state, straight from Figure 2. The paper requires these to
	// survive a crash.
	currentTerm int
	votedFor    int // who we voted for in currentTerm, or noVote
	// log[0] is a placeholder with term 0, so real entries start at index 1
	// like in the paper, and "the last entry" always exists.
	log []LogEntry

	// Everything else is fine to lose in a crash.
	role             role
	electionDeadline time.Time // if nothing pushes this back first, we start an election

	// ctx lives as long as the server does. Kill cancels it, which stops the
	// election timer and abandons every RPC this server still has in flight.
	ctx    context.Context
	cancel context.CancelFunc
}

// RequestVoteArgs is what a candidate sends when asking for a vote.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the answer. Term lets a candidate that's behind find out.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is what a leader sends to tell a server it's still in
// charge.
type AppendEntriesArgs struct {
	Term int
}

// AppendEntriesReply is the answer. Term lets a leader that's behind find out.
type AppendEntriesReply struct {
	Term int
}

// Make starts a Raft server. Servers are numbered 0 to n-1, this one is me,
// and peers is how it reaches the others. Make returns right away; the
// election timer runs on its own goroutine until Kill.
func Make(peers Transport, me, n int) *Raft {
	ctx, cancel := context.WithCancel(context.Background())
	rf := &Raft{
		peers:    peers,
		me:       me,
		n:        n,
		votedFor: noVote,
		log:      []LogEntry{{Term: 0}},
		ctx:      ctx,
		cancel:   cancel,
	}
	rf.resetElectionTimer() // no lock needed: no other goroutine can see rf yet
	go rf.ticker()
	return rf
}

// State returns this server's current term and whether it believes it's the
// leader.
func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// Kill stops the server for good: its election timer stops and any RPC it's
// waiting on is abandoned. Restarting a server means calling Make again.
func (rf *Raft) Kill() {
	rf.cancel()
}

// ticker is the election timer. It sleeps until the election deadline, and if
// nothing pushed the deadline back in the meantime, it starts an election.
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		switch {
		case rf.role == leader:
			// Leaders never time out on themselves. Pushing the deadline
			// forward just keeps this loop from spinning.
			rf.resetElectionTimer()
		case !time.Now().Before(rf.electionDeadline):
			rf.startElection()
		}
		wait := time.Until(rf.electionDeadline)
		rf.mu.Unlock()

		select {
		case <-rf.ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// startElection makes this server a candidate in a new term and asks every
// other server for its vote. The caller must hold rf.mu.
//
// Each vote request goes out on its own goroutine. Those goroutines only take
// the lock after their reply comes back, so the lock is never held while
// waiting on the network.
func (rf *Raft) startElection() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	term := rf.currentTerm
	last := len(rf.log) - 1
	args := &RequestVoteArgs{Term: term, CandidateID: rf.me, LastLogIndex: last, LastLogTerm: rf.log[last].Term}
	// A vote that shows up after this election has timed out is no use to
	// us, because by then we've moved on to a new term. So that's exactly how
	// long each request gets.
	deadline := rf.electionDeadline
	votes := 1 // our own vote; only touched while holding rf.mu

	for peer := range rf.n {
		if peer == rf.me {
			continue
		}
		go func() {
			ctx, cancel := context.WithDeadline(rf.ctx, deadline)
			defer cancel()
			var reply RequestVoteReply
			if err := rf.peers.Call(ctx, peer, "Raft.RequestVote", args, &reply); err != nil {
				return // no answer, which is normal; we may still win without this vote
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			// Things may have changed while we waited. Only count the vote if
			// we're still a candidate in the same term we asked for it in.
			if rf.role != candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes == rf.n/2+1 { // == rather than >=, so this happens exactly once
				rf.role = leader
				go rf.sendHeartbeats(term)
			}
		}()
	}
}

// sendHeartbeats runs for as long as this server is the leader of term. It
// sends a heartbeat to every other server right away, before any of their
// timers can run out, and then again every heartbeatInterval.
func (rf *Raft) sendHeartbeats(term int) {
	args := &AppendEntriesArgs{Term: term}
	for {
		rf.mu.Lock()
		stillLeader := rf.role == leader && rf.currentTerm == term
		rf.mu.Unlock()
		if !stillLeader {
			return
		}

		for peer := range rf.n {
			if peer == rf.me {
				continue
			}
			go func() {
				// The next heartbeat replaces this one after heartbeatInterval,
				// so there's no point waiting longer than that for a reply.
				ctx, cancel := context.WithTimeout(rf.ctx, heartbeatInterval)
				defer cancel()
				var reply AppendEntriesReply
				if err := rf.peers.Call(ctx, peer, "Raft.AppendEntries", args, &reply); err != nil {
					return // lost; the next heartbeat tries again
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term) // someone newer is around, so we're not the leader anymore
				}
			}()
		}

		select {
		case <-rf.ctx.Done():
			return
		case <-time.After(heartbeatInterval):
		}
	}
}

// RequestVote is the handler a candidate calls to ask for this server's vote.
// It follows "RequestVote RPC" in Figure 2.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term) // a newer term clears our vote, so we're free to vote below
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil // stale candidate; the term in our reply tells it so
	}

	// Only vote for a candidate whose log is at least as up to date as ours
	// (Section 5.4.1): its last entry has a later term, or the same term and
	// the log is at least as long.
	last := len(rf.log) - 1
	upToDate := args.LastLogTerm > rf.log[last].Term ||
		(args.LastLogTerm == rf.log[last].Term && args.LastLogIndex >= last)
	if (rf.votedFor == noVote || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.resetElectionTimer() // granting a vote resets the timer; refusing one doesn't
		reply.VoteGranted = true
	}
	return nil
}

// AppendEntries is the handler a leader calls to say it's still in charge.
// It follows the term rules of "AppendEntries RPC" in Figure 2.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil // a leader from an old term; the term in our reply tells it to step down
	}

	// The sender is the leader of our current term. If we were a candidate in
	// this term, we lost, so we go back to being a follower. We keep our vote
	// as it is: clearing it here would let us vote twice in the same term.
	rf.role = follower
	rf.resetElectionTimer()
	return nil
}

// becomeFollower moves this server to a newer term it just heard about. The
// old vote belonged to the old term, so it's cleared. This does not reset the
// election timer: seeing a higher term isn't the same as hearing from a
// leader. The caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = noVote
	rf.role = follower
}

// resetElectionTimer moves the election deadline to a new random point in the
// future. The caller must hold rf.mu.
func (rf *Raft) resetElectionTimer() {
	timeout := electionTimeoutMin + rand.N(electionTimeoutMax-electionTimeoutMin+1)
	rf.electionDeadline = time.Now().Add(timeout)
}
