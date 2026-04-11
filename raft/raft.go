// Package raft is a Raft consensus server, built from Section 5 and Figure 2
// of the Raft paper. It elects a leader, copies the leader's log to the other
// servers, and hands committed commands to the application in log order.
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"slices"
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

// heartbeatInterval is how often a leader sends AppendEntries to each
// server. Five of them fit inside the shortest election timeout, so a
// follower only gives up on a live leader if five in a row get lost. Even on
// a network that drops one message in ten, that's a 1 in 100,000 chance.
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

// ApplyMsg is how a server hands a committed command to the application.
// They arrive in log order, one index at a time, with no gaps.
type ApplyMsg struct {
	Index   int
	Command any
}

// Raft is one server in a Raft cluster.
type Raft struct {
	mu      sync.Mutex
	peers   Transport
	storage Storage
	me      int // this server's id
	n       int // how many servers are in the cluster, counting this one
	applyCh chan<- ApplyMsg

	// Persistent state, straight from Figure 2. These have to survive a
	// crash, so every change to them goes through persist.
	currentTerm int
	votedFor    int // who we voted for in currentTerm, or noVote
	// log[0] is a placeholder with term 0, so real entries start at index 1
	// like in the paper, and "the last entry" always exists.
	log []LogEntry

	// Volatile state on every server, from Figure 2.
	commitIndex int // the highest log index known to be committed
	lastApplied int // the highest log index the applier has taken for applyCh

	// Volatile state on the leader, from Figure 2. Reset every time this
	// server wins an election.
	nextIndex  []int // for each server, the next log index to send it
	matchIndex []int // for each server, the highest log index known to be stored there

	// The rest is our own bookkeeping, not from the paper.
	role             role
	electionDeadline time.Time     // if nothing pushes this back first, we start an election
	applyKick        chan struct{} // wakes the applier when commitIndex moves; holds at most one nudge

	// ctx lives as long as the server does. Kill cancels it, which stops all
	// of this server's goroutines and abandons every RPC it still has in
	// flight.
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

// AppendEntriesArgs is what a leader sends: the entries a server is missing,
// plus the position they go after, so the server can check that its log
// lines up with the leader's there. With no entries, it's a heartbeat.
type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int // the index of the entry just before Entries
	PrevLogTerm  int // the term of that entry
	Entries      []LogEntry
	LeaderCommit int // the leader's commitIndex
}

// AppendEntriesReply is the answer. Term lets a leader that's behind find
// out, and Success says whether the entries were taken.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// persistentState is what persist writes: Figure 2's persistent state.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// Make starts a Raft server. Servers are numbered 0 to n-1, this one is me,
// and peers is how it reaches the others. storage holds whatever this server
// saved before it last crashed, if anything, and it's where the server saves
// from now on. Committed commands come out of applyCh in log order. Make
// returns right away; the server's goroutines run until Kill.
//
// A restarted server picks up its term, vote and log, but nothing else:
// commitIndex starts at 0 again, so it hands its log to applyCh again from
// the beginning as the leader tells it what's committed.
func Make(peers Transport, me, n int, storage Storage, applyCh chan<- ApplyMsg) *Raft {
	ctx, cancel := context.WithCancel(context.Background())
	rf := &Raft{
		peers:      peers,
		storage:    storage,
		me:         me,
		n:          n,
		applyCh:    applyCh,
		votedFor:   noVote,
		log:        []LogEntry{{Term: 0}},
		nextIndex:  make([]int, n),
		matchIndex: make([]int, n),
		applyKick:  make(chan struct{}, 1),
		ctx:        ctx,
		cancel:     cancel,
	}
	if data := storage.Load(); len(data) > 0 {
		// Decode into a fresh value: gob leaves fields that are zero on the
		// wire untouched, so decoding over values that are already set could
		// keep stale ones.
		var saved persistentState
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&saved); err != nil {
			panic(fmt.Sprintf("raft: server %d can't read its saved state: %v", me, err))
		}
		rf.currentTerm, rf.votedFor, rf.log = saved.CurrentTerm, saved.VotedFor, saved.Log
	}
	rf.resetElectionTimer() // no lock needed: no other goroutine can see rf yet
	go rf.ticker()
	go rf.applier()
	return rf
}

// Start asks the server to add command to the log. If this server isn't the
// leader, it says so and does nothing. Otherwise it appends the command and
// returns right away with the index the command will have if it commits.
// There's no promise that it will: the leader can lose its job first.
func (rf *Raft) Start(command any) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader {
		return 0, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	rf.matchIndex[rf.me] = len(rf.log) - 1 // the leader's own copy counts toward a majority, so it has to be saved first
	return len(rf.log) - 1, rf.currentTerm, true
}

// State returns this server's current term and whether it believes it's the
// leader.
func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// Kill stops the server for good: all its goroutines stop and any RPC it's
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
	rf.persist() // before any vote request goes out
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
				// Start out assuming every follower's log matches ours. When
				// that's wrong, the follower turns down our AppendEntries and
				// we step back until we find where the logs agree.
				for i := range rf.n {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = len(rf.log) - 1
				go rf.replicate(term)
			}
		}()
	}
}

// replicate runs for as long as this server is the leader of term. Right
// away, and then every heartbeatInterval, it sends every other server the
// entries it's missing. A server that's caught up gets an empty
// AppendEntries, which is a heartbeat.
func (rf *Raft) replicate(term int) {
	for {
		rf.mu.Lock()
		if rf.role != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for peer := range rf.n {
			if peer == rf.me {
				continue
			}
			prev := rf.nextIndex[peer] - 1
			args := &AppendEntriesArgs{
				Term:         term,
				PrevLogIndex: prev,
				PrevLogTerm:  rf.log[prev].Term,
				// A copy, because the network reads these after we let go of
				// the lock, and by then our log may have changed.
				Entries:      slices.Clone(rf.log[prev+1:]),
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendEntries(term, peer, args)
		}
		rf.mu.Unlock()

		select {
		case <-rf.ctx.Done():
			return
		case <-time.After(heartbeatInterval):
		}
	}
}

// sendEntries sends one AppendEntries to peer and acts on the answer.
func (rf *Raft) sendEntries(term, peer int, args *AppendEntriesArgs) {
	// A late reply is still useful, since it tells us what peer has stored.
	// But if a round trip takes longer than the shortest election timeout,
	// followers time out before they hear from us and the cluster can't keep
	// a leader anyway (Section 5.6), so there's no point waiting longer.
	ctx, cancel := context.WithTimeout(rf.ctx, electionTimeoutMin)
	defer cancel()
	var reply AppendEntriesReply
	if err := rf.peers.Call(ctx, peer, "Raft.AppendEntries", args, &reply); err != nil {
		return // lost; the next round tries again
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term) // someone newer is around, so we're not the leader anymore
		return
	}
	if rf.role != leader || rf.currentTerm != term {
		return // an answer to a term we're no longer leading
	}

	if !reply.Success {
		// peer's log doesn't line up with ours at PrevLogIndex, so next round
		// we try one entry further back. Only step back if this rejection is
		// about where we're aiming right now; a late reply to an older request
		// shouldn't move us.
		if rf.nextIndex[peer] == args.PrevLogIndex+1 {
			rf.nextIndex[peer]--
		}
		return
	}

	// Work out what peer has from what this request carried, not from our log
	// as it is now, which may have grown since we sent it. Replies can also
	// arrive out of order, so matchIndex only ever moves forward.
	if match := args.PrevLogIndex + len(args.Entries); match > rf.matchIndex[peer] {
		rf.matchIndex[peer] = match
		rf.nextIndex[peer] = match + 1
		rf.advanceCommit()
	}
}

// advanceCommit moves commitIndex up to the newest entry that a majority of
// servers have stored, as long as that entry is from the current term. The
// caller must hold rf.mu.
//
// The current-term condition is Figure 8 in the paper. An entry from an older
// term can be on a majority of servers and still get overwritten by a later
// leader, so counting copies is only safe for entries from our own term.
// Older entries still get committed, just indirectly: committing an entry
// commits everything before it.
func (rf *Raft) advanceCommit() {
	for index := len(rf.log) - 1; index > rf.commitIndex && rf.log[index].Term == rf.currentTerm; index-- {
		copies := 0
		for _, match := range rf.matchIndex {
			if match >= index {
				copies++
			}
		}
		if copies > rf.n/2 {
			rf.commitIndex = index
			rf.kickApplier()
			return
		}
	}
}

// applier hands committed entries to applyCh, in order. It has its own
// goroutine so that sending on applyCh never happens while holding rf.mu: the
// application reading applyCh may call Start, which needs the lock, and the
// two would wait on each other forever.
func (rf *Raft) applier() {
	for {
		select {
		case <-rf.ctx.Done():
			return
		case <-rf.applyKick:
		}

		rf.mu.Lock()
		first := rf.lastApplied + 1
		entries := slices.Clone(rf.log[first : rf.commitIndex+1])
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for i, entry := range entries {
			select {
			case rf.applyCh <- ApplyMsg{Index: first + i, Command: entry.Command}:
			case <-rf.ctx.Done():
				return
			}
		}
	}
}

// kickApplier wakes the applier without waiting for it. If a nudge is already
// pending, that one is enough: the applier reads commitIndex fresh each time
// it wakes.
func (rf *Raft) kickApplier() {
	select {
	case rf.applyKick <- struct{}{}:
	default:
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
	// the log is at least as long. This is what guarantees a new leader
	// already has every committed entry.
	last := len(rf.log) - 1
	upToDate := args.LastLogTerm > rf.log[last].Term ||
		(args.LastLogTerm == rf.log[last].Term && args.LastLogIndex >= last)
	if (rf.votedFor == noVote || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		// Save before the candidate hears yes. Otherwise a crash could make us
		// forget this vote and vote again in the same term.
		rf.persist()
		rf.resetElectionTimer() // granting a vote resets the timer; refusing one doesn't
		reply.VoteGranted = true
	}
	return nil
}

// AppendEntries is the handler a leader calls to copy its log here, or, with
// no entries, just to say it's still in charge. It follows "AppendEntries
// RPC" in Figure 2.
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

	// Only take the entries if we have the entry they're meant to follow.
	// This check is what keeps every log an exact copy of the leader's, up to
	// the point where they agree.
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	// Go through the new entries. Ones we already have get skipped. At the
	// first real conflict (same index, different term) we drop that entry and
	// everything after it, then append the rest. We never cut the log just
	// because a message is shorter than our log: it may be an old message
	// that arrived late, and cutting would throw away entries we've already
	// told the leader we have.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:index]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist() // before we tell the leader we have these
		break
	}

	// Commit as far as the leader has, but no further than the last entry
	// this message vouched for. Anything past that in our log hasn't been
	// checked against the leader's yet.
	if commit := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)); commit > rf.commitIndex {
		rf.commitIndex = commit
		rf.kickApplier()
	}
	reply.Success = true
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
	rf.persist()
}

// persist saves currentTerm, votedFor and log. Everything that changes one of
// them calls this before letting go of rf.mu, which is always before the
// change can reach another server: replies go out after a handler returns,
// and requests go out after the lock is released. The caller must hold rf.mu.
func (rf *Raft) persist() {
	var buf bytes.Buffer
	state := persistentState{CurrentTerm: rf.currentTerm, VotedFor: rf.votedFor, Log: rf.log}
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		panic(fmt.Sprintf("raft: server %d can't save its state: %v", rf.me, err))
	}
	rf.storage.Save(buf.Bytes())
}

// resetElectionTimer moves the election deadline to a new random point in the
// future. The caller must hold rf.mu.
func (rf *Raft) resetElectionTimer() {
	timeout := electionTimeoutMin + rand.N(electionTimeoutMax-electionTimeoutMin+1)
	rf.electionDeadline = time.Now().Add(timeout)
}
