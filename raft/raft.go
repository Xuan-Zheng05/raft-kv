// Package raft is a Raft consensus server, built from Sections 5 and 7 and
// Figures 2 and 13 of the Raft paper. It elects a leader, copies the leader's
// log to the other servers, hands committed commands to the application in
// log order, and keeps the log short with snapshots the application provides.
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
//
// ElectionTimeoutMax is exported because code built on Raft can use it as a
// yardstick: a leader that hasn't made progress for that long has most likely
// been replaced, since its followers would have started an election by then.
const (
	electionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
)

// heartbeatInterval is how often a leader sends AppendEntries to each
// server. Five of them fit inside the shortest election timeout, so a
// follower only gives up on a live leader if five in a row get lost. Even on
// a network that drops one message in ten, that's a 1 in 100,000 chance.
const heartbeatInterval = electionTimeoutMin / 5

// noVote is what votedFor holds when this server hasn't voted in the current
// term yet.
const noVote = -1

// noLeader is what leaderID holds when this server doesn't know who the
// leader of its current term is.
const noLeader = -1

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

// ApplyMsg is how a server hands committed work to the application, in log
// order. Usually it's one command. When the server has been sent a snapshot,
// or is starting up from one, it's that snapshot instead: it stands in for
// every command up to Index, and the application should replace its state
// with it.
type ApplyMsg struct {
	Index      int
	Command    any // the command at Index, unless IsSnapshot
	IsSnapshot bool
	Snapshot   []byte // the application's state with everything up to Index applied
}

// Raft is one server in a Raft cluster.
type Raft struct {
	mu      sync.Mutex
	peers   Transport
	storage Storage
	me      int // this server's id
	n       int // how many servers are in the cluster, counting this one
	applyCh chan<- ApplyMsg

	// Persistent state: Figure 2's, plus what the snapshot covers. These have
	// to survive a crash, so every change to them goes through persist.
	currentTerm int
	votedFor    int // who we voted for in currentTerm, or noVote
	// The log only holds what comes after the snapshot. log[0] stands for the
	// last entry the snapshot covers: index lastIncludedIndex, term
	// log[0].Term. With no snapshot yet that's index 0 and term 0, so real
	// entries start at index 1 like in the paper. Either way, the entry
	// before the first real one always exists. pos turns a log index into a
	// position in this slice.
	log               []LogEntry
	lastIncludedIndex int
	// snapshot is the application's state up to lastIncludedIndex. It only
	// ever gets replaced, never changed in place, so it's safe to hand out.
	snapshot []byte

	// Volatile state on every server, from Figure 2.
	commitIndex int // the highest log index known to be committed
	lastApplied int // the highest log index the applier has taken for applyCh

	// Volatile state on the leader, from Figure 2. Reset every time this
	// server wins an election.
	nextIndex  []int // for each server, the next log index to send it
	matchIndex []int // for each server, the highest log index known to be stored there

	// The rest is our own bookkeeping, not from the paper.
	role role
	// leaderID is who we last heard from as leader of currentTerm, or
	// noLeader. Raft itself doesn't need it; it's there so a server that
	// isn't the leader can point clients to the one that is (Section 8).
	leaderID         int
	electionDeadline time.Time     // if nothing pushes this back first, we start an election
	applyKick        chan struct{} // wakes the applier when there's more to apply; holds at most one nudge

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
	LeaderID     int
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

// InstallSnapshotArgs is what a leader sends a server that's so far behind
// that the entries it needs have been compacted away: the leader's whole
// snapshot, and the index and term of the last entry it covers.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply is the answer. Term lets a leader that's behind find
// out.
type InstallSnapshotReply struct {
	Term int
}

// persistentState is what persist writes, next to the snapshot.
type persistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
}

// Make starts a Raft server. Servers are numbered 0 to n-1, this one is me,
// and peers is how it reaches the others. storage holds whatever this server
// saved before it last crashed, if anything, and it's where the server saves
// from now on. Committed commands come out of applyCh in log order. Make
// returns right away; the server's goroutines run until Kill.
//
// A restarted server picks up its term, vote, log and snapshot. Everything
// the snapshot covers is committed, so that's where commitIndex starts. The
// first thing it hands to applyCh is the snapshot, so the application can
// rebuild its state from it, and then the entries after it as the leader
// tells the server they're committed.
func Make(peers Transport, me, n int, storage Storage, applyCh chan<- ApplyMsg) *Raft {
	ctx, cancel := context.WithCancel(context.Background())
	rf := &Raft{
		peers:      peers,
		storage:    storage,
		me:         me,
		n:          n,
		applyCh:    applyCh,
		votedFor:   noVote,
		leaderID:   noLeader,
		log:        []LogEntry{{Term: 0}},
		nextIndex:  make([]int, n),
		matchIndex: make([]int, n),
		applyKick:  make(chan struct{}, 1),
		ctx:        ctx,
		cancel:     cancel,
	}
	state, snapshot := storage.Load()
	if len(state) > 0 {
		// Decode into a fresh value: gob leaves fields that are zero on the
		// wire untouched, so decoding over values that are already set could
		// keep stale ones.
		var saved persistentState
		if err := gob.NewDecoder(bytes.NewReader(state)).Decode(&saved); err != nil {
			panic(fmt.Sprintf("raft: server %d can't read its saved state: %v", me, err))
		}
		rf.currentTerm, rf.votedFor = saved.CurrentTerm, saved.VotedFor
		rf.log, rf.lastIncludedIndex = saved.Log, saved.LastIncludedIndex
	}
	rf.snapshot = snapshot
	rf.commitIndex = rf.lastIncludedIndex
	if rf.lastIncludedIndex > 0 {
		rf.kickApplier()
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
	index = rf.lastIndex()
	rf.matchIndex[rf.me] = index // the leader's own copy counts toward a majority, so it has to be saved first
	return index, rf.currentTerm, true
}

// Snapshot tells the server that the application has captured its state,
// with everything up to index applied, in data. The server keeps data in
// place of the log entries it covers and drops them. The application calls
// this some time after it has applied index; if the server has moved past
// index by then (say the leader sent it a newer snapshot), the call does
// nothing.
func (rf *Raft) Snapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastApplied {
		panic(fmt.Sprintf("raft: server %d was asked to snapshot up to index %d, but it has only applied up to %d", rf.me, index, rf.lastApplied))
	}
	rf.trimLog(index)
	rf.snapshot = slices.Clone(data) // our own copy, since the application might reuse its buffer
	rf.persist()
}

// State returns this server's current term and whether it believes it's the
// leader.
func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// Leader returns the id of the server this one last heard from as the leader
// of its current term, which may be itself, or -1 if it doesn't know. It's a
// hint for clients, not a promise: that server may have lost its job since.
func (rf *Raft) Leader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
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
	rf.leaderID = noLeader
	rf.persist() // before any vote request goes out
	rf.resetElectionTimer()

	term := rf.currentTerm
	last := rf.lastIndex()
	args := &RequestVoteArgs{Term: term, CandidateID: rf.me, LastLogIndex: last, LastLogTerm: rf.termAt(last)}
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
				rf.leaderID = rf.me
				// Start out assuming every follower's log matches ours. When
				// that's wrong, the follower turns down our AppendEntries and
				// we step back until we find where the logs agree.
				for i := range rf.n {
					rf.nextIndex[i] = rf.lastIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = rf.lastIndex()
				go rf.replicate(term)
			}
		}()
	}
}

// replicate runs for as long as this server is the leader of term. Right
// away, and then every heartbeatInterval, it sends every other server what
// it's missing: the entries after the ones it has, or our snapshot if those
// entries have been compacted away. A server that's caught up gets an empty
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
			if prev < rf.lastIncludedIndex {
				args := &InstallSnapshotArgs{
					Term:              term,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.snapshot,
				}
				go rf.sendSnapshot(term, peer, args)
				continue
			}
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.me,
				PrevLogIndex: prev,
				PrevLogTerm:  rf.termAt(prev),
				// A copy, because the network reads these after we let go of
				// the lock, and by then our log may have changed.
				Entries:      slices.Clone(rf.log[rf.pos(prev)+1:]),
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

// sendSnapshot sends our snapshot to peer and acts on the answer.
func (rf *Raft) sendSnapshot(term, peer int, args *InstallSnapshotArgs) {
	// The same wait as sendEntries, for the same reason.
	ctx, cancel := context.WithTimeout(rf.ctx, electionTimeoutMin)
	defer cancel()
	var reply InstallSnapshotReply
	if err := rf.peers.Call(ctx, peer, "Raft.InstallSnapshot", args, &reply); err != nil {
		return // lost; the next round tries again
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.role != leader || rf.currentTerm != term {
		return
	}
	// peer now has everything the snapshot covers: either it just installed
	// it, or it had already committed that far, which means the same thing.
	// Everything a snapshot covers is committed already, so there's nothing
	// new to commit here.
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
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
	for index := rf.lastIndex(); index > rf.commitIndex && rf.termAt(index) == rf.currentTerm; index-- {
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

// applier hands committed work to applyCh, in order. It has its own
// goroutine so that sending on applyCh never happens while holding rf.mu: the
// application reading applyCh may call Start or Snapshot, which need the
// lock, and the two would wait on each other forever.
func (rf *Raft) applier() {
	for {
		select {
		case <-rf.ctx.Done():
			return
		case <-rf.applyKick:
		}

		// Keep going until there's nothing left, since one nudge can stand
		// for several changes.
		for {
			rf.mu.Lock()
			var msgs []ApplyMsg
			if rf.lastApplied < rf.lastIncludedIndex {
				// We've been sent a snapshot past what we've applied, or we're
				// starting up from one. It replaces everything up to its index,
				// so it goes out on its own, before anything after it.
				msgs = append(msgs, ApplyMsg{Index: rf.lastIncludedIndex, IsSnapshot: true, Snapshot: rf.snapshot})
				rf.lastApplied = rf.lastIncludedIndex
			} else {
				for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
					msgs = append(msgs, ApplyMsg{Index: index, Command: rf.log[rf.pos(index)].Command})
				}
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()

			if len(msgs) == 0 {
				break
			}
			for _, msg := range msgs {
				select {
				case rf.applyCh <- msg:
				case <-rf.ctx.Done():
					return
				}
			}
		}
	}
}

// kickApplier wakes the applier without waiting for it. If a nudge is already
// pending, that one is enough: the applier reads everything fresh each time
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
	last := rf.lastIndex()
	upToDate := args.LastLogTerm > rf.termAt(last) ||
		(args.LastLogTerm == rf.termAt(last) && args.LastLogIndex >= last)
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
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()

	// Any part of the message our snapshot already covers can be skipped. A
	// snapshot only ever holds committed entries, and committed entries match
	// the leader's, so we know our log lines up with the leader's at
	// lastIncludedIndex.
	prev, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prev < rf.lastIncludedIndex {
		skip := min(rf.lastIncludedIndex-prev, len(entries))
		prev, prevTerm, entries = rf.lastIncludedIndex, rf.log[0].Term, entries[skip:]
	}

	// Only take the entries if we have the entry they're meant to follow.
	// This check is what keeps every log an exact copy of the leader's, up to
	// the point where they agree.
	if prev > rf.lastIndex() || rf.termAt(prev) != prevTerm {
		return nil
	}

	// Go through the new entries. Ones we already have get skipped. At the
	// first real conflict (same index, different term) we drop that entry and
	// everything after it, then append the rest. We never cut the log just
	// because a message is shorter than our log: it may be an old message
	// that arrived late, and cutting would throw away entries we've already
	// told the leader we have.
	for i, entry := range entries {
		index := prev + 1 + i
		if index <= rf.lastIndex() {
			if rf.termAt(index) == entry.Term {
				continue
			}
			rf.log = rf.log[:rf.pos(index)]
		}
		rf.log = append(rf.log, entries[i:]...)
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

// InstallSnapshot is the handler a leader calls when this server is so far
// behind that the entries it needs have been compacted away. It follows
// "InstallSnapshot RPC" in Figure 13, with the whole snapshot sent in one
// message.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil
	}
	rf.role = follower
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()

	// A snapshot that doesn't reach past what we know is committed tells us
	// nothing new. This is also what stops an old snapshot that arrived late
	// from rolling us back.
	if args.LastIncludedIndex <= rf.commitIndex {
		return nil
	}

	if args.LastIncludedIndex <= rf.lastIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Our log goes past the snapshot and agrees with it where it ends,
		// so the entries after that are still good. Keep them.
		rf.trimLog(args.LastIncludedIndex)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		rf.lastIncludedIndex = args.LastIncludedIndex
	}
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()
	rf.kickApplier() // the applier sees lastApplied is behind the snapshot and hands it over
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
	rf.leaderID = noLeader // whoever led the old term doesn't lead this one
	rf.persist()
}

// persist saves currentTerm, votedFor and the log, together with the
// snapshot. Everything that changes one of them calls this before letting go
// of rf.mu, which is always before the change can reach another server:
// replies go out after a handler returns, and requests go out after the lock
// is released. The caller must hold rf.mu.
func (rf *Raft) persist() {
	var buf bytes.Buffer
	state := persistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
	}
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		panic(fmt.Sprintf("raft: server %d can't save its state: %v", rf.me, err))
	}
	rf.storage.Save(buf.Bytes(), rf.snapshot)
}

// trimLog drops every entry up to index, and index itself becomes the new
// log[0]. What's left gets copied into a new slice. Re-slicing would also
// shorten the log, but the dropped entries would stay in memory until the
// slice next grows, and after a burst of writes that might not be for a long
// time. The caller must hold rf.mu.
func (rf *Raft) trimLog(index int) {
	rest := rf.log[rf.pos(index):]
	rf.log = append([]LogEntry{{Term: rest[0].Term}}, rest[1:]...)
	rf.lastIncludedIndex = index
}

// pos turns a log index into a position in rf.log. Every log lookup goes
// through here, so the snapshot offset lives in exactly one place. The index
// has to be between lastIncludedIndex and lastIndex. The caller must hold
// rf.mu.
func (rf *Raft) pos(index int) int {
	return index - rf.lastIncludedIndex
}

// lastIndex returns the index of the last entry in the log, or
// lastIncludedIndex if nothing comes after the snapshot. The caller must
// hold rf.mu.
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// termAt returns the term of the entry at index. The caller must hold rf.mu.
func (rf *Raft) termAt(index int) int {
	return rf.log[rf.pos(index)].Term
}

// resetElectionTimer moves the election deadline to a new random point in the
// future. The caller must hold rf.mu.
func (rf *Raft) resetElectionTimer() {
	timeout := electionTimeoutMin + rand.N(ElectionTimeoutMax-electionTimeoutMin+1)
	rf.electionDeadline = time.Now().Add(timeout)
}
