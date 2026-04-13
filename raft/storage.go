package raft

import (
	"slices"
	"sync"
)

// Storage is where a server keeps what has to survive a crash: its Raft
// state and its latest snapshot. Save replaces both together, so a crash can
// never leave a trimmed log without the snapshot that replaced its entries.
// Load returns what was saved last, or nothing if Save was never called.
type Storage interface {
	Save(state, snapshot []byte)
	Load() (state, snapshot []byte)
}

// MemStorage is a Storage that keeps everything in memory. Tests use it in
// place of a disk: it outlives the server that writes to it, so a restarted
// server can pick up where the crashed one left off.
type MemStorage struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

// Save keeps a copy of state and snapshot, replacing what was there.
func (s *MemStorage) Save(state, snapshot []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state, s.snapshot = slices.Clone(state), slices.Clone(snapshot)
}

// Load returns a copy of what was saved last.
func (s *MemStorage) Load() (state, snapshot []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.state), slices.Clone(s.snapshot)
}
