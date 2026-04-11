package raft

import (
	"slices"
	"sync"
)

// Storage is where a server keeps the state that has to survive a crash.
// Save replaces whatever was saved before, and Load returns the last thing
// saved, or nothing if Save was never called.
type Storage interface {
	Save(state []byte)
	Load() []byte
}

// MemStorage is a Storage that keeps everything in memory. Tests use it in
// place of a disk: it outlives the server that writes to it, so a restarted
// server can pick up where the crashed one left off.
type MemStorage struct {
	mu    sync.Mutex
	state []byte
}

// Save keeps a copy of state, replacing what was there.
func (s *MemStorage) Save(state []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = slices.Clone(state)
}

// Load returns a copy of what was saved last.
func (s *MemStorage) Load() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.state)
}
