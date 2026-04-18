package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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

// FileStorage is a Storage that keeps everything in one file on disk, for
// servers running as real processes.
//
// Each Save writes the new contents to a temporary file, forces it to the
// disk, and then renames it over the old file. A rename replaces the file in
// one step, so a crash in the middle of a Save leaves the previous contents
// whole. Save only returns once the data is on the disk, because Raft is
// about to tell another server it has saved it.
//
// Storage has no way to report an error, and a server that can't save its
// state mustn't carry on as if it had, so Save and Load panic when the disk
// fails them.
type FileStorage struct {
	mu   sync.Mutex
	path string
}

// fileContents is what a FileStorage writes.
type fileContents struct {
	State    []byte
	Snapshot []byte
}

// NewFileStorage returns a FileStorage that keeps its file at path. The
// directory has to exist already.
func NewFileStorage(path string) *FileStorage {
	return &FileStorage{path: path}
}

// Save writes state and snapshot to the disk, replacing what was there.
func (s *FileStorage) Save(state, snapshot []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(fileContents{State: state, Snapshot: snapshot}); err != nil {
		panic(fmt.Sprintf("raft: can't encode saved state: %v", err))
	}
	tmp := s.path + ".tmp"
	if err := writeAndSync(tmp, buf.Bytes()); err != nil {
		panic(fmt.Sprintf("raft: can't save state: %v", err))
	}
	if err := os.Rename(tmp, s.path); err != nil {
		panic(fmt.Sprintf("raft: can't save state: %v", err))
	}
	// The rename itself lives in the directory, so that has to reach the
	// disk too.
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		panic(fmt.Sprintf("raft: can't save state: %v", err))
	}
}

// Load returns what was saved last, or nothing if the file doesn't exist yet.
func (s *FileStorage) Load() (state, snapshot []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		panic(fmt.Sprintf("raft: can't read saved state: %v", err))
	}
	var c fileContents
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&c); err != nil {
		panic(fmt.Sprintf("raft: saved state in %s is unreadable: %v", s.path, err))
	}
	return c.State, c.Snapshot
}

func writeAndSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
