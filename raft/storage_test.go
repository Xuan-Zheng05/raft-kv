package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorageStartsEmpty(t *testing.T) {
	t.Parallel()
	state, snapshot := NewFileStorage(filepath.Join(t.TempDir(), "raft")).Load()
	if state != nil || snapshot != nil {
		t.Fatalf("got (%q, %q) from a file that was never saved, want nothing", state, snapshot)
	}
}

// What one FileStorage saves, a new one on the same file reads back, which
// is what a restarted process does. Saving again replaces it.
func TestFileStorageKeepsTheLastSave(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "raft")
	NewFileStorage(path).Save([]byte("state 1"), []byte("snapshot 1"))
	NewFileStorage(path).Save([]byte("state 2"), []byte("snapshot 2"))

	state, snapshot := NewFileStorage(path).Load()
	if !bytes.Equal(state, []byte("state 2")) || !bytes.Equal(snapshot, []byte("snapshot 2")) {
		t.Fatalf("got (%q, %q), want (%q, %q)", state, snapshot, "state 2", "snapshot 2")
	}
}

// A crash in the middle of a Save can leave a half-written temporary file
// behind. The last finished Save must still be what comes back.
func TestFileStorageIgnoresAnInterruptedSave(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "raft")
	NewFileStorage(path).Save([]byte("state"), []byte("snapshot"))
	if err := os.WriteFile(path+".tmp", []byte("half a file"), 0o644); err != nil {
		t.Fatal(err)
	}

	state, snapshot := NewFileStorage(path).Load()
	if !bytes.Equal(state, []byte("state")) || !bytes.Equal(snapshot, []byte("snapshot")) {
		t.Fatalf("got (%q, %q), want (%q, %q)", state, snapshot, "state", "snapshot")
	}
}
