package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"kv-store-raft/internal/raft"
)

func TestWAL_AppendLoadOrder(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer wal.Close()

	entries := []raft.LogEntry{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
	}
	if err := wal.AppendEntries(entries); err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	got, err := wal.LoadEntries()
	if err != nil {
		t.Fatalf("load entries: %v", err)
	}
	if len(got) != 2 || got[0].Index != 1 || got[1].Index != 2 {
		t.Fatalf("unexpected entries: %+v", got)
	}
}

func TestWAL_MalformedJSONLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft_log.jsonl")
	if err := os.WriteFile(path, []byte("{\n"), 0o644); err != nil {
		t.Fatalf("write malformed log: %v", err)
	}
	wal, err := NewWAL(dir, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer wal.Close()
	if _, err := wal.LoadEntries(); err == nil {
		t.Fatalf("expected load error for malformed jsonl")
	}
}
