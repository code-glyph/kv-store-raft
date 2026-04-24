package storage

import (
	"testing"
	"time"

	"kv-store-raft/internal/raft"
)

func TestLoadRecoveredState(t *testing.T) {
	dir := t.TempDir()
	meta := NewMetaStore(dir)
	if err := meta.SaveMeta(4, "n2"); err != nil {
		t.Fatalf("save meta: %v", err)
	}
	wal, err := NewWAL(dir, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer wal.Close()
	if err := wal.AppendEntries([]raft.LogEntry{
		{Index: 1, Term: 1, Command: []byte("x")},
		{Index: 2, Term: 2, Command: []byte("y")},
	}); err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatalf("sync wal: %v", err)
	}

	snapshot := NewSnapshotStore(dir)
	if err := snapshot.SaveSnapshot(1, 1, []byte("state")); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	recovered, err := LoadRecoveredState(meta, wal, snapshot)
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}
	if recovered.CurrentTerm != 4 || recovered.VotedFor != "n2" {
		t.Fatalf("unexpected recovered meta: %+v", recovered)
	}
	if len(recovered.LogEntries) != 2 || recovered.LogEntries[1].Term != 2 {
		t.Fatalf("unexpected recovered entries: %+v", recovered.LogEntries)
	}
	if recovered.SnapshotIndex != 1 || recovered.SnapshotTerm != 1 || string(recovered.SnapshotData) != "state" {
		t.Fatalf("unexpected recovered snapshot: %+v", recovered)
	}
}
