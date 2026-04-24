package storage

import (
	"path/filepath"
	"testing"
)

func TestMetaStore_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := NewMetaStore(dir)
	if err := m.SaveMeta(7, "n2"); err != nil {
		t.Fatalf("save meta: %v", err)
	}
	term, votedFor, err := m.LoadMeta()
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if term != 7 || votedFor != "n2" {
		t.Fatalf("unexpected meta term=%d votedFor=%s", term, votedFor)
	}

	// Verify file path is under data dir.
	if got := filepath.Dir(m.path); got != dir {
		t.Fatalf("meta file outside dir: %s", got)
	}
}
