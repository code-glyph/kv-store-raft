package storage

import "testing"

func TestSnapshotStore_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	store := NewSnapshotStore(dir)

	if err := store.SaveSnapshot(12, 3, []byte("payload")); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	idx, term, data, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if idx != 12 || term != 3 {
		t.Fatalf("unexpected snapshot metadata idx=%d term=%d", idx, term)
	}
	if string(data) != "payload" {
		t.Fatalf("unexpected snapshot payload %q", string(data))
	}
}
