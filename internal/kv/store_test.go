package kv

import (
	"errors"
	"testing"

	"kv-store-raft/internal/raft"
)

func mustEntry(t *testing.T, index uint64, term uint64, cmd Command) raft.LogEntry {
	t.Helper()
	t.Logf("building log entry index=%d term=%d op=%s key=%s value=%s", index, term, cmd.Op, cmd.Key, cmd.Value)
	raw, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	return raft.LogEntry{
		Index:   index,
		Term:    term,
		Command: raw,
	}
}

func TestStoreApplyAndGet_CommandSequenceDeterministic(t *testing.T) {
	store := NewStore()
	t.Logf("created new in-memory store for deterministic sequence test")

	entries := []raft.LogEntry{
		mustEntry(t, 1, 1, Command{Op: OpSet, Key: "a", Value: "1"}),
		mustEntry(t, 2, 1, Command{Op: OpSet, Key: "a", Value: "2"}),
		mustEntry(t, 3, 1, Command{Op: OpDelete, Key: "a"}),
		mustEntry(t, 4, 1, Command{Op: OpSet, Key: "b", Value: "x"}),
	}

	for _, entry := range entries {
		resultAny, err := store.Apply(entry)
		if err != nil {
			t.Fatalf("apply entry %d failed: %v", entry.Index, err)
		}
		result := resultAny.(ApplyResult)
		t.Logf(
			"applied entry index=%d term=%d op=%s key=%s value=%s existed=%v",
			entry.Index, entry.Term, result.Op, result.Key, result.Value, result.Existed,
		)
	}

	if _, err := store.Get("a"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for key a, got %v", err)
	}
	t.Logf("verified key=a is missing after delete")

	value, err := store.Get("b")
	if err != nil {
		t.Fatalf("get key b: %v", err)
	}
	t.Logf("final read key=b value=%s", value)
	if value != "x" {
		t.Fatalf("expected b=x, got %q", value)
	}
}

func TestStoreApply_OverwriteSemantics(t *testing.T) {
	store := NewStore()
	t.Logf("created new in-memory store for overwrite semantics test")

	tests := []struct {
		name            string
		entry           raft.LogEntry
		wantExisted     bool
		wantStoredValue string
	}{
		{
			name:            "first write creates key",
			entry:           mustEntry(t, 1, 1, Command{Op: OpSet, Key: "k", Value: "v1"}),
			wantExisted:     false,
			wantStoredValue: "v1",
		},
		{
			name:            "second write overwrites key",
			entry:           mustEntry(t, 2, 1, Command{Op: OpSet, Key: "k", Value: "v2"}),
			wantExisted:     true,
			wantStoredValue: "v2",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("running case=%s wantExisted=%v wantStoredValue=%s", tc.name, tc.wantExisted, tc.wantStoredValue)
			resultAny, err := store.Apply(tc.entry)
			if err != nil {
				t.Fatalf("apply failed: %v", err)
			}

			result, ok := resultAny.(ApplyResult)
			if !ok {
				t.Fatalf("unexpected result type %T", resultAny)
			}
			if result.Existed != tc.wantExisted {
				t.Fatalf("expected existed=%v, got %v", tc.wantExisted, result.Existed)
			}
			t.Logf(
				"overwrite test op=%s key=%s value=%s existed=%v",
				result.Op, result.Key, result.Value, result.Existed,
			)

			got, err := store.Get("k")
			if err != nil {
				t.Fatalf("get key k: %v", err)
			}
			t.Logf("post-apply get key=k value=%s", got)
			if got != tc.wantStoredValue {
				t.Fatalf("expected value %q, got %q", tc.wantStoredValue, got)
			}
		})
	}
}

func TestStoreApply_DeleteSemantics(t *testing.T) {
	store := NewStore()
	t.Logf("created new in-memory store for delete semantics test")

	if _, err := store.Apply(mustEntry(t, 1, 1, Command{Op: OpSet, Key: "k", Value: "v1"})); err != nil {
		t.Fatalf("seed set failed: %v", err)
	}

	// First delete should report Existed=true.
	resultAny, err := store.Apply(mustEntry(t, 2, 1, Command{Op: OpDelete, Key: "k"}))
	if err != nil {
		t.Fatalf("first delete failed: %v", err)
	}
	result := resultAny.(ApplyResult)
	t.Logf("first delete key=%s existed=%v old_value=%s", result.Key, result.Existed, result.Value)
	if !result.Existed {
		t.Fatalf("expected existed=true on first delete")
	}
	if _, err := store.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected key missing after delete, got %v", err)
	}
	t.Logf("verified key=k is missing after first delete")

	// Second delete is idempotent and should report Existed=false.
	resultAny, err = store.Apply(mustEntry(t, 3, 1, Command{Op: OpDelete, Key: "k"}))
	if err != nil {
		t.Fatalf("second delete failed: %v", err)
	}
	result = resultAny.(ApplyResult)
	t.Logf("second delete key=%s existed=%v old_value=%s", result.Key, result.Existed, result.Value)
	if result.Existed {
		t.Fatalf("expected existed=false on second delete")
	}
}

func TestStoreApply_InvalidCommandPayload(t *testing.T) {
	store := NewStore()
	t.Logf("created new in-memory store for invalid payload tests")

	tests := []struct {
		name    string
		entry   raft.LogEntry
		wantErr error
	}{
		{
			name: "empty payload",
			entry: raft.LogEntry{
				Index:   1,
				Term:    1,
				Command: nil,
			},
			wantErr: ErrInvalidCommandData,
		},
		{
			name: "invalid json",
			entry: raft.LogEntry{
				Index:   2,
				Term:    1,
				Command: []byte("{"),
			},
			wantErr: ErrInvalidCommandData,
		},
		{
			name: "unsupported operation",
			entry: raft.LogEntry{
				Index:   3,
				Term:    1,
				Command: []byte(`{"op":"increment","key":"k"}`),
			},
			wantErr: ErrUnsupportedOperation,
		},
		{
			name: "empty key",
			entry: raft.LogEntry{
				Index:   4,
				Term:    1,
				Command: []byte(`{"op":"set","key":"","value":"v"}`),
			},
			wantErr: ErrEmptyKey,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("applying invalid payload case=%s index=%d term=%d", tc.name, tc.entry.Index, tc.entry.Term)
			_, err := store.Apply(tc.entry)
			if err == nil {
				t.Fatalf("expected error but got nil")
			}
			t.Logf("invalid payload case=%s error=%v", tc.name, err)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestStoreSnapshotRoundTrip(t *testing.T) {
	store := NewStore()
	_, _ = store.Apply(mustEntry(t, 1, 1, Command{Op: OpSet, Key: "a", Value: "1"}))
	_, _ = store.Apply(mustEntry(t, 2, 1, Command{Op: OpSet, Key: "b", Value: "2"}))

	raw := store.Snapshot()
	if len(raw) == 0 {
		t.Fatalf("expected non-empty snapshot payload")
	}

	restored := NewStore()
	if err := restored.ApplySnapshot(raw); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	va, err := restored.Get("a")
	if err != nil || va != "1" {
		t.Fatalf("expected a=1 after restore, got value=%q err=%v", va, err)
	}
	vb, err := restored.Get("b")
	if err != nil || vb != "2" {
		t.Fatalf("expected b=2 after restore, got value=%q err=%v", vb, err)
	}
}
