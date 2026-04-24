package raft

import (
	"encoding/json"
	"io"
	"log"
	"testing"
	"time"
)

type memorySnapshotStore struct {
	index uint64
	term  uint64
	data  []byte
}

func (m *memorySnapshotStore) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	m.index = lastIncludedIndex
	m.term = lastIncludedTerm
	m.data = append([]byte(nil), data...)
	return nil
}

func (m *memorySnapshotStore) LoadSnapshot() (uint64, uint64, []byte, error) {
	return m.index, m.term, append([]byte(nil), m.data...), nil
}

type memoryLogStore struct {
	entries []LogEntry
}

func (m *memoryLogStore) AppendEntries(entries []LogEntry) error {
	m.entries = append(m.entries, entries...)
	return nil
}

func (m *memoryLogStore) LoadEntries() ([]LogEntry, error) {
	out := make([]LogEntry, len(m.entries))
	copy(out, m.entries)
	return out, nil
}

func (m *memoryLogStore) RewriteEntries(entries []LogEntry) error {
	m.entries = make([]LogEntry, len(entries))
	copy(m.entries, entries)
	return nil
}

func (m *memoryLogStore) Sync() error  { return nil }
func (m *memoryLogStore) Close() error { return nil }

type snapshotCountingStateMachine struct {
	Count int `json:"count"`
}

func (s *snapshotCountingStateMachine) Apply(entry LogEntry) (interface{}, error) {
	s.Count++
	return s.Count, nil
}

func (s *snapshotCountingStateMachine) Snapshot() []byte {
	raw, _ := json.Marshal(s)
	return raw
}

func (s *snapshotCountingStateMachine) ApplySnapshot(raw []byte) error {
	if len(raw) == 0 {
		s.Count = 0
		return nil
	}
	return json.Unmarshal(raw, s)
}

func TestSnapshottingAndRestoreFromSnapshotIndex(t *testing.T) {
	mt := newMockTransport()
	meta := &noopMetaStore{}
	logStore := &memoryLogStore{
		entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
		},
	}
	snapStore := &memorySnapshotStore{}
	sm := &snapshotCountingStateMachine{}

	node, err := NewNode(NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "inproc-1"},
		ElectionTimeout:  100 * time.Millisecond,
		HeartbeatTimeout: 30 * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		MaxBatchSize:     64,
		SnapshotInterval: 2,
		Transport:        mt,
		StateMachine:     sm,
		MetaStore:        meta,
		LogStore:         logStore,
		SnapshotStore:    snapStore,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	node.mu.Lock()
	node.commitIndex = 2
	if err := node.applyCommittedLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply committed: %v", err)
	}
	node.mu.Unlock()

	if snapStore.index != 2 || snapStore.term != 1 {
		t.Fatalf("unexpected snapshot metadata index=%d term=%d", snapStore.index, snapStore.term)
	}
	if len(logStore.entries) != 0 {
		t.Fatalf("expected wal truncated after snapshot, got entries=%d", len(logStore.entries))
	}

	restoredSM := &snapshotCountingStateMachine{}
	restored, err := NewNode(NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "inproc-1"},
		ElectionTimeout:  100 * time.Millisecond,
		HeartbeatTimeout: 30 * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		MaxBatchSize:     64,
		SnapshotInterval: 2,
		Transport:        mt,
		StateMachine:     restoredSM,
		MetaStore:        meta,
		LogStore:         logStore,
		SnapshotStore:    snapStore,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new restored node: %v", err)
	}

	restored.mu.Lock()
	defer restored.mu.Unlock()
	if restored.commitIndex != 2 || restored.lastApplied != 2 {
		t.Fatalf("expected commit/lastApplied restored to snapshot index, got commit=%d lastApplied=%d", restored.commitIndex, restored.lastApplied)
	}
	if restoredSM.Count != 2 {
		t.Fatalf("expected state machine restored from snapshot count=2, got %d", restoredSM.Count)
	}
}

type noopMetaStore struct{}

func (noopMetaStore) SaveMeta(term uint64, votedFor string) error { return nil }
func (noopMetaStore) LoadMeta() (uint64, string, error)           { return 0, "", nil }
