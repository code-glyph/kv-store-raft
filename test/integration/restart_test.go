package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"kv-store-raft/internal/kv"
	"kv-store-raft/internal/raft"
	"kv-store-raft/internal/storage"
)

type noopTransport struct{}

func (noopTransport) RequestVote(context.Context, string, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, errors.New("not used")
}
func (noopTransport) AppendEntries(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, errors.New("not used")
}

func TestRestart_RestoreTermVoteAndLog(t *testing.T) {
	dir := t.TempDir()

	// First boot: elect leader in single-node cluster and append one entry.
	store1 := kv.NewStore()
	wal1, err := storage.NewWAL(dir, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	node1, err := raft.NewNode(raft.NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "127.0.0.1:0"},
		ElectionTimeout:  120 * time.Millisecond,
		HeartbeatTimeout: 40 * time.Millisecond,
		Transport:        noopTransport{},
		StateMachine:     store1,
		MetaStore:        storage.NewMetaStore(dir),
		LogStore:         wal1,
	})
	if err != nil {
		t.Fatalf("new node1: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	node1.Run(ctx1)
	time.Sleep(300 * time.Millisecond)
	if _, err := node1.Submit(context.Background(), []byte(`{"op":"set","key":"k","value":"v"}`)); err != nil {
		t.Fatalf("submit on node1: %v", err)
	}
	cancel1()
	if err := node1.Close(); err != nil {
		t.Fatalf("close node1: %v", err)
	}

	// Second boot: verify persisted term/vote/log are restored.
	store2 := kv.NewStore()
	wal2, err := storage.NewWAL(dir, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("new wal2: %v", err)
	}
	node2, err := raft.NewNode(raft.NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "127.0.0.1:0"},
		ElectionTimeout:  120 * time.Millisecond,
		HeartbeatTimeout: 40 * time.Millisecond,
		Transport:        noopTransport{},
		StateMachine:     store2,
		MetaStore:        storage.NewMetaStore(dir),
		LogStore:         wal2,
	})
	if err != nil {
		t.Fatalf("new node2: %v", err)
	}
	defer node2.Close()

	_, term, votedFor, _ := node2.State()
	if term == 0 {
		t.Fatalf("expected restored term > 0")
	}
	if votedFor == "" {
		t.Fatalf("expected restored votedFor")
	}
	if node2.LogLength() == 0 {
		t.Fatalf("expected restored log entries")
	}
}
