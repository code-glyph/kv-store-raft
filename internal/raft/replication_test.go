package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

type recordingStateMachine struct {
	mu      sync.Mutex
	applied []LogEntry
}

func (r *recordingStateMachine) Apply(entry LogEntry) (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, entry)
	return fmt.Sprintf("applied-%d", entry.Index), nil
}

func (r *recordingStateMachine) Snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, _ := json.Marshal(r.applied)
	return raw
}

func (r *recordingStateMachine) ApplySnapshot(raw []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(raw) == 0 {
		r.applied = nil
		return nil
	}
	var entries []LogEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return err
	}
	r.applied = entries
	return nil
}

func (r *recordingStateMachine) entries() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]LogEntry, len(r.applied))
	copy(out, r.applied)
	return out
}

func TestSubmit_ReplicatesToQuorumAndApplies(t *testing.T) {
	cluster := buildClusterWithControl(t, 3, 80*time.Millisecond, 25*time.Millisecond)
	for _, node := range cluster.nodes {
		node.stateMachine = &recordingStateMachine{}
	}

	leaderID := assertSingleLeaderEventually(t, cluster.nodes, 900*time.Millisecond)
	leader := cluster.nodeByID[leaderID]
	t.Logf("leader selected for submit test: %s", leaderID)

	index, err := leader.Submit(context.Background(), []byte(`{"op":"set","key":"x","value":"1"}`))
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if index == 0 {
		t.Fatalf("expected non-zero log index")
	}

	// Give followers time to apply via leaderCommit propagation.
	time.Sleep(150 * time.Millisecond)

	for _, node := range cluster.nodes {
		node.mu.Lock()
		if node.commitIndex < index {
			node.mu.Unlock()
			t.Fatalf("node=%s commitIndex=%d want>=%d", node.nodeID, node.commitIndex, index)
		}
		if len(node.logEntries) < int(index) {
			node.mu.Unlock()
			t.Fatalf("node=%s logLen=%d want>=%d", node.nodeID, len(node.logEntries), index)
		}
		if node.lastApplied < index {
			node.mu.Unlock()
			t.Fatalf("node=%s lastApplied=%d want>=%d", node.nodeID, node.lastApplied, index)
		}
		node.mu.Unlock()
	}
}

func TestSubmit_RequiresLeader(t *testing.T) {
	cluster := buildClusterWithControl(t, 3, 80*time.Millisecond, 25*time.Millisecond)
	leaderID := assertSingleLeaderEventually(t, cluster.nodes, 900*time.Millisecond)

	var follower *Node
	for _, node := range cluster.nodes {
		if node.nodeID != leaderID {
			follower = node
			break
		}
	}
	if follower == nil {
		t.Fatalf("expected a follower node")
	}

	_, err := follower.Submit(context.Background(), []byte(`{"op":"set","key":"k","value":"v"}`))
	if err == nil {
		t.Fatalf("expected not leader error")
	}
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestHandleAppendEntries_ConflictRepair(t *testing.T) {
	mt := newMockTransport()
	node := newTestNode(t, "n1", map[string]string{}, mt)

	node.mu.Lock()
	node.currentTerm = 2
	node.logEntries = []LogEntry{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
		{Index: 3, Term: 3, Command: []byte("conflict")},
	}
	node.mu.Unlock()

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 3, Term: 2, Command: []byte("c")},
			{Index: 4, Term: 2, Command: []byte("d")},
		},
		LeaderCommit: 4,
	})
	if !resp.Success {
		t.Fatalf("expected append entries success, got failure")
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	if len(node.logEntries) != 4 {
		t.Fatalf("expected log len 4, got %d", len(node.logEntries))
	}
	if string(node.logEntries[2].Command) != "c" {
		t.Fatalf("expected index 3 repaired to c, got %q", string(node.logEntries[2].Command))
	}
	if string(node.logEntries[3].Command) != "d" {
		t.Fatalf("expected index 4 appended d, got %q", string(node.logEntries[3].Command))
	}
	if node.commitIndex != 4 {
		t.Fatalf("expected commitIndex=4, got %d", node.commitIndex)
	}
}

func TestSubmit_FailsWithoutMajority(t *testing.T) {
	cluster := buildClusterWithControl(t, 3, 80*time.Millisecond, 25*time.Millisecond)
	leaderID := assertSingleLeaderEventually(t, cluster.nodes, 900*time.Millisecond)
	leader := cluster.nodeByID[leaderID]

	// Leave only leader alive; majority cannot be reached.
	for _, node := range cluster.nodes {
		if node.nodeID != leaderID {
			cluster.stopNode(node.nodeID)
		}
	}

	_, err := leader.Submit(context.Background(), []byte(`{"op":"set","key":"z","value":"9"}`))
	if err == nil {
		t.Fatalf("expected submit timeout error when majority unavailable")
	}
}
