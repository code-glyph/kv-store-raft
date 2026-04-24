package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"
)

type mockTransport struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

func newMockTransport() *mockTransport {
	return &mockTransport{nodes: make(map[string]*Node)}
}

func (m *mockTransport) register(nodeID string, node *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[nodeID] = node
}

func (m *mockTransport) unregister(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

func (m *mockTransport) RequestVote(_ context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	m.mu.RLock()
	node := m.nodes[peerID]
	m.mu.RUnlock()
	if node == nil {
		return RequestVoteResponse{}, errors.New("peer unavailable")
	}
	return node.HandleRequestVote(req), nil
}

func (m *mockTransport) AppendEntries(_ context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	m.mu.RLock()
	node := m.nodes[peerID]
	m.mu.RUnlock()
	if node == nil {
		return AppendEntriesResponse{}, errors.New("peer unavailable")
	}
	return node.HandleAppendEntries(req), nil
}

func newTestNode(t *testing.T, id string, peers map[string]string, mt *mockTransport) *Node {
	t.Helper()
	n, err := NewNode(NodeConfig{
		NodeID:           id,
		Peers:            peers,
		ElectionTimeout:  80 * time.Millisecond,
		HeartbeatTimeout: 25 * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		MaxBatchSize:     64,
		Transport:        mt,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	mt.register(id, n)
	return n
}

func TestHandleRequestVote_GrantsAndRejects(t *testing.T) {
	mt := newMockTransport()
	node := newTestNode(t, "n1", map[string]string{}, mt)

	t.Logf("initial node state: %s", node.String())
	resp := node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "n2"})
	t.Logf("request vote term=1 candidate=n2 -> granted=%v term=%d", resp.VoteGranted, resp.Term)
	if !resp.VoteGranted {
		t.Fatalf("expected vote granted")
	}
	if resp.Term != 1 {
		t.Fatalf("expected term 1, got %d", resp.Term)
	}

	resp = node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "n3"})
	t.Logf("request vote term=1 candidate=n3 -> granted=%v term=%d", resp.VoteGranted, resp.Term)
	if resp.VoteGranted {
		t.Fatalf("expected vote denied after already voting in term")
	}
}

func TestHandleAppendEntries_StepsDownOnHigherTerm(t *testing.T) {
	mt := newMockTransport()
	node := newTestNode(t, "n1", map[string]string{}, mt)
	node.state = RoleCandidate
	node.currentTerm = 2
	node.votedFor = "n1"

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:     3,
		LeaderID: "n2",
	})
	t.Logf("append entries heartbeat term=3 leader=n2 -> success=%v term=%d", resp.Success, resp.Term)
	if !resp.Success {
		t.Fatalf("expected heartbeat accepted")
	}

	role, term, votedFor, leaderID := node.State()
	if role != RoleFollower {
		t.Fatalf("expected follower, got %s", role.String())
	}
	if term != 3 {
		t.Fatalf("expected term 3, got %d", term)
	}
	if votedFor != "" {
		t.Fatalf("expected votedFor reset, got %q", votedFor)
	}
	if leaderID != "n2" {
		t.Fatalf("expected leader n2, got %q", leaderID)
	}
	t.Logf("post-heartbeat state: role=%s term=%d votedFor=%q leader=%q", role.String(), term, votedFor, leaderID)
}

func TestElection_LeaderIsChosen(t *testing.T) {
	nodes := buildCluster(t, 3, 80*time.Millisecond, 25*time.Millisecond)
	leaderID := assertSingleLeaderEventually(t, nodes, 700*time.Millisecond)
	t.Logf("3-node cluster elected leader=%s", leaderID)
}

func TestElection_LeaderIsChosenWithMoreNodes(t *testing.T) {
	tests := []struct {
		name      string
		nodeCount int
		waitFor   time.Duration
	}{
		{
			name:      "five node cluster",
			nodeCount: 5,
			waitFor:   900 * time.Millisecond,
		},
		{
			name:      "seven node cluster",
			nodeCount: 7,
			waitFor:   1200 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			nodes := buildCluster(t, tc.nodeCount, 80*time.Millisecond, 25*time.Millisecond)
			leaderID := assertSingleLeaderEventually(t, nodes, tc.waitFor)
			t.Logf("%d-node cluster elected leader=%s", tc.nodeCount, leaderID)
		})
	}
}

func TestElection_FailoverWithFiveNodes(t *testing.T) {
	cluster := buildClusterWithControl(t, 5, 80*time.Millisecond, 25*time.Millisecond)
	initialLeaderID := assertSingleLeaderEventually(t, cluster.nodes, 900*time.Millisecond)
	t.Logf("initial 5-node leader=%s", initialLeaderID)
	cluster.stopNode(initialLeaderID)
	t.Logf("stopped leader node=%s; waiting for re-election", initialLeaderID)

	newLeaderID := assertSingleLeaderEventually(t, cluster.nodesExcluding(initialLeaderID), 1200*time.Millisecond)
	t.Logf("new leader after failover=%s", newLeaderID)
	if newLeaderID == initialLeaderID {
		t.Fatalf("expected a new leader after failover, got same leader %s", newLeaderID)
	}
}

func TestRandomizedElectionTimeout_Range(t *testing.T) {
	base := 100 * time.Millisecond
	mt := newMockTransport()
	node := newTestNode(t, "n1", map[string]string{}, mt)

	for i := 0; i < 100; i++ {
		d := randomizedElectionTimeout(base, node.rnd)
		if i < 5 {
			t.Logf("sample timeout[%d]=%s", i, d)
		}
		if d < base {
			t.Fatalf("timeout %s less than base %s", d, base)
		}
		if d >= 2*base {
			t.Fatalf("timeout %s must be less than %s", d, 2*base)
		}
	}
}

func TestNewNode_SubmitTimeoutDefaultsToTwoSeconds(t *testing.T) {
	mt := newMockTransport()
	node, err := NewNode(NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "inproc-1"},
		ElectionTimeout:  80 * time.Millisecond,
		HeartbeatTimeout: 25 * time.Millisecond,
		MaxBatchSize:     64,
		Transport:        mt,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if node.submitTimeout != 2*time.Second {
		t.Fatalf("expected default submit timeout %s, got %s", 2*time.Second, node.submitTimeout)
	}
}

func TestNewNode_SubmitTimeoutRejectsNegative(t *testing.T) {
	mt := newMockTransport()
	_, err := NewNode(NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "inproc-1"},
		ElectionTimeout:  80 * time.Millisecond,
		HeartbeatTimeout: 25 * time.Millisecond,
		SubmitTimeout:    -1 * time.Millisecond,
		MaxBatchSize:     64,
		Transport:        mt,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err == nil {
		t.Fatalf("expected error for negative submit timeout")
	}
}

func TestNewNode_MaxBatchSizeDefaultsToSixtyFour(t *testing.T) {
	mt := newMockTransport()
	node, err := NewNode(NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "inproc-1"},
		ElectionTimeout:  80 * time.Millisecond,
		HeartbeatTimeout: 25 * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		Transport:        mt,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if node.maxBatchSize != 64 {
		t.Fatalf("expected default max batch size 64, got %d", node.maxBatchSize)
	}
}

func TestNewNode_MaxBatchSizeRejectsNegative(t *testing.T) {
	mt := newMockTransport()
	_, err := NewNode(NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "inproc-1"},
		ElectionTimeout:  80 * time.Millisecond,
		HeartbeatTimeout: 25 * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		MaxBatchSize:     -1,
		Transport:        mt,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err == nil {
		t.Fatalf("expected error for negative max batch size")
	}
}

func TestLeaderLeaseValidityWindow(t *testing.T) {
	mt := newMockTransport()
	node, err := NewNode(NodeConfig{
		NodeID:           "n1",
		Peers:            map[string]string{"n1": "inproc-1"},
		ElectionTimeout:  80 * time.Millisecond,
		HeartbeatTimeout: 40 * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		MaxBatchSize:     64,
		LeaseReads:       true,
		Transport:        mt,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	node.mu.Lock()
	node.state = RoleLeader
	node.leaseRenewedAt = time.Now().Add(-20 * time.Millisecond)
	node.mu.Unlock()
	if !node.CanServeReadLocally() {
		t.Fatalf("expected fresh lease to allow local read")
	}

	node.mu.Lock()
	node.leaseRenewedAt = time.Now().Add(-100 * time.Millisecond)
	node.mu.Unlock()
	if node.CanServeReadLocally() {
		t.Fatalf("expected stale lease to disallow local read")
	}
}

func TestConfirmReadQuorumRenewsLease(t *testing.T) {
	mt := newMockTransport()
	peers := map[string]string{
		"n1": "inproc-1",
		"n2": "inproc-2",
		"n3": "inproc-3",
	}
	newNode := func(id string) *Node {
		n, err := NewNode(NodeConfig{
			NodeID:           id,
			Peers:            peers,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 40 * time.Millisecond,
			SubmitTimeout:    2 * time.Second,
			MaxBatchSize:     64,
			LeaseReads:       id == "n1",
			Transport:        mt,
			Logger:           log.New(io.Discard, "", 0),
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		mt.register(id, n)
		return n
	}
	leader := newNode("n1")
	_ = newNode("n2")
	_ = newNode("n3")

	leader.mu.Lock()
	leader.state = RoleLeader
	leader.currentTerm = 1
	leader.leaderID = "n1"
	leader.leaseRenewedAt = time.Time{}
	leader.mu.Unlock()

	if err := leader.ConfirmReadQuorum(context.Background()); err != nil {
		t.Fatalf("confirm read quorum failed: %v", err)
	}
	if !leader.CanServeReadLocally() {
		t.Fatalf("expected lease renewal after quorum confirmation")
	}
}

func buildCluster(t *testing.T, n int, electionTimeout time.Duration, heartbeatTimeout time.Duration) []*Node {
	cluster := buildClusterWithControl(t, n, electionTimeout, heartbeatTimeout)
	return cluster.nodes
}

type testCluster struct {
	nodes      []*Node
	nodeByID   map[string]*Node
	cancelByID map[string]context.CancelFunc
	transport  *mockTransport
}

func buildClusterWithControl(t *testing.T, n int, electionTimeout time.Duration, heartbeatTimeout time.Duration) *testCluster {
	t.Helper()
	mt := newMockTransport()
	peers := make(map[string]string, n)
	for i := 1; i <= n; i++ {
		nodeID := fmt.Sprintf("n%d", i)
		peers[nodeID] = fmt.Sprintf("inproc-%d", i)
	}
	t.Logf("building cluster size=%d electionTimeout=%s heartbeatTimeout=%s", n, electionTimeout, heartbeatTimeout)

	cluster := &testCluster{
		nodes:      make([]*Node, 0, n),
		nodeByID:   make(map[string]*Node, n),
		cancelByID: make(map[string]context.CancelFunc, n),
		transport:  mt,
	}
	for i := 1; i <= n; i++ {
		nodeID := fmt.Sprintf("n%d", i)
		node, err := NewNode(NodeConfig{
			NodeID:           nodeID,
			Peers:            peers,
			ElectionTimeout:  electionTimeout,
			HeartbeatTimeout: heartbeatTimeout,
			SubmitTimeout:    2 * time.Second,
			MaxBatchSize:     64,
			Transport:        mt,
			Logger:           log.New(io.Discard, "", 0),
		})
		if err != nil {
			t.Fatalf("new node %s: %v", nodeID, err)
		}
		mt.register(nodeID, node)
		cluster.nodes = append(cluster.nodes, node)
		cluster.nodeByID[nodeID] = node
		t.Logf("registered node=%s with %d peers", nodeID, len(peers)-1)
	}

	for i := 1; i <= n; i++ {
		nodeID := fmt.Sprintf("n%d", i)
		node := cluster.nodeByID[nodeID]
		ctx, cancel := context.WithCancel(context.Background())
		cluster.cancelByID[nodeID] = cancel
		t.Cleanup(cancel)
		node.Run(ctx)
		t.Logf("started node=%s run loops", nodeID)
	}

	return cluster
}

func (c *testCluster) stopNode(nodeID string) {
	if cancel, ok := c.cancelByID[nodeID]; ok {
		cancel()
	}
	c.transport.unregister(nodeID)
}

func (c *testCluster) nodesExcluding(nodeID string) []*Node {
	out := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		if n.nodeID != nodeID {
			out = append(out, n)
		}
	}
	return out
}

func assertSingleLeaderEventually(t *testing.T, nodes []*Node, maxWait time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(maxWait)
	lastLeaderCount := -1
	for time.Now().Before(deadline) {
		leaderCount := 0
		leaderID := ""
		for _, node := range nodes {
			role, _, _, _ := node.State()
			if role == RoleLeader {
				leaderCount++
				leaderID = node.nodeID
			}
		}
		if leaderCount != lastLeaderCount {
			t.Logf("leader observation: leaders=%d candidate_leader=%s", leaderCount, leaderID)
			lastLeaderCount = leaderCount
		}
		if leaderCount == 1 {
			t.Logf("stable leader observed: leader=%s within=%s", leaderID, maxWait)
			return leaderID
		}
		time.Sleep(20 * time.Millisecond)
	}

	leaderCount := 0
	for _, node := range nodes {
		role, _, _, _ := node.State()
		if role == RoleLeader {
			leaderCount++
		}
	}
	t.Fatalf("expected exactly one leader within %s, got %d", maxWait, leaderCount)
	return ""
}
