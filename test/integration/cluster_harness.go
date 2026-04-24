package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"kv-store-raft/internal/kv"
	"kv-store-raft/internal/raft"
	"kv-store-raft/internal/storage"
)

type reliabilityNode struct {
	id      string
	node    *raft.Node
	store   *kv.Store
	cancel  context.CancelFunc
	dataDir string
}

type reliabilityHarness struct {
	t        *testing.T
	net      *faultNetwork
	peers    map[string]string
	nodes    map[string]*reliabilityNode
	dataDirs map[string]string
	opsMu    sync.Mutex
	ops      []porcupine.Operation
	clientID map[string]int
}

func newReliabilityHarness(t *testing.T, n int) *reliabilityHarness {
	t.Helper()
	h := &reliabilityHarness{
		t:        t,
		net:      newFaultNetwork(),
		peers:    make(map[string]string, n),
		nodes:    make(map[string]*reliabilityNode, n),
		dataDirs: make(map[string]string, n),
		clientID: make(map[string]int, n),
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		h.peers[id] = id
		h.clientID[id] = i - 1
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		h.startNode(id)
	}
	t.Cleanup(func() { h.stopAll() })
	return h
}

func (h *reliabilityHarness) startNode(id string) {
	h.t.Helper()
	dataDir := filepath.Join(h.t.TempDir(), id)
	_ = os.MkdirAll(dataDir, 0o755)
	store := kv.NewStore()
	if existing, ok := h.dataDirs[id]; ok {
		dataDir = existing
	} else {
		h.dataDirs[id] = dataDir
	}
	meta := storage.NewMetaStore(dataDir)
	wal, err := storage.NewWAL(dataDir, 30*time.Millisecond)
	if err != nil {
		h.t.Fatalf("new wal %s: %v", id, err)
	}
	transport := &faultTransport{fromID: id, net: h.net}
	node, err := raft.NewNode(raft.NodeConfig{
		NodeID:           id,
		Peers:            h.peers,
		ElectionTimeout:  250 * time.Millisecond,
		HeartbeatTimeout: 70 * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		MaxBatchSize:     64,
		Transport:        transport,
		StateMachine:     store,
		MetaStore:        meta,
		LogStore:         wal,
	})
	if err != nil {
		h.t.Fatalf("new node %s: %v", id, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	node.Run(ctx)
	h.net.registerNode(id, node)
	h.nodes[id] = &reliabilityNode{id: id, node: node, store: store, cancel: cancel, dataDir: dataDir}
}

func (h *reliabilityHarness) stopNode(id string) {
	if n, ok := h.nodes[id]; ok {
		n.cancel()
		_ = n.node.Close()
		h.net.unregisterNode(id)
		delete(h.nodes, id)
	}
}

func (h *reliabilityHarness) stopAll() {
	for id := range h.nodes {
		h.stopNode(id)
	}
}

func (h *reliabilityHarness) waitForSingleLeader(timeout time.Duration) string {
	h.t.Helper()
	// After partitions / elections, it is common to briefly observe exactly one
	// node still reporting RoleLeader while leadership is actually in flux.
	// Require a short stable window so callers (e.g. immediate Submit) do not
	// race a stale leader that is about to step down.
	const stability = 150 * time.Millisecond
	return h.waitForStableSingleLeader(timeout, stability)
}

func (h *reliabilityHarness) waitForStableSingleLeader(timeout, stability time.Duration) string {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var stableID string
	var stableSince time.Time
	for time.Now().Before(deadline) {
		leaders := make([]string, 0)
		for id, n := range h.nodes {
			if n.node.CurrentRole() == raft.RoleLeader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			id := leaders[0]
			if id != stableID {
				stableID = id
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= stability {
				return id
			}
		} else {
			stableID = ""
			stableSince = time.Time{}
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.dumpState("waitForStableSingleLeader timeout")
	h.t.Fatalf("no stable single leader within %s (need %s stability)", timeout, stability)
	return ""
}

func (h *reliabilityHarness) waitForSingleLeaderInGroup(nodeIDs []string, timeout time.Duration) string {
	h.t.Helper()
	const stability = 150 * time.Millisecond
	set := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		set[id] = true
	}
	deadline := time.Now().Add(timeout)
	var stableID string
	var stableSince time.Time
	for time.Now().Before(deadline) {
		leaders := make([]string, 0)
		for id, n := range h.nodes {
			if !set[id] {
				continue
			}
			if n.node.CurrentRole() == raft.RoleLeader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			id := leaders[0]
			if id != stableID {
				stableID = id
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= stability {
				return id
			}
		} else {
			stableID = ""
			stableSince = time.Time{}
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.dumpState("waitForSingleLeaderInGroup timeout")
	h.t.Fatalf("no stable single leader in group %v within %s (need %s stability)", nodeIDs, timeout, stability)
	return ""
}

func (h *reliabilityHarness) firstFollower() string {
	leader := h.waitForSingleLeader(3 * time.Second)
	for id := range h.nodes {
		if id != leader {
			return id
		}
	}
	return ""
}

func (h *reliabilityHarness) submitOnLeader(command []byte) (uint64, error) {
	leader := h.waitForSingleLeader(3 * time.Second)
	return h.submitOnNode(leader, command)
}

func (h *reliabilityHarness) submitOnNode(id string, command []byte) (uint64, error) {
	n := h.nodes[id]
	if n == nil {
		return 0, fmt.Errorf("node not found: %s", id)
	}
	input := parseLinearizabilityInput(command)
	call := time.Now().UnixNano()
	index, err := n.node.Submit(context.Background(), command)
	ret := time.Now().UnixNano()
	h.recordOperation(id, call, ret, input, linearizabilityOutput{
		OK:    err == nil,
		Value: linearizabilityOutputValue(input, err),
	})
	return index, err
}

func (h *reliabilityHarness) recordOperation(nodeID string, call, ret int64, input linearizabilityInput, output linearizabilityOutput) {
	h.opsMu.Lock()
	h.ops = append(h.ops, porcupine.Operation{
		ClientId: h.clientID[nodeID],
		Input:    input,
		Call:     call,
		Output:   output,
		Return:   ret,
	})
	h.opsMu.Unlock()
}

func (h *reliabilityHarness) assertLinearizable(timeout time.Duration) {
	h.t.Helper()
	h.opsMu.Lock()
	ops := append([]porcupine.Operation(nil), h.ops...)
	h.opsMu.Unlock()
	assertLinearizableOperations(h.t, ops, timeout)
}

func (h *reliabilityHarness) isolateGroup(group []string) {
	set := make(map[string]bool, len(group))
	for _, id := range group {
		set[id] = true
	}
	for from := range h.nodes {
		for to := range h.nodes {
			if from == to {
				continue
			}
			if set[from] != set[to] {
				h.net.setBlocked(from, to, true)
			}
		}
	}
}

func (h *reliabilityHarness) heal() {
	h.net.healAll()
}

func (h *reliabilityHarness) addLatency(to string, d time.Duration) {
	for from := range h.nodes {
		if from == to {
			continue
		}
		h.net.setLatency(from, to, d)
	}
}

func (h *reliabilityHarness) dumpState(reason string) {
	h.t.Logf("cluster dump reason=%s", reason)
	for id, n := range h.nodes {
		role, term, votedFor, leader := n.node.State()
		h.t.Logf("node=%s role=%s term=%d votedFor=%s leader=%s logLen=%d", id, role.String(), term, votedFor, leader, n.node.LogLength())
	}
}

func (h *reliabilityHarness) snapshotTerms() map[uint64][]string {
	out := make(map[uint64][]string)
	for id, n := range h.nodes {
		_, term, _, _ := n.node.State()
		out[term] = append(out[term], id)
	}
	return out
}

func (h *reliabilityHarness) runConcurrentWrites(duration time.Duration) int {
	deadline := time.Now().Add(duration)

	var submitted int64
	var seq int64
	var wg sync.WaitGroup

	for time.Now().Before(deadline) {
		i := atomic.AddInt64(&seq, 1) - 1 // unique immutable index per goroutine

		wg.Add(1)
		go func(idx int64) {
			defer wg.Done()
			_, _ = h.submitOnLeader([]byte(
				fmt.Sprintf(`{"op":"set","key":"k%d","value":"v%d"}`, idx, idx),
			))
			atomic.AddInt64(&submitted, 1)
		}(i)

		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()
	return int(atomic.LoadInt64(&submitted))
}
