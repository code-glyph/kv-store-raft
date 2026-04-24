package integration

import (
	"testing"
	"time"

	"kv-store-raft/internal/raft"
)

func assertAtMostOneLeaderPerTerm(t *testing.T, h *reliabilityHarness, observe time.Duration) {
	t.Helper()
	deadline := time.Now().Add(observe)
	for time.Now().Before(deadline) {
		leadersByTerm := make(map[uint64][]string)
		for id, n := range h.nodes {
			role, term, _, _ := n.node.State()
			if role == raft.RoleLeader {
				leadersByTerm[term] = append(leadersByTerm[term], id)
			}
		}
		for term, leaders := range leadersByTerm {
			if len(leaders) > 1 {
				h.dumpState("multiple leaders in same term")
				t.Fatalf("term %d has multiple leaders: %v", term, leaders)
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func assertEventuallySingleLeader(t *testing.T, h *reliabilityHarness, timeout time.Duration) string {
	t.Helper()
	return h.waitForSingleLeader(timeout)
}

func assertFollowerEventuallyCatchesUp(t *testing.T, h *reliabilityHarness, nodeID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		maxLen := 0
		for _, n := range h.nodes {
			if l := n.node.LogLength(); l > maxLen {
				maxLen = l
			}
		}
		target := h.nodes[nodeID]
		if target != nil && target.node.LogLength() == maxLen {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	h.dumpState("follower did not catch up")
	t.Fatalf("node %s did not catch up in %s", nodeID, timeout)
}
