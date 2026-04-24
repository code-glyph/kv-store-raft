package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestReliability_LeaderCrashDuringWrites(t *testing.T) {
	h := newReliabilityHarness(t, 5)
	leader := assertEventuallySingleLeader(t, h, 4*time.Second)
	t.Logf("initial leader=%s", leader)

	_ = h.runConcurrentWrites(500 * time.Millisecond)
	h.stopNode(leader)
	t.Logf("stopped leader=%s", leader)

	newLeader := assertEventuallySingleLeader(t, h, 5*time.Second)
	if newLeader == leader {
		t.Fatalf("expected new leader after crash, got same leader %s", newLeader)
	}
	t.Logf("new leader=%s", newLeader)

	for i := 0; i < 5; i++ {
		_, err := h.submitOnLeader([]byte(fmt.Sprintf(`{"op":"set","key":"post%d","value":"ok"}`, i)))
		if err != nil {
			t.Fatalf("post-failover write %d failed: %v", i, err)
		}
	}

	assertAtMostOneLeaderPerTerm(t, h, 400*time.Millisecond)
}
