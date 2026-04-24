package integration

import (
	"testing"
	"time"
)

func TestReliability_MinorityPartitionCannotCommit(t *testing.T) {
	h := newReliabilityHarness(t, 5)
	leader := assertEventuallySingleLeader(t, h, 4*time.Second)
	t.Logf("leader before partition=%s", leader)

	// Isolate minority group containing the old leader.
	minority := []string{leader, "n4"}
	h.isolateGroup(minority)
	t.Logf("applied partition minority=%v majority=others", minority)

	// Old leader (in minority) should not commit new writes.
	if _, err := h.submitOnNode(leader, []byte(`{"op":"set","key":"minority","value":"x"}`)); err == nil {
		t.Fatalf("expected minority leader submit to fail/timeout")
	}

	// Majority side should elect a new leader and continue.
	isMinority := map[string]bool{}
	for _, id := range minority {
		isMinority[id] = true
	}
	majorityGroup := make([]string, 0, len(h.nodes)-len(minority))
	for id := range h.nodes {
		if !isMinority[id] {
			majorityGroup = append(majorityGroup, id)
		}
	}
	newLeader := h.waitForSingleLeaderInGroup(majorityGroup, 5*time.Second)
	t.Logf("leader during partition=%s", newLeader)
	if newLeader == leader {
		t.Fatalf("expected majority to elect a different leader")
	}
	if _, err := h.submitOnNode(newLeader, []byte(`{"op":"set","key":"majority","value":"ok"}`)); err != nil {
		t.Fatalf("majority leader write failed: %v", err)
	}

	assertAtMostOneLeaderPerTerm(t, h, 300*time.Millisecond)
}

func TestReliability_PartitionHealConvergence(t *testing.T) {
	h := newReliabilityHarness(t, 5)
	leader := assertEventuallySingleLeader(t, h, 4*time.Second)

	// Split cluster and then heal.
	h.isolateGroup([]string{leader, "n5"})
	time.Sleep(600 * time.Millisecond)
	h.heal()
	t.Logf("healed partition")

	leaderAfterHeal := assertEventuallySingleLeader(t, h, 5*time.Second)
	t.Logf("leader after heal=%s", leaderAfterHeal)
	if _, err := h.submitOnNode(leaderAfterHeal, []byte(`{"op":"set","key":"heal","value":"done"}`)); err != nil {
		t.Fatalf("write after heal failed: %v", err)
	}
	assertAtMostOneLeaderPerTerm(t, h, 400*time.Millisecond)
}
