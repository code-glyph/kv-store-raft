package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestReliability_FollowerLagAndCatchup(t *testing.T) {
	h := newReliabilityHarness(t, 5)
	_ = assertEventuallySingleLeader(t, h, 4*time.Second)

	lagging := "n5"
	h.addLatency(lagging, 250*time.Millisecond)
	t.Logf("added latency to follower=%s", lagging)

	for i := 0; i < 8; i++ {
		_, err := h.submitOnLeader([]byte(fmt.Sprintf(`{"op":"set","key":"lag%d","value":"v"}`, i)))
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	// Remove latency and ensure catch-up.
	h.heal()
	assertFollowerEventuallyCatchesUp(t, h, lagging, 5*time.Second)
}
