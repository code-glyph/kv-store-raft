package raft

import (
	"math/rand"
	"time"
)

func randomizedElectionTimeout(base time.Duration, rnd *rand.Rand) time.Duration {
	if base <= 0 {
		return 150 * time.Millisecond
	}
	jitter := time.Duration(rnd.Int63n(int64(base)))
	return base + jitter
}
