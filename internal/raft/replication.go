package raft

import (
	"context"
	"errors"
	"time"
)

var ErrNotLeader = errors.New("not leader")

func (n *Node) Submit(ctx context.Context, command []byte) (uint64, error) {
	n.mu.Lock()
	if n.state != RoleLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	newIndex := n.lastLogIndexLocked() + 1
	entry := LogEntry{
		Index:   newIndex,
		Term:    n.currentTerm,
		Command: command,
	}
	n.logEntries = append(n.logEntries, entry)
	n.matchIndex[n.nodeID] = newIndex
	for peerID := range n.peers {
		if n.nextIndex[peerID] == 0 {
			n.nextIndex[peerID] = newIndex
		}
	}
	n.mu.Unlock()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := n.replicateOnce(ctx); err != nil {
			return 0, err
		}

		n.mu.Lock()
		committed := n.commitIndex >= newIndex
		n.mu.Unlock()
		if committed {
			return newIndex, nil
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-deadline.C:
			return 0, errors.New("submit timeout before commit")
		case <-ticker.C:
		}
	}
}

func (n *Node) replicateOnce(ctx context.Context) error {
	n.mu.Lock()
	if n.state != RoleLeader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	term := n.currentTerm
	peerIDs := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	n.mu.Unlock()

	for _, peerID := range peerIDs {
		n.replicateToPeer(ctx, peerID, term)
	}

	n.mu.Lock()
	err := n.advanceCommitIndexLocked()
	n.mu.Unlock()
	return err
}

func (n *Node) replicateToPeer(ctx context.Context, peerID string, term uint64) {
	for attempt := 0; attempt < 8; attempt++ {
		n.mu.Lock()
		if n.state != RoleLeader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}

		nextIdx := n.nextIndex[peerID]
		if nextIdx == 0 {
			nextIdx = n.lastLogIndexLocked() + 1
			n.nextIndex[peerID] = nextIdx
		}
		prevIdx := nextIdx - 1
		prevTerm, _ := n.termAtLocked(prevIdx)
		var entries []LogEntry
		if nextIdx <= n.lastLogIndexLocked() {
			start := int(nextIdx - 1)
			entries = append(entries, n.logEntries[start:]...)
		}
		leaderCommit := n.commitIndex
		n.mu.Unlock()

		req := AppendEntriesRequest{
			Term:         term,
			LeaderID:     n.nodeID,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		resp, err := n.transport.AppendEntries(ctx, peerID, req)
		if err != nil {
			return
		}

		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			n.mu.Unlock()
			return
		}
		if resp.Success {
			match := prevIdx + uint64(len(entries))
			n.matchIndex[peerID] = match
			n.nextIndex[peerID] = match + 1
			n.mu.Unlock()
			return
		}
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
		n.mu.Unlock()
	}
}
