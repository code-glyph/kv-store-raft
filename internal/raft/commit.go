package raft

func (n *Node) advanceCommitIndexLocked() error {
	if n.state != RoleLeader {
		return nil
	}
	lastIndex := n.lastLogIndexLocked()
	for candidate := lastIndex; candidate > n.commitIndex; candidate-- {
		term, ok := n.termAtLocked(candidate)
		if !ok || term != n.currentTerm {
			continue
		}
		replicated := 1 // leader itself
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= candidate {
				replicated++
			}
		}
		if replicated >= n.majority() {
			n.commitIndex = candidate
			return n.applyCommittedLocked()
		}
	}
	return nil
}
