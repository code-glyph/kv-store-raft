package raft

func (n *Node) applyCommittedLocked() error {
	if n.stateMachine == nil {
		n.lastApplied = n.commitIndex
		return nil
	}
	for n.lastApplied < n.commitIndex {
		next := n.lastApplied + 1
		entry, ok := n.entryAtLocked(next)
		if !ok {
			break
		}
		_, err := n.stateMachine.Apply(entry)
		if err != nil {
			return err
		}
		n.lastApplied = next
		n.logger.Printf("node=%s applied index=%d term=%d", n.nodeID, entry.Index, entry.Term)
	}
	return nil
}
