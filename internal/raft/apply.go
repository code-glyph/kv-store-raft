package raft

import "errors"

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

		if err := n.maybeSnapshotLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) maybeSnapshotLocked() error {
	if n.snapshotStore == nil || n.logStore == nil || n.stateMachine == nil {
		return nil
	}
	if n.snapshotInterval == 0 {
		return nil
	}
	if n.lastApplied <= n.snapshotIndex {
		return nil
	}
	if n.lastApplied-n.snapshotIndex < n.snapshotInterval {
		return nil
	}

	snapIndex := n.lastApplied
	snapTerm, ok := n.termAtLocked(snapIndex)
	if !ok {
		return errors.New("snapshot term lookup failed")
	}
	data := n.stateMachine.Snapshot()
	if err := n.snapshotStore.SaveSnapshot(snapIndex, snapTerm, data); err != nil {
		return err
	}

	remaining := make([]LogEntry, 0, len(n.logEntries))
	for _, entry := range n.logEntries {
		if entry.Index > snapIndex {
			remaining = append(remaining, entry)
		}
	}
	if err := n.logStore.RewriteEntries(remaining); err != nil {
		return err
	}

	n.logEntries = remaining
	n.snapshotIndex = snapIndex
	n.snapshotTerm = snapTerm
	return nil
}
