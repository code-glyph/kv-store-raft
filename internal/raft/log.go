package raft

func (n *Node) lastLogIndexLocked() uint64 {
	if len(n.logEntries) == 0 {
		return n.snapshotIndex
	}
	return n.logEntries[len(n.logEntries)-1].Index
}

func (n *Node) lastLogTermLocked() uint64 {
	if len(n.logEntries) == 0 {
		return n.snapshotTerm
	}
	return n.logEntries[len(n.logEntries)-1].Term
}

func (n *Node) termAtLocked(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index == n.snapshotIndex {
		return n.snapshotTerm, true
	}
	if index < n.snapshotIndex {
		return 0, false
	}
	offset := int(index - (n.snapshotIndex + 1))
	if offset < 0 || offset >= len(n.logEntries) {
		return 0, false
	}
	return n.logEntries[offset].Term, true
}

func (n *Node) entryAtLocked(index uint64) (LogEntry, bool) {
	if index == 0 || index <= n.snapshotIndex {
		return LogEntry{}, false
	}
	offset := int(index - (n.snapshotIndex + 1))
	if offset < 0 || offset >= len(n.logEntries) {
		return LogEntry{}, false
	}
	return n.logEntries[offset], true
}
