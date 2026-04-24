package storage

import "kv-store-raft/internal/raft"

type RecoveredState struct {
	CurrentTerm        uint64
	VotedFor           string
	SnapshotIndex      uint64
	SnapshotTerm       uint64
	SnapshotData       []byte
	LogEntries         []raft.LogEntry
}

func LoadRecoveredState(meta *MetaStore, wal *WAL, snapshot *SnapshotStore) (RecoveredState, error) {
	var out RecoveredState
	if meta != nil {
		term, vote, err := meta.LoadMeta()
		if err != nil {
			return out, err
		}
		out.CurrentTerm = term
		out.VotedFor = vote
	}
	if wal != nil {
		entries, err := wal.LoadEntries()
		if err != nil {
			return out, err
		}
		out.LogEntries = entries
	}
	if snapshot != nil {
		index, term, data, err := snapshot.LoadSnapshot()
		if err != nil {
			return out, err
		}
		out.SnapshotIndex = index
		out.SnapshotTerm = term
		out.SnapshotData = data
	}
	return out, nil
}
