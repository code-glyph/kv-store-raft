package storage

import "kv-store-raft/internal/raft"

type RecoveredState struct {
	CurrentTerm uint64
	VotedFor    string
	LogEntries  []raft.LogEntry
}

func LoadRecoveredState(meta *MetaStore, wal *WAL) (RecoveredState, error) {
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
	return out, nil
}
