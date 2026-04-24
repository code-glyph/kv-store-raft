package raft

type MetaStore interface {
	SaveMeta(term uint64, votedFor string) error
	LoadMeta() (uint64, string, error)
}

type LogStore interface {
	AppendEntries(entries []LogEntry) error
	LoadEntries() ([]LogEntry, error)
	RewriteEntries(entries []LogEntry) error
	Sync() error
	Close() error
}

type SnapshotStore interface {
	SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error
	LoadSnapshot() (lastIncludedIndex, lastIncludedTerm uint64, data []byte, err error)
}
