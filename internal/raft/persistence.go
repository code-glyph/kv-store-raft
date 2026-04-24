package raft

type MetaStore interface {
	SaveMeta(term uint64, votedFor string) error
	LoadMeta() (uint64, string, error)
}

type LogStore interface {
	AppendEntries(entries []LogEntry) error
	LoadEntries() ([]LogEntry, error)
	Sync() error
	Close() error
}
