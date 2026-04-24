package raft

// LogEntry represents one replicated operation in the Raft log.
// For now Command stores opaque bytes that state machines decode.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// StateMachine is the consumer of committed log entries.
type StateMachine interface {
	Apply(entry LogEntry) (interface{}, error)
}
