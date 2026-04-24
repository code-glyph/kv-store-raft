package kv

import (
	"fmt"
	"sync"

	"kv-store-raft/internal/raft"
)

var ErrKeyNotFound = fmt.Errorf("key not found")

// ApplyResult is a deterministic result returned from applying a command.
type ApplyResult struct {
	Op      string
	Key     string
	Value   string
	Existed bool
}

// Store is an in-memory KV state machine.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Apply applies one committed Raft log entry.
func (s *Store) Apply(entry raft.LogEntry) (interface{}, error) {
	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case OpSet:
		_, existed := s.data[cmd.Key]
		s.data[cmd.Key] = cmd.Value
		return ApplyResult{
			Op:      OpSet,
			Key:     cmd.Key,
			Value:   cmd.Value,
			Existed: existed,
		}, nil
	case OpDelete:
		prev, existed := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		return ApplyResult{
			Op:      OpDelete,
			Key:     cmd.Key,
			Value:   prev,
			Existed: existed,
		}, nil
	default:
		// Defensive branch. DecodeCommand validates operations.
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, cmd.Op)
	}
}

func (s *Store) Get(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return value, nil
}
