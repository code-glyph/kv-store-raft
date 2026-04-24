package kv

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	OpSet    = "set"
	OpDelete = "delete"
)

var (
	ErrInvalidCommandData  = errors.New("invalid command data")
	ErrUnsupportedOperation = errors.New("unsupported operation")
	ErrEmptyKey            = errors.New("key cannot be empty")
)

// Command is the payload encoded into a Raft log entry.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (c Command) Validate() error {
	if c.Key == "" {
		return ErrEmptyKey
	}
	switch c.Op {
	case OpSet, OpDelete:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedOperation, c.Op)
	}
}

func (c Command) Encode() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	return b, nil
}

func DecodeCommand(raw []byte) (Command, error) {
	if len(raw) == 0 {
		return Command{}, ErrInvalidCommandData
	}
	var cmd Command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return Command{}, fmt.Errorf("%w: %v", ErrInvalidCommandData, err)
	}
	if err := cmd.Validate(); err != nil {
		return Command{}, err
	}
	return cmd, nil
}
