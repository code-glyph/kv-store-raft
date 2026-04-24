package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type snapshotFile struct {
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

type SnapshotStore struct {
	path string
}

func NewSnapshotStore(dataDir string) *SnapshotStore {
	return &SnapshotStore{
		path: filepath.Join(dataDir, "raft_snapshot.json"),
	}
}

func (s *SnapshotStore) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	payload := snapshotFile{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              append([]byte(nil), data...),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *SnapshotStore) LoadSnapshot() (uint64, uint64, []byte, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, nil
		}
		return 0, 0, nil, err
	}
	var payload snapshotFile
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, 0, nil, err
	}
	return payload.LastIncludedIndex, payload.LastIncludedTerm, payload.Data, nil
}
