package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type metaFile struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

type MetaStore struct {
	path string
}

func NewMetaStore(dataDir string) *MetaStore {
	return &MetaStore{
		path: filepath.Join(dataDir, "raft_meta.json"),
	}
}

func (m *MetaStore) SaveMeta(term uint64, votedFor string) error {
	if err := os.MkdirAll(filepath.Dir(m.path), 0o755); err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	b, err := json.Marshal(metaFile{CurrentTerm: term, VotedFor: votedFor})
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}

func (m *MetaStore) LoadMeta() (uint64, string, error) {
	b, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil
		}
		return 0, "", err
	}
	var meta metaFile
	if err := json.Unmarshal(b, &meta); err != nil {
		return 0, "", err
	}
	return meta.CurrentTerm, meta.VotedFor, nil
}
