package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"kv-store-raft/internal/raft"
)

type WAL struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	pending  bool
	done     chan struct{}
	stopped  chan struct{}
	interval time.Duration
}

func NewWAL(dataDir string, syncInterval time.Duration) (*WAL, error) {
	if syncInterval <= 0 {
		syncInterval = 40 * time.Millisecond
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, "raft_log.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		file:     f,
		path:     path,
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
		interval: syncInterval,
	}
	go w.syncLoop()
	return w, nil
}

func (w *WAL) AppendEntries(entries []raft.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, entry := range entries {
		b, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if _, err := w.file.Write(append(b, '\n')); err != nil {
			return err
		}
		w.pending = true
	}
	return nil
}

func (w *WAL) LoadEntries() ([]raft.LogEntry, error) {
	f, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	out := make([]raft.LogEntry, 0)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e raft.LogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *WAL) RewriteEntries(entries []raft.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		b, err := json.Marshal(entry)
		if err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	appendFile, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	w.file = appendFile
	w.pending = false
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *WAL) syncLocked() error {
	if !w.pending {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.pending = false
	return nil
}

func (w *WAL) syncLoop() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	defer close(w.stopped)
	for {
		select {
		case <-w.done:
			_ = w.Sync()
			return
		case <-ticker.C:
			_ = w.Sync()
		}
	}
}

func (w *WAL) Close() error {
	close(w.done)
	<-w.stopped
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.syncLocked()
	return w.file.Close()
}
