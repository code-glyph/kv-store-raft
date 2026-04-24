package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kv-store-raft/internal/kv"
	"kv-store-raft/internal/raft"
)

type fakeNode struct {
	nodeID   string
	role     raft.Role
	leaderID string
	submits  [][]byte
}

func (f *fakeNode) Submit(_ context.Context, command []byte) (uint64, error) {
	f.submits = append(f.submits, command)
	return uint64(len(f.submits)), nil
}

func (f *fakeNode) NodeID() string         { return f.nodeID }
func (f *fakeNode) CurrentRole() raft.Role { return f.role }
func (f *fakeNode) LeaderID() string       { return f.leaderID }

type fakeStore struct {
	values map[string]string
}

func (s *fakeStore) Get(key string) (string, error) {
	v, ok := s.values[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return v, nil
}

func TestHandleKV_LeaderWrite(t *testing.T) {
	node := &fakeNode{nodeID: "n1", role: raft.RoleLeader, leaderID: "n1"}
	store := &fakeStore{values: map[string]string{}}
	h := NewHandler(node, store, map[string]string{"n1": "localhost:9000"})

	mux := http.NewServeMux()
	RegisterHandlers(mux, h)

	req := httptest.NewRequest(http.MethodPut, "/kv/a", strings.NewReader(`{"value":"1"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if len(node.submits) != 1 {
		t.Fatalf("expected one submit call, got %d", len(node.submits))
	}
}

func TestHandleKV_FollowerWriteProxy(t *testing.T) {
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"index":2}`))
	}))
	defer leaderSrv.Close()

	addr := strings.TrimPrefix(leaderSrv.URL, "http://")
	node := &fakeNode{nodeID: "n2", role: raft.RoleFollower, leaderID: "n1"}
	store := &fakeStore{values: map[string]string{}}
	h := NewHandler(node, store, map[string]string{"n1": addr})

	mux := http.NewServeMux()
	RegisterHandlers(mux, h)

	req := httptest.NewRequest(http.MethodDelete, "/kv/a", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected proxied 200, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleKV_LeaderDefaultReadWithFollowerProxy(t *testing.T) {
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"key":"a","value":"v"}`))
	}))
	defer leaderSrv.Close()

	addr := strings.TrimPrefix(leaderSrv.URL, "http://")
	node := &fakeNode{nodeID: "n2", role: raft.RoleFollower, leaderID: "n1"}
	store := &fakeStore{values: map[string]string{"a": "local"}}
	h := NewHandler(node, store, map[string]string{"n1": addr})

	mux := http.NewServeMux()
	RegisterHandlers(mux, h)
	req := httptest.NewRequest(http.MethodGet, "/kv/a", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected proxied read 200, got %d", rr.Code)
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["value"] != "v" {
		t.Fatalf("expected leader value v, got %v", resp["value"])
	}
}

func TestHandleKV_MissingKey(t *testing.T) {
	node := &fakeNode{nodeID: "n1", role: raft.RoleLeader, leaderID: "n1"}
	store := &fakeStore{values: map[string]string{}}
	h := NewHandler(node, store, map[string]string{"n1": "localhost:9000"})
	mux := http.NewServeMux()
	RegisterHandlers(mux, h)

	req := httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", rr.Code, rr.Body.String())
	}
}
