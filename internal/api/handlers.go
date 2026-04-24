package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"kv-store-raft/internal/kv"
	"kv-store-raft/internal/raft"
)

type raftNode interface {
	Submit(ctx context.Context, command []byte) (uint64, error)
	NodeID() string
	CurrentRole() raft.Role
	LeaderID() string
}

type storeReader interface {
	Get(key string) (string, error)
}

type Handler struct {
	node         raftNode
	store        storeReader
	peerAPIAddrs map[string]string
	client       *http.Client
	proxyTimeout time.Duration
}

func NewHandler(node raftNode, store storeReader, peerAPIAddrs map[string]string) *Handler {
	peers := make(map[string]string, len(peerAPIAddrs))
	for id, addr := range peerAPIAddrs {
		peers[id] = addr
	}
	return &Handler{
		node:         node,
		store:        store,
		peerAPIAddrs: peers,
		client:       &http.Client{Timeout: defaultProxyTimeout()},
		proxyTimeout: defaultProxyTimeout(),
	}
}

type okResponse struct {
	OK      bool   `json:"ok"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
	Index   uint64 `json:"index,omitempty"`
	NodeID  string `json:"node_id,omitempty"`
	Role    string `json:"role,omitempty"`
	Leader  string `json:"leader,omitempty"`
	Message string `json:"message,omitempty"`
}

type errResponse struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	Leader string `json:"leader,omitempty"`
}

func (h *Handler) HandleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, okResponse{
		OK:     true,
		NodeID: h.node.NodeID(),
		Role:   h.node.CurrentRole().String(),
		Leader: h.node.LeaderID(),
	})
}

func (h *Handler) HandleLeader(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, okResponse{
		OK:     true,
		NodeID: h.node.NodeID(),
		Role:   h.node.CurrentRole().String(),
		Leader: h.node.LeaderID(),
	})
}

func (h *Handler) HandleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required", h.node.LeaderID())
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handleWrite(w, r, key, kv.OpSet)
	case http.MethodDelete:
		h.handleWrite(w, r, key, kv.OpDelete)
	case http.MethodGet:
		h.handleGet(w, r, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", h.node.LeaderID())
	}
}

func (h *Handler) handleWrite(w http.ResponseWriter, r *http.Request, key string, op string) {
	if h.node.CurrentRole() != raft.RoleLeader {
		_ = h.proxyToLeader(w, r)
		return
	}

	value := ""
	if op == kv.OpSet {
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON payload", h.node.LeaderID())
			return
		}
		value = body.Value
	}

	cmd := kv.Command{
		Op:    op,
		Key:   key,
		Value: value,
	}
	raw, err := cmd.Encode()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), h.node.LeaderID())
		return
	}

	index, err := h.node.Submit(r.Context(), raw)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, raft.ErrNotLeader) {
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, err.Error(), h.node.LeaderID())
		return
	}

	writeJSON(w, http.StatusOK, okResponse{
		OK:     true,
		Key:    key,
		Value:  value,
		Index:  index,
		NodeID: h.node.NodeID(),
		Role:   h.node.CurrentRole().String(),
		Leader: h.node.LeaderID(),
	})
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if h.node.CurrentRole() != raft.RoleLeader {
		_ = h.proxyToLeader(w, r)
		return
	}
	value, err := h.store.Get(key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			writeError(w, http.StatusNotFound, "key not found", h.node.LeaderID())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error(), h.node.LeaderID())
		return
	}
	writeJSON(w, http.StatusOK, okResponse{
		OK:     true,
		Key:    key,
		Value:  value,
		NodeID: h.node.NodeID(),
		Role:   h.node.CurrentRole().String(),
		Leader: h.node.LeaderID(),
	})
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string, leader string) {
	writeJSON(w, status, errResponse{
		OK:     false,
		Error:  msg,
		Leader: leader,
	})
}
