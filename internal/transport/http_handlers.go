package transport

import (
	"encoding/json"
	"net/http"

	"kv-store-raft/internal/raft"
)

type RaftRPCHandler interface {
	HandleRequestVote(req raft.RequestVoteRequest) raft.RequestVoteResponse
	HandleAppendEntries(req raft.AppendEntriesRequest) raft.AppendEntriesResponse
}

func RegisterRaftHandlers(mux *http.ServeMux, handler RaftRPCHandler) {
	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req raft.RequestVoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		resp := handler.HandleRequestVote(req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req raft.AppendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		resp := handler.HandleAppendEntries(req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}
