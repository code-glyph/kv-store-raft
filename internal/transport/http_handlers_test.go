package transport

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"kv-store-raft/internal/raft"
)

type fakeRaftHandler struct {
	lastVoteReq   raft.RequestVoteRequest
	lastAppendReq raft.AppendEntriesRequest
}

func (f *fakeRaftHandler) HandleRequestVote(req raft.RequestVoteRequest) raft.RequestVoteResponse {
	f.lastVoteReq = req
	return raft.RequestVoteResponse{Term: req.Term, VoteGranted: true}
}

func (f *fakeRaftHandler) HandleAppendEntries(req raft.AppendEntriesRequest) raft.AppendEntriesResponse {
	f.lastAppendReq = req
	return raft.AppendEntriesResponse{Term: req.Term, Success: true}
}

func TestRegisterRaftHandlers_MethodNotAllowed(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRaftHandlers(mux, &fakeRaftHandler{})

	req := httptest.NewRequest(http.MethodGet, "/raft/request-vote", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestRegisterRaftHandlers_InvalidJSON(t *testing.T) {
	mux := http.NewServeMux()
	RegisterRaftHandlers(mux, &fakeRaftHandler{})

	req := httptest.NewRequest(http.MethodPost, "/raft/append-entries", bytes.NewBufferString("{"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestRegisterRaftHandlers_DispatchesValidRequest(t *testing.T) {
	fake := &fakeRaftHandler{}
	mux := http.NewServeMux()
	RegisterRaftHandlers(mux, fake)

	body, _ := json.Marshal(raft.RequestVoteRequest{Term: 7, CandidateID: "n2", LastLogIndex: 5, LastLogTerm: 6})
	req := httptest.NewRequest(http.MethodPost, "/raft/request-vote", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if fake.lastVoteReq.CandidateID != "n2" || fake.lastVoteReq.Term != 7 {
		t.Fatalf("handler did not receive expected request: %+v", fake.lastVoteReq)
	}

	var resp raft.RequestVoteResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.VoteGranted || resp.Term != 7 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}
