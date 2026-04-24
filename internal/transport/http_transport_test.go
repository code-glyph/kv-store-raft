package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kv-store-raft/internal/raft"
)

func TestHTTPTransport_RequestVote_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/raft/request-vote" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"term":3,"vote_granted":true}`))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	tr := NewHTTPTransport(map[string]string{"n2": addr})
	resp, err := tr.RequestVote(context.Background(), "n2", raft.RequestVoteRequest{Term: 3, CandidateID: "n1"})
	if err != nil {
		t.Fatalf("request vote failed: %v", err)
	}
	if !resp.VoteGranted || resp.Term != 3 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHTTPTransport_AppendEntries_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	tr := NewHTTPTransport(map[string]string{"n2": addr})
	_, err := tr.AppendEntries(context.Background(), "n2", raft.AppendEntriesRequest{Term: 1, LeaderID: "n1"})
	if err == nil {
		t.Fatalf("expected error for non-200 status")
	}
	if !strings.Contains(err.Error(), "status=502") {
		t.Fatalf("expected status in error, got: %v", err)
	}
}

func TestHTTPTransport_RequestVote_InvalidResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{"))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	tr := NewHTTPTransport(map[string]string{"n2": addr})
	_, err := tr.RequestVote(context.Background(), "n2", raft.RequestVoteRequest{Term: 1, CandidateID: "n1"})
	if err == nil {
		t.Fatalf("expected decode error")
	}
	if !strings.Contains(err.Error(), "decode response") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPTransport_RequestVote_ContextTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"term":1,"vote_granted":false}`))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	tr := NewHTTPTransport(map[string]string{"n2": addr})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err := tr.RequestVote(ctx, "n2", raft.RequestVoteRequest{Term: 1, CandidateID: "n1"})
	if err == nil {
		t.Fatalf("expected timeout/cancel error")
	}
	if !strings.Contains(err.Error(), "send request") {
		t.Fatalf("unexpected error: %v", err)
	}
}
