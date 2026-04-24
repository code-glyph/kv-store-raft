package raft

import "context"

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// Transport is the boundary for node-to-node Raft RPCs.
type Transport interface {
	RequestVote(ctx context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
