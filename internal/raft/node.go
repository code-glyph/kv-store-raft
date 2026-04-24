package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type NodeConfig struct {
	NodeID           string
	Peers            map[string]string
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	Transport        Transport
	StateMachine     StateMachine
	Logger           *log.Logger
}

type Node struct {
	mu sync.Mutex

	nodeID string
	peers  map[string]string

	transport Transport
	logger    *log.Logger
	rnd       *rand.Rand

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	state       Role
	currentTerm uint64
	votedFor    string
	leaderID    string

	logEntries  []LogEntry
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	stateMachine StateMachine

	electionResetCh chan struct{}
}

func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("node id is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("transport is required")
	}
	if cfg.ElectionTimeout <= 0 {
		return nil, errors.New("election timeout must be > 0")
	}
	if cfg.HeartbeatTimeout <= 0 {
		return nil, errors.New("heartbeat timeout must be > 0")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	peers := make(map[string]string, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		if id == cfg.NodeID {
			continue
		}
		peers[id] = addr
	}

	return &Node{
		nodeID:           cfg.NodeID,
		peers:            peers,
		transport:        cfg.Transport,
		logger:           logger,
		rnd:              rand.New(rand.NewSource(time.Now().UnixNano() + nodeIDSeed(cfg.NodeID))),
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		state:            RoleFollower,
		logEntries:       make([]LogEntry, 0),
		nextIndex:        make(map[string]uint64),
		matchIndex:       make(map[string]uint64),
		stateMachine:     cfg.StateMachine,
		electionResetCh:  make(chan struct{}, 1),
	}, nil
}

func nodeIDSeed(nodeID string) int64 {
	var seed int64 = 17
	for _, ch := range nodeID {
		seed = (seed * 31) + int64(ch)
	}
	return seed
}

func (n *Node) Run(ctx context.Context) {
	go n.electionLoop(ctx)
	go n.heartbeatLoop(ctx)
}

func (n *Node) State() (Role, uint64, string, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm, n.votedFor, n.leaderID
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}

	selfLastTerm := n.lastLogTermLocked()
	selfLastIndex := n.lastLogIndexLocked()
	candidateIsUpToDate := req.LastLogTerm > selfLastTerm ||
		(req.LastLogTerm == selfLastTerm && req.LastLogIndex >= selfLastIndex)
	if !candidateIsUpToDate {
		resp.Term = n.currentTerm
		return resp
	}

	if n.votedFor == "" || n.votedFor == req.CandidateID {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		resp.Term = n.currentTerm
		n.resetElectionTimer()
		return resp
	}

	resp.Term = n.currentTerm
	return resp
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm || n.state != RoleFollower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID

	// Validate prev log linkage.
	prevTerm, ok := n.termAtLocked(req.PrevLogIndex)
	if !ok || prevTerm != req.PrevLogTerm {
		resp.Term = n.currentTerm
		resp.Success = false
		return resp
	}

	// Merge entries with conflict truncation.
	for i, incoming := range req.Entries {
		localIndex := req.PrevLogIndex + 1 + uint64(i)
		localTerm, exists := n.termAtLocked(localIndex)
		if exists && localTerm != incoming.Term {
			n.logEntries = n.logEntries[:localIndex-1]
		}
		if !exists || localTerm != incoming.Term {
			if incoming.Index == 0 {
				incoming.Index = localIndex
			}
			n.logEntries = append(n.logEntries, incoming)
		}
	}

	lastIndex := n.lastLogIndexLocked()
	if req.LeaderCommit > n.commitIndex {
		if req.LeaderCommit < lastIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastIndex
		}
		if err := n.applyCommittedLocked(); err != nil {
			n.logger.Printf("node=%s apply error: %v", n.nodeID, err)
		}
	}
	resp.Term = n.currentTerm
	resp.Success = true
	n.resetElectionTimer()
	return resp
}

func (n *Node) electionLoop(ctx context.Context) {
	for {
		timeout := randomizedElectionTimeout(n.electionTimeout, n.rnd)
		timer := time.NewTimer(timeout)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-n.electionResetCh:
			timer.Stop()
		case <-timer.C:
			n.startElection(ctx)
		}
	}
}

func (n *Node) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(n.heartbeatTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.state == RoleLeader
			term := n.currentTerm
			n.mu.Unlock()
			if isLeader {
				n.sendHeartbeats(ctx, term)
			}
		}
	}
}

func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	if n.state == RoleLeader {
		n.mu.Unlock()
		return
	}
	n.state = RoleCandidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.nodeID
	n.leaderID = ""
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()
	n.mu.Unlock()

	n.logger.Printf("node=%s role=%s term=%d election started", n.nodeID, RoleCandidate.String(), term)

	votesNeeded := n.majority()
	votesGranted := 1

	var wg sync.WaitGroup
	type voteResult struct {
		resp RequestVoteResponse
		err  error
	}
	ch := make(chan voteResult, len(n.peers))

	req := RequestVoteRequest{
		Term:         term,
		CandidateID:  n.nodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for peerID := range n.peers {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()
			resp, err := n.transport.RequestVote(ctx, pid, req)
			ch <- voteResult{resp: resp, err: err}
		}(peerID)
	}
	wg.Wait()
	close(ch)

	for result := range ch {
		if result.err != nil {
			continue
		}
		if result.resp.Term > term {
			n.mu.Lock()
			n.becomeFollowerLocked(result.resp.Term, "")
			n.mu.Unlock()
			return
		}
		if result.resp.VoteGranted {
			votesGranted++
		}
	}

	if votesGranted >= votesNeeded {
		n.mu.Lock()
		if n.currentTerm == term && n.state == RoleCandidate {
			n.state = RoleLeader
			n.leaderID = n.nodeID
			n.matchIndex[n.nodeID] = n.lastLogIndexLocked()
			for peerID := range n.peers {
				n.nextIndex[peerID] = n.lastLogIndexLocked() + 1
				n.matchIndex[peerID] = 0
			}
			n.mu.Unlock()
			n.logger.Printf("node=%s role=%s term=%d votes=%d", n.nodeID, RoleLeader.String(), term, votesGranted)
			n.sendHeartbeats(ctx, term)
			return
		}
		n.mu.Unlock()
	}
}

func (n *Node) sendHeartbeats(ctx context.Context, term uint64) {
	for peerID := range n.peers {
		go n.replicateToPeer(ctx, peerID, term)
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leaderID string) {
	if term < n.currentTerm {
		return
	}
	n.currentTerm = term
	n.state = RoleFollower
	n.votedFor = ""
	n.leaderID = leaderID
	n.logger.Printf("node=%s role=%s term=%d leader=%s", n.nodeID, RoleFollower.String(), n.currentTerm, leaderID)
	n.resetElectionTimer()
}

func (n *Node) majority() int {
	totalNodes := len(n.peers) + 1
	return (totalNodes / 2) + 1
}

func (n *Node) resetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}

func (n *Node) String() string {
	state, term, votedFor, leader := n.State()
	return fmt.Sprintf("node=%s state=%s term=%d votedFor=%s leader=%s", n.nodeID, state.String(), term, votedFor, leader)
}

