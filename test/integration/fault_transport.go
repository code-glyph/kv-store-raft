package integration

import (
	"context"
	"errors"
	"sync"
	"time"

	"kv-store-raft/internal/raft"
)

var ErrLinkBlocked = errors.New("network link blocked")

type raftRPCHandler interface {
	HandleRequestVote(req raft.RequestVoteRequest) raft.RequestVoteResponse
	HandleAppendEntries(req raft.AppendEntriesRequest) raft.AppendEntriesResponse
}

type faultNetwork struct {
	mu           sync.RWMutex
	nodes        map[string]raftRPCHandler
	blockedEdges map[string]map[string]bool
	latency      map[string]map[string]time.Duration
}

func newFaultNetwork() *faultNetwork {
	return &faultNetwork{
		nodes:        make(map[string]raftRPCHandler),
		blockedEdges: make(map[string]map[string]bool),
		latency:      make(map[string]map[string]time.Duration),
	}
}

func (n *faultNetwork) registerNode(nodeID string, h raftRPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[nodeID] = h
}

func (n *faultNetwork) unregisterNode(nodeID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodes, nodeID)
	delete(n.blockedEdges, nodeID)
	delete(n.latency, nodeID)
	for from := range n.blockedEdges {
		delete(n.blockedEdges[from], nodeID)
	}
	for from := range n.latency {
		delete(n.latency[from], nodeID)
	}
}

func (n *faultNetwork) setBlocked(from, to string, blocked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.blockedEdges[from]; !ok {
		n.blockedEdges[from] = make(map[string]bool)
	}
	n.blockedEdges[from][to] = blocked
}

func (n *faultNetwork) setLatency(from, to string, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.latency[from]; !ok {
		n.latency[from] = make(map[string]time.Duration)
	}
	n.latency[from][to] = d
}

func (n *faultNetwork) healAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blockedEdges = make(map[string]map[string]bool)
	n.latency = make(map[string]map[string]time.Duration)
}

func (n *faultNetwork) isBlocked(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.blockedEdges[from][to]
}

func (n *faultNetwork) getLatency(from, to string) time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.latency[from][to]
}

func (n *faultNetwork) getNode(nodeID string) raftRPCHandler {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.nodes[nodeID]
}

type faultTransport struct {
	fromID string
	net    *faultNetwork
}

func (t *faultTransport) RequestVote(ctx context.Context, peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if t.net.isBlocked(t.fromID, peerID) {
		return raft.RequestVoteResponse{}, ErrLinkBlocked
	}
	if d := t.net.getLatency(t.fromID, peerID); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return raft.RequestVoteResponse{}, ctx.Err()
		}
	}
	peer := t.net.getNode(peerID)
	if peer == nil {
		return raft.RequestVoteResponse{}, errors.New("peer unavailable")
	}
	return peer.HandleRequestVote(req), nil
}

func (t *faultTransport) AppendEntries(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if t.net.isBlocked(t.fromID, peerID) {
		return raft.AppendEntriesResponse{}, ErrLinkBlocked
	}
	if d := t.net.getLatency(t.fromID, peerID); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return raft.AppendEntriesResponse{}, ctx.Err()
		}
	}
	peer := t.net.getNode(peerID)
	if peer == nil {
		return raft.AppendEntriesResponse{}, errors.New("peer unavailable")
	}
	return peer.HandleAppendEntries(req), nil
}
