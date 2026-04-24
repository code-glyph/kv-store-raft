package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"kv-store-raft/internal/api"
	"kv-store-raft/internal/kv"
	"kv-store-raft/internal/raft"
	"kv-store-raft/internal/transport"
)

type clusterNode struct {
	id     string
	addr   string
	node   *raft.Node
	store  *kv.Store
	server *http.Server
	cancel context.CancelFunc
}

func TestAPICluster_FollowerWriteReadAndFailover(t *testing.T) {
	nodes := startCluster(t, 3)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)
	follower := firstFollower(nodes, leader.id)

	sendJSON(t, http.MethodPut, follower.addr, "/kv/k1", `{"value":"v1"}`, http.StatusOK)
	assertGetValue(t, follower.addr, "/kv/k1", "v1")
	assertGetValue(t, leader.addr, "/kv/k1", "v1")

	// Fail leader and validate retry/write on surviving follower after re-election.
	leader.cancel()
	_ = leader.server.Shutdown(context.Background())

	alive := filterAlive(nodes, leader.id)
	newLeader := waitForLeader(t, alive, 4*time.Second)
	target := firstFollower(alive, newLeader.id)
	sendJSON(t, http.MethodPut, target.addr, "/kv/k2", `{"value":"v2"}`, http.StatusOK)
	assertGetValue(t, target.addr, "/kv/k2", "v2")
}

func startCluster(t *testing.T, n int) []*clusterNode {
	t.Helper()
	peers := make(map[string]string, n)
	listeners := make(map[string]net.Listener, n)
	for i := 1; i <= n; i++ {
		id := nodeID(i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		listeners[id] = ln
		peers[id] = ln.Addr().String()
	}

	nodes := make([]*clusterNode, 0, n)
	for i := 1; i <= n; i++ {
		id := nodeID(i)
		store := kv.NewStore()
		nTransport := transport.NewHTTPTransport(peers)
		node, err := raft.NewNode(raft.NodeConfig{
			NodeID:           id,
			Peers:            peers,
			ElectionTimeout:  280 * time.Millisecond,
			HeartbeatTimeout: 80 * time.Millisecond,
			SubmitTimeout:    2 * time.Second,
			MaxBatchSize:     64,
			Transport:        nTransport,
			StateMachine:     store,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}

		mux := http.NewServeMux()
		transport.RegisterRaftHandlers(mux, node)
		apiHandler := api.NewHandler(node, store, peers)
		api.RegisterHandlers(mux, apiHandler)

		srv := &http.Server{Handler: mux}
		ctx, cancel := context.WithCancel(context.Background())
		node.Run(ctx)
		go func(ln net.Listener, s *http.Server) {
			_ = s.Serve(ln)
		}(listeners[id], srv)

		nodes = append(nodes, &clusterNode{
			id:     id,
			addr:   peers[id],
			node:   node,
			store:  store,
			server: srv,
			cancel: cancel,
		})
	}
	return nodes
}

func stopCluster(nodes []*clusterNode) {
	for _, n := range nodes {
		n.cancel()
		_ = n.server.Shutdown(context.Background())
	}
}

func waitForLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *clusterNode
		count := 0
		for _, n := range nodes {
			if n.node.CurrentRole() == raft.RoleLeader {
				count++
				leader = n
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

func sendJSON(t *testing.T, method, addr, path, body string, wantCode int) {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantCode {
		t.Fatalf("expected %d from %s, got %d", wantCode, path, resp.StatusCode)
	}
}

func assertGetValue(t *testing.T, addr, path, want string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from %s, got %d", path, resp.StatusCode)
	}
	var payload struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Value != want {
		t.Fatalf("expected value %q, got %q", want, payload.Value)
	}
}

func firstFollower(nodes []*clusterNode, leaderID string) *clusterNode {
	for _, n := range nodes {
		if n.id != leaderID {
			return n
		}
	}
	return nil
}

func filterAlive(nodes []*clusterNode, deadID string) []*clusterNode {
	out := make([]*clusterNode, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.id != deadID {
			out = append(out, n)
		}
	}
	return out
}

func nodeID(i int) string {
	return fmt.Sprintf("n%d", i)
}
