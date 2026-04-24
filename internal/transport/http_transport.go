package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"kv-store-raft/internal/raft"
)

type HTTPTransport struct {
	peerAddrs map[string]string
	client    *http.Client
}

func NewHTTPTransport(peerAddrs map[string]string) *HTTPTransport {
	peers := make(map[string]string, len(peerAddrs))
	for id, addr := range peerAddrs {
		peers[id] = addr
	}
	return &HTTPTransport{
		peerAddrs: peers,
		client: &http.Client{
			Timeout: 800 * time.Millisecond,
		},
	}
}

func (t *HTTPTransport) RequestVote(ctx context.Context, peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	err := t.postJSON(ctx, peerID, "/raft/request-vote", req, &resp)
	return resp, err
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	err := t.postJSON(ctx, peerID, "/raft/append-entries", req, &resp)
	return resp, err
}

func (t *HTTPTransport) postJSON(ctx context.Context, peerID string, path string, reqBody interface{}, respBody interface{}) error {
	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return fmt.Errorf("unknown peer id=%s endpoint=%s", peerID, path)
	}
	url := "http://" + strings.TrimSpace(addr) + path

	b, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request peer=%s endpoint=%s: %w", peerID, path, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("create request peer=%s endpoint=%s: %w", peerID, path, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("send request peer=%s endpoint=%s: %w", peerID, path, err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status peer=%s endpoint=%s status=%d", peerID, path, httpResp.StatusCode)
	}
	if err := json.NewDecoder(httpResp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("decode response peer=%s endpoint=%s: %w", peerID, path, err)
	}
	return nil
}
