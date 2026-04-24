package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const proxyHeader = "X-KV-Proxied"

func (h *Handler) proxyToLeader(w http.ResponseWriter, r *http.Request) bool {
	leaderID := h.node.LeaderID()
	if leaderID == "" {
		writeError(w, http.StatusServiceUnavailable, "leader unavailable", "")
		return true
	}
	leaderAddr, ok := h.peerAPIAddrs[leaderID]
	if !ok || strings.TrimSpace(leaderAddr) == "" {
		writeError(w, http.StatusServiceUnavailable, "leader address unavailable", leaderID)
		return true
	}

	if r.Header.Get(proxyHeader) == "1" {
		writeError(w, http.StatusServiceUnavailable, "proxy loop prevented", leaderID)
		return true
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body", leaderID)
		return true
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.proxyTimeout)
	defer cancel()

	targetURL := fmt.Sprintf("http://%s%s", strings.TrimSpace(leaderAddr), r.URL.RequestURI())
	proxyReq, err := http.NewRequestWithContext(ctx, r.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusBadGateway, "failed to create proxy request", leaderID)
		return true
	}
	proxyReq.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	proxyReq.Header.Set(proxyHeader, "1")

	resp, err := h.client.Do(proxyReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("leader proxy failed: %v", err), leaderID)
		return true
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
	return true
}

func defaultProxyTimeout() time.Duration {
	return 1200 * time.Millisecond
}
