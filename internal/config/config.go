package config

import (
	"flag"
	"log"
	"fmt"
	"strings"
)

type Config struct {
	NodeID           string
	HTTPAddr         string
	RaftAddr         string
	PeerMap          map[string]string
	ElectionTimeout  int
	HeartbeatTimeout int
	DataDir          string
}

func parsePeerAddrs(peerAddrs string) map[string]string {
	result := make(map[string]string)
	if peerAddrs == "" {
		return result
	}
	for _, peer := range strings.Split(peerAddrs, ",") {
		parts := strings.Split(peer, "=")
		if len(parts) == 2 {
			result[parts[0]] = parts[1] // id = address
		} else {
			log.Fatal("invalid peer address", peer)
		}
	}
	return result
}

func LoadConfig() (*Config, error) {
	nodeID := flag.String("node-id", "", "unique node identifier")
	httpAddr := flag.String("http-addr", "localhost:8080", "HTTP API address")
	raftAddr := flag.String("raft-addr", "localhost:8081", "raft RPC address")
	peerAddrs := flag.String("peers", "", "comma-separated list of peer addresses")
	electionTimeout := flag.Int("election-timeout", 1000, "election timeout in milliseconds")
	heartbeatTimeout := flag.Int("heartbeat-timeout", 50, "heartbeat timeout in milliseconds")
	dataDir := flag.String("data-dir", "./data", "data directory")

	flag.Parse()
	peerMap := parsePeerAddrs(*peerAddrs)
	cfg := Config{
		NodeID:           *nodeID,
		HTTPAddr:         *httpAddr,
		RaftAddr:         *raftAddr,
		PeerMap:          peerMap,
		ElectionTimeout:  *electionTimeout,
		HeartbeatTimeout: *heartbeatTimeout,
		DataDir:          *dataDir,
	}

	err := cfg.ValidateConfig()
	if (err != nil) {
		return nil, err
	} else {
		return &cfg, nil
	}
}

func (c *Config) ValidateConfig() error {
	if c.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("HTTP address is required")
	}
	if c.RaftAddr == "" {
		return fmt.Errorf("raft address is required")
	}
	if c.PeerMap == nil {
		return fmt.Errorf("peer map is required")
	}
	if c.ElectionTimeout <= 0 {
		return fmt.Errorf("election timeout must be greater than 0")
	}
	if c.HeartbeatTimeout <= 0 {
		return fmt.Errorf("heartbeat timeout must be greater than 0")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	return nil
}