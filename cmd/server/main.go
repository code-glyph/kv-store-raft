package main

import (
	"context"
	"errors"
	"kv-store-raft/internal/config"
	"kv-store-raft/internal/raft"
	"kv-store-raft/internal/transport"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configuration, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Printf("starting raft node id=%s raft_addr=%s peers=%d", configuration.NodeID, configuration.RaftAddr, len(configuration.PeerMap))

	transportClient := transport.NewHTTPTransport(configuration.PeerMap)
	node, err := raft.NewNode(raft.NodeConfig{
		NodeID:           configuration.NodeID,
		Peers:            configuration.PeerMap,
		ElectionTimeout:  time.Duration(configuration.ElectionTimeout) * time.Millisecond,
		HeartbeatTimeout: time.Duration(configuration.HeartbeatTimeout) * time.Millisecond,
		Transport:        transportClient,
		Logger:           logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	transport.RegisterRaftHandlers(mux, node)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:    configuration.RaftAddr,
		Handler: mux,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.Run(ctx)

	go func() {
		logger.Printf("raft rpc server listening on %s", configuration.RaftAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("raft server error: %v", err)
			cancel()
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		logger.Printf("shutdown signal received for node=%s", configuration.NodeID)
	case <-ctx.Done():
		logger.Printf("context cancelled for node=%s", configuration.NodeID)
	}
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("server shutdown error: %v", err)
	}
}
