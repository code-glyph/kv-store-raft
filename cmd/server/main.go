package main

import (
	"context"
	"errors"
	"kv-store-raft/internal/api"
	"kv-store-raft/internal/config"
	"kv-store-raft/internal/kv"
	"kv-store-raft/internal/raft"
	"kv-store-raft/internal/storage"
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

	kvStore := kv.NewStore()
	metaStore := storage.NewMetaStore(configuration.DataDir)
	snapshotStore := storage.NewSnapshotStore(configuration.DataDir)
	logStore, err := storage.NewWAL(configuration.DataDir, 40*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	transportClient := transport.NewHTTPTransport(configuration.PeerMap)
	node, err := raft.NewNode(raft.NodeConfig{
		NodeID:           configuration.NodeID,
		Peers:            configuration.PeerMap,
		ElectionTimeout:  time.Duration(configuration.ElectionTimeout) * time.Millisecond,
		HeartbeatTimeout: time.Duration(configuration.HeartbeatTimeout) * time.Millisecond,
		SubmitTimeout:    2 * time.Second,
		MaxBatchSize:     64,
		SnapshotInterval: 1000,
		LeaseReads:       false,
		Transport:        transportClient,
		StateMachine:     kvStore,
		MetaStore:        metaStore,
		LogStore:         logStore,
		SnapshotStore:    snapshotStore,
		Logger:           logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	raftMux := http.NewServeMux()
	transport.RegisterRaftHandlers(raftMux, node)
	apiHandler := api.NewHandler(node, kvStore, configuration.PeerMap)
	api.RegisterHandlers(raftMux, apiHandler)

	apiMux := http.NewServeMux()
	api.RegisterHandlers(apiMux, apiHandler)

	raftServer := &http.Server{
		Addr:    configuration.RaftAddr,
		Handler: raftMux,
	}
	apiServer := &http.Server{
		Addr:    configuration.HTTPAddr,
		Handler: apiMux,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.Run(ctx)

	go func() {
		logger.Printf("raft rpc server listening on %s", configuration.RaftAddr)
		if err := raftServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("raft server error: %v", err)
			cancel()
		}
	}()
	go func() {
		logger.Printf("client api server listening on %s", configuration.HTTPAddr)
		if err := apiServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("api server error: %v", err)
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
	if err := raftServer.Shutdown(shutdownCtx); err != nil {
		logger.Printf("raft server shutdown error: %v", err)
	}
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		logger.Printf("api server shutdown error: %v", err)
	}
	if err := node.Close(); err != nil {
		logger.Printf("node close error: %v", err)
	}
}