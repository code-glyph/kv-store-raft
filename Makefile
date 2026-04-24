APP_NAME=kvraft
BIN_DIR=bin
BUILD_DIR=build
VERSION=1.0.0

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) cmd/server/main.go

test:
	go test -race ./...

run-node:
	./$(BIN_DIR)/$(APP_NAME) \
	--node-id $(NODE_ID) \
	--http-addr $(HTTP_ADDR) \
	--raft-addr $(RAFT_ADDR) \
	--peers $(PEERS) \
	--election-timeout $(ELECTION_TIMEOUT) \
	--heartbeat-timeout $(HEARTBEAT_TIMEOUT) \
	--data-dir $(DATA_DIR)

run-cluster:
	./scripts/run-cluster.sh 

stop-cluster:
	./scripts/stop-cluster.sh

clean:
	rm -rf $(BIN_DIR)
	rm -rf $(BUILD_DIR)