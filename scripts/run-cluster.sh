#! /usr/bin/env bash
set -e # exit on error
mkdir -p data/n1 data/n2 data/n3

ELECTION_TIMEOUT=1000
HEARTBEAT_TIMEOUT=50
PEERS="n1=localhost:9000,n2=localhost:9001,n3=localhost:9002"
DATA_DIR="data"

# run the nodes
./bin/kvraft --node-id n1 --http-addr localhost:8080 --raft-addr localhost:9000 --peers $PEERS --election-timeout $ELECTION_TIMEOUT --heartbeat-timeout $HEARTBEAT_TIMEOUT --data-dir $DATA_DIR/n1 &
./bin/kvraft --node-id n2 --http-addr localhost:8081 --raft-addr localhost:9001 --peers $PEERS --election-timeout $ELECTION_TIMEOUT --heartbeat-timeout $HEARTBEAT_TIMEOUT --data-dir $DATA_DIR/n2 &
./bin/kvraft --node-id n3 --http-addr localhost:8082 --raft-addr localhost:9002 --peers $PEERS --election-timeout $ELECTION_TIMEOUT --heartbeat-timeout $HEARTBEAT_TIMEOUT --data-dir $DATA_DIR/n3 &

# wait for the nodes to start
wait

# print the nodes
echo "nodes started"