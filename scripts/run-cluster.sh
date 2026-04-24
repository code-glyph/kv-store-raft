#! /usr/bin/env bash
pkill -f kvraft || true
sleep 1
rm -rf data/n1 data/n2 data/n3
mkdir -p data/n1 data/n2 data/n3

ELECTION_TIMEOUT=1000
HEARTBEAT_TIMEOUT=50
PEERS="n1=localhost:9000,n2=localhost:9001,n3=localhost:9002"
DATA_DIR="data"

./bin/kvraft --node-id n1 --http-addr localhost:8080 --raft-addr localhost:9000 --peers $PEERS --election-timeout $ELECTION_TIMEOUT --heartbeat-timeout $HEARTBEAT_TIMEOUT --data-dir $DATA_DIR/n1 > /tmp/n1.log 2>&1 &
./bin/kvraft --node-id n2 --http-addr localhost:8081 --raft-addr localhost:9001 --peers $PEERS --election-timeout $ELECTION_TIMEOUT --heartbeat-timeout $HEARTBEAT_TIMEOUT --data-dir $DATA_DIR/n2 > /tmp/n2.log 2>&1 &
./bin/kvraft --node-id n3 --http-addr localhost:8082 --raft-addr localhost:9002 --peers $PEERS --election-timeout $ELECTION_TIMEOUT --heartbeat-timeout $HEARTBEAT_TIMEOUT --data-dir $DATA_DIR/n3 > /tmp/n3.log 2>&1 &

echo "nodes started (PIDs: $!), logs at /tmp/n1.log /tmp/n2.log /tmp/n3.log"
echo "waiting for leader election..."
sleep 5
curl -s http://localhost:8080/leader | python3 -m json.tool
