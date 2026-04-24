#!/usr/bin/env bash
pkill -f "kvraft" || true
rm -rf data/n1 data/n2 data/n3