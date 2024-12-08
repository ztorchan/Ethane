#!/bin/bash

set -ex

SCRIPT_DIR=$(dirname "$(realpath "$0")")
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../../")

rm -rf /dev/shm/ethane-log
trace_prob_ethane_rdma_write=1048576 $PROJECT_DIR/build/logd -z localhost:2181 -c $SCRIPT_DIR/logd.yaml -t $SCRIPT_DIR/logd_cli.yaml -n 1 -g true 
