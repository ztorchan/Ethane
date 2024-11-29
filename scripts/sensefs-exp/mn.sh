#!/bin/bash

set -ex

SCRIPT_DIR=$(dirname "$(realpath "$0")")
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../../")

stop_all() {
  kill $memd_pid 
}

trap stop_all EXIT

source $SCRIPT_DIR/common.sh

# init cluster zookeeper
cluster_init

# $PROJECT_DIR/build/memd -z localhost:2181 -c $SCRIPT_DIR/memd.yaml

$PROJECT_DIR/build/memd -z localhost:2181 -c $SCRIPT_DIR/memd.yaml &
memd_pid=$!

sleep 4

$PROJECT_DIR/build/format -z localhost:2181 -t $SCRIPT_DIR/fs.yaml

sleep 4

rm -rf /dev/shm/ethane-log
trace_prob_ethane_rdma_write=1048576 $PROJECT_DIR/build/logd -z localhost:2181 -c $SCRIPT_DIR/logd.yaml -t $SCRIPT_DIR/logd_cli.yaml -n 0 -g true

sleep 10