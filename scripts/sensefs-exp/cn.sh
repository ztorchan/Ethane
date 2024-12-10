#!/bin/bash

set -ex

SCRIPT_DIR=$(dirname "$(realpath "$0")")
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../../")

stop_all() {
  kill $launch_id 
}

trap stop_all EXIT

source $SCRIPT_DIR/common.sh

cluster_clear_ready
cluster_disable

# bench
$PROJECT_DIR/build/launch -t $SCRIPT_DIR/cli.yaml -z localhost:2181 -n 1 -c 1 -l $PROJECT_DIR/build/libbench.so &
launch_id=$!

cluster_wait_ready 1

cluster_enable

wait $launch_id
