#!/bin/bash

set -ex

SCRIPT_DIR=$(dirname "$(realpath "$0")")
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../../")

source $SCRIPT_DIR/common.sh

# bench
$PROJECT_DIR/build/launch -t $SCRIPT_DIR/cli.yaml -z localhost:2181 -n 32 -c 4 -l $PROJECT_DIR/build/libbench.so
