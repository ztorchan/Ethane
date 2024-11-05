#!/bin/bash

set -ex

SCRIPT_DIR=$(dirname "$(realpath "$0")")
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../../")

source $SCRIPT_DIR/common.sh

# init cluster zookeeper
cluster_init