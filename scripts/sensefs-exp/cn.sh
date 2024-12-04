set -ex

SCRIPT_DIR=$(dirname "$(realpath "$0")")
PROJECT_DIR=$(realpath "$SCRIPT_DIR/../../")

stop_all() {
  kill $logd_id $launch_id 
}

trap stop_all EXIT

source $SCRIPT_DIR/common.sh

cluster_clear_ready

# logd
rm -rf /dev/shm/ethane-log
trace_prob_ethane_rdma_write=1048576 $PROJECT_DIR/build/logd -z localhost:2181 -c $SCRIPT_DIR/logd.yaml -t $SCRIPT_DIR/logd_cli.yaml -n 0 -g false &
logd_id=$!

sleep 4

# bench
$PROJECT_DIR/build/launch -t $SCRIPT_DIR/cli.yaml -z localhost:2181 -n 4 -c 1 -l $PROJECT_DIR/build/libbench.so &
launch_id=$!

cluster_wait_ready 1

cluster_enable

wait $launch_id
