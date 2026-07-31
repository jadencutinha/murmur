#!/usr/bin/env bash
# demo.sh — spin up a local 3-node murmur cluster and drive it with the CLI.
#
# Builds the release binaries, launches three `murmurd` nodes that replicate a
# key/value store over Raft, then runs a sequence of `murmur` client commands
# showing writes replicating and reads served through consensus. Optionally runs
# the benchmark. Everything is torn down and the temp data dir removed on exit.
#
#   ./demo.sh            # run the scripted demo
#   ./demo.sh --bench    # ...and then a short benchmark

set -euo pipefail
cd "$(dirname "$0")"

PEERS="127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003"
DATA="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$DATA"
}
trap cleanup EXIT

echo "==> building release binaries"
cargo build --release --quiet --bins

start_node() {
  local id="$1" raft="$2" kv="$3"; shift 3
  ./target/release/murmurd --id "$id" \
    --raft-listen "$raft" --kv-listen "$kv" --data "$DATA/node$id" "$@" \
    >"$DATA/node$id.log" 2>&1 &
  PIDS+=("$!")
  disown %% 2>/dev/null || true  # keep the shell from printing "Terminated" on teardown
}

echo "==> launching a 3-node cluster (logs in $DATA)"
start_node 1 127.0.0.1:6001 127.0.0.1:5001 --peer 2@127.0.0.1:6002 --peer 3@127.0.0.1:6003
start_node 2 127.0.0.1:6002 127.0.0.1:5002 --peer 1@127.0.0.1:6001 --peer 3@127.0.0.1:6003
start_node 3 127.0.0.1:6003 127.0.0.1:5003 --peer 1@127.0.0.1:6001 --peer 2@127.0.0.1:6002

echo "==> waiting for a leader to be elected"
sleep 2

cli() { ./target/release/murmur --peers "$PEERS" "$@"; }

echo
echo "==> put color=amber   (routed to the leader, replicated to a majority)"
cli put color amber
echo "==> get color         (linearizable read through the log)"
cli get color
echo "==> append log 'hello '   then   append log 'world'"
cli append log "hello "
cli append log "world"
echo "==> get log"
cli get log
echo "==> del color, then get color (should be absent)"
cli del color
cli get color || echo "   -> not found, as expected"

if [[ "${1:-}" == "--bench" ]]; then
  echo
  echo "==> benchmark"
  ./target/release/murmur-bench --peers "$PEERS" --clients 8 --ops 200
fi

echo
echo "==> demo complete; tearing down the cluster"
