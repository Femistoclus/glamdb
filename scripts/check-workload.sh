#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

mode=${1:-native}
if [[ $# -gt 1 || ! $mode =~ ^(unit|native)$ ]]; then
  echo 'Usage: scripts/check-workload.sh [unit|native]' >&2
  exit 2
fi
python="${GLAMDB_WORKLOAD_PYTHON:-$PWD/.venv/bin/python}"
"$python" -m pytest tools/workload/tests -q
if [[ $mode == native ]]; then
  client="${GLAMDB_CLI:-$PWD/build/local/apps/cli/glamdb-cli}"
  server="${GLAMDB_NATIVE_SERVER:-$PWD/build/local/apps/server/glamdb}"
  [[ -x $client && -x $server ]] || {
    echo 'Build the native server and CLI, or set GLAMDB_NATIVE_SERVER and GLAMDB_CLI. No mock fallback is used.' >&2
    exit 2
  }
  "$python" tools/workload/tests/bridge_test.py "$client"
  export GLAMDB_CLICKBENCH_CATALOG="${GLAMDB_CLICKBENCH_CATALOG:-$PWD/artifacts/clickbench-reference}"
  if [[ ! -f "$GLAMDB_CLICKBENCH_CATALOG/material.json" ]]; then
    GLAMDB_PYTHON="$python" "$client" bench clickbench fetch-queries --output "$GLAMDB_CLICKBENCH_CATALOG"
  fi
  "$python" tools/workload/tests/native_e2e_test.py "$client" "$server"
fi
