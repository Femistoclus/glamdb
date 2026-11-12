#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 2 ]]; then
  echo "Usage: $0 NATIVE_BUILD_DIRECTORY STANDALONE_CLI" >&2
  exit 2
fi
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
python="${GLAMDB_NATIVE_PYTHON:-$root/.venv/bin/python}"
build=$(realpath -- "$1")
client=$(realpath -- "$2")
server="$build/apps/server/glamdb"
[[ -x $server && -x $client ]] || { echo 'Native server or standalone CLI is missing' >&2; exit 1; }
"$python" -c 'import glamdb_sdk, httpx, pyarrow, jsonschema, pytest, fastavro'
export GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1
export GLAMDB_TEST_BACKEND=native
export GLAMDB_NATIVE_SERVER="$server" GLAMDB_E2E_PYTHON="$python"
mkdir -p artifacts/native-normal
exec > >(tee artifacts/native-normal/client-browser.log) 2>&1
"$python" tests/client/cli_network_test.py "$client" "$server"
export GLAMDB_CLICKBENCH_CATALOG="$root/artifacts/native-normal/clickbench-reference"
if [[ ! -f "$GLAMDB_CLICKBENCH_CATALOG/material.json" ]]; then
  GLAMDB_PYTHON="$python" "$client" bench clickbench fetch-queries --output "$GLAMDB_CLICKBENCH_CATALOG"
fi
"$python" tools/workload/tests/native_e2e_test.py "$client" "$server" --report-root artifacts/native-normal/workload
"$python" -m pytest tests/integration/test_network_sdk.py -q \
  --junitxml=artifacts/native-normal/sdk-conformance-junit.xml
# Native and mock run the shared contract; backend-specific scenarios stay separate.
npm --prefix ui run test:e2e
