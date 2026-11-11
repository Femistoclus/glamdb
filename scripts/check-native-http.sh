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
"$python" -c 'import glamdb_sdk, httpx, pyarrow, jsonschema'
export GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1
export GLAMDB_NATIVE_SERVER="$server" GLAMDB_E2E_PYTHON="$python"
mkdir -p artifacts/native-normal
exec > >(tee artifacts/native-normal/client-browser.log) 2>&1
"$python" tests/client/cli_network_test.py "$client" "$server"
"$python" tools/workload/tests/native_e2e_test.py "$client" "$server" --report-root artifacts/native-normal/workload
# The native Playwright profile selects native.spec.mjs and starts a real server.
# Existing workbench CI separately retains all mock, session and public-demo checks.
npm --prefix ui run test:e2e
