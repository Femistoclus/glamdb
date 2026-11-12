#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f tools/logging/requirements.txt ]]; then
  git submodule update --init -- third_party/quill
fi

"${GLAMDB_PYTHON:-python3.13}" -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check \
  --require-hashes --only-binary=:all: --no-deps -r tools/workload/requirements.txt
.venv/bin/python -m pip install --no-deps --no-build-isolation \
  -e tools/verification -e sdk/python -e apps/mock_server -e tools/workload
npm --prefix ui ci
printf '%s\n' 'Ready. See docs/development/WORKBENCH.md for launch and browser-test commands.'
