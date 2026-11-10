#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p artifacts
.venv/bin/python -m pytest --cov=glamdb_verify --cov=glamdb_mock --cov=glamdb_sdk \
  --cov-report=term-missing --cov-report=xml:artifacts/python-coverage.xml -q
.venv/bin/python -m glamdb_verify check-fixture fixtures/golden/int64-null-duplicates.json
.venv/bin/python -m glamdb_verify check-fixture fixtures/golden/all-types-boundaries.json
.venv/bin/python -m glamdb_verify model --seed 17 --seeded 100 > artifacts/ordering-model.json
npm --prefix ui run format:check
npm --prefix ui test
npm --prefix ui run test:e2e
