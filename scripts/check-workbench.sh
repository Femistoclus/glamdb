#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

suite=${1:-all}
if [[ $# -gt 1 || ! $suite =~ ^(all|python|node|browser)$ ]]; then
  echo "Usage: $0 [all|python|node|browser]" >&2
  exit 2
fi
mkdir -p artifacts
if [[ $suite == all ]]; then
  result=0
  for part in python node browser; do
    bash scripts/check-workbench.sh "$part" || result=1
  done
  exit "$result"
fi
exec > >(tee "artifacts/$suite-tests.log") 2>&1
case "$suite" in
  python)
    .venv/bin/python -m pytest --cov=glamdb_verify --cov=glamdb_mock --cov=glamdb_sdk \
      --cov-report=term-missing --cov-report=xml:artifacts/python-coverage.xml \
      --junitxml=artifacts/python-junit.xml --tb=long -ra
    .venv/bin/python -m glamdb_verify check-fixture fixtures/golden/int64-null-duplicates.json
    .venv/bin/python -m glamdb_verify check-fixture fixtures/golden/all-types-boundaries.json
    .venv/bin/python -m glamdb_verify model --seed 17 --seeded 100 > artifacts/ordering-model.json
    ;;
  node)
    npm --prefix ui test
    npm --prefix ui run format:check
    ;;
  browser)
    npm --prefix ui run test:e2e
    ;;
esac
