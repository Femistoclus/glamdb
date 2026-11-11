# Native HTTP conformance fixtures

`v0.json` inventories routes; `v0.schema.json` defines native request and
response shapes. `health-states.json` pins readiness responses.

`outcomes-v0.json` supplies literal expectations shared by native C++, the
Python SDK, the disposable mock and the browser API tests. It covers committed
append, pre-binding rejection, registered failure, unknown publication, blocked
lookup, lost create, and A/B/retry-A. A definite failure and an unknown outcome
both use HTTP 503 in these vectors; status alone cannot resolve an operation.

Requests contain duplicate rows, NULL, int64 `9007199254740993` and
`-9223372036854775808`, and exact six-digit UTC timestamps. Canonical v1 schema
and append preimages were written independently; their literal SHA-256 values
were calculated with Python's standard-library `hashlib`, without importing the
engine, SDK, mock or verifier. Do not regenerate expected responses with the
production serializer. Receipt snapshot IDs, byte counts and creation times are
synthetic values for serializer checks.

Each `cases` entry has a name, request kind, HTTP status and complete response.
`invalid_recovery` describes one omitted or replaced field per negative case.
Every recovery field is required, including nullable candidate/base snapshot
fields. Clients reject incomplete objects and contradictory identities or
blocked-lookup flags while preserving extension fields. The two entries marked
`clients: ["python"]` check the SDK's original canonical payload/schema hashes;
the browser preserves these hashes but does not compute canonical hashes.

Consumers:

- `tests/server/outcome_conformance_test.cpp` independently constructs native
  outcomes, compares full serialized responses, round-trips stored terminal
  receipts, and checks typed rows and canonical bytes/hashes with real Arrow.
- `sdk/python/tests/test_outcome_conformance.py` parses every envelope and
  negative case through the SDK using an HTTP transport double.
- `ui/tests/unit/outcome-conformance.test.mjs` parses the same envelopes and
  applicable negative cases through `WorkbenchAPI` and checks exact row values.
- `NativeHTTP.test_route_schema_fixtures` checks literal responses and missing
  fields against the versioned JSON Schema when `GLAMDB_VALIDATE_SCHEMAS=1`.
- `apps/mock_server/tests/test_outcome_conformance.py` exercises real mock state
  and SDK parsing with the shared requests. Mock locations remain `mock://`;
  mock state has no physical publication or durability guarantee.
- `NativeHTTP.test_shared_outcome_fixtures_with_native_storage_and_sdk` uses
  the same requests/hashes over actual native HTTP, JSON/Arrow scans, historical
  and initial views, and a fresh-process reopen. Retry A returns A's full receipt
  after B and restart without adding rows or Parquet files. Native snapshot IDs,
  physical byte counts and creation times come from the engine. Scan assertions
  compare row multisets because result order across files is unspecified.

From the repository root, using Python with the workbench/native dependencies:

```sh
PYTHONPATH=sdk/python/src:apps/mock_server/src:tools/verification/src \
python -m pytest sdk/python/tests/test_outcome_conformance.py \
  apps/mock_server/tests/test_outcome_conformance.py
npm --prefix ui test
cmake --build --preset local --parallel 2
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
ctest --preset local -R 'OutcomeConformance|glamdb\.native_http$'
```

Fixture/transport-double results and disposable mock results are separate from
native storage/network evidence. These tests do not qualify physical power-loss
durability or replace native fault-injection and qualification campaigns.

## Validation recorded on 2026-11-11

Base revision: `4e53c89`. Linux x86-64, Clang/libc++ 18, pinned native dependencies
(Arrow 25), Python 3.12, Node 24.13 and Playwright 1.56.1. Local browser execution
used the existing Chromium support-library directory through `LD_LIBRARY_PATH`.

| Scope | Command | Executed result |
| --- | --- | --- |
| Native build | `cmake --build --preset local --parallel 2` | PASS |
| Native C++/storage/HTTP/SDK | `GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 ctest --preset local --parallel 2 --output-on-failure` | Zero failures out of 532 enabled cases; existing recovery child helper skipped; one additional existing cost benchmark disabled |
| Client, verifier and disposable mock | `PYTHONPATH=sdk/python/src:apps/mock_server/src:tools/verification/src python -m pytest -q --tb=short` | 550 passed; existing Starlette/httpx deprecation warning |
| Browser API units/Fetch | `npm --prefix ui test` | 106 passed |
| Native Chromium | `GLAMDB_NATIVE_SERVER="$PWD/build/local/apps/server/glamdb" npm --prefix ui run test:e2e` | 3 passed |
| Mock/static-demo Chromium | `npm --prefix ui run test:e2e` | 31 passed; 3 native-only cases excluded by their existing guard |
| C++ formatting/negative controls | `cmake --build --preset local --target format-check quality-negative-check` | PASS; 178 project files formatted correctly |
| Changed C++ analysis | `clang-tidy-18 apps/transport/outcome_json.cpp tests/server/outcome_conformance_test.cpp -p build/local --config-file=.clang-tidy --warnings-as-errors='*'` | PASS; vendor warnings suppressed by the project header filter |
| Workbench formatting | `npm --prefix ui run format:check` | PASS |

Python commands used the existing environment with native/workbench dependencies;
browser runs selected it with `GLAMDB_E2E_PYTHON`. SDK/schema dependencies were
required in the reported native run. A separate system-Python negative control
confirmed the ordinary optional-SDK guard; it is not native SDK evidence.
Full-tree clang-tidy, sanitizer profiles and physical power-loss tests were not run
for this change.
