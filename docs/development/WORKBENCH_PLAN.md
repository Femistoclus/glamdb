# GlamDB workbench and independent verification: implementation plan

> Execute the checked tasks continuously. The user has explicitly requested
> autonomous implementation, tests, fork pushes and an upstream main PR.
> Questions are recorded in OPEN_QUESTIONS.md, not used as approval checkpoints.

**Goal:** deliver the participant-four fixtures/oracles/fault-evidence tooling,
network SDK and complete configurable pink browser workbench, with explicit
mock adapters for the unimplemented storage/coordinator/HTTP dependencies.

**Architecture:** keep the existing C++23 core build independent of Python/UI.
Use a separate loopback-only development mock server, real HTTP + Arrow IPC,
an independently packaged Python client, and a browser UI served directly from
editable source files. Use configuration read at runtime for visual tokens,
branding, labels, decoration and layout preferences. Never represent a mock
receipt as evidence of C++ or power-loss durability.

**Tech stack:** Python 3.13, pytest, httpx, PyArrow, FastAPI/Uvicorn for the mock
adapter; dependency-free browser HTML/CSS/ES modules; Node test runner and
Playwright browser tests. The C++ API/catalog/coordinator and their Linux build
remain independent of client/mock dependencies.

**Spec:** glamdb-rfc README, PERSON_4_SDK_UI, CONTRACTS C-TYPES/C-RESULT/C-API/
C-PAGES, ACCEPTANCE T-TYPES/T-SCAN/T-NET/T-HANDOFF, EXTENSIONS X-NET; repository
AGENTS.md and docs/ARCHITECTURE.md. The referenced RFC checkout is an input, not
a runtime dependency of installed clients.

## Scope rulings

- R1: the initial base had no usable core/HTTP API. Rebase on PR #5 preserves its
  C++ facade/catalog/coordinator; production files/scanner/HTTP remain pending. Implement
  the complete requested integrated path with a clearly marked mock seam for
  roles 1–3, as explicitly requested for missing collaborators' implementations.
  Real fixtures, SDK, UI, transport/IPC tests and evidence validation are not mocks.
- R2: the mock is not a Python storage writer: it never opens a warehouse,
  Parquet catalog or production C++ database. Its synthetic state is disposable
  and used only for development/testing. No PyIceberg writer is introduced.
- R3: append mode preserves duplicates. No implicit upsert/delete/compaction,
  early 202, uniqueness or tested durability capabilities.
- R4: no request is made to approve design/implementation between tasks; the user
  delegated those decisions. Record assumptions/questions rather than guessing
  silently. Do not merge the PR or run destructive power tests.
- R5: the latest repository explicitly reserves a browser UI and npm tests.
  Plain ES modules are used for that browser code; backend/verification/SDK are
  Python and the existing engine remains C++. No Rust or alternative writer.
- R6: macOS validates clients and the historical scaffold baseline, but cannot qualify the
  LINUX-LOCAL-1 power-loss profile. NOT RUN is a first-class report status.

## Ownership and exact boundaries

- Verification track: tools/verification/, fixtures/golden/, tests/verification/.
  Python package glamdb_verify provides independent canonical encoding, schema
  validation, UTC microsecond parsing, SQL 3VL row oracle and fault evidence checks.
- SDK track: sdk/python/, package glamdb_sdk. Only public /v0 HTTP endpoints;
  caller-provided UUIDs, original three-way outcomes, context-managed Arrow stream.
- Mock adapter track: apps/mock_server/, package glamdb_mock. Supplies complete
  contract responses, deterministic snapshots/pagination and development-only
  fault scenarios. It exposes /workbench/config and serves ui/ assets.
- Main track: ui/, root test orchestration/CI/docs, integration/browser review,
  fork/branch/commit/PR. Avoid editing independent packages until their owner
  finishes; resolve interfaces through the contract below.

## Adapter contract (extension N, versioned /v0)

Requests use Authorization: Bearer TOKEN. Bind to loopback by default; a supplied
token is mandatory. UI stores the token only in memory, never URL/localStorage.

- GET /v0/capabilities: mode, backend=mock, mock=true, durability_validation=designed,
  key_unique=false, cow/compaction/async_admission/page_pruning/bloom_pruning=false.
- GET /v0/collections: Page{items,next_cursor}; POST /v0/collections with
  collection_id, name, schema{fields:[{name,type,nullable}]}. IDs supplied by caller.
- GET /v0/collections:resolve?name=; GET /v0/collections/{id}; GET .../schema;
  GET .../stats?snapshot_id=; GET .../snapshots; GET .../files; GET .../files/{id}.
- POST /v0/collections/{id}/append with operation_id, expected_snapshot (optional;
  null means NoSnapshot), rows. Canonical hash is verified using RFC encoding.
  Response: outcome=committed + result; definitely_not_committed + error; or
  commit_unknown + operation_id/recovery_required. Retry same ID returns the
  original result even after a later append; different hash conflicts.
- GET .../operations/{operation_id}: immutable terminal outcome or in_progress/
  commit_unknown; 404 only for absent operation.
- POST /v0/collections/{id}/scan with projection, predicate, snapshot_id, limit:
  JSON response schema/snapshot/rows/profile for UI, or Accept Arrow IPC stream
  for SDK. Resolve latest once. This mock synchronous scan is explicitly not an
  async production query service and needs no job queue.
- POST .../explain: schema, fixed identity, read/output columns, conservative plan
  and capabilities, no invented actual IO or speedup.
- Pagination tokens bind query parameters, collection/view and position; mock
  snapshots remain retained. Results include precise int64 and UTC timestamps.
- GET /workbench/config: reload ui/config/theme.json (or explicit --theme-file)
  every request; validate values and reject unsafe/malformed config. UI keeps
  last valid theme and reports reload errors. No server binary rebuild.
- Static /, /assets/*: only UI root, no arbitrary path access. Health endpoint
  explicitly identifies mock mode. Development faults only under explicit
  CLI/test configuration, never a production default/claim.

The mock's extra collection/scan endpoint shapes are provisional adapter
contracts for N, not an unauthorized change to the future public C++ API.
Examples are shipped in this checkout so another team can replace the adapter.

## Review focus

1. int64 >2^53 and UTC year boundaries survive JSON/Arrow/browser round trips.
2. Unknown/lost-response retry does not create a new operation or report failure.
3. Snapshot switching, repeated submits and late responses cannot display/write
   data for another collection/view.
4. Early/failed Arrow consumption closes resources and does not claim checksum
   verification; mock execution never becomes production durability evidence.
5. Runtime theme edits update the UI without restart, invalid CSS/config does
   not execute code or erase the last valid theme, reduced motion is respected.

## Tasks and verification

### 1. Baseline and setup
- [x] Record upstream revision, fork origin/upstream and branch; read all guides.
- [x] Run existing debug build/smoke before changes. Install missing test/build
      tools in a local venv, keep dependencies outside core and tracked artifacts.
- [x] Pin reproducible Python dependencies and npm Playwright dependencies.

### 2. Independent golden fixtures and oracle
- [x] Write failing tests for exact canonical bytes/hash, schema IDs, timestamp
      min/max/negative epoch/offset overflow, -0 normalization and duplicates.
- [x] Implement fixture generation/validation independent of future C++ code.
- [x] Implement typed predicate 3VL oracle, projection/limit/fixed-view cases.
- [x] Implement independent ACK evidence ledger/checker and deterministic fault
      schedules. Model/process/real-power evidence must remain distinct.
- [x] Run tests and generate checked-in small golden vectors; no fake PASS.

### 3. Network client
- [x] Write transport/outcome/codec tests before SDK implementation.
- [x] Implement collections/schema/stats/history/files/append/find-outcome/scan.
- [x] Preserve operation identity on retry, distinguish client timeout from
      failed/unknown outcomes, enforce caller deadlines and TLS verification.
- [x] Provide context-managed Arrow reader; close on early exit/error; validate
      complete downloads and checksum/byte count where supplied.
- [x] Run isolated tests against mock transport and then actual HTTP integration.

### 4. Replaceable mock adapter
- [x] Write tests for append duplicates, original outcomes, fixed snapshots,
      correct counts, cursor parameter mismatches and resource/body limits.
- [x] Implement disposable mock state and real Arrow IPC responses, auth,
      invalid inputs, not-found/unknown scenarios and static asset confinement.
- [x] Runtime config loading stays separate from engine/mock state.
- [x] Label capabilities/metadata/profile as synthetic; no durability assertion.

### 5. Pink browser workbench
- [x] Write unit tests for state transitions, codecs and safe theme application.
- [x] Implement connection/auth, collection sidebar/create, overview, schema,
      data grid, projection/filter builder, key search as a scan, append/import,
      immutable snapshot selector, files/row groups, Explain and operation status.
- [x] Use a polished fuchsia/rose palette, editorial typography, bows/sparkles/
      kisses as configurable decoration, strong readable text and responsive layout.
- [x] Move colors, sizing, typography, radii, shadows, branding, text labels and
      decoration switches into hot-loaded JSON/CSS tokens. Keep all assets local.
- [x] Keep navigation/session state and credentials only in memory. Cancel
      stale fetches, clear mismatched state and prevent duplicate mutation submits.
- [x] Expose backend/validation state prominently and disable unsupported actions.

### 6. Integration and maximum available testing
- [x] Python unit suite with coverage; Node UI unit suite.
- [x] Real localhost HTTP: SDK create/append/old retry/history/JSON↔Arrow/early close.
- [x] Playwright Chromium: login, create/append/filter/history/schema/files/Explain,
      unknown/error states, runtime theme edit, mobile viewport, keyboard use,
      reduced-motion and page/console errors.
- [x] Operate real Chromium through Playwright and inspect desktop/mobile screenshots; CUA host unavailable, recorded in validation.
- [x] CMake debug/release/sanitizer scaffold and C++ quality where tools available.
- [x] Report actual PASS/FAIL/NOT RUN. C++ storage qualification/S3/real power cuts
      remain NOT RUN until an actual core and dedicated environment exist.

### 7. Documentation, review and PR
- [x] Update AGENTS/README/architecture/roadmap/build/status for implemented tools.
- [x] Save assumptions/questions, exact commands, evidence and screenshots paths.
- [x] Fresh whole-branch review, fix actionable issues and rerun affected tests.
- [x] Commit/push only to nnrpi fork branch; PR to glamourdb/glamdb:main with
      explicit mock boundary, validation and remaining integration gates. Do not merge.

## Completion boundary

Checked tasks mean the implemented helper/client/mock scope, with explicit
NOT RUN for real C++ and physical qualification as required above. They do not
close G1–G4 or production N. Source-served CSS/ES modules need no frontend build.
The workbench keeps tokens and navigation state only in tab memory.
Detailed commands/results are in WORKBENCH_VALIDATION.md; owner context and
remaining integration duties are in OWNER_HANDOFF_RU.md.

Published for review: https://github.com/glamourdb/glamdb/pull/4. The branch
remains in the nnrpi fork; no merge or upstream branch was performed.
