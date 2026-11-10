# GlamDB independent verification tools

`glamdb-verify` is an installable, standard-library-only Python package for the
append-v0 contracts. It contains executable fixtures, a row oracle, controller
receipt checks, schedules, and a small ordering model. It imports no engine,
mock server, Arrow, Iceberg, Parquet, SQLite, or SDK implementation.

Run from the repository root with the development environment:

```sh
python -m pip install -e tools/verification
python -m pytest tests/verification
python -m glamdb_verify check-fixture fixtures/golden/int64-null-duplicates.json
python -m glamdb_verify check-fixture fixtures/golden/all-types-boundaries.json
python -m glamdb_verify generate --seed 17 --rows 16 --output /tmp/glamdb-fixture.json
python -m glamdb_verify check-fixture /tmp/glamdb-fixture.json
python -m glamdb_verify schedule --seed 17 --seeded 100
python -m glamdb_verify model --seed 17 --seeded 100
```

An uninstalled checkout works by prefixing commands with
`PYTHONPATH=tools/verification/src`. The `glamdb-verify` entry point accepts the
same arguments as `python -m glamdb_verify`. Check commands emit JSON and exit
nonzero on failure. No command cuts power, kills a process, opens an engine store,
or writes a warehouse.

## Canonical fixtures and native rows

The frozen public API exports `normalize_schema`, `normalize_rows`, `wire_rows`,
`canonical_append`, `append_hash`, `schema_fingerprint`, `parse_timestamp`,
`format_timestamp`, and `scan_rows`. Invalid input raises `ValueError`.

`normalize_schema(schema)` returns an owned schema with `schema_id` defaulting
to zero and fields `{field_id, name, type, nullable}`. Initial fixed-schema IDs
must be consecutive in declaration order starting at one. It validates 1–64
fields, unique UTF-8 names of at most 128 bytes, supported types, and the required
string `key` rule. Omitted `nullable` defaults to false. This validates a complete
collection schema; projected/reordered output schemas may retain sparse stable
IDs and should not be passed back as collection-creation schemas.

`normalize_rows(schema, rows)` returns owned dictionaries, retaining input order
and duplicate rows. Int64 and timestamp values become Python integers, finite
float64 values become floats, and nullable omissions become `None`. Int64 accepts
native integers and decimal strings; timestamps accept native microseconds,
RFC3339 strings, or aware Python datetimes. Naive datetimes, booleans masquerading
as integers, unknown fields, invalid UTF-8, out-of-range values, and NaN/Inf are
rejected. Float64 accepts JSON numeric integers as well as floats, without string
coercion, and normalizes negative zero. Key strings are at most 1024 UTF-8 bytes;
other strings at most 1 MiB; a canonical decoded row at most 2 MiB.

`wire_rows` converts int64 to decimal strings and timestamps to exactly six UTC
fractional digits while retaining all field names. Wire a complete row with its
complete schema before applying a reordered/empty projection.

`parse_timestamp` uses integer calendar arithmetic, validates the UTC range after
subtracting the offset, and never rounds. Years 0001–9999, negative epoch values,
microsecond precision, leap seconds, overprecision, mandatory zones, and offset
overflow have literal tests. Timestamp bounds are
`[-62135596800000000,253402300799999999]` inclusive. `format_timestamp` always
produces a four-digit year and six fractional digits followed by `Z`.

`canonical_schema` exposes the exact schema preimage; `schema_fingerprint`
returns its SHA-256 as 32 bytes. `canonical_append` returns the RFC v1 bytes;
`append_hash` returns lowercase SHA-256 hex. Append requires 1–10,000 rows and
at most 16 MiB of canonical decoded values. Row/append byte limits include value
presence bytes, string lengths, and encoded values, excluding the append header.
`normalize_rows` has no append-wide cap so accumulated historical scan fixtures
can exceed a single append. Canonical encoding has no sorting, deduplication,
Unicode normalization, Arrow chunk boundaries, operation ID, or snapshot selector.

The checked-in JSON fixtures and adjacent `.hex` files cover all five types,
min/max timestamps and int64 values, int64 above 2^53, nulls, UTF-8, negative zero,
and duplicate rows. Their schema/row preimages were written explicitly and their
digests computed with a separate `hashlib` calculation without importing this
package. Tests contain a second literal copy of the minimal vector, so changing
the encoder cannot silently regenerate its expectation. The generator produces
synthetic fixtures using an explicitly specified 32-bit LCG; a fixed seed/count
produces identical JSON values without wall-clock time or random UUIDs.

`fixtures/metadata/file-metrics.json` supplies additional expected writer/manifest
inputs: keys with a common ASCII prefix longer than 16 bytes, a 1024-byte UTF-8
key, binary ordering without Unicode normalization, truncated footer bounds,
and missing footer statistics. Expected full key bounds/counts and matching
rows are explicit and checked independently. No physical manifest is generated
or validated here; feed these inputs to the future native writer's T-METRICS
harness and compare actual manifests, stable field IDs and pruning on/off.

## Predicate oracle

`scan_rows(schema, rows, projection, predicate=None, limit=None)` returns native
owned rows and preserves duplicates/order. `projection=None` selects all columns;
`[]` retains matching row cardinality as empty dictionaries. Limits apply after
exact filtering. Projection and the complete predicate are validated even for an
empty input or `limit=0`.

Predicates are JSON objects with `op`:

- `eq`, `ne`, `lt`, `le`, `gt`, `ge`: `field`, `value`.
- `in`: `field`, array `values`.
- `is_null`, `is_not_null`: `field`.
- `and`, `or`: nonempty array `args`.
- `not`: one `arg`.

Comparison literals follow the field's exact wire/native value rules; there are
no bool-to-int or string-to-float casts. NULL comparisons and `NOT UNKNOWN` remain
UNKNOWN; only TRUE survives. Empty IN is FALSE for non-null and UNKNOWN for NULL.
IN containing NULL follows SQL 3VL. Strings use binary UTF-8 ordering, with no
Unicode normalization. Tests exercise all nine AND/OR truth-table combinations,
NOT, nullable IN, filter-only columns, projection, empty input, and limits.

This is an independent expected-result oracle for a future engine. The current
development mock deliberately reuses it as its evaluator. Mock tests therefore
do not constitute an independent comparison against a C++ scanner or pruning.

## Receipts, fault schedules, and evidence

`validate_receipt` checks the full C-RESULT AppendResult shape, UUIDs, supported
hash/receipt version, digest, exact counters, one-file append mode, and retained
snapshot creation time in whole milliseconds. `canonical_receipt` is the
controller's deterministic JSON serialization; it makes no claim to define the
future engine's persisted receipt bytes.

`AckLedger(path, test_volume=..., evidence_kind=...)` creates a JSONL ledger on an
existing controller directory outside the selected test-volume subtree. Each
record has a sequence and SHA-256 and is flushed and fsynced; ledger creation and
every reopen sync the parent directory before accepting receipts or writes.
A directory-sync error rejects the open, and a later attempt fences again even
if the header already exists. Reopening checks every record, rejects duplicate
operation identities, truncation/checksum damage, and evidence-label changes.
There is one serialized controller writer. Record an operation's ACK once; log
repeated retry/transport events in a separate trace. Symlink aliases into the cut
volume are rejected. Separate path names alone cannot establish independent
physical controller storage; a real campaign must verify that separately.

`check_recovery(acked, recovered)` checks that every ACKed operation occurs
exactly once and has the same full receipt. Unacknowledged complete commits are
allowed; malformed/duplicate recovered receipts are rejected. This is receipt
verification. An engine harness must also read and compare the actual snapshot
rows/files to exclude partial data or forged receipts.

`DisposableRun(parent)` creates a unique marked synthetic directory and cleans
up only that directory. Cleanup refuses a replaced/symlinked directory or changed
marker. It never accepts an existing store as its disposable target.

`fault_schedule(seed, seeded=100)` emits 104 explicit before/after cut points
covering four file classes, SQLite/publication, response, and ACK, followed by 100
reproducible seeded choices by default. A schedule is an input, not an executed
fault campaign.

`run_power_model` executes these cuts against separate volatile/durable file data
and namespace state, discards volatile state at each cut, and checks publication
closure and ACK survival. It must detect three deliberately broken variants:
omitted data sync, omitted directory sync, and premature ACK. **The catalog commit
is assumed to be one atomic durable primitive.** This helper does not emulate
SQLite page/journal writes, real POSIX faults, process death, or hardware flushes;
it does not satisfy the full unified-image T-POWER model or close a release gate.

`check_evidence(report)` and `check-evidence FILE` accept version 1 reports with
`evidence_kind`, `status`, `environment`, and `cycles`. Each executed cycle has an
integer `seed`, a `cut_point`, `acked` receipts, and `recovered` receipts. Labels
are exactly `modeled-power`, `process-crash`, and `real-power`; statuses are
`PASS`, `FAIL`, and `NOT RUN`. A minimal honest unexecuted report is:

```json
{
  "evidence_version": 1,
  "evidence_kind": "real-power",
  "status": "NOT RUN",
  "environment": {},
  "cycles": []
}
```

Model/process evidence always leaves `power_durability` as `NOT VALIDATED`.
Real-power structural checking requires at least 20 cycles, the LINUX-LOCAL-1
Linux/x86-64/ext4 profile, device/firmware/kernel/flush/cache/mount/controller
details, an explicit disjoint-controller declaration, and a raw image/trace
path-and-SHA256 index. Even then it returns `REVIEW REQUIRED`, never a durability
PASS: declarations cannot prove hardware behavior, external-controller durability,
artifact authenticity, or real execution. Every result retains an OPEN acceptance
gate. No actual real-power or C++ process-crash evidence is shipped here.

## Open handoff questions

- The RFC fixes canonical schema/append bytes but does not fix an engine receipt
  serialization. The ledger's JSON v1 must be adapted to the real coordinator's
  authoritative canonical bytes, then tested with old retries/recovery.
- Which real engine/IO seams will map the emitted cut-point names to executed
  faults? Until that adapter exists, schedules and the simplified ordering model
  remain helper-level evidence; process/real-power integration is NOT RUN.
- Which qualified dedicated device and independent durable controller will supply
  real-power receipts and raw images? No such environment was selected or tested.
- Creation-schema normalization defaults omitted nullable to false, rejects empty
  names, and validates consecutive initial IDs. Projected result schemas retain
  their original IDs and use full-schema conversion before projection. Confirm
  these adapter choices at the real public API handoff.
