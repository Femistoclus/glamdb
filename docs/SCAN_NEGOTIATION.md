# Native scan pushdown negotiation

Contract 1.3 advertises `ScanNegotiation` / `NegotiateScan` for native append and
KV scans. The installed `storage/scan_negotiation.h` provides the pure function
`NegotiateScan(schema, request, resources = {})`; capability discovery is separate
from readiness, authorization and scan admission. Earlier contract versions keep
their previous feature/operation sets. This is a native C++ source/compute seam,
not a DataFusion, Flight, remote descriptor or stable ABI implementation.

## Expressions and types

Each `TScanExpression` has a unique nonempty `ID`, immutable caller-owned opaque
`Expression`, complete stable `Fields` and optional typed `Translated` predicate.
The request's `Conjuncts` are independent top-level AND conjuncts. A translator
must never push one branch of a partially translated OR. Keep that entire OR
opaque unless an equivalent or proven necessary condition can be supplied.

The trusted adapter asserts `Exact` equivalence or `NecessaryCondition` implication:
whenever the original expression is SQL TRUE the translated predicate must also
be TRUE. Storage cannot prove semantics of an opaque expression. In particular,
casts, functions/UDFs and nonbinary collations are not translated by storage.
They stay opaque unless the adapter can make the stated semantic assertion.

Storage reuses the existing typed binder, exact evaluator and conservative
file/row-group pruning. `Exact` discharges a conjunct; `PruningOnly` accepts a
necessary condition for pruning **and row prefiltering**, always retaining the
original expression as a residual. `Unsupported` leaves the complete expression
in compute. A bad typed translation returns `TranslationError` and remains
residual; no implicit cast is introduced. Unknown/mismatched input field identity,
duplicate IDs, malformed enums/limits/projections and structurally oversized trees
fail the request. A conjunct that would exceed the combined scanner predicate
limits remains an unsupported residual, leaving previously accepted conjuncts.

Field IDs are fixed schema positions starting at 1. Every supplied reference's
`FieldSpec` must match name, type, nullability, decimal precision and scale. The
existing Predicate uses the corresponding exact schema names; each translated
reference must occur in that expression's declared fields. Use the same captured
read-view schema for negotiation and execution; this pure result is not a schema
or authorization token. SQL three-valued NULL semantics keep only TRUE. Strings
compare binary UTF-8 bytes, including embedded NUL; there is no locale collation.
Decimal128 uses exact coefficients with identical precision/scale, dates are
strict calendar days, timestamps are UTC integer microseconds in years 0001–9999.
Float/integer/date/decimal/timestamp literals are never implicitly converted.

## Projection, execution and limits

`Projection` is a vector of stable IDs, `nullopt` for all fields or empty for zero
final columns. `ComputeFields` declares additional inputs for operators such as
ORDER BY. Negotiation appends all residual and compute inputs to the storage
projection, retaining requested field order. `StorageFieldIDs` identifies scan
columns; `FinalProjection` maps the final output to their indices. Compute must
filter residuals and perform its operators before removing helper columns. Empty
final projection still returns the surviving row count. Exact filter-only fields
stay internal to the scanner and need not appear in storage output.

`Scan` is directly usable with `ScanReadView` or `PlanScan`. `ResidualIDs` and the
original objects in `Expressions` identify what compute must evaluate. Negotiate
with `Execution::Split` before split planning: even LIMIT 0 stays in compute;
no global limit may be applied independently to workers. Serial unordered scans
push a limit only when every conjunct is exact. Otherwise `ComputeLimit` retains
it and `Limit` explains placement (`Residual`, `Split` or `Ordered`; ordering takes
precedence). `Order::Unsupported` explicitly rejects storage ordering/Top-N even
without a limit. Compute must sort, then apply its global limit. No physical scan
order is promised. Switching execution modes requires renegotiation.

## Statistics and budgets

`MetricKind` describes provenance, not scalar-bound exactness. Selected-view
record/file counts derived from complete committed descriptors are exact metadata
counts; estimates such as physical split size or pruning work are not result
cardinality. Present optional field counts are recorded metadata counts; absent
counts are unknown. Do not manufacture zero from absence.

Public manifest and Parquet bounds are advisory encoded bytes with no exposed
exactness flag. String prefixes may be truncated; long-string bounds may be
missing. Validate types/encoding and truncation semantics before using conservative
bounds, and otherwise treat them as unknown. They are never exact min/max scalars,
equality proofs, aggregate substitutions or a reason to remove a residual.
The internal pruner additionally validates malformed/contradictory bounds and
uses private exactness flags conservatively.

The planner admits 4 MiB metadata plus four times the measured translated
predicate capacities (aggregate maximum 16 MiB), using the supplied owner or the
finite process default. IDs are at most 256 bytes, expressions at most 64,
references/compute fields/projection at most 128. Returned results, extracted
expressions and predicates retain the reservation. Host-owned opaque compute
payloads and independent deep DTO copies remain host-budgeted. Executing a scan
uses its existing separate resource admission. See [shared resources](SHARED_RESOURCES.md).
