# Native DECIMAL and DATE

Collections support `decimal` with immutable precision 1..38 and scale
0..precision, and Gregorian `date` from `0001-01-01` through `9999-12-31`.
These complement the existing string, bool, int64, float64 and UTC microsecond
timestamp types. Field IDs and append semantics are unchanged.

```json
{"fields":[
  {"name":"amount","type":"decimal","precision":15,"scale":2,"nullable":true},
  {"name":"day","type":"date","nullable":true}
]}
```

Decimal fields require both integer parameters; other types reject those
parameters. A coefficient must satisfy `abs(coefficient) < 10^precision`.
Scale is part of the type. Neither append nor predicate binding silently casts
an Arrow decimal with different parameters. DATE does not accept timestamps,
epoch-microsecond integers or a timezone.

## Values and predicates

JSON decimal values are strings with grammar `-?[0-9]+(\.[0-9]+)?`, at most
128 characters. Binary floats, exponents, plus signs, whitespace, infinity and
NaN are rejected. Normalization removes redundant leading zeros and negative
zero and pads to the declared scale. Extra fractional digits are accepted only
when all discarded digits are zero: decimal(15,2) accepts `"0012.3400"` as
`"12.34"`, but rejects `"12.341"`. Overflow is an input error before operation
binding. JSON output always uses fixed-scale text, including scales above six.

Dates use exactly `YYYY-MM-DD`. Calendar validity includes Gregorian leap-year
rules: `2000-02-29` is valid, `1900-02-29` is invalid. Omitted nullable fields
become NULL; required fields reject missing and NULL values.

Typed JSON predicate literals identify their type explicitly:

```json
{"op":"eq","field":"amount","value":
  {"type":"decimal","precision":15,"scale":2,"value":"12.34"}}
```

DATE uses `{"type":"date","value":"2000-02-29"}`. Compare and IN use exact
coefficient/day ordering. Decimal literals must match both schema parameters.
Raw NULL and matching typed NULL follow SQL three-valued logic, including under
NOT; only TRUE rows survive. Existing manifest and row-group pruning remains
available for other scalar fields. DECIMAL and DATE value bounds are treated
conservatively as unknown, while valid null counts may still prune impossible
NULL predicates. Exact decoded predicates decide which rows survive. Decimal
aggregate execution is outside this feature.

The installed C++ `storage/scalars.h` provides `ParseDecimal`, `FormatDecimal`,
`ParseDate` and `FormatDate`. `Decimal` carries a signed 128-bit coefficient as
high/low words and precision/scale; `Date` carries signed epoch days. The Python
SDK accepts exact decimal strings or finite `decimal.Decimal` objects, returns
`Decimal`, and accepts/returns `datetime.date` without accepting `datetime` as
a date. Decimal objects are formatted exactly without dependence on the active
decimal arithmetic context. Arrow uses decimal128(p,s) and date32.

## Persistence and compatibility

Parquet carries DECIMAL(p,s) and DATE annotations, stable field IDs and matching
physical types. Iceberg carries native decimal/date schema fields, signed
big-endian decimal coefficient bounds and little-endian signed int32 day bounds.
Footer validation checks the logical parameters and physical representations;
normal scan validation checks decoded coefficient/calendar domains.

Scalar-only schemas retain canonical schema version **1** byte for byte. A
schema containing either new type uses version **2**. The existing field
encoding is unchanged; decimal tag 6 appends unsigned one-byte precision and
scale, and date tag 7 has no parameters. Decoder validation rejects unknown,
invalid, truncated and noncanonical encodings. Older binaries reject version 2
rather than interpreting a new type as an old scalar. A store containing new
types therefore cannot be downgraded to an older binary.

The organization gateway includes decimal parameters when matching authoritative
create receipts; a mismatched precision or scale keeps the write unresolved.
The legacy disposable mock rejects both native-only types before collection
creation because its Arrow encoder supports only the existing scalar domain.

Native scalar types do not change append header, hash or catalog versions.
Ordinary appends retain result receipt version 1; `BulkAppendDurable` uses
[result receipt version 2](BULK_INGESTION.md) for its extended counts. The schema
fingerprint distinguishes the extended domain. A non-NULL decimal encodes its
coefficient in 16 little-endian two's-complement bytes; a date encodes signed
int32 epoch days in little endian. Existing presence bytes, scalar encodings,
schema fingerprints, operation hashes and retained receipts are unchanged. No
existing schema or receipt is migrated during open. Exact retries use the
original normalized values and immutable schema, independent of transport,
chunk boundaries or decimal spelling.

Canonical and Arrow resource checks account for 16-byte decimal and four-byte
date slots, including omitted/NULL column allocations. Arrow appends copy caller
buffers into owned canonical columns; streaming hashing and the writer use those
owned values with the original precision/scale. Row-form and column-form inputs
share the same exact encoding and reservation contract. Existing input, snapshot,
publication, resource ownership and durability boundaries still apply.

Parquet encoding capabilities include plain and dictionary policies for all
decimal precisions. DATE also supports delta binary packed and byte stream split
encodings through its INT32 physical representation. The recorded Iceberg
encoding patch accepts explicit plain for decimals; dependency pins remain
unchanged. Native tests inspect actual encodings and decode every physical
decimal width and encoded date column back to exact values.

## Validation and TPC-H

The independent literal fixture
[`decimal-date-boundaries.json`](../../fixtures/golden/decimal-date-boundaries.json)
pins schema bytes, mixed canonical rows and hashes, including 38-digit signed
extremes, zero, calendar limits and NULL. Native unit/storage tests check exact
predicates, physical types and metric bounds. `glamdb.native_scalar_types` checks
real HTTP JSON/Arrow, CLI, fresh-process reopen and exact retry behavior.

TPC-H adapters can use decimal(15,2) and DATE when native capabilities advertise
them. This repository currently contains no TPC-H load adapter or schema generator;
its TPC-H cleanup command only removes an explicitly saved database identity.
An external adapter supporting older servers must name its scaled-integer and
UTC-midnight encoding profile explicitly and keep its reports distinct. Native
types do not add a SQL parser, joins or decimal aggregates.

## Design review

A signed 128-bit coefficient supports the full declared precision without exposing
Arrow in the installed value types. Scaled int64 covers the TPC-H profile but
cannot cover precision 38; string-only storage would require extra numeric
ordering and file-type conversions. Calendar-day integers retain DATE semantics
without introducing timestamp or timezone rules.

The compact Parquet profile uses integer-backed decimals through precision 18.
Metadata adapters must therefore preserve provenance when decoding bounds:
Parquet integers are little endian, while Iceberg decimal bounds are big endian.
Tests cover both paths and fixed-byte decimals. Independent review also identified
gateway create-receipt parameter matching, unsupported mock schema admission and
legacy predicate-wrapper parity; regression tests exercise the corrected paths.
Measured NULL/omitted-column reservations and wrong-parameter typed NULL tests
cover additional resource and predicate boundaries.

Pruning integration tests compare zone decisions with exact row evaluation for
DECIMAL and DATE comparisons, IN, NULL, NOT, AND and OR. Counts-only statistics
must never discard a zone containing a matching native scalar. Cached metadata
retains the immutable native schema; predicates bind afresh with precision and
scale on each scan.
