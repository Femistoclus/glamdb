# Literal canonical vectors

Each `.json` carries the input schema, collection UUID, input rows, schema bytes,
schema fingerprint, append bytes, and SHA-256. Its adjacent `.hex` is the same
append byte string for non-Python consumers. Append/hash version remains 1;
schemas containing decimal/date use schema version 2. Legacy schemas keep their
original version 1 bytes. The vectors carry fixture labels and no durability claim.

`int64-null-duplicates` has three rows: 9007199254740993, NULL, and the same large
integer again. Its append SHA-256 is
`e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7`.

`all-types-boundaries` covers UTF-8, bool, min/max int64, finite float/negative zero,
min/max timestamp, and NULL values. String bytes are not Unicode-normalized.

`decimal-date-boundaries` mixes string, decimal(38,2), and date. It covers
coefficients `±(10^38-1)`, dates 0001-01-01 and 9999-12-31, negative decimal zero,
the epoch, and NULLs. Decimal coefficient payloads use signed128 little-endian;
date payloads use signed32 little-endian epoch days. Its schema fingerprint is
`9604413bcbf96f2c63a749c7aa9dee2dcc5fe2224a7c712e9e88cb382731dfb6` and append
SHA-256 is `e3dce310f241d4479f7bf5ed508ebf2a5661e2cc6424cc910658bea647ca0144`.

Preimages were written explicitly; hashes were calculated separately using
`hashlib`, without importing the implementation. Validation and deterministic
synthetic generation are documented in
[the verification package](../../tools/verification/README.md).

From the repository root:

```sh
python -m glamdb_verify check-fixture fixtures/golden/int64-null-duplicates.json
python -m glamdb_verify check-fixture fixtures/golden/all-types-boundaries.json
python -m glamdb_verify check-fixture fixtures/golden/decimal-date-boundaries.json
```
