# Literal canonical v1 vectors

Each `.json` carries the input schema, collection UUID, input rows, schema bytes,
schema fingerprint, append bytes, and SHA-256. Its adjacent `.hex` is the same
append byte string for non-Python consumers. Both use the exact RFC C-TYPES v1
encoding; they carry fixture labels and no durability claim.

`int64-null-duplicates` has three rows: 9007199254740993, NULL, and the same large
integer again. Its append SHA-256 is
`e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7`.

`all-types-boundaries` covers UTF-8, bool, min/max int64, finite float/negative zero,
min/max timestamp, and NULL values. String bytes are not Unicode-normalized.

Preimages were written explicitly; hashes were calculated separately using
`hashlib`, without importing the implementation. Validation and deterministic
synthetic generation are documented in
[the verification package](../../tools/verification/README.md).

From the repository root:

```sh
python -m glamdb_verify check-fixture fixtures/golden/int64-null-duplicates.json
python -m glamdb_verify check-fixture fixtures/golden/all-types-boundaries.json
```
