# Native KV measurements for issue #101

Both integrated native 4/16 GiB campaigns passed build, clustered mutation,
full-row lookup, historical reads, durable no-ops, compaction and fresh-process
reopen with exact retries. On the actual 17,179,905,360-byte decoded fixture, a
1,000-row clustered update rewrote two files totaling 17,009,142 bytes in
305.848 ms, with 68,005,616 requested Parquet read-plus-write bytes. Peak
recorded process RSS across both campaigns was 125,337,600 bytes.

This benchmark exercises the public `Engine` facade, catalog, bounded KV
producer and durable Parquet/Iceberg publication. It complements the earlier
[layout component experiment](KV_LAYOUT_MEASUREMENTS.md). These development-host
measurements do not qualify engine durability or the 1 TiB campaign.

## Fixture and measurement scope

The [native benchmark](../../benchmarks/kv_mode.cpp) generates required UTF-8 keys
and 4,096-byte deterministic printable payloads plus an `int64` version. It uses
native uncompressed Parquet with dictionaries disabled. The source replays in
bounded 1,024-row batches, with approximately 256 MiB decoded input per operation;
both passes use the production bulk normalization and hashing path. Creation
used 17/65 independent bounded operations, including the final partial operation.

Rows round upward to the requested decoded size. The recorded decoded count is
actual Arrow `TotalBufferSize` across one complete source pass, including string
offsets. Current physical Parquet bytes are separate. Immutable historical files
remain retained; filesystem counts and sizes agree with all added-file receipts,
and every retained primary Parquet file is below 32 MiB. Both final version-4 catalogs
have identical checked schema fingerprints and pass SQLite `quick_check`.

Each build, exercise and reopen phase uses a fresh process. The exercise holds a
historical cursor, replaces 1,000 clustered full rows, verifies every replacement
value through ordered lookup with a miss and repeated hit, verifies old full
values at the retained snapshot, and records durable unchanged-upsert and
missing-delete no-ops. A second collection is reduced from four small live files
to one with value-preserving compaction. Reopen validates latest and historical
full values, row counts, exact mutation retry and exact no-op retry. No mutation
scratch files remain after shutdown.

An `ILocalFS` wrapper counts requested and accepted external-file reads/writes,
and counts Parquet requests and file syncs separately. Counts include native
publication and recovery verification, including repeated reads. They describe
file operations, not cold device traffic. Catalog SQLite and the independently
bounded mutation-scratch SQLite VFS are excluded. The producer enforces an 8 GiB
attempt I/O/output ceiling plus a separate 16 GiB scratch-I/O ceiling; scratch
main pages are capped at 4 GiB. Peak RSS comes from `getrusage`. Resource-owner
memory peaks in raw evidence are conservative admitted envelopes, not RSS.

OS caches were not dropped. The two fixture creation processes ran concurrently;
development compilation and functional checks could also overlap. Exercise and
reopen phases ran separately. Each latency is one observed sample, so no native
percentile or dedicated-device claim is made. The earlier layout experiment
provides repeated component samples. Both fixtures used the same final native
binary and catalog-v4 schema, including the official append-retention tables.
The measured source is `bb77751b073da06f5acd5a2a4512ed1c452f0010`, integrating
official main `8532c4966fa1da5ce03c54bb5e0067311648fa29`. Creation, exercise,
reopen and the final rebuilt benchmark have the same checked executable SHA-256;
only documentation changed afterward. Raw evidence identifies the artifact,
source and independent catalog/file audit.

## Results

| Metric | Requested 4,096 MiB | Requested 16,384 MiB |
|---|---:|---:|
| Actual decoded source bytes | 4,294,978,412 | 17,179,905,360 |
| Rows | 1,039,441 | 4,157,762 |
| Bounded build operations | 17 | 65 |
| Current Parquet bytes after build | 4,316,408,668 | 17,265,626,695 |
| Current files after build | 508 | 2,031 |
| Retained primary Parquet bytes after exercise | 4,402,149,685 | 17,555,464,672 |
| Build time (s) | 83.606 | 307.761 |
| Clustered update files removed / added | 2 / 2 | 2 / 2 |
| Clustered update bytes removed / added | 17,009,142 / 17,009,142 | 17,009,142 / 17,009,142 |
| Clustered update requested Parquet reads / writes | 50,996,474 / 17,009,142 | 50,996,474 / 17,009,142 |
| Clustered update time (ms) | 188.828 | 305.848 |
| Ordered full-row lookup time (ms) | 21.170 | 29.083 |
| Compaction files removed / added | 4 / 1 | 4 / 1 |
| Compaction time (ms) | 40.633 | 43.261 |
| Fresh reopen time (ms) | 1,394.697 | 5,368.063 |
| Fresh reopen requested Parquet reads | 4,470,969,469 | 17,650,862,921 |
| Peak RSS: build / exercise / reopen (B) | 114,319,360 / 125,337,600 / 59,523,072 | 117,465,088 / 122,671,104 / 63,418,368 |

Raw [operation receipts, artifact hashes and I/O measurements](kv_native_measurements.json)
retain every bounded build operation and all phase results. File sync counts
show that normal durable publication paths ran; they do not simulate power loss.

## Reproduction

Configure the pinned native Release build with storage and tests enabled, then
build `glamdb_kv_mode_bench`. Use a fresh disposable root and ledger for each
size. For example:

```sh
build/benchmarks/glamdb_kv_mode_bench build /tmp/kv-4096 /tmp/kv-4096.json 4096
build/benchmarks/glamdb_kv_mode_bench exercise /tmp/kv-4096 /tmp/kv-4096.json
build/benchmarks/glamdb_kv_mode_bench reopen /tmp/kv-4096 /tmp/kv-4096.json
```

Repeat with `16384` and fresh paths. Allow space for retained history and bounded
scratch, then remove only those disposable roots after all reopen checks. The
2026-11-12 run used Linux 5.4 x86-64, ext4, a KVM AMD EPYC 7713 host with 48
visible vCPUs and approximately 120 GiB RAM, no swap, Clang 18/libc++ and the
repository's pinned Arrow 25/Parquet/Iceberg/SQLite dependencies. Generated
fixture roots were retained through final verification; the ledgers
and an independent file-count, size, scratch-cleanup and catalog integrity audit
remain in the raw repository evidence.
