# KV layout measurements for issue #101

Measured on 2026-11-12 against reference revision `37b58faec3704e00a69d253eaaaff3c682a1d8ed`.

**Decision:** use disjoint sorted full-key file ranges and route from snapshot file min/max metadata. The selected layout meets the measured 16 GiB locality target. A per-key index was not evaluated because range routing passed.

This is a physical Parquet component experiment. It does not qualify integrated GlamDB KV, public bulk ingestion, durable publication/receipts, native recovery, native compaction, power loss or 1 TiB bulk.

## Executed results

All 20 main cases and 20 full-value point-lookup cases completed. Ten fixture pairs match deterministic row fingerprints and seven independently read physical row hashes per pair. Every selected 16 GiB clustered 1,000-key update rewrote two files; ordered 1,000-key updates rewrote one. Full-value point hits read the complete matching row and verify its payload.

| Key family | Full-value Get p50 / p95 / p99 ms | Clustered 1,000 update p50 / p95 / p99 ms | COW read + write MiB | Decoded read / write amplification |
|---|---:|---:|---:|---:|
| ordered | 26.65 / 28.30 / 28.30 | 173.34 / 176.01 / 176.01 | 24.35 | 8.280 / 8.192 |
| clustered | 24.22 / 24.44 / 24.44 | 175.54 / 180.44 / 180.44 | 24.31 | 8.292 / 8.192 |
| random | 26.40 / 32.13 / 32.13 | 203.39 / 230.21 / 230.21 | 24.35 | 8.280 / 8.192 |
| prefix | 26.47 / 31.89 / 31.89 | 161.67 / 180.68 / 180.68 | 21.33 | 9.328 / 8.192 |
| UTF-8 | 29.51 / 31.91 / 31.91 | 200.13 / 232.44 / 232.44 | 24.13 | 8.356 / 8.192 |

Across these five 16 GiB families, maximum selected clustered I/O was 25,533,204 bytes and maximum empirical p99 was 232.44 ms, against 8 GiB and 60 seconds. Maximum full-value Get I/O was 6,447,444 bytes and maximum empirical p99 was 32.13 ms.

The overlapping 16 GiB baseline physically read 2,048 candidate key files per clustered operation and discovered 1,000 affected files. Its projected COW read+write was 10.44–11.90 GiB, so it refused before writing. Those writes are projections from actual source sizes, not measured mutations. A complete 4 GiB overlapping rewrite validated the model: 512 rewritten files, 3,301,524,388 compressed bytes read plus 3,235,450,314 written, 47.928 seconds including output equality rereads; projection differed by less than 0.5%.

On 4 GiB, scattered 1,000-key updates physically rewrote 447 disjoint files, with maximum measured COW I/O 5,706,544,042 bytes and maximum elapsed 45.310 seconds. All five 16 GiB scattered cases exceeded the projected I/O budget and refused after measured key discovery with zero writes. This is explicit workload backpressure, not a scatter throughput promise. Exact misses and 1,000-key delete-missing checks returned no matches and wrote zero files in all cases; durable no-op receipts are outside the experiment.

Peak recorded process RSS was 196,571,136 bytes (187.5 MiB) across 4/16 GiB fixtures. Three new-process samples per case reloaded full-range JSON metadata and performed a physical key lookup; 16 GiB disjoint reopen medians were 6.24–10.88 ms, and reopen-plus-key-lookup medians 9.62–15.72 ms. Python/import startup is excluded. OS caches were not flushed; these are component reopen measurements, not `Engine::OpenExisting`.

## Fixture and accounting

Each pair contains exactly 4/16 GiB of Arrow `Table.nbytes`: 2,097,152/8,388,608 rows at 2,048 decoded bytes/row. Required UTF-8 key/value columns use deterministic printable payloads with six entropy bits/byte. Both layouts contain the same rows. Files target 8 MiB decoded with one row group, stay below 8 MiB compressed, and are checked against the 32 MiB native file ceiling. Logical decoded accounting excludes allocator capacity and final offset tails; native resource envelopes remain separate. ZSTD level 1, dictionary disabled, 64 KiB pages.

The 512/2,048 overlapping files are globally spanning sorted runs. The 512/2,048 selected files are sorted disjoint full-key ranges. Key families use fixed-width hexadecimal suffixes, group/local suffixes, permuted logical row IDs plus scattered selection, a 264-byte ASCII common prefix, and a fixed Cyrillic/Japanese/emoji prefix. The UTF-8 fixture does not exhaust mixed-script ordering. Clustered updates straddle one selected file boundary.

Compressed reads count exact bytes returned by a seekable file wrapper, including 64 KiB footer rereads and page-cache hits. They are operation I/O bytes, not cold block-device traffic. Compressed writes are actual output-file bytes. COW uses key-only discovery followed by full reads of affected files, replacement, sort, temporary Parquet writes and complete equality rereads. Validation read I/O is excluded from COW counters while validation time is included. The operation names `point_hit` and `lookup_1000_hits` in raw evidence mean key existence; `point_hit_full_value` means complete row retrieval. Full-value lookup uses a conservative two-pass candidate-file read.

Quantiles use five samples for local rewrites and lookups, one for scatter and three for process-cold reopen. With five samples p95/p99 equal the maximum. Repeats reuse the same immutable base and do not accumulate retained snapshot history. Some fixture generation and two full-value supplement workers ran concurrently on a shared host. No uncontended latency, service-tail or device-cold guarantee is made. Only disposable experiment fixtures were removed; no global cache eviction occurred.

## Environment and reproduction

Linux 5.4.210-39.1.pagevecsize x86-64, KVM AMD EPYC 7713, 48 vCPUs, 119 GiB RAM, no swap; `/dev/vda1` ext4 with `relatime,errors=panic`. Python 3.13.18, PyArrow 25.0.0 manylinux228 release wheel (GNU 14.2.1); payload helper compiled with GNU 13.3.0 and `-O3 -std=c++23 -shared -fPIC`. Arrow CPU/I/O thread counts are one. PyArrow matches the pinned Arrow 25 release but is a separate binary build from the engine. Seed 10120261112. This is not a dedicated G4 device.

Run the [benchmark harness](../../benchmarks/kv_layout/README.md) with a Python environment providing `pyarrow==25.0.0`. Runnable helpers use their current interpreter and script-relative paths. Allow up to 80 GiB of disposable space.

- [Raw operation samples without per-file inventories](../../benchmarks/kv_layout/measurements.json).
- [All operation counters and p50/p95/p99](../../benchmarks/kv_layout/summary.csv).
- [Acceptance checks](../../benchmarks/kv_layout/acceptance.json) and [verification](../../benchmarks/kv_layout/verification.json).
- [Exact environment](../../benchmarks/kv_layout/environment.json) and [commands/provenance](../../benchmarks/kv_layout/commands.json).
