# KV layout component experiment

The measured results and their limits are in
[KV_LAYOUT_MEASUREMENTS.md](../../docs/development/KV_LAYOUT_MEASUREMENTS.md).
This harness uses actual PyArrow Parquet files and a C++23 deterministic payload
generator. It does not open or mutate a GlamDB engine.

Use Python with `pyarrow==25.0.0` and a C++23 compiler. From the repository root:

```sh
python3 benchmarks/kv_layout/run_benchmark.py --work-dir artifacts/kv-layout-reproduction
```

The disposable directory must be absent or empty. Allow up to 80 GiB of space
and sustained execution: the full run streams 200 GiB of logical fixture data,
plus measured rewrite work. Files are removed after each case. No global cache
eviction is performed. The runner copies its sources into the disposable
directory and compiles the payload helper there. It uses its current Python
interpreter and has no machine-specific executable or library paths.

`measurements.json` retains raw operation samples, compressed byte counters,
decoded amplification inputs, process-cold samples and small-sample quantiles.
It excludes the original per-file range inventory and fixture data.
`summary.csv`, `verification.json`, `environment.json`, `commands.json` and
`acceptance.json` contain the reviewable aggregate evidence and provenance.
