"""Append preparation micro-benchmark: pre-plan oracle vs current SDK (#105).

Measures ``AppendRequest.prepare`` alone (no HTTP, journal or server) on
deterministic rows for 2/16/64 fields x 1/100/1,000/10,000 rows. Every case
first checks that both implementations return the same payload hash and body.

Usage (from the repository root, with the SDK installed)::

    python sdk/python/benchmarks/prepare_bench.py [--repeat 7] [--inputs wire,native]

Columns: median wall time per call over ``--repeat`` samples after one warm-up
(calls under 20 ms are looped within a sample), the
old/new speedup, tracemalloc peak bytes of one separate traced run, and body
size. ``wire`` inputs are already in wire form (decimal int64 strings, six-digit
UTC timestamp text); ``native`` inputs use Python ints and aware datetimes.
Sources: ``list`` (sized; the hash streams) and ``generator`` (unsized; canonical
rows are buffered until the row count is known). Schema modes: ``reused`` prepares with one Schema object; ``fresh`` builds a new
Schema and clears the plan cache before each run, so plan compilation is timed.
"""

import argparse
from datetime import datetime, timedelta, timezone
import gc
import importlib.util
from pathlib import Path
import statistics
import sys
import time
import tracemalloc

from glamdb_sdk import Field, Schema
from glamdb_sdk.codec import _compile
from glamdb_sdk.request import AppendRequest

_LEGACY = Path(__file__).resolve().parents[1] / "tests" / "legacy_prepare.py"
_SPEC = importlib.util.spec_from_file_location("legacy_prepare", _LEGACY)
legacy = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(legacy)

CID = "00112233-4455-6677-8899-aabbccddeeff"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
KINDS = ("string", "int64", "float64", "timestamp_us_utc", "bool")
ZONE = timezone(timedelta(hours=2))


def make_fields(count: int) -> list[Field]:
    fields = [Field("key", "string", False)]
    for index in range(1, count):
        fields.append(Field(f"c{index:02d}", KINDS[index % len(KINDS)], index % 3 == 0))
    return fields


def make_rows(fields: list[Field], count: int, inputs: str) -> list[dict]:
    rows = []
    base = 1_700_000_000_000_000
    for row_index in range(count):
        row = {}
        for field_index, field in enumerate(fields):
            seed = row_index * 131 + field_index * 17
            if field.nullable and seed % 7 == 0:
                continue  # missing nullable value
            if field.type == "string":
                row[field.name] = f"k{row_index:06d}" if field.name == "key" else f"value-{seed % 9973}-é"
            elif field.type == "int64":
                number = (seed * 2654435761) % (2**63) - 2**62
                row[field.name] = str(number) if inputs == "wire" else number
            elif field.type == "float64":
                row[field.name] = (seed % 10007) / 7.0
            elif field.type == "bool":
                row[field.name] = seed % 2 == 0
            else:
                micros = base + seed * 1_000_003
                if inputs == "wire":
                    row[field.name] = legacy.format_timestamp(micros)
                else:
                    row[field.name] = (datetime(1970, 1, 1, tzinfo=timezone.utc)
                                       + timedelta(microseconds=micros)).astimezone(ZONE)
        rows.append(row)
    return rows


def run_new(fields, rows, fresh, schema, generator):
    if fresh:
        _compile.cache_clear()
        schema = Schema(fields)
    request = AppendRequest.prepare(CID, OP, schema, iter(rows) if generator else rows)
    return request.payload_hash, request.body


def run_old(fields, rows, fresh, schema, generator):
    if fresh:
        schema = Schema(fields)
    return legacy.prepare(CID, OP, schema, iter(rows) if generator else rows)[2:]


def measure(function, repeat: int) -> tuple[float, int]:
    """Median seconds per call and tracemalloc peak bytes of one call."""
    start = time.perf_counter()
    function()  # warm-up, also calibrates the inner loop for tiny cases
    loops = max(1, int(0.02 / max(time.perf_counter() - start, 1e-9)))
    samples = []
    for _ in range(repeat):
        gc.collect()
        start = time.perf_counter()
        for _ in range(loops):
            function()
        samples.append((time.perf_counter() - start) / loops)
    gc.collect()
    tracemalloc.start()
    function()
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return statistics.median(samples), peak


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--fields", default="2,16,64")
    parser.add_argument("--rows", default="1,100,1000,10000")
    parser.add_argument("--inputs", default="wire,native")
    parser.add_argument("--schema", default="reused,fresh")
    parser.add_argument("--source", default="list,generator",
                        help="list: sized input (streamed hash); generator: unsized (buffered canonical rows)")
    args = parser.parse_args()
    print(f"Python {sys.version.split()[0]}, repeat={args.repeat}")
    print("| inputs | source | schema | fields | rows | body bytes | old median ms | new median ms | speedup "
          "| old peak KiB | new peak KiB |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    cases = [(inputs, source, mode, field_count, row_count)
             for inputs in args.inputs.split(",") for source in args.source.split(",")
             for mode in args.schema.split(",") for field_count in map(int, args.fields.split(","))
             for row_count in map(int, args.rows.split(","))
             # Plan compilation only matters for small requests; generators are
             # measured with the reused schema only.
             if mode != "fresh" or (source == "list" and row_count <= 1000)]
    for inputs, source, mode, field_count, row_count in cases:
        generator, fresh = source == "generator", mode == "fresh"
        fields = make_fields(field_count)
        schema = Schema(fields)
        rows = make_rows(fields, row_count, inputs)
        if run_old(fields, rows, fresh, schema, generator) != run_new(fields, rows, fresh, schema, generator):
            raise SystemExit(f"MISMATCH inputs={inputs} fields={field_count} rows={row_count}")
        old_time, old_peak = measure(lambda: run_old(fields, rows, fresh, schema, generator), args.repeat)
        new_time, new_peak = measure(lambda: run_new(fields, rows, fresh, schema, generator), args.repeat)
        body = len(run_new(fields, rows, fresh, schema, generator)[1])
        print(f"| {inputs} | {source} | {mode} | {field_count} | {row_count} | {body} "
              f"| {old_time * 1e3:.3f} | {new_time * 1e3:.3f} | {old_time / new_time:.2f}x "
              f"| {old_peak / 1024:.0f} | {new_peak / 1024:.0f} |", flush=True)

if __name__ == "__main__":
    main()
