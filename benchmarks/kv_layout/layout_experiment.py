#!/usr/bin/env python3
"""Throwaway physical Parquet layout experiment for GlamDB issue #101.

Does not exercise GlamDB, Iceberg publication, durable receipts, or fsync.
Every byte counter names whether it is measured transfer or projected work.
"""
import argparse
import array
import bisect
import ctypes
import gc
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import resource
import random
import shutil
import subprocess
import sys
import time

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

pa.set_cpu_count(1)
pa.set_io_thread_count(1)
ROOT = Path(__file__).resolve().parent
MIB = 1024 ** 2
GIB = 1024 ** 3
ROW_BYTES = 2048
FILE_ROWS = 4096
HARD_FILE = 32 * MIB
IO_BUDGET = 8 * GIB
DEADLINE = 60.0
SEED = 10120261112
SCHEMA = pa.schema([pa.field("key", pa.string(), nullable=False),
                    pa.field("value", pa.string(), nullable=False)])
_GEN = ctypes.CDLL(str(ROOT / "payload_generator.so"))
_GEN.FillPayload.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t,
                              ctypes.c_uint64, ctypes.c_int, ctypes.c_int, ctypes.c_void_p]


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def percentile(v):
    v = sorted(v)
    return {"n": len(v), "p50_ms": v[math.ceil(len(v) * .50) - 1] * 1000,
            "p95_ms": v[math.ceil(len(v) * .95) - 1] * 1000,
            "p99_ms": v[math.ceil(len(v) * .99) - 1] * 1000,
            "raw_seconds": v, "method": "empirical nearest-rank; p99 is maximum for these small samples"}


def prefix(style):
    return {"ordered": "k/", "clustered": "group/", "random": "r/",
            "prefix": "common/" + "p" * 256 + "/", "utf8": "ключ/東京/😀/"}[style]


def keys(style, ranks):
    p = prefix(style)
    if style == "clustered":
        return [f"{p}{int(r) // 4096:06x}/{int(r) % 4096:08x}" for r in ranks]
    return [f"{p}{int(r):016x}" for r in ranks]


def make_table(style, ranks, total_rows, changed=False):
    ranks = array.array("Q", ranks)
    kk = keys(style, ranks)
    key_len = len(kk[0].encode("utf-8"))
    value_len = ROW_BYTES - 8 - key_len
    # Every row is deterministic independent of file/run/chunk order. The
    # affine permutation changes insertion-ID locality for the random style.
    # Printable ASCII [32,95], six entropy bits per byte, dictionary disabled.
    raw = bytearray(len(ranks) * value_len)
    _GEN.FillPayload(ranks.buffer_info()[0], len(ranks), value_len, total_rows,
                      int(style == "random"), int(changed),
                      ctypes.addressof(ctypes.c_char.from_buffer(raw)))
    offsets = array.array("i", (i * value_len for i in range(len(ranks) + 1)))
    va = pa.Array.from_buffers(pa.string(), len(ranks),
                               [None, pa.py_buffer(offsets), pa.py_buffer(raw)])
    table = pa.Table.from_arrays([pa.array(kk, type=pa.string()), va], schema=SCHEMA)
    assert table.nbytes == len(ranks) * ROW_BYTES
    return table


def write_parquet(table, path):
    pq.write_table(table, path, compression="zstd", compression_level=1,
                   use_dictionary=False, data_page_size=64 * 1024,
                   row_group_size=FILE_ROWS, write_statistics=True,
                   write_batch_size=256, version="2.6")
    size = path.stat().st_size
    assert size <= HARD_FILE, (size, path)
    meta = pq.read_metadata(path)
    assert meta.num_row_groups == 1
    assert table.nbytes <= 8 * MIB
    return size, meta


class CountedReader(io.RawIOBase):
    def __init__(self, path):
        self.f = open(path, "rb", buffering=0)
        self.bytes = 0
        self.calls = 0

    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.f.tell()
    def seek(self, offset, whence=0): return self.f.seek(offset, whence)
    def read(self, size=-1):
        data = self.f.read(size)
        self.bytes += len(data)
        self.calls += 1
        return data
    def readinto(self, b):
        n = self.f.readinto(b)
        self.bytes += n
        self.calls += 1
        return n
    def close(self):
        if not self.closed:
            self.f.close()
        super().close()


def read_table(path, columns):
    f = CountedReader(path)
    try:
        parquet = pq.ParquetFile(f, pre_buffer=False, buffer_size=0)
        table = parquet.read_row_group(0, columns=columns, use_threads=False)
        return table, f.bytes, f.calls
    finally:
        f.close()


def generate(style, size_gib, layout):
    out = ROOT / "fixtures" / f"{size_gib}g-{style}-{layout}"
    out.mkdir(parents=True, exist_ok=True)
    index_path = out / "ranges.json"
    if index_path.exists():
        return json.loads(index_path.read_text())
    n = size_gib * GIB // ROW_BYTES
    count = math.ceil(n / FILE_ROWS)
    index = {"style": style, "size_gib": size_gib, "layout": layout,
             "rows": n, "logical_decoded_bytes": n * ROW_BYTES,
             "files": [], "generation_start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "decoded_file_target_bytes": 8 * MIB, "decoded_row_group_hard_bytes": 8 * MIB,
             "compressed_file_hard_bytes": HARD_FILE,
             "compression": "ZSTD level1; dictionary disabled; 64KiB data pages"}
    started = time.perf_counter()
    # Metadata hashes prove identical logical rank/key/payload generation for
    # both layouts. No whole fixture is materialized in memory.
    for i in range(count):
        if layout == "disjoint":
            ranks = range(i * FILE_ROWS, min((i + 1) * FILE_ROWS, n))
        else:
            ranks = range(i, n, count)
        table = make_table(style, ranks, n)
        file = out / f"data-{i:05d}.parquet"
        size, meta = write_parquet(table, file)
        rg = meta.row_group(0)
        index["files"].append({"name": file.name, "lo": table["key"][0].as_py(),
                                "hi": table["key"][-1].as_py(), "rows": len(ranks),
                                "decoded_bytes": table.nbytes, "physical_bytes": size,
                                "key_column_compressed_bytes": rg.column(0).total_compressed_size,
                                "value_column_compressed_bytes": rg.column(1).total_compressed_size})
        if i % 256 == 255:
            print(json.dumps({"stage": "generate", "style": style, "size_gib": size_gib,
                              "layout": layout, "files": i + 1, "seconds": time.perf_counter() - started}), flush=True)
        del table
    index["compressed_bytes"] = sum(f["physical_bytes"] for f in index["files"])
    index["generation_seconds"] = time.perf_counter() - started
    index["generation_peak_rss_bytes"] = rss()
    index["logical_row_fingerprint"] = hashlib.sha256(
        f"seed={SEED};style={style};rows={n};rank=0..{n-1};rowbytes={ROW_BYTES}".encode()).hexdigest()
    index["physical_sample_row_hashes"] = physical_samples(index, out)
    dump(index_path, index)
    return index


def load_index(path):
    return json.loads(Path(path).read_text())


def physical_samples(index, base):
    samples = {}
    n = index["rows"]
    count = len(index["files"])
    for rank in [0, 1, n // 7, n // 3, n // 2 + 123, n - 2, n - 1]:
        if index["layout"] == "disjoint":
            file_num, offset = rank // FILE_ROWS, rank % FILE_ROWS
        else:
            file_num, offset = rank % count, rank // count
        table, _, _ = read_table(base / index["files"][file_num]["name"], ["key", "value"])
        key = table["key"][offset].as_py()
        assert key == keys(index["style"], [rank])[0]
        samples[str(rank)] = hashlib.sha256(key.encode("utf-8") + b"\0" + table["value"][offset].as_py().encode("utf-8")).hexdigest()
    return samples


def route(index, wanted):
    # This is file full min/max metadata, not a per-key index.
    wanted = sorted(wanted)
    lo, hi = wanted[0], wanted[-1]
    result = []
    for f in index["files"]:
        if f["hi"] < lo or f["lo"] > hi:
            continue
        j = bisect.bisect_left(wanted, f["lo"])
        if j < len(wanted) and wanted[j] <= f["hi"]:
            result.append(f)
    return result


def discover(index, base, wanted):
    started = time.perf_counter()
    candidates = route(index, wanted)
    value_set = pa.array(wanted, type=pa.string())
    affected, count, transferred, decoded, calls = [], 0, 0, 0, 0
    for f in candidates:
        table, b, c = read_table(base / f["name"], ["key"])
        transferred += b
        calls += c
        decoded += table.nbytes
        matching = int(pc.sum(pc.cast(pc.is_in(table["key"], value_set=value_set), pa.int64())).as_py())
        if matching:
            affected.append(f)
            count += matching
        del table
        if time.perf_counter() - started > DEADLINE:
            raise RuntimeError("cooperative 60s discovery deadline")
    return {"candidate_files": len(candidates), "read_files": len(candidates),
            "matching_files": len(affected), "matching_rows": count,
            "compressed_read_bytes": transferred, "decoded_read_bytes": decoded,
            "read_calls": calls, "seconds": time.perf_counter() - started,
            "rewritten_files": 0, "compressed_written_bytes": 0,
            "decoded_written_bytes": 0}, affected


def wanted_ranks(index, workload, sample=0):
    n = index["rows"]
    if workload == "clustered":
        # Straddles the disjoint file boundary, two files, independent of size.
        start = (n // 3 // FILE_ROWS) * FILE_ROWS - 500 + sample * 7
        return range(start, start + 1000)
    if workload == "ordered":
        start = n // 2 + 123 + sample * 7
        return range(start, start + 1000)
    if workload == "random":
        return sorted(random.Random(SEED + sample).sample(range(n), 1000))
    raise ValueError(workload)


def mutate(index, base, workload, sample, execute=True):
    total_start = time.perf_counter()
    ranks = wanted_ranks(index, workload, sample)
    kk = keys(index["style"], ranks)
    d, affected = discover(index, base, kk)
    assert d["matching_rows"] == 1000
    # Projected output uses actual compressed source size for exactly the
    # discovered matching files. It is explicitly not measured mutation I/O.
    projected_rw = sum(f["physical_bytes"] for f in affected) * 2 + d["compressed_read_bytes"]
    projected_write = sum(f["physical_bytes"] for f in affected)
    d.update({"operation": "upsert_1000", "workload": workload, "sample": sample,
              "changed_keys": 1000, "projected_rewritten_files": len(affected),
              "projected_compressed_write_bytes_from_source_sizes": projected_write,
              "projected_compressed_total_io_bytes": projected_rw,
              "projected_decoded_read_write_bytes": sum(f["decoded_bytes"] for f in affected) * 2,
              "discovery_seconds": d["seconds"], "validation_compressed_read_bytes": 0,
              "validation_decoded_read_bytes": 0, "read_file_passes": d["read_files"]})
    if projected_rw > IO_BUDGET or not execute:
        d["status"] = "early_budget_rejection" if projected_rw > IO_BUDGET else "projection_only"
        d["seconds"] = time.perf_counter() - total_start
        return d
    stage = base / f"rewrite-{workload}-{sample}"
    stage.mkdir(exist_ok=False)
    changed = make_table(index["style"], ranks, index["rows"], changed=True)
    value_set = pa.array(kk, type=pa.string())
    try:
        for f in affected:
            table, b, c = read_table(base / f["name"], ["key", "value"])
            d["compressed_read_bytes"] += b
            d["decoded_read_bytes"] += table.nbytes
            d["read_calls"] += c
            d["read_file_passes"] += 1
            old_count = table.num_rows
            mask = pc.is_in(table["key"], value_set=value_set)
            keep = table.filter(pc.invert(mask))
            mask_changed = pc.and_(pc.greater_equal(changed["key"], f["lo"]),
                                   pc.less_equal(changed["key"], f["hi"]))
            # Overlap baseline requires exact membership rather than only bounds.
            if index["layout"] == "overlap":
                mask_changed = pc.is_in(changed["key"], value_set=table["key"].combine_chunks())
            new = changed.filter(mask_changed)
            result = pa.concat_tables([keep, new]).sort_by([("key", "ascending")])
            # Arrow filter/sort can retain all-valid bitmaps. Required fields
            # have no nulls; remove those representation-only bitmaps before
            # checking the exact 8MiB decoded row-group ceiling.
            arrays = []
            for column in result.columns:
                a = column.combine_chunks()
                assert a.null_count == 0
                arrays.append(pa.Array.from_buffers(a.type, len(a),
                              [None, *a.buffers()[1:]], offset=a.offset))
            result = pa.Table.from_arrays(arrays, schema=SCHEMA)
            assert result.num_rows == old_count
            assert pc.count_distinct(result["key"]).as_py() == old_count
            file = stage / f["name"]
            size, _ = write_parquet(result, file)
            # Read written footer + all data to verify actual file contents,
            # counted separately from COW I/O below.
            check, vb, _ = read_table(file, ["key", "value"])
            assert check.equals(result)
            d["validation_compressed_read_bytes"] += vb
            d["validation_decoded_read_bytes"] += check.nbytes
            d["rewritten_files"] += 1
            d["compressed_written_bytes"] += size
            d["decoded_written_bytes"] += result.nbytes
            if d["compressed_read_bytes"] + d["compressed_written_bytes"] > IO_BUDGET:
                d["status"] = "measured_partial_rewrite_budget_refusal"
                break
            if time.perf_counter() - total_start > DEADLINE:
                d["status"] = "measured_partial_rewrite_deadline_refusal"
                break
            del table, keep, new, result, check
        else:
            d["status"] = "measured_component_rewrite"
    finally:
        shutil.rmtree(stage)
    d["seconds"] = time.perf_counter() - total_start
    d["verification_read_io_included_in_cow_counter"] = False
    d["verification_read_io_included_in_latency"] = True
    return d


def repeated_discovery(index, base, op, workload, repeats):
    results = []
    for sample in range(repeats):
        if op.startswith("point_"):
            ranks = [index["rows"] // 2 + 123 + sample * 131]
        else:
            ranks = wanted_ranks(index, workload, sample)
        kk = keys(index["style"], ranks)
        missing = "miss" in op
        if missing:
            kk = [k + "~" for k in kk]
        d, _ = discover(index, base, kk)
        assert d["matching_rows"] == (0 if missing else len(kk))
        d.update({"operation": op, "workload": workload, "keys": len(kk), "sample": sample,
                  "status": "measured_component_read"})
        results.append(d)
    return {"operation": op, "workload": workload, "latency": percentile([r["seconds"] for r in results]),
            "samples": results}


def repeated_full_value_point(index, base, repeats):
    results = []
    for sample in range(repeats):
        rank = index["rows"] // 2 + 123 + sample * 131
        wanted = keys(index["style"], [rank])
        expected = make_table(index["style"], [rank], index["rows"])
        start = time.perf_counter()
        d, affected = discover(index, base, wanted)
        assert d["matching_rows"] == 1
        outputs = []
        for f in affected:
            table, b, c = read_table(base / f["name"], ["key", "value"])
            d["compressed_read_bytes"] += b
            d["decoded_read_bytes"] += table.nbytes
            d["read_calls"] += c
            outputs.append(table.filter(pc.is_in(table["key"], value_set=pa.array(wanted))))
        output = pa.concat_tables(outputs)
        assert output.equals(expected)
        d.update({"seconds": time.perf_counter() - start, "operation": "point_hit_full_value",
                  "workload": "ordered", "keys": 1, "sample": sample,
                  "read_file_passes": d["read_files"] + len(affected),
                  "returned_full_row_decoded_bytes": output.nbytes,
                  "status": "measured_component_full_value_lookup"})
        results.append(d)
    return {"operation": "point_hit_full_value", "workload": "ordered",
            "latency": percentile([r["seconds"] for r in results]), "samples": results}


def cold_worker(index_path):
    start = time.perf_counter()
    index = load_index(index_path)
    reopen = time.perf_counter() - start
    base = Path(index_path).parent
    wanted = keys(index["style"], [index["rows"] // 2 + 123])
    d, _ = discover(index, base, wanted)
    d.update({"process_cold_index_reopen_seconds": reopen,
              "process_cold_reopen_plus_lookup_seconds": time.perf_counter() - start,
              "peak_rss_bytes": rss(), "os_caches_flushed": False,
              "scope": "new Python process; JSON file-range metadata reopen + physical Parquet key read; not GlamDB engine reopen"})
    print(json.dumps(d))


def run_case(style, size_gib, layout, repeats=5, scatter_repeats=1):
    index = generate(style, size_gib, layout)
    base = ROOT / "fixtures" / f"{size_gib}g-{style}-{layout}"
    result_path = ROOT / "results" / f"{size_gib}g-{style}-{layout}.json"
    result_path.parent.mkdir(exist_ok=True)
    result = {"fixture": index, "operations": [], "scope": "physical Parquet component experiment",
              "budget_bytes": IO_BUDGET, "cooperative_deadline_seconds": DEADLINE,
              "seed": SEED, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    for op, workload in [("point_hit", "ordered"), ("point_miss", "ordered"),
                          ("lookup_1000_hits", "clustered"), ("lookup_1000_misses", "clustered"),
                          ("delete_1000_missing", "clustered")]:
        result["operations"].append(repeated_discovery(index, base, op, workload, repeats))
        dump(result_path, result)
        print(json.dumps({"stage": "operation", "case": base.name, "operation": op,
                          "p50_ms": result["operations"][-1]["latency"]["p50_ms"]}), flush=True)
    result["operations"].append(repeated_full_value_point(index, base, repeats))
    dump(result_path, result)
    print(json.dumps({"stage": "operation", "case": base.name,
                      "operation": "point_hit_full_value",
                      "p50_ms": result["operations"][-1]["latency"]["p50_ms"]}), flush=True)
    for workload, nrepeats in [("ordered", repeats), ("clustered", repeats), ("random", scatter_repeats)]:
        # The overlapping baseline uses discovered affected-file sizes to
        # project COW; it is never reported as a measured successful mutation.
        # The selected disjoint candidate executes every admitted rewrite.
        results = [mutate(index, base, workload, sample,
                          execute=(layout == "disjoint")) for sample in range(nrepeats)]
        result["operations"].append({"operation": "upsert_1000", "workload": workload,
                                      "latency": percentile([r["seconds"] for r in results]), "samples": results})
        dump(result_path, result)
        print(json.dumps({"stage": "operation", "case": base.name, "operation": "upsert_1000",
                          "workload": workload, "status": results[0]["status"],
                          "candidate_files": results[0]["candidate_files"],
                          "rewritten_files": results[0]["rewritten_files"],
                          "read_bytes": results[0]["compressed_read_bytes"],
                          "written_bytes": results[0]["compressed_written_bytes"],
                          "projected_io_bytes": results[0]["projected_compressed_total_io_bytes"],
                          "p50_ms": result["operations"][-1]["latency"]["p50_ms"]}), flush=True)
    if size_gib == 4 and style == "ordered" and layout == "overlap":
        actual = mutate(index, base, "clustered", 0, execute=True)
        result["operations"].append({"operation": "upsert_1000_measured_baseline", "workload": "clustered",
                                      "latency": percentile([actual["seconds"]]), "samples": [actual]})
        dump(result_path, result)
        print(json.dumps({"stage": "measured_overlap_baseline", "status": actual["status"],
                          "seconds": actual["seconds"], "rewritten_files": actual["rewritten_files"],
                          "read_bytes": actual["compressed_read_bytes"],
                          "written_bytes": actual["compressed_written_bytes"]}), flush=True)
    cold = []
    for _ in range(3):
        proc = subprocess.run([sys.executable, str(Path(__file__).resolve()), "cold", str(base / "ranges.json")],
                              capture_output=True, text=True, check=True,
                              env={**os.environ, "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"})
        cold.append(json.loads(proc.stdout))
    result["process_cold"] = {"samples": cold,
                               "index_reopen_latency": percentile([s["process_cold_index_reopen_seconds"] for s in cold]),
                               "reopen_plus_lookup_latency": percentile([s["process_cold_reopen_plus_lookup_seconds"] for s in cold])}
    result["peak_rss_bytes"] = rss()
    result["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    dump(result_path, result)
    print(json.dumps({"stage": "case_complete", "case": base.name, "result": str(result_path),
                      "peak_rss_bytes": rss()}), flush=True)
    return result


def environment():
    def command(args):
        p = subprocess.run(args, capture_output=True, text=True)
        return {"argv": args, "exit_code": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
    return {"python": sys.version, "python_executable": sys.executable,
            "pyarrow": pa.__version__, "payload_generator": "C++23 shared library via ctypes", "platform": platform.platform(),
            "cpu_count": os.cpu_count(), "seed": SEED,
            "arrow_cpu_count": pa.cpu_count(), "arrow_io_thread_count": pa.io_thread_count(),
            "commands": [command(c) for c in [["uname", "-a"], ["lscpu"], ["findmnt", "-T", str(ROOT), "-o", "SOURCE,FSTYPE,OPTIONS"],
                                              ["df", "-B1", str(ROOT)], ["free", "-b"]]],
            "limits": {"io_budget_bytes": IO_BUDGET, "cooperative_deadline_seconds": DEADLINE,
                       "file_hard_compressed_bytes": HARD_FILE, "row_group_hard_decoded_bytes": 8 * MIB,
                       "fixture_disk_budget_bytes": 80 * GIB},
            "scope_limits": ["Parquet component fixture, not public bulk ingestion or Engine mutation",
                             "PyArrow25 matches the pinned Arrow25 release, but is a separate binary build from GlamDB",
                             "No fsync/SQLite/Iceberg/publication/receipt durability measured",
                             "Fresh process only: global OS page caches remain populated",
                             "Shared virtual ext4 host, not dedicated G4 device",
                             "p95/p99 small sample estimates, not service tail guarantees"]}


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="action", required=True)
    c = sub.add_parser("cold"); c.add_argument("index")
    r = sub.add_parser("case")
    r.add_argument("--style", choices=["ordered", "clustered", "random", "prefix", "utf8"], required=True)
    r.add_argument("--gib", type=int, choices=[4,16], required=True)
    r.add_argument("--layout", choices=["overlap", "disjoint"], required=True)
    r.add_argument("--repeats", type=int, default=5)
    r.add_argument("--scatter-repeats", type=int, default=1)
    r.add_argument("--clean", action="store_true")
    q = sub.add_parser("lookup-supplement")
    q.add_argument("--style", choices=["ordered", "clustered", "random", "prefix", "utf8"], required=True)
    q.add_argument("--gib", type=int, choices=[4,16], required=True)
    q.add_argument("--layout", choices=["overlap", "disjoint"], required=True)
    sub.add_parser("env")
    a = p.parse_args()
    if a.action == "cold":
        cold_worker(a.index)
    elif a.action == "env":
        dump(ROOT / "environment.json", environment())
    elif a.action == "lookup-supplement":
        index = generate(a.style, a.gib, a.layout)
        base = ROOT / "fixtures" / f"{a.gib}g-{a.style}-{a.layout}"
        result = {"fixture": index, "operation": repeated_full_value_point(index, base, 5),
                  "peak_rss_bytes": rss(), "scope": "regenerated full-size physical fixture; complete full-value point lookup"}
        out = ROOT / "results" / f"{a.gib}g-{a.style}-{a.layout}-fullvalue.json"
        dump(out, result)
        print(json.dumps({"stage": "full_value_supplement", "gib": a.gib, "style": a.style,
                          "layout": a.layout, "latency": result["operation"]["latency"]}), flush=True)
        shutil.rmtree(base)
    else:
        run_case(a.style, a.gib, a.layout, a.repeats, a.scatter_repeats)
        if a.clean:
            shutil.rmtree(ROOT / "fixtures" / f"{a.gib}g-{a.style}-{a.layout}")


if __name__ == "__main__":
    main()
