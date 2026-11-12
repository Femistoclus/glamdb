"""Committed Parquet descriptors and an optional read-only local footprint.

File lengths, allocated blocks, and committed snapshot membership are separate
measurements. An observed stable directory walk is never an atomic database view.
"""

import csv
import io
import os
from pathlib import Path
import stat
import time
from uuid import UUID

from ..runtime import client_for
from .ingest import Blocked, _backend, _config, _dataset, read_state
from .model import read_json, save_json


def _integer(value, name):
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    raise ValueError(f"{name} must be a nonnegative integer")


def _pages(fetch, *, max_pages=10000):
    cursor, seen = None, set()
    for _ in range(max_pages):
        page = fetch(cursor)
        yield from page.items
        cursor = page.next_cursor
        if cursor is None:
            return
        if not isinstance(cursor, str) or not cursor or cursor in seen:
            raise ValueError("metadata pagination repeated or returned an invalid cursor")
        seen.add(cursor)
    raise ValueError("metadata pagination budget exhausted")


def collect_sizes(client, state, *, include_footers=False, timeout=30):
    collection, snapshot = state["collection_id"], state["snapshot_id"]
    selector = {"snapshot_id": snapshot, "deadline": timeout}
    stats = client.stats(collection, **selector)
    identifiers, file_bytes, rows, compressed, uncompressed = set(), 0, 0, 0, 0
    for item in _pages(lambda cursor: client.files(collection, cursor=cursor, **selector)):
        file_id = item.get("file_id")
        if not isinstance(file_id, str) or not file_id or file_id in identifiers:
            raise ValueError("snapshot membership contains invalid or duplicate file IDs")
        identifiers.add(file_id)
        if len(identifiers) > 100000:
            raise ValueError("snapshot membership budget exhausted")
        rows += _integer(item.get("record_count"), "file record_count")
        file_bytes += _integer(item.get("compressed_bytes"), "file compressed_bytes")
        if include_footers:
            details = client.file(collection, file_id, **selector)
            groups = details.get("row_groups")
            if not isinstance(groups, list):
                raise ValueError("footer detail does not contain row groups")
            for group in groups:
                compressed += _integer(group.get("compressed_bytes"), "row group compressed_bytes")
                uncompressed += _integer(group.get("uncompressed_bytes"), "row group uncompressed_bytes")
    for name, actual in (("rows", rows), ("data_files", len(identifiers)),
                         ("compressed_data_bytes", file_bytes)):
        reported = stats.get(name)
        if reported is not None and _integer(reported, name) != actual:
            raise ValueError(f"{name} differs between statistics and complete snapshot membership")
        kind = stats.get("metric_kinds", {}).get(name)
        if reported is not None and kind not in (None, "exact", "metadata", "measured"):
            raise ValueError("size report requires exact snapshot membership metrics")
    history, snapshots = set(), 0
    for item in _pages(lambda cursor: client.snapshots(collection, cursor=cursor, **selector)):
        identifier = item.get("snapshot_id")
        if identifier in history or identifier is None:
            raise ValueError("snapshot history contains invalid or duplicate IDs")
        history.add(identifier)
        snapshots += 1
    if snapshot is not None and snapshot not in history:
        raise ValueError("loaded snapshot is absent from the selected history")
    return {"rows": rows, "parquet_files": len(identifiers), "parquet_file_bytes": file_bytes,
            "parquet_bytes_per_row": file_bytes / rows if rows else None,
            "selected_history_snapshots": snapshots, "snapshot_id": snapshot,
            "row_group_compressed_bytes": compressed if include_footers else None,
            "row_group_uncompressed_bytes": uncompressed if include_footers else None,
            "column_chunk_compression_ratio": uncompressed / compressed if compressed else None,
            "database_file_bytes": None, "iceberg_metadata_file_bytes": None, "catalog_file_bytes": None,
            "performance_eligible": state["performance_eligible"],
            "byte_measurement": "committed_data_file_descriptors",
            "unavailable": {name: "not exposed by the remote API; see #162"
                            for name in ("database_file_bytes", "iceberg_metadata_file_bytes", "catalog_file_bytes")}}


def _category(parts):
    if len(parts) == 1 and parts[0] in {"catalog.sqlite", "catalog.sqlite-journal",
                                      "catalog.sqlite-wal", "catalog.sqlite-shm"}:
        return "catalog_file_bytes"
    if parts[0] == "warehouse" and "metadata" in parts[1:-1]:
        return "iceberg_metadata_file_bytes"
    if parts[0] == "warehouse" and "data" in parts[1:-1] and parts[-1].endswith(".parquet"):
        return "warehouse_data_file_bytes"
    return "other_file_bytes"


def filesystem_footprint(root, *, expected_store_id, max_entries=100000, max_depth=64):
    """Walk only an explicitly supplied, caller-owned store directory."""
    root = Path(root).absolute()
    if root.name != str(UUID(expected_store_id)):
        raise ValueError("owned store directory must match the original store UUID")
    if type(max_entries) is not int or max_entries < 1 or type(max_depth) is not int or max_depth < 1:
        raise ValueError("filesystem traversal budgets must be positive")
    started, entries, links, allocated_directories, changed = time.time(), 0, 0, 0, False
    files = {}
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        top = os.open(root, flags)
    except OSError as error:
        raise ValueError("cannot open the owned store without following symlinks") from error
    device = os.fstat(top).st_dev

    def walk(descriptor, parts):
        nonlocal entries, links, allocated_directories, changed
        before = os.fstat(descriptor)
        allocated_directories += before.st_blocks * 512
        with os.scandir(descriptor) as children:
            for child in children:
                entries += 1
                if entries > max_entries:
                    raise ValueError("filesystem entry budget exhausted")
                item = child.stat(follow_symlinks=False)
                if item.st_dev != device or stat.S_ISLNK(item.st_mode):
                    raise ValueError("symlinks and cross-device entries are not measured")
                path = (*parts, child.name)
                if stat.S_ISDIR(item.st_mode):
                    if len(path) > max_depth:
                        raise ValueError("filesystem depth budget exhausted")
                    nested = os.open(child.name, flags, dir_fd=descriptor)
                    try:
                        actual = os.fstat(nested)
                        if (actual.st_dev, actual.st_ino) != (item.st_dev, item.st_ino):
                            raise ValueError("directory identity changed during measurement")
                        walk(nested, path)
                    finally:
                        os.close(nested)
                elif stat.S_ISREG(item.st_mode):
                    category, key = _category(path), (item.st_dev, item.st_ino)
                    value = {"bytes": item.st_size, "allocated": item.st_blocks * 512, "category": category}
                    if key in files:
                        links += 1
                        old = files[key]
                        if (old["bytes"], old["allocated"]) != (value["bytes"], value["allocated"]):
                            raise ValueError("hardlinked file changed during measurement")
                        if old["category"] != category:
                            old["category"] = "shared_file_bytes"
                    else:
                        files[key] = value
                else:
                    raise ValueError("special files are not included in benchmark store measurements")
        after = os.fstat(descriptor)
        changed |= (before.st_mtime_ns, before.st_ctime_ns) != (after.st_mtime_ns, after.st_ctime_ns)
    try:
        walk(top, ())
    finally:
        os.close(top)
    totals = dict.fromkeys(("warehouse_data_file_bytes", "iceberg_metadata_file_bytes",
                           "catalog_file_bytes", "shared_file_bytes", "other_file_bytes"), 0)
    for item in files.values():
        totals[item["category"]] += item["bytes"]
    return {**totals, "database_file_bytes": sum(item["bytes"] for item in files.values()),
            "database_allocated_bytes": allocated_directories + sum(item["allocated"] for item in files.values()),
            "hardlink_paths": links, "entries": entries, "unique_files": len(files),
            "started_at": started, "finished_at": time.time(),
            "consistency": "changed-during-walk" if changed else "observed-stable-not-atomic",
            "mapping": "caller-supplied store path; includes historical and uncommitted files",
            "allocated_bytes_scope": "unique files plus traversed directories; filesystem block accounting"}


def measure(path, report, *, owned_store_dir=None, include_footers=False, timeout=30, ca_file=None):
    if Path(report).suffix != ".json":
        raise ValueError("size report must use a .json path; CSV and Markdown use the same stem")
    report = Path(report).resolve()
    if report == Path(path).resolve():
        raise ValueError("size report must not overwrite the original run state")
    state = read_state(path)
    data = _dataset(state)
    if report.is_relative_to(data.path) or report.is_relative_to(Path(state["run_dir"]).resolve()):
        raise ValueError("size reports must be separate from immutable dataset and journal artifacts")
    if not state["load_complete"]:
        raise Blocked("a complete load is required for the size report")
    with client_for(_config(state, timeout, ca_file)) as client:
        capabilities = client.capabilities(deadline=timeout)
        _backend(capabilities, state["test_backend"])
        sizes = collect_sizes(client, state, include_footers=include_footers, timeout=timeout)
        server_storage = None
        if capabilities.get("features", {}).get("storage_footprint") is True:
            server_storage = client.storage_footprint(store_id=state["store_id"], deadline=timeout)
            for name in sizes["unavailable"]:
                sizes["unavailable"][name] = "whole-store metric is separate in server_storage"
    if sizes["rows"] != data.manifest["rows"]:
        raise ValueError("selected snapshot row count differs from the complete load")
    run = Path(state["run_dir"])
    verification = read_json(run / "verify.json") if (run / "verify.json").exists() else {}
    verified = (verification.get("status") == "PASS" and
                verification.get("dataset_hash") == data.digest and
                verification.get("snapshot_id") == state["snapshot_id"] and
                verification.get("load_id") == state["load_id"])
    local = None
    if owned_store_dir is not None:
        if state["performance_eligible"] is not True:
            raise ValueError("a test fixture is not a physical native database measurement")
        local = filesystem_footprint(owned_store_dir, expected_store_id=state["store_id"])
        if server_storage is not None and local["database_file_bytes"] != int(server_storage["database_file_bytes"]):
            raise ValueError("local and server store sizes changed between checkpoints; retry without external writers")
    result = {"status": "PASS" if verified else "INCOMPLETE", "verification": "PASS" if verified else "NOT_VERIFIED",
              "dataset_hash": data.digest, "source": data.manifest["source"],
              "profile": data.manifest["profile"], "type_profile": data.manifest["type_profile"],
              "converted_bytes": data.manifest["converted_bytes"], "load_id": state["load_id"],
              "database_id": state["database_id"], "store_id": state["store_id"],
              "server_version": state.get("server_version"), "target": state.get("target"),
              "snapshot": sizes, "server_storage": server_storage, "local_filesystem": local,
              "journal_file_bytes": sum(item.stat().st_size for item in run.glob("worker-*.sqlite")),
              "performance_eligible": verified and state["performance_eligible"]}
    save_json(report, result)
    flat = {"status": result["status"], "profile": result["profile"],
            "verification": result["verification"], "performance_eligible": result["performance_eligible"],
            **{key: value for key, value in sizes.items() if not isinstance(value, dict)}}
    flat["performance_eligible"] = result["performance_eligible"]
    if server_storage is not None:
        for name in ("database_file_bytes", "database_allocated_bytes", "catalog_file_bytes",
                     "iceberg_metadata_file_bytes", "committed_data_file_bytes", "unreferenced_warehouse_file_bytes"):
            flat["server_" + name] = server_storage[name]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=tuple(flat))
    writer.writeheader()
    writer.writerow(flat)
    report.with_suffix(".csv").write_text(output.getvalue())
    lines = ["| Metric | Value |", "|---|---|"]
    lines.extend(f"| {key} | {value if value is not None else 'unavailable'} |" for key, value in flat.items())
    report.with_suffix(".md").write_text("\n".join(lines) + "\n")
    return result
