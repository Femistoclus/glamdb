"""Stream the complete hits schema into immutable bounded JSONL parts."""

import hashlib
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from glamdb_sdk import Schema
from glamdb_sdk.codec import decode_rows, encode_rows
from glamdb_sdk.request import json_bytes

from .model import read_json, save_json, sha256_file
from .schema import COLUMNS, REFERENCE_ROWS, UPSTREAM, convert_row, schema_for

MAX_PART_BYTES = 8 * 1024**2
MAX_PARTS = 100000
OFFICIAL_SOURCE = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"


def _source(value, output, max_source_bytes):
    text = str(value)
    if text.startswith(("https://", "http://")):
        parsed = urlsplit(text)
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("dataset download requires HTTPS without credentials, query or fragment")
        path = output / "source.parquet.part"
        size = 0
        with httpx.stream("GET", text, follow_redirects=False, timeout=60) as response:
            response.raise_for_status()
            with path.open("xb") as target:
                for chunk in response.iter_bytes(1024**2):
                    size += len(chunk)
                    if size > max_source_bytes:
                        raise ValueError("dataset source exceeds its download byte budget")
                    target.write(chunk)
        final = output / "source.parquet"
        path.replace(final)
        return final
    path = Path(value).resolve(strict=True)
    if not path.is_file() or path.stat().st_size > max_source_bytes:
        raise ValueError("dataset source exceeds its byte budget or is not a file")
    return path


def prepare(source, output, *, type_profile, rows=None, part_rows=10000,
            part_bytes=4 * 1024**2, max_source_bytes=32 * 1024**3,
            max_converted_bytes=8 * 1024**3):
    schema = schema_for(type_profile)
    if (type(part_rows) is not int or not 1 <= part_rows <= 10000 or
            type(part_bytes) is not int or not 1 <= part_bytes <= MAX_PART_BYTES or
            type(max_source_bytes) is not int or max_source_bytes < 1 or
            type(max_converted_bytes) is not int or max_converted_bytes < 1 or
            (rows is not None and (type(rows) is not int or rows < 0))):
        raise ValueError("invalid bounded dataset configuration")
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    path = _source(source, output, max_source_bytes)
    parquet = pq.ParquetFile(path)
    if tuple(parquet.schema_arrow.names) != COLUMNS:
        raise ValueError("ClickBench source requires all 105 columns in canonical order")
    count = parquet.metadata.num_rows if rows is None else rows
    if count > parquet.metadata.num_rows:
        raise ValueError("requested prefix exceeds the source row count")
    parts, lines, size, converted, consumed = [], [], 0, 0, 0

    def flush():
        nonlocal lines, size
        if not lines:
            return
        if len(parts) >= MAX_PARTS:
            raise ValueError("dataset part-count budget exhausted")
        payload = b"".join(lines)
        name = f"part-{len(parts):06d}.jsonl"
        with (output / name).open("xb") as target:
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        parts.append({"path": name, "rows": len(lines), "bytes": len(payload),
                      "sha256": hashlib.sha256(payload).hexdigest()})
        lines, size = [], 0

    if count:
        for batch in parquet.iter_batches(batch_size=part_rows):
            for row in batch.to_pylist():
                line = json_bytes(encode_rows(schema, [convert_row(row, type_profile=type_profile)])[0]) + b"\n"
                if len(line) > part_bytes:
                    raise ValueError("one ClickBench row exceeds the configured part byte budget")
                if converted + len(line) > max_converted_bytes:
                    raise ValueError("converted dataset byte budget exhausted")
                if lines and (len(lines) >= part_rows or size + len(line) > part_bytes):
                    flush()
                lines.append(line)
                size += len(line)
                converted += len(line)
                consumed += 1
                if consumed == count:
                    break
            if consumed == count:
                break
    flush()
    if consumed != count:
        raise ValueError("source ended before its declared row count")
    manifest = {"version": 1, "kind": "clickbench", "upstream": UPSTREAM,
                "type_profile": type_profile, "schema": schema.to_dict(), "rows": count,
                "profile": "full" if rows is None and count == REFERENCE_ROWS else "prefix",
                "source": {"name": path.name, "sha256": sha256_file(path),
                           "bytes": path.stat().st_size, "rows": parquet.metadata.num_rows},
                "parts": parts, "converted_bytes": converted, "complete": True,
                "versions": {"pyarrow": pa.__version__},
                "prepare_seconds": time.perf_counter() - started}
    save_json(output / "manifest.json", manifest)
    return manifest


class ClickBenchDataset:
    def __init__(self, path):
        self.path = Path(path).resolve(strict=True)
        self.manifest = read_json(self.path / "manifest.json")
        m = self.manifest
        if (type(m.get("version")) is not int or m["version"] != 1 or
                m.get("kind") != "clickbench" or m.get("complete") is not True or
                m.get("upstream") != UPSTREAM or type(m.get("rows")) is not int or m["rows"] < 0):
            raise ValueError("dataset manifest is incomplete or incompatible")
        self.schema = Schema.from_dict(m["schema"])
        if self.schema != schema_for(m["type_profile"]):
            raise ValueError("dataset must retain the complete 105-column schema")
        if not isinstance(m.get("parts"), list) or len(m["parts"]) > MAX_PARTS:
            raise ValueError("invalid bounded dataset part list")
        total = 0
        for index, part in enumerate(m["parts"]):
            if (part.get("path") != f"part-{index:06d}.jsonl" or
                    type(part.get("rows")) is not int or not 1 <= part["rows"] <= 10000 or
                    type(part.get("bytes")) is not int or not 1 <= part["bytes"] <= MAX_PART_BYTES or
                    not re.fullmatch(r"[0-9a-f]{64}", part.get("sha256", ""))):
                raise ValueError("invalid dataset part identity or bounds")
            total += part["rows"]
        if total != m["rows"]:
            raise ValueError("dataset parts do not cover the declared rows")
        self.digest = sha256_file(self.path / "manifest.json")

    def iter_parts(self):
        import json

        for index, part in enumerate(self.manifest["parts"]):
            path = self.path / part["path"]
            if path.is_symlink() or path.stat().st_size != part["bytes"] or sha256_file(path) != part["sha256"]:
                raise ValueError("dataset part hash or size mismatch")
            rows = [json.loads(line) for line in path.read_bytes().splitlines()]
            if len(rows) != part["rows"]:
                raise ValueError("dataset part row count mismatch")
            yield index, decode_rows(self.schema, rows)
