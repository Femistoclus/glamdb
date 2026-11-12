"""Stream the complete hits schema into immutable bounded JSONL parts."""

import hashlib
from copy import deepcopy
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from glamdb_sdk import Schema
from glamdb_sdk.codec import decode_rows, encode_rows
from glamdb_sdk.request import json_bytes

from ..dataset import manifest_json
from .model import MAX_METADATA_BYTES, save_json, sha256_file
from .schema import COLUMNS, REFERENCE_ROWS, STRING_COLUMNS, TIME_COLUMNS, UPSTREAM, convert_row, schema_for

MAX_PART_BYTES = 8 * 1024**2
MAX_PARTS = 100000
MAX_CONVERSION_BATCH_ROWS = 1024
MAX_CONVERSION_BATCH_BYTES = 32 * 1024**2
OFFICIAL_SOURCE = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"


def _identity(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _read_regular(path, limit, label):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
                raise ValueError(f"dataset {label} is not a bounded regular file")
            content = source.read(limit + 1)
            after = os.fstat(source.fileno())
        if len(content) > limit or _identity(before) != _identity(after):
            raise ValueError(f"dataset {label} changed or exceeds its byte budget")
        return content, _identity(after)
    except OSError as exc:
        raise ValueError(f"dataset {label} is missing, unsafe or unreadable") from exc


def _source_date_encoding(schema):
    if tuple(schema.names) != COLUMNS:
        raise ValueError("ClickBench source requires all 105 columns in canonical order")
    for field in schema:
        kind = field.type.value_type if pa.types.is_dictionary(field.type) else field.type
        text = pa.types.is_string(kind) or pa.types.is_large_string(kind)
        binary = pa.types.is_binary(kind) or pa.types.is_large_binary(kind) or pa.types.is_fixed_size_binary(kind)
        if field.name in STRING_COLUMNS:
            valid = text or binary
        elif field.name == "EventDate":
            valid = pa.types.is_integer(kind) or pa.types.is_date(kind) or text
        elif field.name in TIME_COLUMNS:
            valid = pa.types.is_integer(kind) or pa.types.is_timestamp(kind)
        else:
            valid = pa.types.is_integer(kind)
        if not valid:
            raise ValueError(f"ClickBench source column {field.name} has an incompatible physical type")
    date_type = schema.field("EventDate").type
    if pa.types.is_dictionary(date_type):
        date_type = date_type.value_type
    return "epoch-days" if pa.types.is_uint16(date_type) else "yyyymmdd"


def _conversion_batch(batch):
    if batch.nbytes > MAX_CONVERSION_BATCH_BYTES:
        raise ValueError("ClickBench source batch exceeds the conversion memory byte budget")
    arrays = []
    for name, array in zip(COLUMNS, batch.columns):
        if pa.types.is_dictionary(array.type):
            array = array.cast(array.type.value_type, safe=True)
        if name in TIME_COLUMNS and pa.types.is_timestamp(array.type):
            # Cast before Python conversion so optional pandas support cannot
            # silently discard a source timestamp's sub-microsecond precision.
            array = array.cast(pa.timestamp("us", array.type.tz), safe=True)
        elif name == "EventDate" and pa.types.is_date64(array.type):
            array = array.cast(pa.date32(), safe=True)
        arrays.append(array)
    converted = pa.RecordBatch.from_arrays(arrays, names=COLUMNS)
    if converted.nbytes > MAX_CONVERSION_BATCH_BYTES:
        raise ValueError("ClickBench source batch exceeds the conversion memory byte budget")
    return converted


def _source(value, output, max_source_bytes):
    text = str(value)
    if text.startswith(("https://", "http://")):
        parsed = urlsplit(text)
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("dataset download requires HTTPS without credentials, query or fragment")
        path = output / "source.parquet.part"
        size = 0
        with httpx.stream("GET", text, headers={"Accept-Encoding": "identity"},
                          follow_redirects=False, timeout=60) as response:
            response.raise_for_status()
            if response.headers.get("Content-Encoding", "identity") != "identity":
                raise ValueError("dataset download requires an identity-encoded Parquet source")
            if int(response.headers.get("Content-Length", "0")) > max_source_bytes:
                raise ValueError("dataset source exceeds its download byte budget")
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
    source_identity = _identity(path.stat())
    parquet = pq.ParquetFile(path)
    try:
        date_encoding = _source_date_encoding(parquet.schema_arrow)
        source_rows = parquet.metadata.num_rows
        count = source_rows if rows is None else rows
        if count > source_rows:
            raise ValueError("requested prefix exceeds the source row count")
        # Every JSON row includes all 105 names and the scalar wire framing.
        # This strict lower bound refuses impossible full-source budgets before
        # reading row groups or writing a partial converted dataset.
        minimum = len(json_bytes({field.name: "" if field.type == "string" else
                                  "1970-01-01" if field.type == "date" else
                                  "1970-01-01T00:00:00.000000Z" if field.type == "timestamp_us_utc" else "0"
                                  for field in schema.fields})) + 1
        if count * minimum > max_converted_bytes:
            raise ValueError("converted dataset byte budget is insufficient for the requested rows")
        if count and minimum > part_bytes:
            raise ValueError("one ClickBench row exceeds the configured part byte budget")
        if max((count + part_rows - 1) // part_rows,
               (count * minimum + part_bytes - 1) // part_bytes) > MAX_PARTS:
            raise ValueError("dataset part-count budget is insufficient for the requested rows")
        source_columns = [{"name": field.name, "arrow_type": str(field.type),
                           "physical_type": parquet.schema.column(index).physical_type,
                           "logical_type": str(parquet.schema.column(index).logical_type)}
                          for index, field in enumerate(parquet.schema_arrow)]
    except BaseException:
        parquet.close()
        raise
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

    try:
        if count:
            for batch in parquet.iter_batches(batch_size=min(part_rows, MAX_CONVERSION_BATCH_ROWS)):
                batch = _conversion_batch(batch.slice(0, min(batch.num_rows, count - consumed)))
                encoded = encode_rows(schema, (convert_row(row, type_profile=type_profile,
                                                          date_encoding=date_encoding)
                                               for row in batch.to_pylist()))
                for row in encoded:
                    line = json_bytes(row) + b"\n"
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
    finally:
        parquet.close()
    flush()
    if consumed != count:
        raise ValueError("source ended before its declared row count")
    if _identity(path.stat()) != source_identity:
        raise ValueError("ClickBench source changed during conversion")
    source_digest = sha256_file(path)
    if _identity(path.stat()) != source_identity:
        raise ValueError("ClickBench source changed during conversion")
    manifest = {"version": 1, "kind": "clickbench", "upstream": UPSTREAM,
                "type_profile": type_profile, "schema": schema.to_dict(), "rows": count,
                "profile": "full" if rows is None and count == REFERENCE_ROWS else "prefix",
                "source": {"name": path.name, "sha256": source_digest,
                           "bytes": source_identity[2], "rows": source_rows,
                           "date_encoding": date_encoding, "columns": source_columns},
                "parts": parts, "converted_bytes": converted, "complete": True,
                "part_rows": part_rows, "part_bytes": part_bytes,
                "versions": {"pyarrow": pa.__version__, "conversion": 2},
                "prepare_seconds": time.perf_counter() - started}
    save_json(output / "manifest.json", manifest)
    return manifest


class ClickBenchDataset:
    def __init__(self, path):
        self.path = Path(path).resolve(strict=True)
        raw, self._manifest_identity = _read_regular(self.path / "manifest.json", MAX_METADATA_BYTES, "manifest")
        self.manifest = manifest_json(raw)
        self.digest = hashlib.sha256(raw).hexdigest()
        try:
            self._check_layout()
        except (KeyError, TypeError, AttributeError) as exc:
            raise ValueError("invalid ClickBench dataset manifest") from exc
        self._header = deepcopy({key: value for key, value in self.manifest.items() if key != "parts"})
        self._parts = tuple(dict(part) for part in self.manifest["parts"])

    def _check_layout(self):
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
        part_rows, part_bytes = m.get("part_rows", 10000), m.get("part_bytes", MAX_PART_BYTES)
        if (type(part_rows) is not int or not 1 <= part_rows <= 10000 or
                type(part_bytes) is not int or not 1 <= part_bytes <= MAX_PART_BYTES):
            raise ValueError("invalid bounded dataset part limits")
        source = m["source"]
        if (not isinstance(source, dict) or type(source.get("rows")) is not int or source["rows"] < m["rows"] or
                type(source.get("bytes")) is not int or source["bytes"] < 1 or
                not isinstance(source.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", source["sha256"]) or
                m.get("profile") not in ("prefix", "full") or
                m["profile"] == "full" and (m["rows"] != REFERENCE_ROWS or source["rows"] != REFERENCE_ROWS)):
            raise ValueError("invalid ClickBench source identity or profile")
        total, size = 0, 0
        for index, part in enumerate(m["parts"]):
            if (not isinstance(part, dict) or part.get("path") != f"part-{index:06d}.jsonl" or
                    type(part.get("rows")) is not int or not 1 <= part["rows"] <= part_rows or
                    type(part.get("bytes")) is not int or not 1 <= part["bytes"] <= part_bytes or
                    not isinstance(part.get("sha256"), str) or
                    not re.fullmatch(r"[0-9a-f]{64}", part.get("sha256", ""))):
                raise ValueError("invalid dataset part identity or bounds")
            total += part["rows"]
            size += part["bytes"]
        if total != m["rows"] or type(m.get("converted_bytes")) is not int or size != m["converted_bytes"]:
            raise ValueError("dataset parts do not cover the declared rows and bytes")

    def _check_manifest(self, ordinal=None):
        try:
            current = (self.path / "manifest.json").lstat()
        except OSError as exc:
            raise ValueError("dataset manifest is missing or unreadable") from exc
        if _identity(current) != self._manifest_identity:
            raw, identity = _read_regular(self.path / "manifest.json", MAX_METADATA_BYTES, "manifest")
            if hashlib.sha256(raw).hexdigest() != self.digest:
                raise ValueError("dataset manifest changed after opening")
            self._manifest_identity = identity
        header = {key: value for key, value in self.manifest.items() if key != "parts"}
        parts = self.manifest.get("parts")
        if (header != self._header or not isinstance(parts, list) or len(parts) != len(self._parts) or
                (tuple(parts) != self._parts if ordinal is None else parts[ordinal] != self._parts[ordinal])):
            raise ValueError("dataset manifest changed after opening")

    def read_part(self, ordinal):
        """Validate and decode one immutable part without visiting earlier parts."""
        if type(ordinal) is not int or not 0 <= ordinal < len(self._parts):
            raise ValueError("dataset part ordinal is outside the recorded inventory")
        self._check_manifest(ordinal)
        part = self._parts[ordinal]
        content, _ = _read_regular(self.path / part["path"], part["bytes"], "part")
        if len(content) != part["bytes"] or hashlib.sha256(content).hexdigest() != part["sha256"]:
            raise ValueError("dataset part hash or size mismatch")
        row_count = content.count(b"\n") + int(bool(content) and not content.endswith(b"\n"))
        if row_count != part["rows"]:
            raise ValueError("dataset part row count mismatch")
        lines = content.split(b"\n")
        if lines[-1] == b"":
            lines.pop()
        rows = [manifest_json(line) for line in lines]
        return decode_rows(self.schema, rows)

    def iter_parts(self):
        self._check_manifest()
        for index in range(len(self._parts)):
            yield index, self.read_part(index)
