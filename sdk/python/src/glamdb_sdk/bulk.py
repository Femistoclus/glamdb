"""Replayable disk-backed bulk bodies; preparation never submits a write."""

from contextlib import contextmanager
from dataclasses import dataclass, field
import tempfile
import threading
from typing import Any, BinaryIO, Iterable, Mapping
from uuid import UUID

from .codec import Schema, int64, row_plan
from .errors import ValidationError
from .models import ANY
from .request import APPEND_CANONICAL_LIMIT, APPEND_ROW_LIMIT, _payload_digest, json_bytes, uuid_string

BULK_WIRE_LIMIT = 4 * 1024**3
BULK_ROW_LIMIT = 25_600_000


@dataclass(frozen=True)
class BulkAppendRequest:
    collection_id: str
    operation_id: str
    payload_hash: str
    content_length: int
    _file: BinaryIO = field(repr=False, compare=False)
    _lock: Any = field(default_factory=threading.Lock, repr=False, compare=False)

    @classmethod
    def prepare(cls, collection_id: str | UUID, operation_id: str | UUID, schema: Schema,
                batches: Iterable[Iterable[Mapping[str, Any]]], *, rows: int,
                expected_snapshot: Any = ANY, _check=None) -> "BulkAppendRequest":
        cid = uuid_string(collection_id, "collection_id")
        op = uuid_string(operation_id, "operation_id")
        if type(rows) is not int or not 1 <= rows <= BULK_ROW_LIMIT:
            raise ValidationError("bulk rows must be 1..25,600,000")
        plan = row_plan(schema)
        digest = _payload_digest(cid, plan.fingerprint, rows)
        header = {"operation_id": op, "rows": str(rows)}
        if expected_snapshot is not ANY:
            header["expected_snapshot"] = None if expected_snapshot is None else str(int64(expected_snapshot))
        source = tempfile.TemporaryFile(mode="w+b")
        try:
            source.write(json_bytes(header) + b"\n")
            total = canonical_total = 0
            for batch in batches:
                if _check is not None:
                    _check()
                frame = bytearray(b'{"rows":[')
                count = canonical_bytes = 0
                for row in batch:
                    if _check is not None:
                        _check()
                    if count >= APPEND_ROW_LIMIT or total >= rows:
                        raise ValidationError("bulk batch exceeds 10,000 rows or declared total")
                    encoded, canonical = plan.encode_row(row)
                    canonical_bytes += len(canonical)
                    canonical_total += len(canonical)
                    if canonical_bytes > APPEND_CANONICAL_LIMIT or canonical_total > BULK_WIRE_LIMIT:
                        raise ValidationError("bulk exceeds 16 MiB per batch or 4 GiB canonical total")
                    wire = json_bytes(encoded)
                    if len(frame) + bool(count) + len(wire) + 3 > APPEND_CANONICAL_LIMIT:
                        raise ValidationError("bulk batch exceeds 16 MiB wire limit")
                    if count:
                        frame.extend(b",")
                    frame.extend(wire)
                    digest.update(canonical)
                    count += 1
                    total += 1
                if not count:
                    raise ValidationError("bulk batches must be nonempty")
                frame.extend(b"]}\n")
                if source.tell() + len(frame) > BULK_WIRE_LIMIT:
                    raise ValidationError("bulk body exceeds 4 GiB wire limit")
                source.write(frame)
            if total != rows:
                raise ValidationError("bulk row count differs from declared total")
            source.flush()
            return cls(cid, op, digest.hexdigest(), source.tell(), source)
        except BaseException:
            source.close()
            raise

    @contextmanager
    def open_body(self):
        """Yield bounded immutable blocks from the same prepared retry body."""
        if not self._lock.acquire(blocking=False):
            raise ValidationError("bulk request is already in use")
        try:
            if self._file.closed:
                raise ValidationError("bulk request is closed")
            self._file.seek(0)
            yield iter(lambda: self._file.read(65536), b"")
        finally:
            self._lock.release()

    def close(self) -> None:
        if not self._lock.acquire(blocking=False):
            raise ValidationError("bulk request is already in use")
        try:
            self._file.close()
        finally:
            self._lock.release()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
