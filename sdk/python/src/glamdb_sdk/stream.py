"""Bounded Arrow IPC consumption with explicit HTTP/reader lifetime ownership."""

import hashlib
import hmac
import io
import re
from typing import Any, Callable

import httpx
import pyarrow as pa
import pyarrow.compute as pc

from .codec import MAX_TIMESTAMP_US, MIN_TIMESTAMP_US, Field, Schema, int64
from .errors import IntegrityError, ProtocolError, SDKError, ValidationError
from .transport import Deadline, translate_transport

_MIME = "application/vnd.apache.arrow.stream"
_CONTINUATION = b"\xff\xff\xff\xff"
_ZERO_LENGTH = b"\0\0\0\0"


class _ResponseReader(io.RawIOBase):
    """Satisfy Arrow's bounded reads without buffering an entire HTTP response.

    PyArrow 25 reads IPC framing as separate 4-byte marker/length reads. Record
    the EOS *framing reads*, rather than just looking for eight trailing bytes:
    a valid int64 batch body can itself end in the same byte sequence.
    """

    def __init__(self, response: httpx.Response, budget: Deadline, max_read_bytes: int):
        super().__init__()
        self.response = response
        self.budget = budget
        self.max_read_bytes = max_read_bytes
        self._chunks = iter(response.iter_raw(chunk_size=65536))
        self._pending = memoryview(b"")
        self._eof = False
        self._digest = hashlib.sha256()
        self.bytes_read = 0
        self.eos_seen = False
        self._prefix = False
        self._first_read = True
        self._legacy_framing = False
        self.failure: BaseException | None = None

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self.bytes_read

    def readinto(self, buffer: Any) -> int:
        value = self.read(len(buffer))
        buffer[:len(value)] = value
        return len(value)

    def read(self, size: int = -1) -> bytes:
        try:
            if self.closed:
                raise ValueError("read from closed Arrow source")
            if size < 0 or size > self.max_read_bytes:
                raise IntegrityError("Arrow read exceeds configured allocation bound")
            self.budget.check()
            if size == 0:
                return b""
            pieces = []
            total = 0
            while total < size:
                self.budget.check()
                if not self._pending:
                    if self._eof:
                        break
                    try:
                        self._pending = memoryview(next(self._chunks))
                    except StopIteration:
                        self._eof = True
                        break
                    self.budget.check()
                    if not self._pending:
                        continue
                count = min(size - total, len(self._pending))
                pieces.append(self._pending[:count].tobytes())
                self._pending = self._pending[count:]
                total += count
            value = b"".join(pieces)
            self.budget.check()
            self._digest.update(value)
            self.bytes_read += len(value)
            if value:
                if self._first_read:
                    self._legacy_framing = size == 4 and value != _CONTINUATION
                    self._first_read = False
                self.eos_seen = (size == 4 and value == _ZERO_LENGTH
                                 and (self._prefix or self._legacy_framing))
                self._prefix = size == 4 and value == _CONTINUATION
            return value
        except httpx.HTTPError as exc:
            self.failure = translate_transport(exc, self.budget)
            raise self.failure from exc
        except BaseException as exc:
            self.failure = exc
            raise

    def finish(self, expected_length: int | None, expected_hash: str | None) -> None:
        if not self.eos_seen:
            raise IntegrityError("Arrow stream ended without an explicit IPC EOS marker")
        if self.read(1):
            raise IntegrityError("unexpected bytes after Arrow IPC EOS")
        if expected_length is not None and self.bytes_read != expected_length:
            raise IntegrityError("Arrow Content-Length does not match consumed bytes")
        if expected_hash is not None and not hmac.compare_digest(self._digest.hexdigest(), expected_hash):
            raise IntegrityError("Arrow SHA256 does not match complete response")


class ScanStream:
    """Context-managed iterator of real PyArrow RecordBatches.

    ``integrity`` is ``not_complete`` until the iterator reaches EOS. It becomes
    ``verified`` only when both byte count and SHA256 are supplied and checked;
    otherwise it becomes ``complete``. ``checksum_verified`` specifically
    reports whether a supplied digest was checked. Closing early never drains.
    """

    def __init__(self, response: httpx.Response, budget: Deadline, *, max_read_bytes: int,
                 on_close: Callable[["ScanStream"], Any]):
        self._response = response
        self._budget = budget
        self._on_close = on_close
        self._reader = None
        self._source = None
        self._closed = False
        self.integrity = "not_complete"
        self.checksum_verified = False
        self.schema: pa.Schema
        try:
            self._parse_headers(response.headers)
            self._source = _ResponseReader(response, budget, max_read_bytes)
            self._reader = pa.ipc.open_stream(self._source)
            self.schema = self._reader.schema
            self._validate_schema()
        except BaseException as exc:
            failure = self._source.failure if self._source is not None else None
            self._close_preserving_error()
            if failure is not None:
                raise failure
            if isinstance(exc, (pa.ArrowException, OSError)):
                raise IntegrityError("could not decode Arrow IPC schema", collection_id=budget.collection_id) from exc
            raise

    def _parse_headers(self, headers: httpx.Headers) -> None:
        if headers.get("content-type", "").split(";", 1)[0].strip().lower() != _MIME:
            raise ProtocolError("expected Arrow IPC stream Content-Type")
        if headers.get("content-encoding", "identity").lower() != "identity":
            raise ProtocolError("Arrow response must use identity HTTP content encoding")
        snapshot = headers.get("x-glamdb-snapshot-id")
        if snapshot == "initial":
            self.snapshot_id = None
        else:
            try:
                if not isinstance(snapshot, str) or str(int64(snapshot)) != snapshot:
                    raise ValidationError("invalid snapshot header")
            except ValidationError as exc:
                raise ProtocolError("Arrow response requires fixed decimal snapshot ID or initial") from exc
            self.snapshot_id = snapshot
        length = headers.get("content-length")
        if length is not None and (re.fullmatch(r"[0-9]+", length, re.ASCII) is None or len(length) > 20):
            raise ProtocolError("invalid Arrow Content-Length")
        self._expected_length = None if length is None else int(length)
        checksum = headers.get("x-content-sha256")
        if checksum is not None and re.fullmatch(r"[0-9a-fA-F]{64}", checksum, re.ASCII) is None:
            raise ProtocolError("invalid Arrow X-Content-SHA256 digest")
        self._expected_hash = None if checksum is None else checksum.lower()

    def _validate_schema(self) -> None:
        fields = []
        for field in self.schema:
            logical_type = None
            if pa.types.is_string(field.type):
                logical_type = "string"
            elif pa.types.is_boolean(field.type):
                logical_type = "bool"
            elif pa.types.is_int64(field.type):
                logical_type = "int64"
            elif pa.types.is_float64(field.type):
                logical_type = "float64"
            elif pa.types.is_timestamp(field.type) and field.type.unit == "us" and field.type.tz == "UTC":
                logical_type = "timestamp_us_utc"
            if logical_type is None:
                raise ProtocolError(f"unsupported Arrow logical type for field {field.name}")
            fields.append(Field(field.name, logical_type, field.nullable))
        if fields:
            try:
                Schema(fields)
            except ValidationError as exc:
                raise ProtocolError("invalid Arrow output schema") from exc

    def _validate_batch(self, batch: pa.RecordBatch) -> None:
        batch.validate(full=True)
        for field, values in zip(self.schema, batch.columns):
            if not field.nullable and values.null_count:
                raise ProtocolError(f"NULL in required Arrow field {field.name}")
            if pa.types.is_timestamp(field.type):
                bounds = pc.min_max(values.cast(pa.int64())).as_py()
                if bounds["min"] is not None and (bounds["min"] < MIN_TIMESTAMP_US or bounds["max"] > MAX_TIMESTAMP_US):
                    raise ProtocolError("Arrow timestamp is outside UTC years 0001 through 9999")
            elif pa.types.is_float64(field.type):
                if pc.all(pc.is_finite(values)).as_py() is False:
                    raise ProtocolError("Arrow float64 contains NaN or infinity")
            elif pa.types.is_string(field.type):
                maximum = pc.max(pc.binary_length(values)).as_py()
                if maximum is not None and maximum > (1024 if field.name == "key" else 1024**2):
                    raise ProtocolError("Arrow string exceeds UTF-8 value byte limit")

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def bytes_read(self) -> int:
        return 0 if self._source is None else self._source.bytes_read

    def __enter__(self) -> "ScanStream":
        if self._closed:
            raise ProtocolError("Arrow stream is already closed")
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.close()
        else:
            self._close_preserving_error()

    def __iter__(self) -> "ScanStream":
        return self

    def __next__(self) -> pa.RecordBatch:
        if self._closed:
            raise StopIteration
        try:
            self._budget.check()
            try:
                batch = self._reader.read_next_batch()
            except StopIteration:
                self._source.finish(self._expected_length, self._expected_hash)
                self.checksum_verified = self._expected_hash is not None
                self.integrity = "verified" if self.checksum_verified and self._expected_length is not None else "complete"
                self.close()
                raise
            self._budget.check()
            self._validate_batch(batch)
            return batch
        except StopIteration:
            raise
        except BaseException as exc:
            failure = self._source.failure
            self._close_preserving_error()
            if failure is not None:
                raise failure
            if isinstance(exc, (pa.ArrowException, OSError)):
                raise IntegrityError("could not decode complete Arrow IPC batch", collection_id=self._budget.collection_id) from exc
            raise

    def _close_preserving_error(self) -> None:
        try:
            self.close()
        except BaseException:
            # Preserve the original decode/transport/consumer failure after
            # attempting every owner close. The response close is still tried.
            pass

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        first_error = None
        for owner in (self._reader, self._source, self._response):
            if owner is not None:
                try:
                    owner.close()
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
        self._on_close(self)
        if first_error is not None:
            raise first_error
