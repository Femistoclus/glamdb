"""HTTP ownership, response limits, and caller deadline accounting."""

import json
import math
import time
from typing import Any

import httpx

from .errors import DeadlineExceeded, ProtocolError, TransportError, ValidationError


class Deadline:
    def __init__(self, seconds: float | None, *, collection_id: str | None = None,
                 operation_id: str | None = None):
        if seconds is not None and (type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds < 0):
            raise ValidationError("deadline must be a finite nonnegative number of seconds")
        self.end = None if seconds is None else time.monotonic() + seconds
        self.collection_id = collection_id
        self.operation_id = operation_id

    def check(self) -> float | None:
        remaining = None if self.end is None else self.end - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise DeadlineExceeded("caller deadline exceeded; no authoritative write outcome inferred",
                                   collection_id=self.collection_id, operation_id=self.operation_id)
        return remaining


def translate_transport(exc: httpx.HTTPError, budget: Deadline) -> TransportError:
    kind = DeadlineExceeded if isinstance(exc, httpx.TimeoutException) else TransportError
    return kind("HTTP timeout; no authoritative write outcome inferred" if kind is DeadlineExceeded
                else "HTTP transport failed; no authoritative write outcome inferred",
                collection_id=budget.collection_id, operation_id=budget.operation_id)


def read_json(response: httpx.Response, budget: Deadline, max_bytes: int) -> dict[str, Any]:
    try:
        if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise ProtocolError("expected application/json response")
        chunks = bytearray()
        for chunk in response.iter_bytes(chunk_size=65536):
            budget.check()
            if len(chunks) + len(chunk) > max_bytes:
                raise ProtocolError("JSON response exceeds configured byte limit")
            chunks.extend(chunk)
        budget.check()
        def invalid_number(value: str):
            raise ValueError(f"invalid JSON constant {value}")
        value = json.loads(chunks, parse_constant=invalid_number)
        if not isinstance(value, dict):
            raise ProtocolError("expected a JSON object response", raw=value)
        return value
    except httpx.HTTPError as exc:
        raise translate_transport(exc, budget) from exc
    except (UnicodeError, ValueError) as exc:
        raise ProtocolError("invalid JSON response") from exc
    finally:
        response.close()
