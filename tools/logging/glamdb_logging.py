"""Bounded, allowlisted operational logging for the Python control plane.

Use generated request IDs. This best-effort stream is not a durable audit journal.
"""
from collections import Counter
from datetime import datetime, timezone
import io
import logging
from logging.handlers import QueueHandler, QueueListener
import os
import queue
import re
import secrets
import sys
import threading
import time

from pythonjsonlogger.json import JsonFormatter

MAX_RECORD_BYTES = 4096

EVENTS = frozenset({"runtime_started", "runtime_stopped", "request_completed", "storage_failed",
                    "recovery_required", "logger_health", "access_denied", "org_created", "org_updated",
                    "org_deleted", "database_created", "database_deleted", "session_started", "session_ended",
                    "operation_completed"})
OPERATIONS = frozenset({"other", "create_store", "inspect_store", "health", "collections", "snapshots",
                        "append", "scan", "operation", "auth", "organization", "database", "session",
                        "databases", "capabilities", "readiness", "open_store", "close_store", "reconcile",
                        "create_collection", "schema", "scan_open", "scan_next", "scan_cancel", "stats", "files"})
OUTCOMES = frozenset({"ok", "error", "unknown", "denied", "cancelled"})
NUMBERS = frozenset({"duration_us", "row_count", "status_code", "revision", "byte_count"})
PHASES = frozenset({"none", "validation", "binding", "data", "metadata", "catalog_transaction",
                    "catalog_commit", "recording_failure", "reconciling", "complete"})
ERRORS = frozenset({"none", "INVALID_ARGUMENT", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED", "CANCELLED",
                    "IO_ERROR", "CORRUPTION", "RECOVERY_REQUIRED", "OPERATION_CONFLICT", "PRECONDITION_FAILED",
                    "DEPENDENCY_UNAVAILABLE", "SCHEMA_MISMATCH", "INTERNAL", "OTHER"})
UUID_PATTERN = re.compile(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\Z")
SLUG_PATTERN = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
REQUEST_PATTERN = re.compile(r"[a-f0-9]{32}\Z")


def _enum(value, allowed, default):
    return value if isinstance(value, str) and len(value) <= 64 and value in allowed else default


def sanitize_record(event, **fields):
    """Project known fields; never stringify an arbitrary value or exception."""
    event = _enum(event, EVENTS, "request_completed")
    outcome = _enum(fields.get("outcome", "ok"), OUTCOMES, "error")
    request_id = fields.get("request_id")
    record = {
        "schema": 1, "timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "service": "glamdb", "event": event,
        "level": "error" if outcome in {"error", "unknown"} or event in {"storage_failed", "recovery_required"}
        else "info" if outcome == "ok" else "warn",
        "operation": _enum(fields.get("operation", "other"), OPERATIONS, "other"), "outcome": outcome,
        "request_id": request_id if isinstance(request_id, str) and len(request_id) == 32 and
        REQUEST_PATTERN.fullmatch(request_id) else secrets.token_hex(16),
    }
    for key in NUMBERS:
        value = fields.get(key)
        if type(value) is int and 0 <= value <= 2**63 - 1:
            record[key] = value
    if "phase" in fields:
        record["phase"] = _enum(fields["phase"], PHASES, "none")
    if "error_code" in fields:
        record["error_code"] = _enum(fields["error_code"], ERRORS, "OTHER")
    for key in ("org_id", "database_id"):
        value = fields.get(key)
        if isinstance(value, str) and len(value) <= 64 and (UUID_PATTERN.fullmatch(value) or SLUG_PATTERN.fullmatch(value)):
            record[key] = value
    return record


class _JsonFormatter(JsonFormatter):
    def process_log_record(self, record):
        # The stdlib metadata and exception strings are deliberately not a domain event.
        return {key: value for key, value in record.items()
                if key in {"schema", "timestamp", "service", "event", "level", "operation", "outcome",
                           "request_id", "org_id", "database_id", "phase", "error_code"} | NUMBERS}


class _QueueHandler(QueueHandler):
    def __init__(self, event_queue, count):
        super().__init__(event_queue)
        self._count = count
        self.accepted = False

    def prepare(self, record):
        # emit() created this record from bounded, immutable scalar values. Formatting
        # belongs to the listener; no arbitrary LogRecord or exception reaches here.
        return record

    def enqueue(self, record):
        self.accepted = False
        try:
            super().enqueue(record)  # QueueHandler uses put_nowait.
        except queue.Full:
            self._count("dropped_full")
        else:
            self.accepted = True
            self._count("accepted")

    def handleError(self, record):
        self._count("adapter_errors")


class _StreamHandler(logging.StreamHandler):
    def __init__(self, count):
        stream = sys.stderr
        self._owned_stream = None
        try:
            descriptor = stream.fileno()
        except (AttributeError, io.UnsupportedOperation):
            pass  # An explicitly supplied in-memory runtime/test stream.
        else:
            # Use standard unbuffered IO on an owned duplicate. A failed global
            # stderr buffer otherwise changes CPython's exit status to 120 even
            # after StreamHandler.handleError has counted the error.
            self._owned_stream = io.TextIOWrapper(io.FileIO(os.dup(descriptor), "w"),
                                                  encoding="utf-8", errors="backslashreplace", write_through=True)
            stream = self._owned_stream
        super().__init__(stream)
        self._count = count

    def close(self):
        try:
            if self._owned_stream is not None:
                self._owned_stream.close()
                self._owned_stream = None
        except OSError:
            self._count("adapter_errors")
        finally:
            super().close()

    def handleError(self, record):
        # StreamHandler catches write/flush errors and calls this hook. Do not log
        # recursively and do not claim which buffered records reached the runtime.
        self._count("sink_errors")


class _QueueListener(QueueListener):
    def enqueue_sentinel(self):
        # Only close() may wait for capacity. Stdlib's default put_nowait would
        # raise queue.Full precisely when shutdown must drain a full queue.
        self.queue.put(self._sentinel)


class StructuredLogger:
    """Finite GlamDB events over stdlib handlers and one bounded listener queue.

    Stop application producers before destruction. close() can wait for OS sink IO;
    request threads never write, flush or retry the sink. The logger owns no files.
    """
    def __init__(self, *, capacity=4096, sample_every=100, max_success_per_second=20):
        if (type(capacity) is not int or not 2 <= capacity <= 65536 or
                type(sample_every) is not int or not 1 <= sample_every <= 2**63-1 or
                type(max_success_per_second) is not int or not 1 <= max_success_per_second <= 1000000):
            raise ValueError("invalid logging limits")
        self._queue = queue.Queue(maxsize=capacity)
        self._capacity = capacity
        self._sample_every, self._success_max = sample_every, max_success_per_second
        self._lock, self._admission = threading.Lock(), threading.Lock()
        self._close_lock = threading.Lock()
        self._stats = Counter()
        self._sequence, self._second, self._successes = 0, 0, 0
        self._closed = False
        self._handler = _QueueHandler(self._queue, self._count)
        self._sink = _StreamHandler(self._count)
        self._sink.setFormatter(_JsonFormatter())
        self._logger = logging.Logger("glamdb.events", logging.INFO)
        self._logger.propagate = False
        self._logger.addHandler(self._handler)
        self._listener = _QueueListener(self._queue, self._sink)
        self._listener.start()

    @staticmethod
    def new_request_id():
        return secrets.token_hex(16)

    def _count(self, key):
        with self._lock:
            self._stats[key] += 1

    def snapshot(self):
        with self._lock:
            result = dict(self._stats)
        result["queue_capacity"] = self._capacity
        return result

    def emit(self, event, **fields):
        self._count("submitted")
        if not self._admission.acquire(blocking=False):
            self._count("dropped_contended")
            return False
        try:
            if self._closed:
                self._count("dropped_stopped")
                return False
            record = sanitize_record(event, **fields)
            if record["event"] in {"request_completed", "operation_completed"} and record["outcome"] == "ok":
                self._sequence += 1
                if (self._sequence - 1) % self._sample_every:
                    self._count("sampled")
                    return False
                second = int(time.monotonic())
                if second != self._second:
                    self._second, self._successes = second, 0
                if self._successes >= self._success_max:
                    self._count("rate_limited")
                    return False
                self._successes += 1
            self._logger.log(logging.INFO, record)
            return self._handler.accepted
        except Exception:
            # Best-effort telemetry cannot turn a committed administrative change
            # into an application failure. Never stringify the rejected value.
            self._count("adapter_errors")
            return False
        finally:
            self._admission.release()

    def close(self):
        with self._close_lock:
            with self._admission:
                if self._closed:
                    return
                self._closed = True
            self._listener.stop()
            self._handler.close()
            self._sink.close()
