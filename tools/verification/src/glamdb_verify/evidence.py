"""Controller-side receipts and evidence checks; these do not certify hardware."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid

from .types import normalize_value

EVIDENCE_KINDS = frozenset({"modeled-power", "process-crash", "real-power"})
_RESULT_FIELDS = frozenset({
    "collection_id", "operation_id", "payload_hash", "hash_version", "snapshot_id",
    "parent_snapshot_id", "rows_appended", "data_files_added", "compressed_data_bytes_added",
    "snapshot_created_at_us", "durability_profile", "receipt_version",
})
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _exact_integer(value, kind="int64"):
    return normalize_value({"name": "value", "type": kind, "nullable": False}, value)


def validate_receipt(receipt: dict) -> dict:
    """Validate the entire C-RESULT shape; normalize wide wire integers exactly."""
    if not isinstance(receipt, dict) or set(receipt) != _RESULT_FIELDS:
        raise ValueError("receipt must contain exactly the C-RESULT fields")
    output = dict(receipt)
    for name in ("collection_id", "operation_id"):
        try:
            output[name] = str(uuid.UUID(receipt[name]))
        except (ValueError, AttributeError, TypeError) as exc:
            raise ValueError(f"invalid {name}") from exc
    if not isinstance(receipt["payload_hash"], str) or not _SHA256.fullmatch(receipt["payload_hash"]):
        raise ValueError("payload_hash must be lowercase SHA256 hex")
    for name in ("hash_version", "receipt_version"):
        if type(receipt[name]) is not int or receipt[name] != 1:
            raise ValueError(f"unsupported {name}")
    output["snapshot_id"] = str(_exact_integer(receipt["snapshot_id"]))
    if receipt["parent_snapshot_id"] is not None:
        output["parent_snapshot_id"] = str(_exact_integer(receipt["parent_snapshot_id"]))
    for name in ("rows_appended", "data_files_added", "compressed_data_bytes_added"):
        value = _exact_integer(receipt[name])
        if value < 0:
            raise ValueError(f"negative {name}")
        output[name] = value
    if not 1 <= output["rows_appended"] <= 10000 or output["data_files_added"] != 1:
        raise ValueError("append receipt requires 1..10000 rows and one data file")
    timestamp = _exact_integer(receipt["snapshot_created_at_us"])
    _exact_integer(timestamp, "timestamp_us_utc")
    if timestamp % 1000:
        raise ValueError("Iceberg snapshot time must have millisecond precision")
    output["snapshot_created_at_us"] = str(timestamp)
    if not isinstance(receipt["durability_profile"], str) or not receipt["durability_profile"]:
        raise ValueError("durability_profile is required")
    return output


def canonical_receipt(receipt: dict) -> bytes:
    """Controller serialization v1, not a new engine receipt compatibility promise."""
    return json.dumps(validate_receipt(receipt), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _identity(receipt):
    return receipt["collection_id"], receipt["operation_id"]


def check_recovery(acked: list[dict], recovered: list[dict]) -> list[str]:
    """Every ACKed identity must occur once with identical full receipt after reopen.

    Unacknowledged complete operations are allowed. This checks receipt evidence;
    the engine harness must independently verify snapshot data bytes/row contents.
    """
    errors = []
    sides = []
    for label, receipts in (("ACK", acked), ("recovered", recovered)):
        found = {}
        if not isinstance(receipts, list):
            errors.append(f"invalid {label} receipts: expected array")
            sides.append(found)
            continue
        for index, receipt in enumerate(receipts):
            try:
                receipt = validate_receipt(receipt)
            except ValueError as exc:
                errors.append(f"invalid {label} receipt {index}: {exc}")
                continue
            identity = _identity(receipt)
            if identity in found:
                errors.append(f"duplicate {label} operation {identity}")
            found[identity] = canonical_receipt(receipt)
        sides.append(found)
    expected, actual = sides
    for identity, receipt in expected.items():
        if identity not in actual:
            errors.append(f"missing ACKed operation {identity}")
        elif actual[identity] != receipt:
            errors.append(f"changed ACKed receipt {identity}")
    return errors


def _sync_directory(path: Path):
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _encode_record(record):
    return json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"


class AckLedger:
    """One serialized controller writer, outside the volume that a harness cuts.

    fsync is issued for every record and for the parent on creation and reopen.
    Physical device independence/flush behavior still require external review.
    Interrupted/tampered ledgers are rejected, never silently truncated/repaired.
    """

    def __init__(self, path, *, test_volume, evidence_kind):
        if not isinstance(evidence_kind, str) or evidence_kind not in EVIDENCE_KINDS:
            raise ValueError("unknown evidence kind")
        raw_path = Path(path)
        self.path = raw_path.resolve()
        volume = Path(test_volume).resolve()
        if self.path == volume or volume in self.path.parents or raw_path.is_symlink():
            raise ValueError("ACK ledger must be outside the cut test volume and not a symlink")
        if not volume.is_dir() or not self.path.parent.is_dir():
            raise ValueError("test volume and controller parent must already exist")
        self.evidence_kind = evidence_kind
        if not self.path.exists():
            header = {"ledger_version": 1, "evidence_kind": evidence_kind}
            with self.path.open("xb") as stream:
                stream.write(_encode_record(header))
                stream.flush()
                os.fsync(stream.fileno())
        # A visible ledger may come from creation interrupted before parent sync.
        _sync_directory(self.path.parent)
        self.receipts = self._read()

    def _read(self):
        content = self.path.read_bytes()
        if not content.endswith(b"\n"):
            raise ValueError("truncated ACK ledger")
        try:
            records = [json.loads(line) for line in content.splitlines()]
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError("invalid ACK ledger JSON") from exc
        header = {"ledger_version": 1, "evidence_kind": self.evidence_kind}
        if not records or not isinstance(records[0], dict) or type(records[0].get("ledger_version")) is not int or records[0] != header:
            raise ValueError("ACK ledger version/evidence kind mismatch")
        output, seen = [], set()
        for sequence, record in enumerate(records[1:], 1):
            if not isinstance(record, dict) or set(record) != {"sequence", "receipt", "receipt_sha256"} or type(record["sequence"]) is not int or record["sequence"] != sequence:
                raise ValueError("invalid ACK ledger sequence or record shape")
            receipt = validate_receipt(record["receipt"])
            if hashlib.sha256(canonical_receipt(receipt)).hexdigest() != record["receipt_sha256"]:
                raise ValueError("ACK receipt checksum mismatch")
            if _identity(receipt) in seen:
                raise ValueError("duplicate ACK operation")
            seen.add(_identity(receipt))
            output.append(receipt)
        return output

    def record(self, receipt: dict):
        receipt = validate_receipt(receipt)
        self.receipts = self._read()
        if _identity(receipt) in {_identity(item) for item in self.receipts}:
            raise ValueError("duplicate ACK operation")
        record = {"sequence": len(self.receipts) + 1, "receipt": receipt, "receipt_sha256": hashlib.sha256(canonical_receipt(receipt)).hexdigest()}
        flags = os.O_WRONLY | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags)
        with os.fdopen(descriptor, "ab") as stream:
            stream.write(_encode_record(record))
            stream.flush()
            os.fsync(stream.fileno())
        self.receipts.append(receipt)


class DisposableRun:
    """Allocate and clean up only a unique synthetic directory created here."""

    def __init__(self, parent):
        self.parent = Path(parent).resolve()
        self.path = None
        self.marker = str(uuid.uuid4())

    def __enter__(self):
        self.path = Path(tempfile.mkdtemp(prefix="glamdb-verify-", dir=self.parent))
        identity = self.path.stat()
        self._directory_identity = identity.st_dev, identity.st_ino
        (self.path / ".glamdb-verification-disposable").write_text(self.marker)
        return self.path

    def __exit__(self, exc_type, exc, traceback):
        marker = self.path / ".glamdb-verification-disposable"
        identity = self.path.stat()
        if self.path.is_symlink() or self.path.resolve().parent != self.parent or (identity.st_dev, identity.st_ino) != self._directory_identity or marker.is_symlink() or not marker.is_file() or marker.read_text() != self.marker:
            raise ValueError("refusing cleanup: disposable directory marker changed")
        shutil.rmtree(self.path)


def fault_schedule(seed: int, *, seeded=100) -> list[dict]:
    """104 explicit before/after boundaries plus stable LCG-selected cut points."""
    if type(seed) is not int or not 0 <= seed <= 0xFFFFFFFF or type(seeded) is not int or not 0 <= seeded <= 100000:
        raise ValueError("seed must be uint32; seeded count must be 0..100000")
    file_steps = ("open", "create", "write", "flush", "footer", "file_sync", "close", "rename", "directory_sync", "ancestor_directory_sync")
    points = [f"{kind}.{step}.{side}" for kind in ("data", "manifest", "manifest_list", "root") for step in file_steps for side in ("before", "after")]
    points += [f"sqlite.{step}.{side}" for step in ("binding_commit", "journal_write", "journal_sync", "db_write", "db_sync", "journal_unlink", "directory_sync", "outcome_update", "outer_commit", "outer_result") for side in ("before", "after")]
    points += [f"{step}.{side}" for step in ("response", "ack") for side in ("before", "after")]
    output = [{"schedule_id": index, "kind": "boundary", "seed": seed, "cut_point": point} for index, point in enumerate(points)]
    state = seed
    for _ in range(seeded):
        state = (1664525 * state + 1013904223) & 0xFFFFFFFF
        output.append({"schedule_id": len(output), "kind": "seeded", "seed": state, "cut_point": points[state % len(points)]})
    return output


def check_evidence(evidence: dict) -> dict:
    """Check recorded claims without turning model/process checks into power PASS.

    Even a complete real-power receipt needs device/environment/raw-image review;
    this structural checker always leaves its acceptance gate open.
    """
    errors = []
    if not isinstance(evidence, dict):
        evidence = {}
        errors.append("evidence must be an object")
    kind, status = evidence.get("evidence_kind"), evidence.get("status")
    if type(evidence.get("evidence_version")) is not int or evidence.get("evidence_version") != 1:
        errors.append("unsupported evidence_version")
    if not isinstance(kind, str) or kind not in EVIDENCE_KINDS:
        errors.append("unknown evidence_kind")
    if status not in ("PASS", "FAIL", "NOT RUN"):
        errors.append("status must be PASS, FAIL or NOT RUN")
    if evidence.get("power_durability", "NOT VALIDATED") not in ("NOT VALIDATED", "REVIEW REQUIRED"):
        errors.append("this checker cannot accept a power durability PASS claim")
    if kind != "real-power" and evidence.get("power_durability", "NOT VALIDATED") != "NOT VALIDATED":
        errors.append("modeled/process evidence cannot qualify power durability")
    cycles, environment = evidence.get("cycles"), evidence.get("environment")
    if not isinstance(cycles, list):
        errors.append("cycles must be an array")
        cycles = []
    if not isinstance(environment, dict):
        errors.append("environment must be an object")
        environment = {}
    if status == "PASS" and not cycles:
        errors.append("PASS requires executed cycles")
    for index, cycle in enumerate(cycles):
        if not isinstance(cycle, dict):
            errors.append(f"cycle {index}: expected object")
            continue
        if type(cycle.get("seed")) is not int or not isinstance(cycle.get("cut_point"), str) or not cycle.get("cut_point"):
            errors.append(f"cycle {index}: seed and cut_point required")
        for error in check_recovery(cycle.get("acked"), cycle.get("recovered")):
            errors.append(f"cycle {index}: {error}")
    if kind == "real-power" and status != "NOT RUN":
        expected = {"profile": "LINUX-LOCAL-1", "os": "Linux", "architecture": "x86_64", "filesystem": "ext4"}
        for key, value in expected.items():
            if environment.get(key) != value:
                errors.append(f"real power environment requires {key}={value}")
        for key in ("device", "firmware", "kernel", "flush_behavior", "cache_policy", "mount_options", "controller_device"):
            if not isinstance(environment.get(key), str) or not environment[key]:
                errors.append(f"real power environment requires {key}")
        if environment.get("controller_volume_disjoint") is not True:
            errors.append("real power ACK controller must be independently durable and disjoint")
        if len(cycles) < 20:
            errors.append("real power evidence requires at least 20 cycles")
        artifacts = evidence.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            errors.append("real power evidence requires raw image/trace artifact index")
        else:
            for artifact in artifacts:
                if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str) or not isinstance(artifact.get("sha256"), str) or not _SHA256.fullmatch(artifact["sha256"]):
                    errors.append("invalid raw artifact path/SHA256")
    verification = "FAIL" if errors or status == "FAIL" else status
    power = "REVIEW REQUIRED" if kind == "real-power" and verification == "PASS" else "NOT VALIDATED"
    return {"evidence_kind": kind, "verification_status": verification, "power_durability": power, "acceptance_gate": "OPEN", "cycles_checked": len(cycles), "errors": errors}
