"""Validate resumable identities without discarding supported extension fields."""

import hmac
import re
from typing import Any, Mapping

from .codec import int64
from .errors import ErrorDetail, ProtocolError
from .request import uuid_string


_PHASES = {"binding", "catalog_transaction", "catalog_commit", "recording_failure", "reconciling"}
_HINTS = {"SameIdAfterReconcile", "NewOperationId", "FixInput", "OperatorAction"}


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("invalid recovery field")


def _object(value: Any, fields: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict) and set(fields.split()) <= value.keys())
    return value


def _uuid(value: Any) -> None:
    _require(isinstance(value, str) and uuid_string(value, "recovery identity") == value)


def _text(value: Any, limit: int) -> None:
    _require(isinstance(value, str) and 0 < len(value.encode("utf-8")) <= limit and "\0" not in value)


def _digest(value: Any) -> None:
    _require(isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{64}", value, re.ASCII) is not None)


def _snapshot(value: Any) -> None:
    if value is not None:
        _require(isinstance(value, str))
        int64(value)


def _error(value: Any) -> None:
    value = _object(value, "code message retry_hint")
    detail = ErrorDetail.from_dict(value)
    _text(detail.code, 128)
    _require(len(detail.message.encode("utf-8")) <= 2048 and detail.retry_hint in _HINTS)


def _context(value: Mapping[str, Any], fields: str) -> Mapping[str, Any]:
    _require(value.get("phase") in _PHASES)
    _error(value.get("error"))
    recovery = _object(value.get("recovery"), fields)
    _uuid(recovery["store_id"])
    _uuid(recovery["collection_id"])
    _uuid(recovery["attempt_id"])
    _require(type(recovery["publication_started"]) is bool)
    location = recovery["candidate_metadata_location"]
    if location is not None:
        _text(location, 2048)
    return recovery


def validate_append_recovery(value: Mapping[str, Any], collection_id: str, operation_id: str,
                             expected_payload_hash: str | None) -> None:
    try:
        recovery = _context(value, "store_id collection_id operation_id payload_hash hash_version base attempt_id "
                            "candidate_snapshot_id candidate_metadata_location first_error publication_started lookup_blocked")
        _require(recovery["collection_id"] == collection_id and recovery["operation_id"] == operation_id)
        _digest(recovery["payload_hash"])
        _require(type(recovery["hash_version"]) is int and recovery["hash_version"] == 1)
        if expected_payload_hash is not None:
            _require(hmac.compare_digest(recovery["payload_hash"].lower(), expected_payload_hash.lower()))
        base = _object(recovery["base"], "store_id collection_id table_uuid metadata_location current_snapshot_id schema_id spec_id")
        _require(base["store_id"] == recovery["store_id"] and base["collection_id"] == collection_id)
        _uuid(base["table_uuid"])
        _text(base["metadata_location"], 2048)
        _snapshot(base["current_snapshot_id"])
        for field in ("schema_id", "spec_id"):
            _require(type(base[field]) is int and 0 <= base[field] < 2**31)
        _snapshot(recovery["candidate_snapshot_id"])
        _error(recovery["first_error"])
        _require(type(recovery["lookup_blocked"]) is bool)
        if recovery["lookup_blocked"]:
            _require(not recovery["publication_started"] and recovery["candidate_snapshot_id"] is None
                     and recovery["candidate_metadata_location"] is None)
    except (ValueError, TypeError, KeyError, ProtocolError) as exc:
        raise ProtocolError("invalid or incomplete append recovery context", raw=value) from exc


def validate_create_recovery(value: Mapping[str, Any], collection_id: str, name: str, schema_hash: str) -> None:
    try:
        recovery = _context(value, "store_id collection_id exact_name schema_hash attempt_id candidate_table_uuid "
                            "candidate_metadata_location publication_started")
        _require(recovery["collection_id"] == collection_id and recovery["exact_name"] == name)
        _text(recovery["exact_name"], 128)
        _digest(recovery["schema_hash"])
        _require(hmac.compare_digest(recovery["schema_hash"].lower(), schema_hash.lower()))
        if recovery["candidate_table_uuid"] is not None:
            _uuid(recovery["candidate_table_uuid"])
    except (ValueError, TypeError, KeyError, ProtocolError) as exc:
        raise ProtocolError("invalid or incomplete create recovery context", raw=value) from exc
