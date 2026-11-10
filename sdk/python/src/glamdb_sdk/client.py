"""Synchronous network-only client for the v0 public HTTP adapter contract."""

import hmac
import math
import re
import ssl
from typing import Any, Iterable, Mapping
from urllib.parse import quote
from uuid import UUID

import httpx

from .codec import Schema, decode_rows, int64, timestamp_us
from .errors import (AuthenticationError, ErrorDetail, HTTPError, ProtocolError,
                     TransportError, ValidationError)
from .models import ANY, AppendOutcome, CreateOutcome, OperationInProgress, Page, ScanResult
from .predicates import Predicate
from .request import AppendRequest, json_bytes, schema_fingerprint, uuid_string
from .transport import Deadline, read_json, translate_transport


class Client:
    """Use with a context manager. Mutating POSTs are sent exactly once per call.

    ``timeout`` sets HTTP connect/read/write/pool timeouts. Each method's optional
    ``deadline`` adds a total cooperative budget, checked across body reads.
    """

    def __init__(self, base_url: str, token: str, *, verify: bool | ssl.SSLContext = True,
                 timeout: float = 30.0, transport: httpx.BaseTransport | None = None,
                 max_json_bytes: int = 16 * 1024**2, max_arrow_read_bytes: int = 64 * 1024**2):
        try:
            url = httpx.URL(base_url)
        except (httpx.InvalidURL, TypeError) as exc:
            raise ValidationError("base_url must be an absolute HTTP or HTTPS URL") from exc
        if url.scheme not in ("http", "https") or not url.host or url.username or url.password or url.query or url.fragment:
            raise ValidationError("base_url must be HTTP(S), with no credentials, query, or fragment")
        if not isinstance(token, str) or not token or any(ord(c) < 33 or ord(c) > 126 for c in token):
            raise ValidationError("a nonempty ASCII bearer token without whitespace is required")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValidationError("timeout must be positive finite seconds")
        if not isinstance(verify, (bool, ssl.SSLContext)):
            raise ValidationError("verify must be bool or ssl.SSLContext")
        for name, value in (("max_json_bytes", max_json_bytes), ("max_arrow_read_bytes", max_arrow_read_bytes)):
            if type(value) is not int or value <= 0:
                raise ValidationError(f"{name} must be a positive integer")
        self._timeout = timeout
        self._max_json_bytes = max_json_bytes
        self._max_arrow_read_bytes = max_arrow_read_bytes
        self._schemas: dict[str, Schema] = {}
        self._streams: set[Any] = set()
        self._closed = False
        self._http = httpx.Client(base_url=str(url).rstrip("/") + "/", headers={"Authorization": f"Bearer {token}"},
                                  verify=verify, timeout=timeout, transport=transport, follow_redirects=False)

    def __enter__(self) -> "Client":
        if self._closed:
            raise TransportError("client is closed")
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        first_error = None
        try:
            for stream in tuple(self._streams):
                try:
                    stream.close()
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
        finally:
            try:
                self._http.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def _send(self, method: str, path: str, budget: Deadline, *, params: Mapping[str, Any] | None = None,
              body: bytes | None = None, accept: str = "application/json") -> httpx.Response:
        if self._closed:
            raise TransportError("client is closed", collection_id=budget.collection_id, operation_id=budget.operation_id)
        remaining = budget.check()
        timeout = self._timeout if remaining is None else min(self._timeout, remaining)
        headers = {"Accept": accept}
        if accept == "application/vnd.apache.arrow.stream":
            headers["Accept-Encoding"] = "identity"
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            request = self._http.build_request(method, path, params=params, content=body, headers=headers, timeout=timeout)
            response = self._http.send(request, stream=True)
            try:
                budget.check()
            except BaseException:
                response.close()
                raise
            if response.is_redirect:
                response.close()
                raise ProtocolError("redirects are disabled to preserve endpoint and bearer identity")
            return response
        except httpx.HTTPError as exc:
            raise translate_transport(exc, budget) from exc

    def _json(self, method: str, path: str, budget: Deadline, *, params: Mapping[str, Any] | None = None,
              body: bytes | None = None) -> tuple[int, dict[str, Any]]:
        try:
            response = self._send(method, path, budget, params=params, body=body)
            status = response.status_code
            value = read_json(response, budget, self._max_json_bytes)
            if status in (401, 403):
                self._http_error(status, value, budget)
            return status, value
        except ProtocolError as exc:
            exc.operation_id = budget.operation_id
            exc.collection_id = budget.collection_id
            raise

    @staticmethod
    def _http_error(status: int, value: dict[str, Any], budget: Deadline) -> None:
        raw_error = value.get("error", value)
        if not isinstance(raw_error, dict):
            raise ProtocolError("error response requires ErrorDetail", raw=value)
        detail = ErrorDetail.from_dict(raw_error)
        kind = AuthenticationError if status in (401, 403) else HTTPError
        raise kind(status, detail, raw=value, operation_id=budget.operation_id, collection_id=budget.collection_id)

    def _read(self, path: str, *, params: Mapping[str, Any] | None = None,
              deadline: float | None = None, collection_id: str | None = None) -> dict[str, Any]:
        budget = Deadline(deadline, collection_id=collection_id)
        status, value = self._json("GET", path, budget, params=params)
        if not 200 <= status < 300:
            self._http_error(status, value, budget)
        return value

    @staticmethod
    def _schema(value: Schema | Mapping[str, Any]) -> Schema:
        return value if isinstance(value, Schema) else Schema.from_dict(value)

    @staticmethod
    def _path(collection_id: str | UUID, suffix: str = "") -> tuple[str, str]:
        cid = uuid_string(collection_id, "collection_id")
        return cid, f"v0/collections/{cid}" + (f"/{suffix}" if suffix else "")

    @staticmethod
    def _selector(snapshot_id: Any) -> dict[str, str]:
        if snapshot_id is ANY:
            return {}
        return {"snapshot_id": "initial" if snapshot_id is None else str(int64(snapshot_id))}

    @staticmethod
    def _page_params(page_size: int, cursor: str | None, snapshot_id: Any = ANY) -> dict[str, str | int]:
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ValidationError("page_size must be an integer from 1 through 1000")
        params = {"page_size": page_size} | Client._selector(snapshot_id)
        if cursor is not None:
            if not isinstance(cursor, str) or not cursor:
                raise ValidationError("cursor must be a nonempty opaque string")
            params["cursor"] = cursor
        return params

    @staticmethod
    def _page(value: dict[str, Any]) -> Page:
        items, cursor = value.get("items"), value.get("next_cursor")
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items) or (cursor is not None and not isinstance(cursor, str)):
            raise ProtocolError("invalid Page response", raw=value)
        return Page(tuple(items), cursor, value)

    def capabilities(self, *, deadline: float | None = None) -> dict[str, Any]:
        return self._read("v0/capabilities", deadline=deadline)

    def list_collections(self, *, page_size: int = 100, cursor: str | None = None,
                         deadline: float | None = None) -> Page:
        return self._page(self._read("v0/collections", params=self._page_params(page_size, cursor), deadline=deadline))

    def resolve_collection(self, name: str, *, deadline: float | None = None) -> dict[str, Any]:
        if not isinstance(name, str) or not name:
            raise ValidationError("exact collection name is required")
        return self._read("v0/collections:resolve", params={"name": name}, deadline=deadline)

    def get_collection(self, collection_id: str | UUID, *, deadline: float | None = None) -> dict[str, Any]:
        cid, path = self._path(collection_id)
        return self._read(path, deadline=deadline, collection_id=cid)

    def get_schema(self, collection_id: str | UUID, *, deadline: float | None = None) -> Schema:
        cid, path = self._path(collection_id, "schema")
        value = self._read(path, deadline=deadline, collection_id=cid)
        try:
            schema = self._schema(value.get("schema", value))
        except ValidationError as exc:
            raise ProtocolError("invalid server schema", raw=value) from exc
        self._schemas[cid] = schema
        return schema

    def stats(self, collection_id: str | UUID, *, snapshot_id: Any = ANY, deadline: float | None = None) -> dict[str, Any]:
        cid, path = self._path(collection_id, "stats")
        return self._read(path, params=self._selector(snapshot_id), deadline=deadline, collection_id=cid)

    def snapshots(self, collection_id: str | UUID, *, page_size: int = 100, cursor: str | None = None,
                  snapshot_id: Any = ANY, deadline: float | None = None) -> Page:
        cid, path = self._path(collection_id, "snapshots")
        return self._page(self._read(path, params=self._page_params(page_size, cursor, snapshot_id), deadline=deadline, collection_id=cid))

    def files(self, collection_id: str | UUID, *, page_size: int = 100, cursor: str | None = None,
              snapshot_id: Any = ANY, deadline: float | None = None) -> Page:
        cid, path = self._path(collection_id, "files")
        return self._page(self._read(path, params=self._page_params(page_size, cursor, snapshot_id), deadline=deadline, collection_id=cid))

    def file(self, collection_id: str | UUID, file_id: str, *, snapshot_id: Any = ANY,
             deadline: float | None = None) -> dict[str, Any]:
        if not isinstance(file_id, str) or not file_id:
            raise ValidationError("file_id must be a nonempty opaque string")
        cid, path = self._path(collection_id, "files/" + quote(file_id, safe=""))
        return self._read(path, params=self._selector(snapshot_id), deadline=deadline, collection_id=cid)

    def create_collection(self, name: str, schema: Schema | Mapping[str, Any], *, collection_id: str | UUID,
                          deadline: float | None = None) -> CreateOutcome:
        cid = uuid_string(collection_id, "collection_id")
        schema = self._schema(schema)
        if not isinstance(name, str) or not name:
            raise ValidationError("exact collection name is required")
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", "v0/collections", budget,
                                   body=json_bytes({"collection_id": cid, "name": name, "schema": schema.to_dict()}))
        try:
            kind = value.get("outcome")
            if kind is None and status in (200, 201):
                info, kind = value, "committed"
            else:
                info = value.get("info", value.get("result"))
            if kind == "committed":
                if status not in (200, 201) or not isinstance(info, dict) or "error" in value:
                    raise ProtocolError("invalid committed create outcome", raw=value)
                self._verify_created(info, cid, name, schema)
                self._schemas[cid] = schema
            elif kind == "definitely_not_committed":
                if not isinstance(value.get("error"), dict) or info is not None:
                    raise ProtocolError("invalid noncommitted create outcome", raw=value)
            elif kind == "commit_unknown":
                if status != 503 or value.get("recovery_required") is not True or info is not None:
                    raise ProtocolError("invalid unknown create outcome", raw=value)
            elif status >= 400:
                self._http_error(status, value, budget)
            else:
                raise ProtocolError("missing or unrecognized create outcome", raw=value)
            error = ErrorDetail.from_dict(value["error"]) if isinstance(value.get("error"), dict) else None
            return CreateOutcome(kind, value.get("phase"), cid, info, error, value)
        except ProtocolError as exc:
            exc.collection_id = cid
            raise

    @staticmethod
    def _verify_created(info: Mapping[str, Any], cid: str, name: str, schema: Schema) -> None:
        try:
            actual_schema = Schema.from_dict(info["schema"])
            same = info.get("collection_id") == cid and info.get("name") == name and schema_fingerprint(actual_schema) == schema_fingerprint(schema)
        except (KeyError, ValidationError) as exc:
            raise ProtocolError("created collection has missing or invalid identity/schema", raw=info) from exc
        if not same:
            raise ProtocolError("resolved collection UUID, exact name, or schema does not match original create", raw=info)

    def resolve_create(self, name: str, schema: Schema | Mapping[str, Any], *, collection_id: str | UUID,
                       deadline: float | None = None) -> dict[str, Any]:
        cid = uuid_string(collection_id, "collection_id")
        info = self.resolve_collection(name, deadline=deadline)
        self._verify_created(info, cid, name, self._schema(schema))
        return info

    def prepare_append(self, collection_id: str | UUID, rows: Iterable[Mapping[str, Any]], *, operation_id: str | UUID,
                       schema: Schema | Mapping[str, Any] | None = None, expected_snapshot: Any = ANY,
                       deadline: float | None = None) -> AppendRequest:
        cid = uuid_string(collection_id, "collection_id")
        op = uuid_string(operation_id, "operation_id")
        selected_schema = self._schema(schema) if schema is not None else self._schemas.get(cid)
        if selected_schema is None:
            try:
                selected_schema = self.get_schema(cid, deadline=deadline)
            except (TransportError, ProtocolError, HTTPError) as exc:
                exc.collection_id = cid
                exc.operation_id = op
                raise
        return AppendRequest.prepare(cid, op, selected_schema, rows, expected_snapshot)

    def append(self, collection_id: str | UUID | AppendRequest, rows: Iterable[Mapping[str, Any]] | None = None, *,
               operation_id: str | UUID | None = None, schema: Schema | Mapping[str, Any] | None = None,
               expected_snapshot: Any = ANY, deadline: float | None = None) -> AppendOutcome:
        if isinstance(collection_id, AppendRequest):
            if rows is not None or operation_id is not None or schema is not None or expected_snapshot is not ANY:
                raise ValidationError("prepared append already owns rows, IDs, schema, and precondition")
            request = collection_id
            budget = Deadline(deadline, collection_id=request.collection_id, operation_id=request.operation_id)
        else:
            cid = uuid_string(collection_id, "collection_id")
            op = uuid_string(operation_id, "operation_id")
            budget = Deadline(deadline, collection_id=cid, operation_id=op)
            remaining = budget.check()
            if rows is None:
                raise ValidationError("append rows are required")
            request = self.prepare_append(cid, rows, operation_id=op, schema=schema,
                                          expected_snapshot=expected_snapshot, deadline=remaining)
        status, value = self._json("POST", f"v0/collections/{request.collection_id}/append", budget, body=request.body)
        return self._append_outcome(status, value, budget, expected_payload_hash=request.payload_hash)

    @staticmethod
    def _validate_receipt(result: Mapping[str, Any], expected_payload_hash: str | None) -> None:
        digest = result.get("payload_hash")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-fA-F]{64}", digest, re.ASCII) is None:
            raise ProtocolError("append receipt requires a SHA256 hexadecimal payload_hash")
        if type(result.get("hash_version")) is not int or result["hash_version"] != 1:
            raise ProtocolError("append receipt requires supported hash_version 1")
        if expected_payload_hash is not None and not hmac.compare_digest(digest.lower(), expected_payload_hash.lower()):
            raise ProtocolError("append receipt payload_hash differs from the original request")
        if type(result.get("receipt_version")) is not int or result["receipt_version"] != 1:
            raise ProtocolError("append receipt requires supported receipt_version 1")
        rows = int64(result.get("rows_appended"))
        if not 1 <= rows <= 10000:
            raise ProtocolError("append receipt rows_appended is outside the v0 append profile")
        created_at = result.get("snapshot_created_at_us")
        if not isinstance(created_at, str):
            raise ProtocolError("append receipt requires decimal-string snapshot_created_at_us")
        timestamp_us(int64(created_at))
        # The development adapter has no physical files and omits unavailable
        # measurements. Check optional metrics without inventing missing ones.
        for name, minimum in (("data_files_added", 1), ("compressed_data_bytes_added", 0), ("rows_total", rows)):
            if name in result and int64(result[name]) < minimum:
                raise ProtocolError(f"append receipt has invalid {name}")
        if "parent_snapshot_id" in result and result["parent_snapshot_id"] is not None:
            parent = result["parent_snapshot_id"]
            if not isinstance(parent, str):
                raise ProtocolError("append receipt parent_snapshot_id must be a decimal string or null")
            int64(parent)
        if "durability_profile" in result and (not isinstance(result["durability_profile"], str) or not result["durability_profile"]):
            raise ProtocolError("append receipt has invalid durability_profile")

    def _append_outcome(self, status: int, value: dict[str, Any], budget: Deadline, *,
                        expected_payload_hash: str | None = None) -> AppendOutcome:
        try:
            kind, result, error_value = value.get("outcome"), value.get("result"), value.get("error")
            if kind == "committed":
                if status != 200 or not isinstance(result, dict) or error_value is not None:
                    raise ProtocolError("invalid committed append outcome", raw=value)
                if result.get("operation_id") != budget.operation_id or result.get("collection_id") != budget.collection_id:
                    raise ProtocolError("append receipt has different collection or operation UUID", raw=value)
                if not isinstance(result.get("snapshot_id"), str):
                    raise ProtocolError("append receipt requires decimal-string snapshot_id", raw=value)
                int64(result["snapshot_id"])
                self._validate_receipt(result, expected_payload_hash)
            elif kind == "definitely_not_committed":
                if not isinstance(error_value, dict) or result is not None:
                    raise ProtocolError("invalid noncommitted append outcome", raw=value)
            elif kind == "commit_unknown":
                if status != 503 or value.get("recovery_required") is not True or result is not None:
                    raise ProtocolError("invalid unknown append outcome", raw=value)
            elif status >= 400:
                self._http_error(status, value, budget)
            else:
                raise ProtocolError("missing or unrecognized append outcome", raw=value)
            if value.get("operation_id", budget.operation_id) != budget.operation_id:
                raise ProtocolError("outcome operation UUID differs from request", raw=value)
            error = ErrorDetail.from_dict(error_value) if isinstance(error_value, dict) else None
            return AppendOutcome(kind, value.get("phase"), budget.operation_id, result, error, value)
        except ValidationError as exc:
            raise ProtocolError("invalid append receipt scalar", raw=value, operation_id=budget.operation_id,
                                collection_id=budget.collection_id) from exc
        except ProtocolError as exc:
            exc.operation_id = budget.operation_id
            exc.collection_id = budget.collection_id
            exc.raw = value
            raise

    def find_append_outcome(self, collection_id: str | UUID, operation_id: str | UUID, *,
                            deadline: float | None = None) -> AppendOutcome | OperationInProgress | None:
        op = uuid_string(operation_id, "operation_id")
        cid, path = self._path(collection_id, "operations/" + op)
        budget = Deadline(deadline, collection_id=cid, operation_id=op)
        status, value = self._json("GET", path, budget)
        if status == 404:
            error = value.get("error")
            if not isinstance(error, dict) or error.get("code") != "NOT_FOUND":
                self._http_error(status, value, budget)
            return None
        if status == 200 and value.get("state") == "in_progress" and "outcome" not in value:
            if value.get("operation_id", op) != op:
                raise ProtocolError("in-progress operation UUID differs from request", operation_id=op, collection_id=cid, raw=value)
            return OperationInProgress(op, value)
        return self._append_outcome(status, value, budget)

    @staticmethod
    def _scan_body(projection: Iterable[str] | None, predicate: Predicate | Mapping[str, Any] | None,
                   snapshot_id: Any, limit: int | None) -> bytes:
        if projection is not None:
            if isinstance(projection, (str, bytes)):
                raise ValidationError("projection must be a sequence of field names")
            projection = list(projection)
            if any(not isinstance(v, str) or not v for v in projection) or len(set(projection)) != len(projection):
                raise ValidationError("projection fields must be unique nonempty names")
        if isinstance(predicate, Predicate):
            predicate = predicate.to_dict()
        elif predicate is not None and not isinstance(predicate, Mapping):
            raise ValidationError("predicate must be a Predicate or JSON object")
        body = {"projection": projection, "predicate": predicate}
        if snapshot_id is not ANY:
            body["snapshot_id"] = None if snapshot_id is None else str(int64(snapshot_id))
        if limit is not None:
            if type(limit) is not int or limit < 0:
                raise ValidationError("scan limit must be a nonnegative integer")
            body["limit"] = limit
        return json_bytes(body)

    def explain(self, collection_id: str | UUID, *, projection: Iterable[str] | None = None,
                predicate: Predicate | Mapping[str, Any] | None = None, snapshot_id: Any = ANY,
                limit: int | None = None, deadline: float | None = None) -> dict[str, Any]:
        cid, path = self._path(collection_id, "explain")
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", path, budget, body=self._scan_body(projection, predicate, snapshot_id, limit))
        if status != 200:
            self._http_error(status, value, budget)
        return value

    def scan_json(self, collection_id: str | UUID, *, projection: Iterable[str] | None = None,
                  predicate: Predicate | Mapping[str, Any] | None = None, snapshot_id: Any = ANY,
                  limit: int | None = None, deadline: float | None = None) -> ScanResult:
        cid, path = self._path(collection_id, "scan")
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", path, budget, body=self._scan_body(projection, predicate, snapshot_id, limit))
        if status != 200:
            self._http_error(status, value, budget)
        try:
            schema = Schema.from_dict(value["schema"], allow_empty=True)
            snapshot = value["snapshot_id"]
            if snapshot is not None and (not isinstance(snapshot, str) or str(int64(snapshot)) != snapshot):
                raise ValidationError("snapshot_id must be a canonical decimal string or null")
            if not isinstance(value["rows"], list) or not isinstance(value["profile"], dict) or not isinstance(value["identity"], dict):
                raise ValidationError("invalid scan rows, profile, or identity")
            rows = decode_rows(schema, value["rows"])
            return ScanResult(schema, snapshot, rows, value["profile"], value["identity"], value)
        except (KeyError, ValidationError) as exc:
            raise ProtocolError("invalid scan JSON response", raw=value, collection_id=cid) from exc

    def scan_batches(self, collection_id: str | UUID, *, projection: Iterable[str] | None = None,
                     predicate: Predicate | Mapping[str, Any] | None = None, snapshot_id: Any = ANY,
                     limit: int | None = None, deadline: float | None = None):
        from .stream import ScanStream

        cid, path = self._path(collection_id, "scan")
        budget = Deadline(deadline, collection_id=cid)
        response = self._send("POST", path, budget, body=self._scan_body(projection, predicate, snapshot_id, limit),
                              accept="application/vnd.apache.arrow.stream")
        if response.status_code != 200:
            status = response.status_code
            value = read_json(response, budget, self._max_json_bytes)
            self._http_error(status, value, budget)
        stream = ScanStream(response, budget, max_read_bytes=self._max_arrow_read_bytes,
                            on_close=self._streams.discard)
        self._streams.add(stream)
        return stream
