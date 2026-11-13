"""Synchronous network-only client for the v0 public HTTP adapter contract."""

import hmac
import ipaddress
import math
import re
import ssl
import weakref
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import quote
from uuid import UUID

import httpx

from .cleanup import CleanupQueue
from .bulk import BulkAppendRequest
from .codec import Schema, decode_rows, int64, timestamp_us
from .errors import (AuthenticationError, ErrorDetail, HTTPError, OutcomeUnknownError, ProtocolError,
                     TransportError, ValidationError)
from .models import ANY, AppendOutcome, CreateOutcome, LookupResult, OperationInProgress, Page, ScanResult
from .kv import validate_mutation_receipt
from .predicates import Predicate
from .request import AppendRequest, MutationRequest, _check_request_size, bounded_keys, json_bytes, schema_fingerprint, uuid_string
from .recovery import validate_append_recovery, validate_create_recovery
from .snapshot_archive import (KEEP_ALL_SNAPSHOTS_IN_ROOT, hot_snapshots_value, reported_collection_hot_snapshots,
                               validate_archive_status)
from .transport import JSON_ACCEPT_ENCODING, Deadline, read_json, translate_transport
from .write_settings import canonical_write_settings, request_write_settings, write_settings_version


def _bearer_token(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(ord(c) < 33 or ord(c) > 126 for c in value)
    ):
        raise ValidationError(
            "a nonempty ASCII bearer token without whitespace is required"
        )
    return value


class _ScanPages:
    def __init__(self, iterator, on_close, cleanup_queue):
        self._iterator, self._on_close = iterator, on_close
        self._failure = None
        self._closed = False
        self.integrity = "not_complete"
        self._finalizer = cleanup_queue.register(self, iterator.close)

    def __iter__(self):
        return self

    def __next__(self):
        if self._failure is not None:
            raise self._failure
        if self._closed:
            raise StopIteration
        try:
            return next(self._iterator)
        except StopIteration:
            self.integrity = "complete"
            self.close()
            raise
        except BaseException as exc:
            self._failure = exc
            try:
                self.close()
            except BaseException:
                pass
            raise

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._finalizer.detach()
        try:
            self._iterator.close()
        finally:
            self._on_close(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Client:
    """Use with a context manager. Mutating POSTs are sent exactly once per call.

    ``timeout`` sets HTTP connect/read/write/pool timeouts. Each method's optional
    ``deadline`` adds a total cooperative budget, checked across body reads.
    Supply a direct Bearer token or a callable ``token_provider``. Provider
    failures happen before the API request; HTTP authentication failures never
    replay a mutating request.
    """

    def __init__(self, base_url: str, token: str | None = None, *, username: str | None = None,
                 token_provider: Callable[[], str] | None = None,
                 verify: bool | ssl.SSLContext = True, timeout: float = 30.0,
                 database_id: str | UUID | None = None, transport: httpx.BaseTransport | None = None,
                 max_json_bytes: int = 16 * 1024**2, max_arrow_read_bytes: int = 64 * 1024**2):
        try:
            url = httpx.URL(base_url)
        except (httpx.InvalidURL, TypeError) as exc:
            raise ValidationError("base_url must be an absolute HTTP or HTTPS URL") from exc
        if url.scheme not in ("http", "https") or not url.host or url.username or url.password or url.query or url.fragment:
            raise ValidationError("base_url must be HTTP(S), with no credentials, query, or fragment")
        if username is not None:
            raise ValidationError(
                "HTTP Basic username mode was removed; use an OAuth access token or token_provider"
            )
        if token_provider is not None:
            if token is not None or not callable(token_provider):
                raise ValidationError(
                    "supply either a direct Bearer token or a callable token_provider"
                )
            try:
                loopback = ipaddress.ip_address(url.host).is_loopback
            except ValueError:
                loopback = url.host == "localhost"
            if url.scheme != "https" and not loopback:
                raise ValidationError(
                    "token_provider requires HTTPS or an explicit loopback URL"
                )
        else:
            _bearer_token(token)
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValidationError("timeout must be positive finite seconds")
        if not isinstance(verify, (bool, ssl.SSLContext)):
            raise ValidationError("verify must be bool or ssl.SSLContext")
        for name, value in (("max_json_bytes", max_json_bytes), ("max_arrow_read_bytes", max_arrow_read_bytes)):
            if type(value) is not int or value <= 0:
                raise ValidationError(f"{name} must be a positive integer")
        self._database_prefix = "" if database_id is None else f"databases/{uuid_string(database_id, 'database_id')}/"
        self._timeout = timeout
        self._max_json_bytes = max_json_bytes
        self._max_arrow_read_bytes = max_arrow_read_bytes
        self._schemas: dict[str, Schema] = {}
        self._streams = weakref.WeakSet()
        self._cleanup = CleanupQueue()
        self._closed = False
        self._token_provider = token_provider
        headers = {"Authorization": f"Bearer {token}"} if token_provider is None else {}
        self._http = httpx.Client(base_url=str(url).rstrip("/") + "/", headers=headers,
                                  verify=verify, timeout=timeout, transport=transport, follow_redirects=False)

    def __enter__(self) -> "Client":
        if self._closed:
            raise TransportError("client is closed")
        return self

    def __exit__(self, *args: Any) -> None:
        try:
            self.close()
        except BaseException:
            if args[0] is None:
                raise

    def close(self) -> None:
        if self._closed:
            return
        self._cleanup.drain()
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
                self._cleanup.close()
                self._closed = True
                self._http.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def _send(self, method: str, path: str, budget: Deadline, *, params: Mapping[str, Any] | None = None,
              body: bytes | None = None, accept: str = "application/json",
              bulk: BulkAppendRequest | None = None) -> httpx.Response:
        if self._closed:
            raise TransportError("client is closed", collection_id=budget.collection_id, operation_id=budget.operation_id)
        if body is not None:
            _check_request_size(len(body))
        self._cleanup.drain()
        budget.check()
        headers = {"Accept": accept, "Accept-Encoding": JSON_ACCEPT_ENCODING}
        if self._token_provider is not None:
            try:
                headers["Authorization"] = "Bearer " + _bearer_token(
                    self._token_provider()
                )
            except Exception:
                raise TransportError(
                    "token provider failed; no API request was sent",
                    collection_id=budget.collection_id,
                    operation_id=budget.operation_id,
                ) from None
        remaining = budget.check()
        timeout = self._timeout if remaining is None else min(self._timeout, remaining)
        if accept == "application/vnd.apache.arrow.stream":
            headers["Accept-Encoding"] = "identity"
        if body is not None:
            headers["Content-Type"] = "application/json"
        if bulk is not None:
            headers["Content-Type"] = "application/x-ndjson"
            headers["Content-Length"] = str(bulk.content_length)
        try:
            if self._database_prefix and not path.startswith("databases"):
                path = self._database_prefix + path.removeprefix("./")
            if bulk is None:
                request = self._http.build_request(method, path, params=params, content=body, headers=headers, timeout=timeout)
                response = self._http.send(request, stream=True)
            else:
                with bulk.open_body() as chunks:
                    def checked_chunks():
                        for chunk in chunks:
                            budget.check()
                            yield chunk
                    request = self._http.build_request(method, path, params=params, content=checked_chunks(),
                                                       headers=headers, timeout=timeout)
                    response = self._http.send(request, stream=True)
            try:
                budget.check()
            except BaseException:
                response.close()
                raise
            if response.is_redirect:
                response.close()
                raise ProtocolError("redirects are disabled to preserve endpoint and authentication identity")
            return response
        except httpx.HTTPError as exc:
            raise translate_transport(exc, budget) from exc
        except OSError:
            if bulk is None:
                raise
            raise TransportError("bulk upload source failed; no authoritative write outcome inferred",
                                 collection_id=budget.collection_id, operation_id=budget.operation_id) from None

    def _json(self, method: str, path: str, budget: Deadline, *, params: Mapping[str, Any] | None = None,
              body: bytes | None = None, write: bool = False,
              bulk: BulkAppendRequest | None = None) -> tuple[int, dict[str, Any]]:
        try:
            response = self._send(method, path, budget, params=params, body=body, bulk=bulk)
            status = response.status_code
            try:
                value = read_json(response, budget, self._max_json_bytes)
            except ProtocolError:
                if write and (status >= 500 or status == 408):
                    self._unknown_write(status, {}, budget)
                raise
            if (write and (status >= 500 or status == 408)
                    and value.get("outcome") not in ("committed", "definitely_not_committed", "commit_unknown")
                    and not self._pre_execution_error(status, value)):
                self._unknown_write(status, value, budget)
            if status in (401, 403):
                self._http_error(status, value, budget)
            return status, value
        except ProtocolError as exc:
            exc.operation_id = budget.operation_id
            exc.collection_id = budget.collection_id
            raise

    @staticmethod
    def _pre_execution_error(status, value):
        error = value.get("error")
        return (value.get("execution_started") is False
                and set(value).isdisjoint(("outcome", "phase", "result", "info", "recovery", "recovery_required"))
                and isinstance(error, dict)
                and (status, error.get("code")) in ((503, "SERVER_BUSY"), (408, "REQUEST_TIMEOUT"))
                and isinstance(error.get("message"), str)
                and error.get("retry_hint") == "SameIdAfterReconcile")

    @staticmethod
    def _unknown_write(status, value, budget):
        detail = {"code": "OUTCOME_UNKNOWN", "message": "Server write outcome is unknown; reconcile using the original identity"}
        supplied = value.get("error")
        if isinstance(supplied, dict) and isinstance(supplied.get("code"), str) and isinstance(supplied.get("message"), str):
            detail = dict(supplied)
        # Preserve server diagnostics in detail and the untouched raw envelope,
        # but never propagate a FixInput hint for an unconfirmed publication.
        detail["retry_hint"] = "SameIdAfterReconcile"
        raise OutcomeUnknownError(status, ErrorDetail.from_dict(detail), raw=value,
                                  operation_id=budget.operation_id, collection_id=budget.collection_id)

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
        return cid, f"collections/{cid}" + (f"/{suffix}" if suffix else "")

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
        return self._read("capabilities", deadline=deadline)

    def list_databases(self, *, deadline: float | None = None) -> Page:
        return self._page(self._read("databases", deadline=deadline))

    def get_database(self, database_id: str | UUID, *, deadline: float | None = None) -> dict[str, Any]:
        return self._read(f"databases/{uuid_string(database_id, 'database_id')}", deadline=deadline)

    def _database_post(self, path: str, body: dict[str, Any], deadline: float | None) -> dict[str, Any]:
        budget = Deadline(deadline)
        status, value = self._json("POST", path, budget, body=json_bytes(body))
        if not 200 <= status < 300:
            self._http_error(status, value, budget)
        return value

    def create_database(self, name: str, *, database_id: str | UUID | None = None,
                        limits: dict[str, int] | None = None,
                        deadline: float | None = None) -> dict[str, Any]:
        """Create or open the same named database. Retry with the same name and ID.

        Native max_collections accepts 1..64 (default 8), subject to shared
        resource admission. Managed limits persist across service restarts.
        """
        if not isinstance(name, str) or not name:
            raise ValidationError("database name is required")
        body = {"name": name}
        if database_id is not None:
            body["database_id"] = uuid_string(database_id, "database_id")
        if limits is not None:
            body["limits"] = limits
        return self._database_post("databases", body, deadline)

    def open_database(self, database_id: str | UUID, *, limits: dict[str, int] | None = None,
                      deadline: float | None = None) -> dict[str, Any]:
        body = {} if limits is None else {"limits": limits}
        return self._database_post(f"databases/{uuid_string(database_id, 'database_id')}/open", body, deadline)

    def close_database(self, database_id: str | UUID, *, deadline: float | None = None) -> dict[str, Any]:
        return self._database_post(f"databases/{uuid_string(database_id, 'database_id')}/close", {}, deadline)

    def reconcile_database(self, database_id: str | UUID, *, deadline: float | None = None) -> dict[str, Any]:
        return self._database_post(f"databases/{uuid_string(database_id, 'database_id')}/reconcile", {}, deadline)

    def drop_database(self, database_id: str | UUID, *, store_id: str | UUID,
                      deadline: float | None = None) -> dict[str, Any]:
        """Drop the saved database/store identity once; retry manually with the same IDs.

        A lost reply or timeout is ambiguous. Never resolve a database by name
        to recover this request: that name may already refer to another store.
        """
        database = uuid_string(database_id, "database_id")
        store = uuid_string(store_id, "store_id")
        if store != database:
            raise ValidationError("store_id must match the original database_id")
        value = self._database_post(f"databases/{database}/drop", {"store_id": store}, deadline)
        if (value.get("database_id") != database or value.get("store_id") != store
                or value.get("dropped") is not True):
            raise ProtocolError("invalid database drop response identity or result", raw=value)
        return value

    def list_collections(self, *, page_size: int = 100, cursor: str | None = None,
                         deadline: float | None = None) -> Page:
        """Read a fixed collection inventory; repeat page_size with next_cursor.

        A continuation excludes collections created after the first page.
        """
        return self._page(self._read("collections", params=self._page_params(page_size, cursor), deadline=deadline))

    def storage_footprint(self, *, store_id: str | UUID, deadline: float | None = None) -> dict[str, Any]:
        """Inventory the exact owned store, independently of any selected snapshot."""
        expected = uuid_string(store_id, "store_id")
        value = self._read("storage", deadline=deadline)
        if (type(value.get("version")) is not int or value["version"] != 1 or
                value.get("store_id") != expected or value.get("backend") != "local" or
                value.get("mock") is not False or value.get("consistency") != "writer_quiescent"):
            raise ProtocolError("physical inventory identity or provenance differs", raw=value)
        for name in ("database_file_bytes", "database_allocated_bytes", "data_file_bytes",
                     "iceberg_metadata_file_bytes", "catalog_file_bytes", "other_file_bytes",
                     "shared_file_bytes", "committed_data_file_bytes", "referenced_metadata_file_bytes",
                     "unreferenced_warehouse_file_bytes", "files", "entries", "hardlink_paths"):
            item = value.get(name)
            if not isinstance(item, str) or not item.isascii() or not item.isdecimal() or str(int(item)) != item:
                raise ProtocolError("physical inventory requires exact nonnegative decimal values", raw=value)
        return value

    def resolve_collection(self, name: str, *, deadline: float | None = None) -> dict[str, Any]:
        if not isinstance(name, str) or not name:
            raise ValidationError("exact collection name is required")
        # Keep the colon in the relative path instead of treating it as a URI scheme.
        return self._read("./collections:resolve", params={"name": name}, deadline=deadline)

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
        return self._metadata(self._read(path, params=self._selector(snapshot_id), deadline=deadline, collection_id=cid), cid, snapshot_id)

    @staticmethod
    def _metadata(value: dict[str, Any], collection_id: str, snapshot_id: Any) -> dict[str, Any]:
        identity = value.get("identity")
        if identity is None and value.get("mock") is not False:
            return value
        if not isinstance(identity, dict) or identity.get("collection_id") != collection_id:
            raise ProtocolError("metadata collection identity does not match the request", raw=value)
        # The current root head can differ from the historical view. Modern
        # native responses make that selection explicit; legacy mock identities
        # keep the selected snapshot inside identity.snapshot_id.
        if value.get("mock") is True and "snapshot_id" in identity:
            selected = identity["snapshot_id"]
        elif "snapshot_id" in value:
            selected = value["snapshot_id"]
        elif "snapshot_id" in identity and value.get("mock") is not False:
            selected = identity["snapshot_id"]
        else:
            raise ProtocolError("metadata selected snapshot is missing", raw=value)
        try:
            if selected is not None and (not isinstance(selected, str) or str(int64(selected)) != selected):
                raise ValidationError("snapshot must be a canonical decimal string")
        except ValidationError as error:
            raise ProtocolError("invalid metadata selected snapshot", raw=value) from error
        if snapshot_id is not ANY:
            expected = None if snapshot_id is None else str(int64(snapshot_id))
            if selected != expected:
                raise ProtocolError("metadata selected snapshot does not match the request", raw=value)
        return value

    def snapshots(self, collection_id: str | UUID, *, page_size: int = 100, cursor: str | None = None,
                  snapshot_id: Any = ANY, deadline: float | None = None) -> Page:
        cid, path = self._path(collection_id, "snapshots")
        return self._page(self._metadata(self._read(path, params=self._page_params(page_size, cursor, snapshot_id), deadline=deadline, collection_id=cid), cid, snapshot_id))

    def files(self, collection_id: str | UUID, *, page_size: int = 100, cursor: str | None = None,
              snapshot_id: Any = ANY, deadline: float | None = None) -> Page:
        cid, path = self._path(collection_id, "files")
        return self._page(self._metadata(self._read(path, params=self._page_params(page_size, cursor, snapshot_id), deadline=deadline, collection_id=cid), cid, snapshot_id))

    def file(self, collection_id: str | UUID, file_id: str, *, snapshot_id: Any = ANY,
             deadline: float | None = None) -> dict[str, Any]:
        if not isinstance(file_id, str) or not file_id:
            raise ValidationError("file_id must be a nonempty opaque string")
        cid, path = self._path(collection_id, "files/" + quote(file_id, safe=""))
        return self._metadata(self._read(path, params=self._selector(snapshot_id), deadline=deadline, collection_id=cid), cid, snapshot_id)

    def create_collection(self, name: str, schema: Schema | Mapping[str, Any], *, collection_id: str | UUID,
                          write_settings: Mapping[str, Any] | None = None, mode: str = "append",
                          hot_snapshots: int | str | None = None, deadline: float | None = None) -> CreateOutcome:
        """Create or confirm one collection definition.

        ``write_settings`` (Parquet compression, dictionary policy and value
        encodings of data files written by later appends) is part of the
        definition: send the same value when repeating a lost create.
        ``hot_snapshots`` (1..2^40, append collections on servers advertising
        ``features.snapshot_archive``) keeps only the newest snapshots in the
        Iceberg root and archives older ones, which stay readable. None (the
        default) omits it: such servers then give a new append collection
        their default (100, reported with ``hot_snapshots_defaulted``); 0
        (``KEEP_ALL_SNAPSHOTS_IN_ROOT``) creates it without an archive. The
        effective value is also part of the definition: a repeated create
        with another effective value is refused with 409
        ``SNAPSHOT_ARCHIVE_MISMATCH`` (an omitted value matches only a
        still-defaulted setting; an explicit 100 also matches the default).
        """
        cid = uuid_string(collection_id, "collection_id")
        schema = self._schema(schema)
        if not isinstance(name, str) or not name:
            raise ValidationError("exact collection name is required")
        requested = None if write_settings is None else canonical_write_settings(write_settings, schema)
        hot = hot_snapshots_value(hot_snapshots)
        if mode not in ("append", "kv"):
            raise ValidationError("mode must be append or kv")
        body = {"collection_id": cid, "name": name, "schema": schema.to_dict()}
        if mode != "append":
            body["mode"] = mode
        if requested is not None:
            body["write_settings"] = request_write_settings(requested)
        if hot is not None:
            body["hot_snapshots"] = str(hot)
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", "collections", budget, body=json_bytes(body), write=True)
        try:
            kind = value.get("outcome")
            if kind is None and status in (200, 201):
                info, kind = value, "committed"
            else:
                info = value.get("info", value.get("result"))
            if kind == "committed":
                if status not in (200, 201) or not isinstance(info, dict) or "error" in value:
                    raise ProtocolError("invalid committed create outcome", raw=value)
                self._verify_created(info, cid, name, schema, requested, authoritative=True, mode=mode,
                                     hot_snapshots=hot)
                self._schemas[cid] = schema
            elif kind == "definitely_not_committed":
                if (not isinstance(value.get("error"), dict) or info is not None or not 400 <= status <= 599
                        or value.get("recovery_required", False) is not False or "recovery" in value):
                    raise ProtocolError("invalid noncommitted create outcome", raw=value)
            elif kind == "commit_unknown":
                if status != 503 or value.get("recovery_required") is not True or info is not None:
                    raise ProtocolError("invalid unknown create outcome", raw=value)
                validate_create_recovery(value, cid, name, schema_fingerprint(schema).hex())
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
    def _verify_created(info: Mapping[str, Any], cid: str, name: str, schema: Schema,
                        write_settings: dict[str, Any] | None = None, *, authoritative: bool = True, mode: str = "append",
                        hot_snapshots: int | None = None) -> bool:
        """Check a created or resolved collection against the original create.

        ``authoritative`` responses come from the create POST itself: the
        server compared the settings the collection was created with. A
        resolved (GET) collection reports only its current settings, which
        prove the creation settings only while none were ever stored (version
        "0"). Returns False when only a repeated create can decide.
        """
        try:
            actual_schema = Schema.from_dict(info["schema"])
            same = info.get("collection_id") == cid and info.get("name") == name and schema_fingerprint(actual_schema) == schema_fingerprint(schema)
        except (KeyError, ValidationError) as exc:
            raise ProtocolError("created collection has missing or invalid identity/schema", raw=info) from exc
        if info.get("mode", "append") != mode:
            raise ProtocolError("resolved collection mode differs from the original create", raw=info)
        if not same:
            raise ProtocolError("resolved collection UUID, exact name, or schema does not match original create", raw=info)
        try:
            reported_hot = reported_collection_hot_snapshots(info)
        except ValidationError as exc:
            raise ProtocolError("collection reports an invalid hot_snapshots", raw=info) from exc
        if reported_hot is None and hot_snapshots is not None:
            # Servers without the snapshot archive (the mock, older native
            # builds) cannot have applied it; such a create must not pass.
            raise ProtocolError("created collection does not report the requested hot_snapshots", raw=info)
        # The server compares a repeated create's effective value with the
        # one the collection was created with; the reported value is the
        # current one, which an update can change. A GET proves the creation
        # value only when no archive exists at all (no member), for a KV
        # collection (never archived) asked without a value, or while the
        # value is still the server's default and the create omitted it.
        if authoritative or reported_hot is None:
            hot_proved = True
        elif mode == "kv":
            hot_proved = hot_snapshots in (None, KEEP_ALL_SNAPSHOTS_IN_ROOT)
        else:
            hot_proved = hot_snapshots is None and reported_hot[1]
        if "write_settings" not in info and "write_settings_version" not in info:
            # Servers without write settings (the mock, older native builds)
            # cannot have applied any; a request that carried them must not pass.
            if write_settings is not None:
                raise ProtocolError("created collection does not report the requested write settings", raw=info)
            return hot_proved
        try:
            version = write_settings_version(info["write_settings_version"])
            actual = canonical_write_settings(info["write_settings"], schema, reported=True)
        except (KeyError, ValidationError) as exc:
            raise ProtocolError("created collection has missing, invalid or unsupported write settings",
                                raw=info) from exc
        # Creation stores non-default settings as version 1; defaults store
        # nothing (version 0). Every update adds one, so a stored version can
        # also be a default creation that was updated.
        default = canonical_write_settings(None)
        expected = write_settings or default
        created_version = 0 if expected == default else 1
        if version < created_version or (version == 0 and actual != expected):
            raise ProtocolError("resolved collection write settings do not match original create", raw=info)
        if not authoritative:
            return version == 0 and hot_proved
        if version == created_version and actual != expected:
            raise ProtocolError("created collection write settings do not match the request", raw=info)
        return True

    def resolve_create(self, name: str, schema: Schema | Mapping[str, Any], *, collection_id: str | UUID,
                       write_settings: Mapping[str, Any] | None = None, mode: str = "append",
                       hot_snapshots: int | str | None = None, deadline: float | None = None) -> dict[str, Any]:
        """Resolve a lost create without guessing its write settings.

        When the current collection cannot prove the creation settings (any
        write settings were ever stored, or ``hot_snapshots`` is not the
        server's default for a create that omitted it), the original create
        is repeated: it changes nothing for an existing collection, and the
        server compares it with the settings the collection was created with.
        """
        cid = uuid_string(collection_id, "collection_id")
        schema = self._schema(schema)
        requested = None if write_settings is None else canonical_write_settings(write_settings, schema)
        hot = hot_snapshots_value(hot_snapshots)
        info = self.resolve_collection(name, deadline=deadline)
        if self._verify_created(info, cid, name, schema, requested, authoritative=False, mode=mode, hot_snapshots=hot):
            return info
        outcome = self.create_collection(name, schema, collection_id=cid, write_settings=requested, mode=mode,
                                         hot_snapshots=hot, deadline=deadline)
        if outcome.outcome == "committed":
            return outcome.info
        code = outcome.error.code if outcome.error else None
        if code in ("WRITE_SETTINGS_MISMATCH", "SCHEMA_MISMATCH", "ALREADY_EXISTS", "COLLECTION_MODE_MISMATCH",
                    "SNAPSHOT_ARCHIVE_MISMATCH"):
            raise ProtocolError("resolved collection write settings do not match original create",
                                raw=outcome.raw, collection_id=cid)
        # Recovery or admission refused the confirmation; the create stays unresolved.
        detail = outcome.error or ErrorDetail.from_dict({"code": "RECOVERY_REQUIRED",
                                                         "message": "create confirmation is unresolved",
                                                         "retry_hint": "SameIdAfterReconcile"})
        raise OutcomeUnknownError(503, detail, raw=outcome.raw, collection_id=cid)

    def update_write_settings(self, collection_id: str | UUID, expected_version: int | str,
                              write_settings: Mapping[str, Any], *, deadline: float | None = None) -> dict[str, Any]:
        """Compare-and-set the Parquet write settings used by later appends.

        ``expected_version`` is the collection's current ``write_settings_version``;
        the returned collection has version ``expected_version + 1``. The value
        replaces all settings. Existing data files keep their codecs and
        encodings. A stale version raises ``HTTPError`` 409
        ``WRITE_SETTINGS_CONFLICT``. After a lost response or
        ``OutcomeUnknownError`` (for example 503 ``RECOVERY_REQUIRED``), repeat
        the same call, after reconciling if requested: an applied identical
        request returns the collection unchanged.
        """
        cid, path = self._path(collection_id, "write-settings")
        version = int64(expected_version)
        if version < 0 or version == 2**63 - 1:
            raise ValidationError("expected_version must be a current non-negative write settings version")
        requested = canonical_write_settings(write_settings)
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", path, budget, write=True, body=json_bytes(
            {"expected_version": str(version), "write_settings": request_write_settings(requested)}))
        if status != 200:
            self._http_error(status, value, budget)
        try:
            if value.get("collection_id") != cid:
                raise ProtocolError("write settings response names a different collection")
            schema = Schema.from_dict(value["schema"])
            if write_settings_version(value["write_settings_version"]) != version + 1:
                raise ProtocolError("write settings response has an unexpected version")
            if (canonical_write_settings(value["write_settings"], schema, reported=True)
                    != canonical_write_settings(requested, schema)):
                raise ProtocolError("write settings response differs from the request")
        except (KeyError, ValidationError) as exc:
            raise ProtocolError("write settings response has missing or invalid collection fields",
                                raw=value, collection_id=cid) from exc
        except ProtocolError as exc:
            exc.raw, exc.collection_id = value, cid
            raise
        return value

    def snapshot_archive(self, collection_id: str | UUID, *, deadline: float | None = None) -> dict[str, Any]:
        """Report ``hot_snapshots`` and the root, archived and expired snapshot counts.

        Counts are decimal strings observed from the server's confirmed view;
        ``archive_pending`` is the number of root snapshots above
        ``hot_snapshots`` that ``archive_snapshots`` can still move.
        ``hot_snapshots`` is the effective value, ``"0"`` without an archive.
        """
        cid, path = self._path(collection_id, "snapshot-archive")
        value = self._read(path, deadline=deadline, collection_id=cid)
        try:
            return validate_archive_status(value, maintenance=False)
        except ValidationError as exc:
            raise ProtocolError("invalid snapshot archive status", raw=value, collection_id=cid) from exc

    def update_snapshot_archive(self, collection_id: str | UUID, hot_snapshots: int | str | None, *,
                                deadline: float | None = None) -> dict[str, Any]:
        """Replace ``hot_snapshots`` (1..2^40, or 0/None to stop archiving later snapshots).

        Later appends and ``archive_snapshots`` apply it; nothing moves now and
        archived snapshots never return to the root. Returns the collection.
        Repeating the same call is idempotent, so after a lost response or
        ``OutcomeUnknownError`` (503 ``RECOVERY_REQUIRED``: reconcile first)
        repeat it. KV collections refuse a value with 409
        ``COLLECTION_MODE_MISMATCH``; a value above the retention policy's
        ``keep_last`` is refused with 400 ``INVALID_ARGUMENT``. Any update
        makes the value explicit (``hot_snapshots_defaulted`` false).
        """
        cid, path = self._path(collection_id, "snapshot-archive")
        hot = hot_snapshots_value(hot_snapshots)
        hot = KEEP_ALL_SNAPSHOTS_IN_ROOT if hot is None else hot
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", path, budget, write=True, body=json_bytes({"hot_snapshots": str(hot)}))
        if status != 200:
            self._http_error(status, value, budget)
        try:
            if value.get("collection_id") != cid:
                raise ProtocolError("snapshot archive response names a different collection")
            if reported_collection_hot_snapshots(value) != (hot, False):
                raise ProtocolError("snapshot archive response differs from the request")
        except ValidationError as exc:
            raise ProtocolError("snapshot archive response has an invalid hot_snapshots",
                                raw=value, collection_id=cid) from exc
        except ProtocolError as exc:
            exc.raw, exc.collection_id = value, cid
            raise
        return value

    def archive_snapshots(self, collection_id: str | UUID, *, deadline: float | None = None) -> dict[str, Any]:
        """Run one bounded archive maintenance call (at most 1024 snapshots).

        Returns the status after the call plus ``snapshots_archived``; repeat
        while ``archive_pending`` is not ``"0"``. Archived snapshots stay
        readable by ID through scans, statistics, files and history. After
        ``OutcomeUnknownError`` (503 ``RECOVERY_REQUIRED``) reconcile and repeat.
        """
        cid, path = self._path(collection_id, "snapshot-archive/maintenance")
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", path, budget, write=True, body=b"{}")
        if status != 200:
            self._http_error(status, value, budget)
        try:
            return validate_archive_status(value, maintenance=True)
        except ValidationError as exc:
            raise ProtocolError("invalid snapshot archive maintenance response", raw=value, collection_id=cid) from exc

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
        status, value = self._json("POST", f"collections/{request.collection_id}/append", budget, body=request.body, write=True)
        outcome = self._append_outcome(status, value, budget, expected_payload_hash=request.payload_hash)
        if outcome.result is not None and outcome.result.get("receipt_version") == 3:
            raise ProtocolError("append returned a KV mutation receipt", raw=value,
                                collection_id=request.collection_id, operation_id=request.operation_id)
        return outcome

    def _mutation_schema(self, collection_id, operation_id, schema, deadline):
        cid = uuid_string(collection_id, "collection_id")
        op = uuid_string(operation_id, "operation_id")
        selected = self._schema(schema) if schema is not None else self._schemas.get(cid)
        if selected is None:
            try:
                selected = self.get_schema(cid, deadline=deadline)
            except (TransportError, ProtocolError, HTTPError) as exc:
                exc.collection_id, exc.operation_id = cid, op
                raise
        return cid, op, selected

    def prepare_upsert(self, collection_id, rows, *, operation_id, schema=None, expected_snapshot=ANY,
                       deadline=None) -> MutationRequest:
        cid, op, selected = self._mutation_schema(collection_id, operation_id, schema, deadline)
        return MutationRequest.prepare("upsert", cid, op, selected, rows=rows, expected_snapshot=expected_snapshot)

    def prepare_delete(self, collection_id, keys, *, operation_id, schema=None, expected_snapshot=ANY,
                       deadline=None) -> MutationRequest:
        # Bound a generator before a schema request or any write is sent.
        keys = bounded_keys(keys, 10000)
        cid, op, selected = self._mutation_schema(collection_id, operation_id, schema, deadline)
        return MutationRequest.prepare("delete", cid, op, selected, keys=keys, expected_snapshot=expected_snapshot)

    def prepare_compact(self, collection_id, *, operation_id, schema=None, expected_snapshot=ANY,
                        max_files=64, target_file_bytes=8 * 1024**2, deadline=None) -> MutationRequest:
        if type(max_files) is not int or not 1 <= max_files <= 256:
            raise ValidationError("max_files must be an integer in 1..256")
        if type(target_file_bytes) is not int or not 1024 <= target_file_bytes <= 16 * 1024**2:
            raise ValidationError("target_file_bytes must be an integer in 1024..16777216")
        cid, op, selected = self._mutation_schema(collection_id, operation_id, schema, deadline)
        return MutationRequest.prepare("compact", cid, op, selected, expected_snapshot=expected_snapshot,
                                       max_files=max_files, target_file_bytes=target_file_bytes)

    def _send_mutation(self, request, deadline):
        budget = Deadline(deadline, collection_id=request.collection_id, operation_id=request.operation_id)
        status, value = self._json("POST", f"collections/{request.collection_id}/{request.kind}", budget,
                                   body=request.body, write=True)
        outcome = self._append_outcome(status, value, budget, expected_payload_hash=request.payload_hash)
        if outcome.result is not None and (outcome.result.get("receipt_version") != 3
                                          or outcome.result.get("mutation_kind") != request.kind):
            raise ProtocolError("mutation receipt kind differs from the original request", raw=value,
                                collection_id=request.collection_id, operation_id=request.operation_id)
        if outcome.result is not None:
            body = request.to_dict()
            count = len(body.get("rows", [])) if request.kind == "upsert" else len(body.get("keys", [])) if request.kind == "delete" else 0
            if outcome.result.get("input_rows") != str(count):
                raise ProtocolError("mutation receipt input_rows differs from the original request", raw=value,
                                    collection_id=request.collection_id, operation_id=request.operation_id)
        return outcome

    def upsert(self, collection_id, rows=None, *, operation_id=None, schema=None, expected_snapshot=ANY,
               deadline=None) -> AppendOutcome:
        if isinstance(collection_id, MutationRequest):
            request = collection_id
            if request.kind != "upsert" or rows is not None or operation_id is not None or schema is not None or expected_snapshot is not ANY:
                raise ValidationError("prepared upsert already owns its identity, rows and precondition")
        else:
            budget = Deadline(deadline, collection_id=uuid_string(collection_id, "collection_id"),
                              operation_id=uuid_string(operation_id, "operation_id"))
            request = self.prepare_upsert(collection_id, rows, operation_id=operation_id, schema=schema,
                                          expected_snapshot=expected_snapshot, deadline=budget.check())
            deadline = budget.check()
        return self._send_mutation(request, deadline)

    def delete(self, collection_id, keys=None, *, operation_id=None, schema=None, expected_snapshot=ANY,
               deadline=None) -> AppendOutcome:
        if isinstance(collection_id, MutationRequest):
            request = collection_id
            if request.kind != "delete" or keys is not None or operation_id is not None or schema is not None or expected_snapshot is not ANY:
                raise ValidationError("prepared delete already owns its identity, keys and precondition")
        else:
            budget = Deadline(deadline, collection_id=uuid_string(collection_id, "collection_id"),
                              operation_id=uuid_string(operation_id, "operation_id"))
            request = self.prepare_delete(collection_id, keys, operation_id=operation_id, schema=schema,
                                          expected_snapshot=expected_snapshot, deadline=budget.check())
            deadline = budget.check()
        return self._send_mutation(request, deadline)

    def compact(self, collection_id, *, operation_id=None, schema=None, expected_snapshot=ANY,
                max_files=64, target_file_bytes=8 * 1024**2, deadline=None) -> AppendOutcome:
        if isinstance(collection_id, MutationRequest):
            request = collection_id
            if request.kind != "compact" or operation_id is not None or schema is not None or expected_snapshot is not ANY or max_files != 64 or target_file_bytes != 8 * 1024**2:
                raise ValidationError("prepared compaction already owns its identity, options and precondition")
        else:
            budget = Deadline(deadline, collection_id=uuid_string(collection_id, "collection_id"),
                              operation_id=uuid_string(operation_id, "operation_id"))
            request = self.prepare_compact(collection_id, operation_id=operation_id, schema=schema,
                                           expected_snapshot=expected_snapshot, max_files=max_files,
                                           target_file_bytes=target_file_bytes, deadline=budget.check())
            deadline = budget.check()
        return self._send_mutation(request, deadline)

    def lookup(self, collection_id, keys, *, snapshot_id=ANY, deadline=None) -> LookupResult:
        requested = bounded_keys(keys, 4096)
        cid, path = self._path(collection_id, "lookup")
        body = {"keys": requested}
        if snapshot_id is not ANY:
            body["snapshot_id"] = None if snapshot_id is None else str(int64(snapshot_id))
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", path, budget, body=json_bytes(body))
        if status != 200:
            self._http_error(status, value, budget)
        try:
            selected = value["snapshot_id"]
            if selected is not None and (not isinstance(selected, str) or str(int64(selected)) != selected):
                raise ValidationError("lookup snapshot must be a decimal string or null")
            if snapshot_id is not ANY and selected != body["snapshot_id"]:
                raise ValidationError("lookup snapshot differs from the requested fixed view")
            schema = Schema.from_dict(value["schema"])
            if value.get("collection_id") != cid or not isinstance(value["rows"], list) or len(value["rows"]) != len(requested):
                raise ValidationError("lookup identity or result count differs from the request")
            rows = [None if row is None else decode_rows(schema, [row])[0] for row in value["rows"]]
            if (selected is None and any(row is not None for row in rows)) or any(row is not None and row.get("key") != key for key, row in zip(requested, rows)):
                raise ValidationError("lookup result order differs from the requested keys")
            return LookupResult(schema, selected, rows, value)
        except (KeyError, TypeError, ValidationError) as exc:
            raise ProtocolError("invalid lookup response", collection_id=cid, raw=value) from exc

    def prepare_bulk_append(self, collection_id: str | UUID, batches: Iterable[Iterable[Mapping[str, Any]]], *,
                            rows: int, operation_id: str | UUID, schema: Schema | Mapping[str, Any] | None = None,
                            expected_snapshot: Any = ANY, deadline: float | None = None) -> BulkAppendRequest:
        cid = uuid_string(collection_id, "collection_id")
        op = uuid_string(operation_id, "operation_id")
        budget = Deadline(deadline, collection_id=cid, operation_id=op)
        selected = self._schema(schema) if schema is not None else self._schemas.get(cid)
        if selected is None:
            try:
                selected = self.get_schema(cid, deadline=budget.check())
            except (TransportError, ProtocolError, HTTPError) as exc:
                exc.collection_id, exc.operation_id = cid, op
                raise
        return BulkAppendRequest.prepare(cid, op, selected, batches, rows=rows,
                                         expected_snapshot=expected_snapshot, _check=budget.check)

    def bulk_append(self, collection_id: str | UUID | BulkAppendRequest,
                    batches: Iterable[Iterable[Mapping[str, Any]]] | None = None, *, rows: int | None = None,
                    operation_id: str | UUID | None = None, schema: Schema | Mapping[str, Any] | None = None,
                    expected_snapshot: Any = ANY, deadline: float | None = None) -> AppendOutcome:
        owned = not isinstance(collection_id, BulkAppendRequest)
        if not owned:
            if (batches is not None or rows is not None or operation_id is not None or schema is not None
                    or expected_snapshot is not ANY):
                raise ValidationError("prepared bulk append already owns input, IDs, schema, and precondition")
            request = collection_id
            budget = Deadline(deadline, collection_id=request.collection_id, operation_id=request.operation_id)
        else:
            cid = uuid_string(collection_id, "collection_id")
            op = uuid_string(operation_id, "operation_id")
            budget = Deadline(deadline, collection_id=cid, operation_id=op)
            if batches is None or rows is None:
                raise ValidationError("bulk append requires batches and declared rows")
            request = self.prepare_bulk_append(cid, batches, rows=rows, operation_id=op, schema=schema,
                                               expected_snapshot=expected_snapshot, deadline=budget.check())
        try:
            status, value = self._json("POST", f"collections/{request.collection_id}/bulk-append", budget,
                                       bulk=request, write=True)
            outcome = self._append_outcome(status, value, budget, expected_payload_hash=request.payload_hash)
            if outcome.result is not None and outcome.result.get("receipt_version") == 3:
                raise ProtocolError("bulk append returned a KV mutation receipt", raw=value,
                                    collection_id=request.collection_id, operation_id=request.operation_id)
            return outcome
        finally:
            if owned:
                request.close()

    @staticmethod
    def _validate_receipt(result: Mapping[str, Any], expected_payload_hash: str | None, *, mock: bool = False) -> None:
        digest = result.get("payload_hash")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-fA-F]{64}", digest, re.ASCII) is None:
            raise ProtocolError("append receipt requires a SHA256 hexadecimal payload_hash")
        if type(result.get("hash_version")) is not int or result["hash_version"] != 1:
            raise ProtocolError("append receipt requires supported hash_version 1")
        if expected_payload_hash is not None and not hmac.compare_digest(digest.lower(), expected_payload_hash.lower()):
            raise ProtocolError("append receipt payload_hash differs from the original request")
        version = result.get("receipt_version")
        if type(version) is not int or version not in (1, 2, 3):
            raise ProtocolError("receipt requires supported receipt_version 1, 2 or 3")
        if version == 3:
            validate_mutation_receipt(result)
            return
        rows = int64(result.get("rows_appended"))
        if not 1 <= rows <= (10000 if version == 1 else 25600000):
            raise ProtocolError("append receipt rows_appended is outside the v0 append profile")
        created_at = result.get("snapshot_created_at_us")
        if not isinstance(created_at, str):
            raise ProtocolError("append receipt requires decimal-string snapshot_created_at_us")
        created_at_us = int64(created_at)
        timestamp_us(created_at_us)
        if version == 2 and created_at_us % 1000:
            raise ProtocolError("bulk append receipt timestamp requires millisecond precision")
        # Native receipts retain every core field. The development adapter has
        # no physical files and may omit unavailable measurements.
        if not mock or version == 2:
            for name in ("data_files_added", "compressed_data_bytes_added", "parent_snapshot_id", "durability_profile"):
                if name not in result:
                    raise ProtocolError(f"append receipt requires {name}")
        files = int64(result["data_files_added"]) if "data_files_added" in result else None
        if files is not None:
            if version == 1 and files != 1:
                raise ProtocolError("ordinary append receipt requires one data file")
            if version == 2 and (not 1 <= files <= 256 or rows > files * 100000):
                raise ProtocolError("bulk append receipt exceeds its file or row bounds")
        if "compressed_data_bytes_added" in result:
            size = int64(result["compressed_data_bytes_added"])
            if not 1 <= size <= (files or 1) * 32 * 1024 * 1024:
                raise ProtocolError("append receipt exceeds its compressed byte bound")
        if "rows_total" in result and int64(result["rows_total"]) < rows:
            raise ProtocolError("append receipt has invalid rows_total")
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
            if isinstance(error_value, dict) and error_value.get("code") in ("OPERATION_CONFLICT", "IDEMPOTENCY_CONFLICT"):
                # A conflicting request changes nothing; it is not the original operation's outcome.
                raise HTTPError(status, ErrorDetail.from_dict(error_value), raw=value,
                                operation_id=budget.operation_id, collection_id=budget.collection_id)
            if kind == "committed":
                if status != 200 or not isinstance(result, dict) or error_value is not None:
                    raise ProtocolError("invalid committed append outcome", raw=value)
                if result.get("operation_id") != budget.operation_id or result.get("collection_id") != budget.collection_id:
                    raise ProtocolError("append receipt has different collection or operation UUID", raw=value)
                if not isinstance(result.get("snapshot_id"), str):
                    raise ProtocolError("append receipt requires decimal-string snapshot_id", raw=value)
                int64(result["snapshot_id"])
                self._validate_receipt(result, expected_payload_hash,
                                       mock=value.get("mock") is True or result.get("mock") is True)
                if result.get("receipt_version") == 3 and (value.get("phase") != "complete"
                        or value.get("recovery_required", False) is not False or "recovery" in value):
                    raise ProtocolError("invalid committed KV mutation envelope", raw=value)
            elif kind == "definitely_not_committed":
                if (not isinstance(error_value, dict) or result is not None or not 400 <= status <= 599
                        or value.get("recovery_required", False) is not False or "recovery" in value):
                    raise ProtocolError("invalid noncommitted append outcome", raw=value)
            elif kind == "commit_unknown":
                if status != 503 or value.get("recovery_required") is not True or result is not None:
                    raise ProtocolError("invalid unknown append outcome", raw=value)
                validate_append_recovery(value, budget.collection_id, budget.operation_id, expected_payload_hash)
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
            if not isinstance(error, dict) or error.get("code") != "OPERATION_NOT_FOUND":
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
        return self._metadata(value, cid, snapshot_id)

    def scan_json(self, collection_id: str | UUID, *, projection: Iterable[str] | None = None,
                  predicate: Predicate | Mapping[str, Any] | None = None, snapshot_id: Any = ANY,
                  limit: int | None = None, deadline: float | None = None) -> ScanResult:
        cid, path = self._path(collection_id, "scan")
        budget = Deadline(deadline, collection_id=cid)
        body = self._scan_body(projection, predicate, snapshot_id, limit)
        opened = self._open_scan(path, body, budget)
        cursor = opened["cursor"] if opened is not None else None
        try:
            status, value = (self._cursor_page(path, cursor, budget) if cursor is not None
                             else self._json("POST", path, budget, body=body))
            if status != 200:
                self._http_error(status, value, budget)
            # Legacy single-response servers may still advertise a continuation.
            advertised = value.get("next_cursor")
            if isinstance(advertised, str) and advertised:
                cursor = advertised
            first = self._scan_result(value, cid)
            rows = list(first.rows)
            wire_rows = list(first.raw["rows"])
            cursor = first.next_cursor
            while cursor is not None:
                status, value = self._cursor_page(path, cursor, budget)
                if status != 200:
                    self._http_error(status, value, budget)
                result = self._scan_result(value, cid)
                self._same_scan(first, result)
                if result.next_cursor == cursor:
                    raise ProtocolError("scan cursor did not advance", collection_id=cid)
                rows.extend(result.rows)
                wire_rows.extend(result.raw["rows"])
                cursor = result.next_cursor
        finally:
            if cursor is not None:
                self._cancel_scan_quietly(cid, cursor)
        raw = dict(first.raw, rows=wire_rows, next_cursor=None)
        # Decoded rows are returned independently of wire precision conventions.
        profile = dict(first.profile, rows_returned=str(len(rows)))
        raw["profile"] = profile
        return ScanResult(first.schema, first.snapshot_id, rows, profile, first.identity, raw)

    @staticmethod
    def _scan_result(value, cid):
        try:
            schema = Schema.from_dict(value["schema"], allow_empty=True)
            snapshot = value["snapshot_id"]
            if snapshot is not None and (not isinstance(snapshot, str) or str(int64(snapshot)) != snapshot):
                raise ValidationError("snapshot_id must be a canonical decimal string or null")
            if not isinstance(value["rows"], list) or not isinstance(value["profile"], dict) or not isinstance(value["identity"], dict):
                raise ValidationError("invalid scan rows, profile, or identity")
            cursor = value.get("next_cursor")
            if cursor is not None and (not isinstance(cursor, str) or not cursor):
                raise ValidationError("invalid next_cursor")
            rows = decode_rows(schema, value["rows"])
            return ScanResult(schema, snapshot, rows, value["profile"], value["identity"], value)
        except (KeyError, ValidationError) as exc:
            raise ProtocolError("invalid scan JSON response", raw=value, collection_id=cid) from exc

    @staticmethod
    def _same_scan(first, following):
        if (first.schema != following.schema or first.snapshot_id != following.snapshot_id
                or first.identity != following.identity):
            raise ProtocolError("scan view changed between pages")

    def _open_scan(self, path, body, budget, accept="application/json"):
        response = self._send("POST", path + "/open", budget, body=body, accept=accept)
        status = response.status_code
        value = read_json(response, budget, self._max_json_bytes)
        # Older, single-response mock adapters have no cursor endpoint. Never
        # confuse an unknown database/collection or a generic 404 with absence
        # of this route, and never restart a lost open/first-page response.
        if (status == 404 and isinstance(value.get("error"), dict)
                and value["error"].get("code") == "ROUTE_NOT_FOUND"):
            return None
        if status != 200:
            self._http_error(status, value, budget)
        if not isinstance(value.get("cursor"), str) or not value["cursor"]:
            raise ProtocolError("scan open requires a cursor", raw=value, collection_id=budget.collection_id)
        return value

    def _cursor_page(self, path, cursor, budget):
        body = json_bytes({"cursor": cursor})
        try:
            return self._json("POST", path, budget, body=body)
        except TransportError:
            # Read-only cursor pages replay their cached bytes. Never restart a
            # scan or retry a write after an ambiguous transport failure.
            return self._json("POST", path, budget, body=body)

    def cancel_scan(self, collection_id, cursor, *, deadline=None):
        cid, path = self._path(collection_id, "scan/cancel")
        if not isinstance(cursor, str) or not cursor:
            raise ValidationError("cursor must be a nonempty string")
        budget = Deadline(deadline, collection_id=cid)
        status, value = self._json("POST", path, budget, body=json_bytes({"cursor": cursor}))
        if status != 200:
            self._http_error(status, value, budget)

    def _cancel_scan_quietly(self, cid, cursor):
        try:
            self.cancel_scan(cid, cursor, deadline=min(self._timeout, 5.0))
        except (TransportError, HTTPError, ProtocolError):
            pass  # Server TTL is the fallback when cleanup cannot reach it.

    def scan_pages(self, collection_id, *, projection=None, predicate=None,
                   snapshot_id=ANY, limit=None, deadline=None):
        """Yield bounded JSON pages. Use as a context manager when stopping early.

        Opens a fixed view before reading rows; each page may be retried once
        after a transport failure without duplicating or skipping rows.
        """
        iterator = _ScanPages(self._scan_pages(collection_id, projection=projection, predicate=predicate,
                                              snapshot_id=snapshot_id, limit=limit, deadline=deadline),
                              self._streams.discard, self._cleanup)
        self._streams.add(iterator)
        return iterator

    def _scan_pages(self, collection_id, *, projection, predicate, snapshot_id, limit, deadline):
        cid, path = self._path(collection_id, "scan")
        budget = Deadline(deadline, collection_id=cid)
        status, opened = self._json("POST", path + "/open", budget,
            body=self._scan_body(projection, predicate, snapshot_id, limit))
        if status != 200:
            self._http_error(status, opened, budget)
        cursor = opened.get("cursor")
        if not isinstance(cursor, str) or not cursor:
            raise ProtocolError("scan open requires a cursor", raw=opened)
        try:
            fixed = self._scan_result(dict(opened, rows=[], profile={}), cid)
            while cursor is not None:
                status, value = self._cursor_page(path, cursor, budget)
                if status != 200:
                    self._http_error(status, value, budget)
                page = self._scan_result(value, cid)
                self._same_scan(fixed, page)
                if page.next_cursor == cursor:
                    raise ProtocolError("scan cursor did not advance", collection_id=cid)
                cursor = page.next_cursor
                yield page
        finally:
            if cursor is not None:
                self._cancel_scan_quietly(cid, cursor)

    def scan_batches(self, collection_id: str | UUID, *, projection: Iterable[str] | None = None,
                     predicate: Predicate | Mapping[str, Any] | None = None, snapshot_id: Any = ANY,
                     limit: int | None = None, deadline: float | None = None):
        from .stream import PagedScanStream, ScanStream

        cid, path = self._path(collection_id, "scan")
        budget = Deadline(deadline, collection_id=cid)
        body = self._scan_body(projection, predicate, snapshot_id, limit)
        opened = self._open_scan(path, body, budget, "application/vnd.apache.arrow.stream")
        initial_cursor = opened["cursor"] if opened is not None else None
        def send_page(cursor):
            page_body = json_bytes({"cursor": cursor})
            try:
                following = self._send("POST", path, budget, body=page_body,
                                       accept="application/vnd.apache.arrow.stream")
            except TransportError:
                following = self._send("POST", path, budget, body=page_body,
                                       accept="application/vnd.apache.arrow.stream")
            if following.status_code != 200:
                status = following.status_code
                value = read_json(following, budget, self._max_json_bytes)
                self._http_error(status, value, budget)
            return following
        def next_page(cursor):
            return ScanStream(send_page(cursor), budget, max_read_bytes=self._max_arrow_read_bytes,
                              on_close=lambda stream: None, cleanup_queue=self._cleanup)
        try:
            response = (send_page(initial_cursor) if initial_cursor is not None else
                        self._send("POST", path, budget, body=body, accept="application/vnd.apache.arrow.stream"))
            if response.status_code != 200:
                status = response.status_code
                value = read_json(response, budget, self._max_json_bytes)
                self._http_error(status, value, budget)
        except BaseException:
            if initial_cursor is not None:
                self._cancel_scan_quietly(cid, initial_cursor)
            raise
        cursor = response.headers.get("x-glamdb-next-cursor")
        paged = cursor is not None
        try:
            stream = ScanStream(response, budget, max_read_bytes=self._max_arrow_read_bytes,
                                on_close=(lambda stream: None) if paged else self._streams.discard,
                                cleanup_queue=self._cleanup)
            try:
                if (snapshot_id is not ANY
                        and stream.snapshot_id != (None if snapshot_id is None else str(int64(snapshot_id)))):
                    raise ProtocolError("Arrow response returned a different requested snapshot", collection_id=cid)
                if opened is not None and "snapshot_id" in opened and opened["snapshot_id"] != stream.snapshot_id:
                    raise ProtocolError("Arrow response returned a different opened snapshot", collection_id=cid)
            except BaseException:
                stream._close_preserving_error()
                raise
        except BaseException:
            # ScanStream closes HTTP/Arrow owners on construction failure, but
            # no public iterator exists yet to release the server-side cursor.
            try:
                if cursor or initial_cursor:
                    self._cancel_scan_quietly(cid, cursor or initial_cursor)
            except BaseException:
                pass  # Preserve the original schema/transport/allocation error.
            raise
        if paged:
            stream = PagedScanStream(stream, next_page, lambda cursor: self._cancel_scan_quietly(cid, cursor),
                                     self._streams.discard, self._cleanup)
        self._streams.add(stream)
        return stream
