"""Real HTTP/Arrow transport over explicitly synthetic, process-local fixtures."""

import hashlib
import hmac
import json
import time
from pathlib import Path
from uuid import UUID, uuid5

import pyarrow as pa
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from glamdb_verify import wire_rows
from starlette.exceptions import HTTPException

from .state import CAPABILITIES, HEALTH, MOCK, MockError, MockStore, uuid_value
from .theme import ThemeError, load_theme


def _json(value, status=200):
    return JSONResponse(value, status_code=status, headers={"Cache-Control": "no-store"})


def _error(status, code, message, retry_hint=None):
    error = {"code": code, "message": message}
    if retry_hint is not None:
        error["retry_hint"] = retry_hint
    return _json({"error": error}, status)


async def _body(request, maximum):
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise MockError(415, "UNSUPPORTED_MEDIA_TYPE", "request body must be application/json")
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > maximum:
            raise MockError(413, "RESOURCE_EXHAUSTED", "request body exceeds configured byte limit")
        data.extend(chunk)
    try:
        value = json.loads(data, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise MockError(422, "INVALID_ARGUMENT", "request body must contain valid finite JSON") from error
    if not isinstance(value, dict):
        raise MockError(422, "INVALID_ARGUMENT", "request body must be a JSON object")
    return value


def _page_params(request, allow_snapshot=False):
    allowed = {"page_size", "cursor"} | ({"snapshot_id"} if allow_snapshot else set())
    if set(request.query_params) - allowed or any(len(request.query_params.getlist(key)) != 1 for key in request.query_params):
        raise MockError(400, "INVALID_ARGUMENT", "unknown or repeated pagination query parameter")
    try:
        raw = request.query_params.get("page_size", "100")
        size = int(raw)
        if str(size) != raw or not 1 <= size <= 1000:
            raise ValueError
    except ValueError as error:
        raise MockError(400, "INVALID_ARGUMENT", "page_size must be 1..1000") from error
    result = {"page_size": size}
    if allow_snapshot:
        result["snapshot_id"] = request.query_params.get("snapshot_id", "latest")
    return result


def _file_info(store, collection, snapshot):
    file_id = str(uuid5(UUID(collection.table_uuid), snapshot["snapshot_id"]))
    return {
        "file_id": file_id, "snapshot_id": snapshot["snapshot_id"],
        "path": f"mock://{store.store_id}/{collection.collection_id}/data/{snapshot['snapshot_id']}.parquet",
        "content": "data", "record_count": snapshot["rows_added"],
        "compressed_bytes": None, "field_metrics": None,
        "synthetic": True, "physical_file_exists": False, **MOCK,
    }


def _files(store, collection, snapshot):
    if snapshot is None:
        return []
    result = []
    for item in collection.snapshots:
        result.append(_file_info(store, collection, item))
        if item is snapshot:
            break
    return sorted(result, key=lambda item: item["path"].encode())


def _arrow_bytes(schema, rows, maximum):
    types = {"string": pa.string(), "bool": pa.bool_(), "int64": pa.int64(), "float64": pa.float64(), "timestamp_us_utc": pa.timestamp("us", tz="UTC")}
    fields = [pa.field(field["name"], types[field["type"]], nullable=field["nullable"], metadata={b"PARQUET:field_id": str(field["field_id"]).encode()}) for field in schema["fields"]]
    arrow_schema = pa.schema(fields, metadata={b"glamdb.backend": b"mock", b"glamdb.durability_validation": b"designed"})
    arrays = [pa.array([row[field["name"]] for row in rows], type=types[field["type"]]) for field in schema["fields"]]
    if fields:
        table = pa.Table.from_arrays(arrays, schema=arrow_schema)
    else:
        # A zero-column Arrow table still has a meaningful row count.
        table = pa.table({"__row_count__": pa.nulls(len(rows))}).drop(["__row_count__"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, arrow_schema) as writer:
        if fields:
            writer.write_table(table, max_chunksize=65536)
        else:
            for batch in table.to_batches(max_chunksize=65536):
                writer.write_batch(batch.replace_schema_metadata(arrow_schema.metadata))
    result = sink.getvalue().to_pybytes()
    if len(result) > maximum:
        raise MockError(413, "RESOURCE_EXHAUSTED", "mock Arrow result exceeds configured response limit")
    return result


def _seed(store):
    schema = {"fields": [
        {"name": "key", "type": "string", "nullable": False},
        {"name": "event", "type": "string", "nullable": False},
        {"name": "country", "type": "string", "nullable": True},
        {"name": "revenue", "type": "float64", "nullable": True},
        {"name": "active", "type": "bool", "nullable": False},
        {"name": "occurred_at", "type": "timestamp_us_utc", "nullable": False},
    ]}
    groups = [
        ("dreamhouse_events", [
            {"key": "dream-001", "event": "dreamhouse_opened", "country": "US", "revenue": 249.0, "active": True, "occurred_at": "2026-01-01T10:00:00.000001Z"},
            {"key": "dream-002", "event": "pool_party", "country": "FR", "revenue": 175.5, "active": True, "occurred_at": "2026-01-02T10:00:00Z"},
            {"key": "dream-001", "event": "dreamhouse_opened", "country": None, "revenue": None, "active": False, "occurred_at": "2026-01-03T10:00:00Z"},
            {"key": "dream-004", "event": "wardrobe_refresh", "country": "GB", "revenue": 89.99, "active": True, "occurred_at": "2026-01-04T10:00:00Z"},
        ]),
        ("pink_inventory", [
            {"key": "pink-001", "event": "pink_shoes", "country": "IT", "revenue": 120.0, "active": True, "occurred_at": "2026-01-05T09:00:00Z"},
            {"key": "pink-002", "event": "silk_bow", "country": "FR", "revenue": 24.5, "active": True, "occurred_at": "2026-01-06T09:00:00Z"},
            {"key": "pink-003", "event": "glitter_bag", "country": None, "revenue": 79.0, "active": False, "occurred_at": "2026-01-07T09:00:00Z"},
        ]),
    ]
    namespace = UUID("6591c2bc-b350-4b83-a2b2-d93853893f27")
    for name, rows in groups:
        cid = str(uuid5(namespace, name))
        store.create({"collection_id": cid, "name": name, "schema": schema})
        collection = store.collection(cid)
        for index, chunk in enumerate((rows[:2], rows[2:])):
            store.append(collection, {"operation_id": str(uuid5(namespace, f"{name}-{index}")), "rows": chunk})


def create_app(*, token, ui_dir, theme_file=None, seed_demo=False, enable_development_faults=False,
               max_body_bytes=16 * 1024 * 1024, max_response_bytes=32 * 1024 * 1024,
               max_rows=100_000, max_state_bytes=64 * 1024 * 1024, cursor_ttl=900, clock=time.time):
    """Build a mock adapter. All state vanishes on restart; no store path is accepted."""
    if not isinstance(token, str) or not token.strip() or len(token.encode()) > 4096 or any(ord(char) < 33 for char in token):
        raise ValueError("a nonempty token without whitespace is required")
    if type(max_body_bytes) is not int or max_body_bytes <= 0 or type(max_response_bytes) is not int or max_response_bytes <= 0:
        raise ValueError("body and response limits must be positive integers")
    if type(max_rows) is not int or max_rows <= 0 or type(max_state_bytes) is not int or max_state_bytes <= 0:
        raise ValueError("row and state limits must be positive integers")
    if type(cursor_ttl) not in (int, float) or not 0 < cursor_ttl <= 900:
        raise ValueError("cursor TTL must be positive and at most 900 seconds")
    root = Path(ui_dir).resolve()
    config = Path(theme_file).resolve() if theme_file is not None else root / "config" / "theme.json"
    store = MockStore(token, clock=clock, cursor_ttl=cursor_ttl, max_rows=max_rows, max_state_bytes=max_state_bytes)
    if seed_demo:
        _seed(store)
    app = FastAPI(title="GlamDB disposable MOCK adapter", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.mock_store = store
    app.state.development_faults = bool(enable_development_faults)

    @app.middleware("http")
    async def authenticate(request, call_next):
        if (request.url.path in ("/readyz", "/capabilities", "/collections", "/collections:resolve")
                or request.url.path.startswith(("/collections/", "/development/"))):
            auth = request.headers.get("authorization", "")
            scheme, separator, supplied = auth.partition(" ")
            if not separator or scheme.lower() != "bearer" or not supplied:
                return _error(401, "UNAUTHENTICATED", "a bearer token is required")
            if not hmac.compare_digest(supplied.encode(), token.encode()):
                return _error(403, "PERMISSION_DENIED", "bearer token is not authorized")
            raw_length = request.headers.get("content-length")
            if raw_length is not None:
                try:
                    length = int(raw_length)
                except ValueError:
                    return _error(400, "INVALID_ARGUMENT", "invalid Content-Length")
                if length < 0:
                    return _error(400, "INVALID_ARGUMENT", "invalid Content-Length")
                if length > max_body_bytes:
                    return _error(413, "RESOURCE_EXHAUSTED", "request body exceeds configured byte limit")
        response = await call_next(request)
        response.headers["X-GlamDB-Backend"] = "mock"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.exception_handler(MockError)
    async def mock_error(_request, error):
        return _json({"error": error.detail}, error.status)

    @app.exception_handler(ValueError)
    async def invalid_value(_request, error):
        return _error(422, "INVALID_ARGUMENT", str(error))

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_request, _error_value):
        return _error(422, "INVALID_ARGUMENT", "invalid request parameters")

    @app.exception_handler(HTTPException)
    async def http_error(_request, error):
        return _error(error.status_code, "ROUTE_NOT_FOUND" if error.status_code == 404 else "INVALID_ARGUMENT", "resource does not exist" if error.status_code == 404 else "HTTP request is not supported")

    @app.get("/health")
    @app.get("/healthz")
    async def health():
        return _json({"status": "ok", **MOCK})

    @app.get("/readyz")
    async def readiness():
        return _json(HEALTH)

    @app.get("/capabilities")
    async def capabilities():
        return _json({**CAPABILITIES, "development_faults": bool(enable_development_faults), "limits": {"request_body_bytes": max_body_bytes, "response_bytes": max_response_bytes, "rows": max_rows, "estimated_retained_state_bytes": max_state_bytes}})

    @app.post("/collections")
    async def create_collection(request: Request):
        body = await _body(request, max_body_bytes)
        with store.lock:
            result, created = store.create(body)
        return _json(result, 201 if created else 200)

    @app.get("/collections")
    async def collections(request: Request):
        params = _page_params(request)
        with store.lock:
            result = store.page([item.info() for item in store.collections.values()], kind="collections", collection_id=None,
                                params=params, watermark=len(store.collections), cursor=request.query_params.get("cursor"))
        return _json(result)

    @app.get("/collections:resolve")
    async def resolve_collection(request: Request):
        if set(request.query_params) != {"name"} or len(request.query_params.getlist("name")) != 1:
            raise MockError(422, "INVALID_ARGUMENT", "one exact name is required")
        with store.lock:
            for item in store.collections.values():
                if item.name == request.query_params["name"]:
                    return _json(item.info())
        raise MockError(404, "NOT_FOUND", "exact collection name does not exist")

    @app.get("/collections/{collection_id}")
    async def collection_info(collection_id: str):
        with store.lock:
            return _json(store.collection(collection_id).info())

    @app.get("/collections/{collection_id}/schema")
    async def schema(collection_id: str):
        with store.lock:
            return _json(store.collection(collection_id).schema)

    @app.get("/collections/{collection_id}/stats")
    async def stats(collection_id: str, request: Request):
        if set(request.query_params) - {"snapshot_id"}:
            raise MockError(422, "INVALID_ARGUMENT", "unknown stats parameter")
        with store.lock:
            item = store.collection(collection_id)
            snapshot, rows = store.view(item, request.query_params.get("snapshot_id", "latest"))
            files = _files(store, item, snapshot)
            return _json({"identity": store.identity(item, snapshot), "rows": len(rows), "data_files": len(files),
                          "compressed_data_bytes": None, "retained_snapshots": len(item.snapshots), "root_metadata_bytes": None,
                          "byte_measurement": "unavailable_no_physical_files", "synthetic": True, **MOCK})

    @app.get("/collections/{collection_id}/snapshots")
    async def snapshots(collection_id: str, request: Request):
        params = _page_params(request, allow_snapshot=True)
        with store.lock:
            item = store.collection(collection_id)
            cursor = request.query_params.get("cursor")
            context = store.cursor_for(cursor, kind="snapshots", collection_id=item.collection_id, params=params)
            selector = context["identity"]["snapshot_id"] if context else params["snapshot_id"]
            snapshot, _ = store.view(item, selector)
            items = [{key: value for key, value in entry.items() if key not in {"rows", "added_rows"}} | MOCK for entry in item.snapshots]
            return _json(store.page(items, kind="snapshots", collection_id=item.collection_id, params=params,
                                   identity=store.identity(item, snapshot), cursor=cursor))

    @app.get("/collections/{collection_id}/files")
    async def files(collection_id: str, request: Request):
        params = _page_params(request, allow_snapshot=True)
        with store.lock:
            item = store.collection(collection_id)
            cursor = request.query_params.get("cursor")
            context = store.cursor_for(cursor, kind="files", collection_id=item.collection_id, params=params)
            selector = context["identity"]["snapshot_id"] if context else params["snapshot_id"]
            snapshot, _ = store.view(item, selector)
            return _json(store.page(_files(store, item, snapshot), kind="files", collection_id=item.collection_id, params=params,
                                   identity=store.identity(item, snapshot), cursor=cursor))

    @app.get("/collections/{collection_id}/files/{file_id}")
    async def file_details(collection_id: str, file_id: str, request: Request):
        if set(request.query_params) - {"snapshot_id"}:
            raise MockError(422, "INVALID_ARGUMENT", "unknown file parameter")
        uuid_value(file_id, "file_id")
        with store.lock:
            item = store.collection(collection_id)
            snapshot, _ = store.view(item, request.query_params.get("snapshot_id", "latest"))
            for entry in _files(store, item, snapshot):
                if entry["file_id"] == file_id:
                    return _json({**entry, "identity": store.identity(item, snapshot),
                                  "row_groups": [{"ordinal": 0, "record_count": entry["record_count"], "compressed_bytes": None, "field_metrics": None, "synthetic": True}],
                                  "footer_statistics": None, "indexes": {"page": False, "bloom": False},
                                  "execution_capabilities": {"page_pruning": False, "bloom_pruning": False}})
        raise MockError(404, "NOT_FOUND", "file is not a member of the selected view")

    @app.post("/collections/{collection_id}/append")
    async def append(collection_id: str, request: Request):
        body = await _body(request, max_body_bytes)
        scenario = request.headers.get("x-glamdb-mock-scenario")
        if scenario:
            if not enable_development_faults:
                raise MockError(403, "MOCK_SCENARIO_DISABLED", "development fault scenarios are explicitly disabled")
            if scenario not in {"definitely_not_committed", "commit_unknown", "lost_response"}:
                raise MockError(422, "INVALID_ARGUMENT", "unknown development scenario")
        with store.lock:
            result, status = store.append(store.collection(collection_id), body, scenario)
        return _json(result, status)

    @app.get("/collections/{collection_id}/operations/{operation_id}")
    async def operation(collection_id: str, operation_id: str):
        operation_id = uuid_value(operation_id, "operation_id")
        with store.lock:
            item = store.collection(collection_id)
            existing = item.operations.get(operation_id)
            if existing:
                return _json(existing.response, existing.status)
            if item.pending_operation:
                raise MockError(503, "RECOVERY_REQUIRED", "operation absence is not authoritative during mock recovery", "resolve_original_operation")
        raise MockError(404, "OPERATION_NOT_FOUND", "operation does not exist")

    @app.post("/development/collections/{collection_id}/operations/{operation_id}:resolve")
    async def development_resolve(collection_id: str, operation_id: str, request: Request):
        if not enable_development_faults:
            raise MockError(404, "NOT_FOUND", "development fault scenarios are disabled")
        body = await _body(request, max_body_bytes)
        if set(body) != {"outcome"}:
            raise MockError(422, "INVALID_ARGUMENT", "resolution requires only outcome")
        with store.lock:
            result, status = store.resolve(store.collection(collection_id), operation_id, body["outcome"])
        return _json(result, status)

    @app.post("/collections/{collection_id}/scan")
    async def scan(collection_id: str, request: Request):
        body = await _body(request, max_body_bytes)
        with store.lock:
            item = store.collection(collection_id)
            snapshot, all_rows, selected, output_schema = store.scan(item, body)
            identity = store.identity(item, snapshot)
            profile = {"rows_considered": len(all_rows), "rows_returned": len(selected), "compressed_bytes_read": None,
                       "physical_files_read": None, "actual_io": None, "execution": "in_memory_reference_filter", **MOCK}
        sid = identity["snapshot_id"]
        if "application/vnd.apache.arrow.stream" in request.headers.get("accept", "").lower():
            data = _arrow_bytes(output_schema, selected, max_response_bytes)
            return Response(data, media_type="application/vnd.apache.arrow.stream", headers={
                "X-GlamDB-Snapshot-Id": sid or "initial", "X-Content-SHA256": hashlib.sha256(data).hexdigest(),
                "Content-Length": str(len(data)), "Cache-Control": "no-store",
            })
        complete_rows = wire_rows(item.schema, selected)
        names = [field["name"] for field in output_schema["fields"]]
        encoded_rows = [{name: row[name] for name in names} for row in complete_rows]
        result = {"schema": output_schema, "snapshot_id": sid, "rows": encoded_rows, "profile": profile, "identity": identity, **MOCK}
        response = _json(result)
        if len(response.body) > max_response_bytes:
            raise MockError(413, "RESOURCE_EXHAUSTED", "mock JSON result exceeds configured response limit")
        return response

    @app.post("/collections/{collection_id}/explain")
    async def explain(collection_id: str, request: Request):
        body = await _body(request, max_body_bytes)
        with store.lock:
            item = store.collection(collection_id)
            snapshot, rows, _selected, output_schema = store.scan(item, body)
            outputs = [field["name"] for field in output_schema["fields"]]
            reads = set(outputs)
            def fields(predicate):
                if not isinstance(predicate, dict):
                    return
                if "field" in predicate:
                    reads.add(predicate["field"])
                for child in predicate.get("args", []):
                    fields(child)
                fields(predicate.get("arg"))
            fields(body.get("predicate"))
            return _json({"schema": output_schema, "identity": store.identity(item, snapshot), "snapshot_id": snapshot["snapshot_id"] if snapshot else None,
                          "read_columns": [field["name"] for field in item.schema["fields"] if field["name"] in reads],
                          "output_columns": outputs, "predicate": body.get("predicate"),
                          "plan": ["resolve fixed mock view", "read all in-memory rows", "exact SQL three-valued filter", "projection", "limit"],
                          "estimates": {"rows_before_filter": len(rows), "output_rows": None, "compressed_bytes": None, "synthetic": True},
                          "actual_io": None, "capabilities": {**CAPABILITIES, "development_faults": bool(enable_development_faults)}, **MOCK})

    @app.get("/workbench/config")
    async def workbench_config():
        try:
            return _json(load_theme(config))
        except FileNotFoundError as error:
            raise MockError(404, "CONFIG_NOT_FOUND", "theme config file does not exist") from error
        except (ThemeError, PermissionError, IsADirectoryError) as error:
            raise MockError(422, "INVALID_CONFIG", str(error)) from error

    def static_file(relative):
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise MockError(404, "NOT_FOUND", "static asset does not exist")
        return FileResponse(path, headers={"Cache-Control": "no-cache"})

    @app.get("/")
    async def index():
        return static_file("index.html")

    @app.get("/assets/{asset_path:path}")
    async def assets(asset_path: str):
        path = (root / "assets" / asset_path).resolve()
        if not path.is_relative_to(root / "assets"):
            raise MockError(404, "NOT_FOUND", "static asset does not exist")
        return static_file(path.relative_to(root))

    return app
