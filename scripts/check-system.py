#!/usr/bin/env python3
"""Exercise a configured disposable database with the real network SDK.

The default requires a native backend. A development adapter is accepted only
with --backend mock and is reported as such. No hosts, credentials, accounts or
databases are discovered or provisioned. Each run creates a fresh collection;
the disposable database must be reset by its owner after testing.

Private mode0600 JSON configuration (paths are relative to the configuration):
  {"version": 1, "disposable": true,
   "base_url": "https://workbench.example/databases/<database UUID>",
   "ca_file": "test-ca.pem", "token_file": "access-token.secret"}
Instead of token_file, oauth may contain token_url, client_id,
client_secret_file and scope. TLS verification cannot be disabled.
"""

import argparse
import ipaddress
import json
import os
from pathlib import Path
import ssl
import stat
import sys
import time
from urllib.parse import urlsplit
from uuid import uuid4


class CheckError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def private_bytes(path, limit=65536):
    """Read an owned private regular file without following a symlink."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise CheckError("PRIVATE_FILE_REQUIRED")
            with os.fdopen(descriptor, "rb", closefd=False) as source:
                value = source.read(limit + 1)
            if len(value) > limit:
                raise CheckError("CONFIG_LIMIT")
            return value
        finally:
            os.close(descriptor)
    except OSError:
        raise CheckError("PRIVATE_FILE_REQUIRED") from None


def endpoint(value):
    try:
        parsed = urlsplit(value)
        if (
            not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.scheme not in ("http", "https")
        ):
            raise ValueError()
        if parsed.scheme == "http":
            try:
                local = ipaddress.ip_address(parsed.hostname).is_loopback
            except ValueError:
                local = parsed.hostname == "localhost"
            if not local:
                raise ValueError()
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError()
        return value.rstrip("/")
    except (AttributeError, TypeError, ValueError):
        raise CheckError("HTTPS_OR_EXPLICIT_LOOPBACK_REQUIRED") from None


def load_config(path):
    if path is None:
        raise CheckError("CONFIG_REQUIRED")
    path = Path(path).absolute()
    try:

        def unique(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError()
                result[key] = value
            return result

        value = json.loads(private_bytes(path), object_pairs_hook=unique)
        if (
            not isinstance(value, dict)
            or value.get("version") != 1
            or value.get("disposable") is not True
            or set(value)
            - {"version", "disposable", "base_url", "ca_file", "token_file", "oauth"}
        ):
            raise ValueError()
        value["base_url"] = endpoint(value["base_url"])
        if ("token_file" in value) == ("oauth" in value):
            raise ValueError()
        if "oauth" in value:
            oauth = value["oauth"]
            if (
                not isinstance(oauth, dict)
                or set(oauth)
                != {"token_url", "client_id", "client_secret_file", "scope"}
                or any(not isinstance(item, str) or not item for item in oauth.values())
            ):
                raise ValueError()
            oauth["token_url"] = endpoint(oauth["token_url"])
            oauth["client_secret_file"] = str(path.parent / oauth["client_secret_file"])
        for key in ("ca_file", "token_file"):
            if key in value:
                if not isinstance(value[key], str) or not value[key]:
                    raise ValueError()
                value[key] = str(path.parent / value[key])
        return value
    except (ValueError, KeyError, TypeError):
        raise CheckError("DISPOSABLE_CONFIG_REQUIRED") from None


def require_backend(capabilities, backend):
    if not isinstance(capabilities, dict) or type(capabilities.get("mock")) is not bool:
        raise CheckError("BACKEND_IDENTITY_REQUIRED")
    if capabilities["mock"]:
        if backend != "mock":
            raise CheckError("EXPLICIT_MOCK_REQUIRED")
        return "explicit_mock"
    if backend != "native":
        raise CheckError("BACKEND_SELECTION_MISMATCH")
    return "native"


def token_provider(config, verify):
    def secret(path):
        try:
            value = private_bytes(path, 16384).decode("ascii").rstrip("\r\n")
            if not value or any(ord(char) < 33 or ord(char) > 126 for char in value):
                raise ValueError()
            return value
        except (UnicodeError, ValueError):
            raise CheckError("INVALID_CREDENTIAL_FILE") from None

    if "token_file" in config:
        return lambda: secret(config["token_file"])
    cache = []

    def acquire():
        if cache and time.monotonic() < cache[1]:
            return cache[0]
        from authlib.integrations.httpx_client import OAuth2Client

        oauth = config["oauth"]
        try:
            with OAuth2Client(
                oauth["client_id"],
                secret(oauth["client_secret_file"]),
                scope=oauth["scope"],
                verify=verify,
                trust_env=False,
                timeout=10,
                follow_redirects=False,
            ) as client:
                value = client.fetch_token(
                    oauth["token_url"], grant_type="client_credentials"
                )
            token = value["access_token"]
            if not isinstance(token, str) or not token or len(token) > 16384:
                raise ValueError()
            cache[:] = [
                token,
                time.monotonic() + max(1, int(value.get("expires_in", 30)) - 5),
            ]
            return token
        except Exception:
            raise CheckError("TOKEN_ACQUISITION_FAILED") from None

    return acquire


def require(condition, code):
    if not condition:
        raise CheckError(code)


def require_unsupported(request):
    """An advertised missing feature must give its typed protocol rejection."""
    from glamdb_sdk import HTTPError

    try:
        request()
    except HTTPError as error:
        require(
            error.status_code == 501 and error.detail.code == "UNSUPPORTED_OPERATION",
            "UNSUPPORTED_CAPABILITY_CONTRACT",
        )
    else:
        raise CheckError("UNSUPPORTED_CAPABILITY_CONTRACT")


def roundtrip(client, backend="native"):
    """Cover every SDK API route using only a fresh UUID collection."""
    from glamdb_sdk import Field, Schema, col

    capabilities = client.capabilities()
    selected = require_backend(capabilities, backend)
    features = capabilities.get("features", {})
    require(
        isinstance(features, dict)
        and all(type(value) is bool for value in features.values()),
        "INVALID_CAPABILITIES",
    )
    missing = [
        name
        for name in (
            "stats",
            "snapshots",
            "files",
            "explain",
            "pagination",
            "zero_column_projection",
        )
        if features.get(name) is False
    ]
    collection_id, operation_id = str(uuid4()), str(uuid4())
    # A space exercises exact-name query encoding through the deployment proxy.
    name = "system " + uuid4().hex
    schema = Schema([Field("key", "string", False), Field("n", "int64", True)])
    rows = [
        {"key": "duplicate", "n": 9007199254740993},
        {"key": "duplicate", "n": None},
    ]
    created = client.create_collection(name, schema, collection_id=collection_id)
    require(created.outcome == "committed", "CREATE_NOT_COMMITTED")
    require(
        client.resolve_create(name, schema, collection_id=collection_id)[
            "collection_id"
        ]
        == collection_id,
        "EXACT_NAME_RESOLUTION_FAILED",
    )
    require(
        client.get_collection(collection_id)["name"] == name,
        "COLLECTION_IDENTITY_FAILED",
    )
    require(client.get_schema(collection_id) == schema, "SCHEMA_CHANGED")
    require(
        any(
            item["collection_id"] == collection_id
            for item in client.list_collections(page_size=1000).items
        ),
        "COLLECTION_LIST_FAILED",
    )
    require(
        client.scan_json(collection_id, snapshot_id=None).rows == [],
        "INITIAL_VIEW_NOT_EMPTY",
    )
    prepared = client.prepare_append(
        collection_id, rows, operation_id=operation_id, expected_snapshot=None
    )
    first = client.append(prepared)
    require(first.outcome == "committed", "APPEND_NOT_COMMITTED")
    snapshot = first.result["snapshot_id"]
    require(client.append(prepared).raw == first.raw, "IDEMPOTENT_RECEIPT_CHANGED")
    found = client.find_append_outcome(collection_id, operation_id)
    require(found is not None and found.raw == first.raw, "APPEND_LOOKUP_FAILED")
    require(
        client.find_append_outcome(collection_id, str(uuid4())) is None,
        "UNKNOWN_OPERATION_NOT_ABSENT",
    )
    second = client.append(
        collection_id,
        [{"key": "later", "n": -2}],
        operation_id=str(uuid4()),
        expected_snapshot=snapshot,
    )
    require(second.outcome == "committed", "SECOND_APPEND_NOT_COMMITTED")
    latest = rows + [{"key": "later", "n": -2}]
    require(client.scan_json(collection_id).rows == latest, "ROUNDTRIP_ROWS_CHANGED")
    require(
        client.scan_json(collection_id, snapshot_id=snapshot).rows == rows,
        "HISTORICAL_VIEW_CHANGED",
    )
    require(
        client.scan_json(collection_id, snapshot_id=None).rows == [],
        "INITIAL_VIEW_CHANGED",
    )
    filtered = client.scan_json(
        collection_id, projection=["key"], predicate=col("n", "int64").gt(0)
    )
    require(filtered.rows == [{"key": "duplicate"}], "FILTER_OR_NULL_SEMANTICS_FAILED")
    checks = [
        "create",
        "resolve",
        "get_collection",
        "list",
        "schema",
        "append",
        "idempotent_receipt",
        "operation_lookup",
        "historical_and_initial_views",
        "typed_filter",
    ]
    if "stats" in missing:
        require_unsupported(lambda: client.stats(collection_id, snapshot_id=snapshot))
    else:
        stats = client.stats(collection_id, snapshot_id=snapshot)
        require(stats.get("rows") in (2, "2"), "HISTORICAL_STATS_CHANGED")
        checks.append("stats")
    if "snapshots" in missing:
        require_unsupported(lambda: client.snapshots(collection_id, page_size=1))
    else:
        snapshots = client.snapshots(
            collection_id, page_size=1000 if "pagination" in missing else 1
        )
        require(bool(snapshots.items), "SNAPSHOTS_EMPTY")
        if features.get("pagination") is True:
            require(bool(snapshots.next_cursor), "SNAPSHOT_PAGINATION_FAILED")
        if snapshots.next_cursor:
            require(
                bool(
                    client.snapshots(
                        collection_id, page_size=1, cursor=snapshots.next_cursor
                    ).items
                ),
                "SNAPSHOT_PAGINATION_FAILED",
            )
        checks.append("snapshots")
    if "files" in missing:
        require_unsupported(lambda: client.files(collection_id, snapshot_id=snapshot))
        require_unsupported(
            lambda: client.file(collection_id, str(uuid4()), snapshot_id=snapshot)
        )
    else:
        files = client.files(collection_id, snapshot_id=snapshot, page_size=1)
        require(bool(files.items), "FILES_EMPTY")
        file_id = files.items[0]["file_id"]
        require(
            client.file(collection_id, file_id, snapshot_id=snapshot)["file_id"]
            == file_id,
            "FILE_METADATA_FAILED",
        )
        checks.extend(("files", "file_metadata"))
    if "explain" in missing:
        require_unsupported(lambda: client.explain(collection_id, snapshot_id=snapshot))
    else:
        require(
            isinstance(client.explain(collection_id, snapshot_id=snapshot), dict),
            "EXPLAIN_FAILED",
        )
        checks.append("explain")
    if "pagination" in missing:
        require_unsupported(
            lambda: client.list_collections(
                page_size=1, cursor="unsupported-profile-probe"
            )
        )
    if "zero_column_projection" in missing:
        require_unsupported(lambda: client.scan_json(collection_id, projection=[]))
    elif features.get("zero_column_projection") is True:
        require(
            client.scan_json(collection_id, projection=[]).rows == [{}, {}, {}],
            "ZERO_COLUMN_PROJECTION_CHANGED",
        )
        checks.append("zero_column_projection")
    with client.scan_batches(collection_id) as stream:
        arrow = [row for batch in stream for row in batch.to_pylist()]
        require(arrow == latest, "ARROW_ROWS_CHANGED")
        require(
            stream.snapshot_id == second.result["snapshot_id"]
            and stream.integrity in ("complete", "verified"),
            "ARROW_IDENTITY_OR_INTEGRITY_FAILED",
        )
    with client.scan_batches(collection_id, snapshot_id=snapshot) as stream:
        require(
            [row for batch in stream for row in batch.to_pylist()] == rows,
            "ARROW_HISTORY_CHANGED",
        )
    checks.append("arrow")
    result = {
        "status": "blocked" if missing else "pass",
        "backend": selected,
        "collection_id": collection_id,
        "checks": checks,
    }
    if missing:
        result.update(code="UNSUPPORTED_CAPABILITIES", missing_capabilities=missing)
    return result


def run(config, backend="native"):
    from glamdb_sdk import Client

    verify = (
        ssl.create_default_context(cafile=config["ca_file"])
        if config.get("ca_file")
        else True
    )
    with Client(
        config["base_url"],
        token_provider=token_provider(config, verify),
        verify=verify,
        timeout=15,
    ) as client:
        return roundtrip(client, backend)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--backend", choices=("native", "mock"), default="native")
    args = parser.parse_args()
    # Repository-local use works after bootstrap and with an installed SDK.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk/python/src"))
    try:
        result = run(load_config(args.config), args.backend)
        print(json.dumps(result, sort_keys=True))
        return 2 if result["status"] == "blocked" else 0
    except Exception as error:
        code = error.code if isinstance(error, CheckError) else "CHECK_FAILED"
        print(
            json.dumps(
                {
                    "status": (
                        "blocked"
                        if code in ("CONFIG_REQUIRED", "EXPLICIT_MOCK_REQUIRED")
                        else "fail"
                    ),
                    "code": code,
                    "error_class": type(error).__name__,
                }
            )
        )
        return 2 if code in ("CONFIG_REQUIRED", "EXPLICIT_MOCK_REQUIRED") else 1


if __name__ == "__main__":
    sys.exit(main())
