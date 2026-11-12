"""Real network/CLI checks against an explicit disposable mock capability profile.

The test-only middleware changes advertised capabilities and unsupported replies;
the SDK still uses TCP and the mock executes real JSON/Arrow protocol operations.
No fixture here claims to be a native engine.
"""

from contextlib import contextmanager
import json
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

from glamdb_mock import create_app
from glamdb_mock.state import CAPABILITIES
from starlette.responses import JSONResponse
import pytest
import uvicorn

ROOT = Path(__file__).resolve().parents[2]
TOKEN = "disposable-system-capability-test-only"
MISSING = [
    "stats",
    "snapshots",
    "files",
    "explain",
    "pagination",
    "zero_column_projection",
]
LEGACY = object()


@contextmanager
def profile_server(
    *, features=LEGACY, denied=(), error_status=501, error_code="UNSUPPORTED_OPERATION"
):
    app = create_app(token=TOKEN, ui_dir=ROOT / "ui")
    seen = []

    @app.middleware("http")
    async def capability_profile(request, call_next):
        path = request.url.path
        body = await request.json() if request.method == "POST" else None
        seen.append((path, request.headers.get("accept"), body))
        if request.headers.get("authorization") != "Bearer " + TOKEN:
            return await call_next(request)
        if path == "/capabilities":
            value = dict(CAPABILITIES)
            if features is not LEGACY:
                value["features"] = features
            return JSONResponse(value)
        feature = path.rsplit("/", 1)[-1]
        if "/files/" in path:
            feature = "files"
        if path == "/collections" and "cursor" in request.query_params:
            feature = "pagination"
        if feature == "scan" and body.get("projection") == []:
            feature = "zero_column_projection"
        if feature in denied:
            return JSONResponse(
                {
                    "error": {
                        "code": error_code,
                        "message": "explicit disposable profile does not support this operation",
                        "retry_hint": "FixInput",
                    }
                },
                status_code=error_status,
            )
        return await call_next(request)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                access_log=False,
                log_level="warning",
                loop="asyncio",
                timeout_graceful_shutdown=2,
            )
        )
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]})
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while (
                not server.started and thread.is_alive() and time.monotonic() < deadline
            ):
                time.sleep(0.005)
            assert server.started, "disposable profile server failed to start"
            yield url, seen, app.state.mock_store
        finally:
            server.should_exit = True
            thread.join(5)
            assert not thread.is_alive(), "disposable profile server failed to stop"


def cli(tmp_path, url, *, explicit_mock=True):
    token = tmp_path / "token.secret"
    token.write_text(TOKEN)
    token.chmod(0o600)
    config = tmp_path / "system.json"
    config.write_text(
        json.dumps(
            {
                "version": 1,
                "disposable": True,
                "base_url": url,
                "token_file": token.name,
            }
        )
    )
    config.chmod(0o600)
    command = [
        sys.executable,
        str(ROOT / "scripts/check-system.py"),
        "--config",
        str(config),
    ]
    if explicit_mock:
        command += ["--backend", "mock"]
    result = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True, timeout=30
    )
    return result.returncode, json.loads(result.stdout)


def test_false_native_profile_flags_block_after_supported_network_checks(tmp_path):
    # Ignoring capabilities currently fails at stats before exercising Arrow.
    with profile_server(features={name: False for name in MISSING}, denied=MISSING) as (
        url,
        seen,
        store,
    ):
        status, report = cli(tmp_path, url)
    assert status == 2
    assert report["status"] == "blocked"
    assert report["code"] == "UNSUPPORTED_CAPABILITIES"
    assert report["backend"] == "explicit_mock"
    assert report["missing_capabilities"] == MISSING
    assert {"create", "get_collection", "append", "typed_filter", "arrow"} <= set(
        report["checks"]
    )
    assert not set(MISSING) & set(report["checks"])
    assert len(store.collections) == 1
    assert (
        len([item for item in seen if item[1] == "application/vnd.apache.arrow.stream" and item[0].endswith("/scan")])
        == 2
    )
    for feature in ("stats", "snapshots", "files", "explain"):
        assert any(path.endswith("/" + feature) for path, _, _ in seen)
    assert any(body and body.get("projection") == [] for _, _, body in seen)


@pytest.mark.parametrize("feature", MISSING)
def test_each_explicit_false_feature_is_verified_and_reported(tmp_path, feature):
    with profile_server(features={feature: False}, denied=[feature]) as (url, _, _):
        status, report = cli(tmp_path, url)
    assert status == 2
    assert report["status"] == "blocked"
    assert report["missing_capabilities"] == [feature]
    assert "arrow" in report["checks"]


def test_legacy_capabilities_keep_full_explicit_mock_checks(tmp_path):
    with profile_server() as (url, _, _):
        status, report = cli(tmp_path, url)
    assert status == 0
    assert report["status"] == "pass" and report["backend"] == "explicit_mock"
    assert {"stats", "snapshots", "files", "file_metadata", "explain", "arrow"} <= set(
        report["checks"]
    )


def test_explicit_supported_flags_keep_full_network_checks(tmp_path):
    with profile_server(features={name: True for name in MISSING}) as (url, _, _):
        status, report = cli(tmp_path, url)
    assert status == 0 and report["status"] == "pass"
    assert {
        "get_collection",
        "stats",
        "snapshots",
        "files",
        "explain",
        "zero_column_projection",
        "arrow",
    } <= set(report["checks"])


def test_claimed_supported_feature_failure_remains_failure(tmp_path):
    with profile_server(features={"stats": True}, denied=["stats"]) as (url, _, _):
        status, report = cli(tmp_path, url)
    assert status == 1 and report["status"] == "fail"


@pytest.mark.parametrize(
    "error_status,error_code",
    [(503, "UNSUPPORTED_OPERATION"), (501, "INVALID_ARGUMENT")],
)
def test_false_capability_requires_actual_501_and_typed_unsupported(
    tmp_path, error_status, error_code
):
    with profile_server(
        features={"stats": False},
        denied=["stats"],
        error_status=error_status,
        error_code=error_code,
    ) as (url, _, _):
        status, report = cli(tmp_path, url)
    assert status == 1 and report["status"] == "fail"
    assert report["code"] == "UNSUPPORTED_CAPABILITY_CONTRACT"


def test_false_capability_with_success_is_contract_failure(tmp_path):
    with profile_server(features={"stats": False}) as (url, _, _):
        status, report = cli(tmp_path, url)
    assert status == 1 and report["status"] == "fail"
    assert report["code"] == "UNSUPPORTED_CAPABILITY_CONTRACT"


@pytest.mark.parametrize("value", [0, 1, "false", None, []])
def test_malformed_feature_flag_fails_before_writes(tmp_path, value):
    with profile_server(features={"stats": value}) as (url, _, store):
        status, report = cli(tmp_path, url)
    assert status == 1 and report["code"] == "INVALID_CAPABILITIES"
    assert not store.collections


@pytest.mark.parametrize("value", [None, [], False, "unavailable"])
def test_malformed_features_object_is_not_legacy_absence(tmp_path, value):
    with profile_server(features=value) as (url, _, store):
        status, report = cli(tmp_path, url)
    assert status == 1 and report["code"] == "INVALID_CAPABILITIES"
    assert not store.collections


def test_default_native_rejects_explicit_mock_before_writes(tmp_path):
    with profile_server(features={name: False for name in MISSING}, denied=MISSING) as (
        url,
        _,
        store,
    ):
        status, report = cli(tmp_path, url, explicit_mock=False)
    assert status == 2 and report["status"] == "blocked"
    assert report["code"] == "EXPLICIT_MOCK_REQUIRED"
    assert not store.collections
