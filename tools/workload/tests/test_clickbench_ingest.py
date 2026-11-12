from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace

import pytest

from glamdb_sdk import AppendOutcome, OutcomeUnknownError
from glamdb_sdk.codec import decode_rows
from glamdb_sdk.errors import ErrorDetail


class ProtocolDouble:
    """Explicit in-memory fixture; never native storage/performance evidence."""

    def __init__(self):
        self.caps = {"mock": True, "backend": "mock", "features": {
            key: True for key in ("append", "scan", "scan_pagination", "scan_cancel", "database_management")},
            "limits": {"schema_fields": 128, "append_rows": 10000, "request_bytes": 16 * 1024**2}}
        self.database = None
        self.collection = None
        self.schema = None
        self.requests = []
        self.receipts = {}
        self.lose_reply = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def capabilities(self, **kwargs):
        return deepcopy(self.caps)

    def create_database(self, name, *, database_id, limits, **kwargs):
        self.database = {"database_id": database_id, "name": name, "state": "open", "limits": limits,
                         "store": {"store_id": database_id}}
        return self.database

    def get_database(self, *args, **kwargs):
        return self.database

    def create_collection(self, name, schema, *, collection_id, **kwargs):
        self.schema = schema
        self.collection = {"collection_id": collection_id, "schema": schema.to_dict(),
                           "name": name, "current_snapshot_id": None}
        return SimpleNamespace(outcome="committed", info=self.collection)

    def get_collection(self, *args, **kwargs):
        return dict(self.collection)

    def get_schema(self, *args, **kwargs):
        return self.schema

    def append(self, request, **kwargs):
        self.requests.append(request)
        if request.operation_id not in self.receipts:
            result = {"payload_hash": request.payload_hash,
                      "rows_appended": str(len(request.to_dict()["rows"])),
                      "snapshot_id": str(len(self.receipts) + 1)}
            self.receipts[request.operation_id] = AppendOutcome(
                "committed", "publication", request.operation_id, result, None, {})
            self.collection["current_snapshot_id"] = result["snapshot_id"]
        if self.lose_reply:
            self.lose_reply = False
            raise OutcomeUnknownError(503, ErrorDetail.from_dict({
                "code": "COMMIT_UNKNOWN", "message": "explicit lost-reply fixture"}))
        return self.receipts[request.operation_id]

    def find_append_outcome(self, collection, operation_id, **kwargs):
        return self.receipts.get(operation_id)

    def stats(self, collection, *, snapshot_id, **kwargs):
        count = 0
        for request in self.requests:
            receipt = self.receipts[request.operation_id]
            if snapshot_id is not None and int(receipt.result["snapshot_id"]) <= int(snapshot_id):
                count += int(receipt.result["rows_appended"])
        return {"rows": str(count), "snapshot_id": snapshot_id}

    @contextmanager
    def scan_pages(self, collection, *, snapshot_id, **kwargs):
        pages = []
        for request in self.requests:
            receipt = self.receipts[request.operation_id]
            if snapshot_id is not None and int(receipt.result["snapshot_id"]) <= int(snapshot_id):
                pages.append(SimpleNamespace(rows=decode_rows(self.schema, request.to_dict()["rows"])))
        yield iter(pages)


@pytest.fixture
def dataset(tmp_path):
    from tools.workload.tests.test_clickbench_dataset import source_table
    from glamdb_workload.clickbench.dataset import prepare

    path = tmp_path / "dataset"
    prepare(source_table(tmp_path / "hits.parquet"), path, type_profile="legacy-date", part_rows=7)
    return path


@pytest.fixture
def server(monkeypatch):
    from glamdb_workload.clickbench import ingest

    fixture = ProtocolDouble()
    monkeypatch.setattr(ingest, "client_for", lambda *args, **kwargs: fixture)
    return fixture


def initialize(dataset, state, **kwargs):
    from glamdb_workload.clickbench.ingest import initialize

    return initialize(dataset, state, url="http://127.0.0.1:8080", name="hits-fixture",
                      test_backend="mock", **kwargs)


def test_load_and_verify_use_original_requests_and_fixed_snapshot(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load, verify
    from glamdb_workload.clickbench.model import read_json

    state = tmp_path / "run.json"
    initialize(dataset, state)
    result = load(state)
    assert result["status"] == "PASS" and result["confirmed_rows"] == 31
    assert result["performance_eligible"] is False
    saved = read_json(state)
    assert saved["load_complete"] is True and saved["snapshot_id"] == "5"
    assert verify(state, format="json")["status"] == "PASS"
    assert len(server.requests) == 5
    resumed = load(state, resume=True)
    assert resumed["status"] == "PASS"
    assert resumed["newly_confirmed_rows"] == 0 and resumed["rows_per_second"] == 0
    assert len(server.requests) == 5


def test_lost_reply_stops_then_resolves_without_new_ids(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load, resolve

    state = tmp_path / "run.json"
    initialize(dataset, state)
    server.lose_reply = True
    assert load(state)["status"] == "INCOMPLETE"
    original = server.requests[0]
    assert load(state, resume=True)["status"] == "INCOMPLETE"
    assert len(server.requests) == 1
    assert resolve(state)["status"] == "PASS"
    assert load(state, resume=True)["status"] == "PASS"
    assert len(server.requests) == 5
    assert server.requests[0].operation_id == original.operation_id
    assert server.requests[0].body == original.body


def test_loading_scoped_database_does_not_require_root_management_capability(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load

    state = tmp_path / "run.json"
    initialize(dataset, state)
    server.caps["features"]["database_management"] = False
    assert load(state)["status"] == "PASS"


def test_capacity_and_schema_fail_before_target_creation(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import Blocked

    with pytest.raises(Blocked, match="156"):
        initialize(dataset, tmp_path / "small.json", limits={"max_operations": 2})
    assert server.database is None
    with pytest.raises(Blocked, match="156"):
        initialize(dataset, tmp_path / "oversized.json", limits={"max_operations": 10001})
    assert server.database is None
    server.caps["limits"]["schema_fields"] = 64
    with pytest.raises(Blocked, match="161"):
        initialize(dataset, tmp_path / "narrow.json")
    assert server.database is None


def test_real_server_is_the_default_and_mock_requires_explicit_selection(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import Blocked, initialize

    with pytest.raises(Blocked, match="mock"):
        initialize(dataset, tmp_path / "run.json", url="http://127.0.0.1:8080", name="hits")
    assert server.database is None


def test_changed_dataset_and_journal_budget_are_not_silently_accepted(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import Blocked, load
    from glamdb_workload.clickbench.model import read_json, save_json

    with pytest.raises(Blocked, match="journal"):
        initialize(dataset, tmp_path / "small.json", max_journal_bytes=1)
    state = tmp_path / "run.json"
    initialize(dataset, state)
    manifest = dataset / "manifest.json"
    value = read_json(manifest)
    value["prepare_seconds"] += 1
    save_json(manifest, value)
    with pytest.raises(ValueError, match="dataset"):
        load(state)


def test_size_reports_require_the_same_verified_run_and_never_qualify_mock_performance(dataset, server, tmp_path, monkeypatch):
    from glamdb_workload.clickbench import measure
    from glamdb_workload.clickbench.ingest import load, verify
    from glamdb_workload.clickbench.model import read_json, save_json

    state = tmp_path / "run.json"
    initialize(dataset, state)
    load(state)
    verify(state, format="json")
    monkeypatch.setattr(measure, "client_for", lambda *args, **kwargs: server)
    server.files = lambda *args, **kwargs: SimpleNamespace(
        items=tuple({"file_id": request.operation_id, "record_count": str(len(request.to_dict()["rows"])),
                     "compressed_bytes": "100"} for request in server.requests), next_cursor=None)
    server.snapshots = lambda *args, **kwargs: SimpleNamespace(items=({"snapshot_id": "5"},), next_cursor=None)
    report = tmp_path / "sizes.json"
    result = measure.measure(state, report)
    assert result["status"] == "PASS"
    assert result["performance_eligible"] is False
    assert result["snapshot"]["database_file_bytes"] is None
    assert report.with_suffix(".csv").exists() and report.with_suffix(".md").exists()
    verification = tmp_path / "run-artifacts" / "verify.json"
    saved = read_json(verification)
    saved["dataset_hash"] = "changed"
    save_json(verification, saved)
    assert measure.measure(state, report)["status"] == "INCOMPLETE"
    with pytest.raises(ValueError, match="fixture"):
        measure.measure(state, report, owned_store_dir=tmp_path / "unrelated")


def test_clean_uses_only_the_original_database_and_retains_state_and_journal(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import clean, load
    from glamdb_workload.clickbench.model import read_json

    path = tmp_path / "run.json"
    original = initialize(dataset, path)
    load(path)
    calls = []
    def drop(database, *, store_id, **kwargs):
        calls.append((database, store_id))
        return {"database_id": database, "store_id": store_id, "dropped": True}
    server.drop_database = drop
    assert clean(path)["status"] == "PASS"
    assert calls == [(original["database_id"], original["store_id"])]
    assert read_json(path)["phase"] == "dropped"
    assert (tmp_path / "run-artifacts" / "worker-000000.sqlite").is_file()
    with pytest.raises(ValueError, match="not initialized"):
        load(path, resume=True)
