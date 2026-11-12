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
            key: True for key in ("append", "key_value", "upsert", "scan", "scan_pagination", "scan_cancel", "database_management")},
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

    def create_collection(self, name, schema, *, collection_id, mode="append", **kwargs):
        self.schema = schema
        self.collection = {"collection_id": collection_id, "schema": schema.to_dict(),
                           "name": name, "mode": mode, "current_snapshot_id": None}
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

    def upsert(self, request, **kwargs):
        outcome = self.append(request, **kwargs)
        rows = int(outcome.result.pop("rows_appended", len(request.to_dict()["rows"])))
        outcome.result.update(receipt_version=3, mutation_kind="upsert", snapshot_created=True,
                              input_rows=str(rows), rows_affected=str(rows), rows_written=str(rows),
                              rows_removed="0", rows_appended="0", data_files_added="1",
                              data_files_removed="0", compressed_data_bytes_added="100",
                              compressed_data_bytes_removed="0")
        return outcome

    def stats(self, collection, *, snapshot_id, **kwargs):
        count = 0
        for request in self.requests:
            receipt = self.receipts[request.operation_id]
            if snapshot_id is not None and int(receipt.result["snapshot_id"]) <= int(snapshot_id):
                count += int(receipt.result.get("input_rows", receipt.result["rows_appended"]))
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
    server.caps["limits"]["schema_fields"] = 64
    with pytest.raises(Blocked, match="161"):
        initialize(dataset, tmp_path / "narrow.json")
    assert server.database is None


def test_explicit_extended_operation_profile_is_supported(dataset, server, tmp_path):
    saved = initialize(dataset, tmp_path / "extended.json", limits={"max_operations": 10001})
    assert saved["limits"]["max_operations"] == 10001


def test_kv_loading_preserves_all_source_rows_and_mode_on_resume(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load, verify

    path = tmp_path / "kv.json"
    saved = initialize(dataset, path, mode="kv")
    assert saved["collection_mode"] == "kv"
    assert initialize(dataset, path)["collection_mode"] == "kv"
    with pytest.raises(ValueError, match="mode|original"):
        initialize(dataset, path, mode="append")
    result = load(path, batch_rows=3, workers=2)
    assert result["status"] == "PASS" and result["confirmed_rows"] == 31
    assert result["collection_mode"] == "kv"
    assert len(server.schema.fields) == 106
    rows = [row for request in server.requests for row in request.to_dict()["rows"]]
    assert len({row["key"] for row in rows}) == 31
    assert len({row["UserID"] for row in rows}) == 1
    assert all(request.kind == "upsert" for request in server.requests)
    assert verify(path, format="json")["status"] == "PASS"
    assert load(path, resume=True)["newly_confirmed_rows"] == 0
    with pytest.raises(ValueError, match="plan"):
        load(path, resume=True, workers=1)


def test_foreign_write_is_rejected_before_resume(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load

    path = tmp_path / "foreign.json"
    initialize(dataset, path)
    assert load(path)["status"] == "PASS"
    server.collection["current_snapshot_id"] = "999"
    with pytest.raises(ValueError, match="changed|foreign"):
        load(path, resume=True)


def test_unbound_refusal_requires_explicit_original_id_retry(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load, resolve
    from glamdb_sdk.errors import ErrorDetail

    state = tmp_path / "refused.json"
    initialize(dataset, state)
    send = server.append
    originals = []

    def refused(request, **kwargs):
        originals.append(request)
        return AppendOutcome("definitely_not_committed", "admission", request.operation_id, None,
                             ErrorDetail.from_dict({"code": "RESOURCE_EXHAUSTED", "message": "fixture refusal"}), {})

    server.append = refused
    result = load(state)
    assert result["status"] == "INCOMPLETE" and result["confirmed_rows"] == 0
    assert result["operations"]["failure_lookup_required"] == 1
    assert resolve(state)["operations"]["not_found"] == 1
    assert load(state, resume=True)["status"] == "INCOMPLETE"
    assert len(originals) == 1
    server.append = send
    assert resolve(state, retry=True)["status"] == "PASS"
    assert server.requests[0] == originals[0]
    assert load(state, resume=True)["status"] == "PASS"


def test_tampered_journal_body_cannot_be_verified_against_itself(dataset, server, tmp_path):
    import hashlib
    import json
    import sqlite3

    from glamdb_workload.clickbench.ingest import load, verify

    state = tmp_path / "tampered.json"
    initialize(dataset, state)
    assert load(state)["status"] == "PASS"
    with sqlite3.connect(tmp_path / "tampered-artifacts" / "worker-000000.sqlite") as db:
        operation, body = db.execute("SELECT operation_id,body FROM operations LIMIT 1").fetchone()
        value = json.loads(body)
        value["rows"][0]["CLID"] = "123456789"
        changed = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
        db.execute("UPDATE operations SET body=?,body_hash=?,bytes=? WHERE operation_id=?",
                   (changed, hashlib.sha256(changed).hexdigest(), len(changed), operation))
    with pytest.raises(ValueError, match="source|request|journal"):
        verify(state, format="json")


def test_load_report_keeps_timing_and_source_verification_separate(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load
    from glamdb_workload.clickbench.model import read_json

    state, report = tmp_path / "measured.json", tmp_path / "load.json"
    initialize(dataset, state)
    result = load(state, batch_rows=3, batch_bytes=30000, workers=3, report=report)
    assert read_json(report) == result
    assert result["metrics"]["scheduler"] == "Locust" and result["metrics"]["workers"] == 3
    assert result["metrics"]["publication_concurrency"] == 1
    assert result["metrics"]["verification_seconds"] > 0
    assert result["elapsed_seconds"] > 0 and result["newly_confirmed_rows"] == 31
    assert result["verification"]["source_columns"] == 105
    assert report.with_suffix(".csv").exists() and report.with_suffix(".md").exists()


@pytest.mark.parametrize("suffix", [".csv", ".md"])
def test_load_report_sidecars_cannot_overwrite_state(dataset, server, tmp_path, suffix):
    from glamdb_workload.clickbench.ingest import load

    state = tmp_path / ("load" + suffix)
    initialize(dataset, state)
    original = state.read_bytes()
    with pytest.raises(ValueError, match="separate.*path"):
        load(state, report=tmp_path / "load.json")
    assert state.read_bytes() == original
    assert not server.requests
    assert not (tmp_path / "load.json").exists()


@pytest.mark.parametrize("protected", ["dataset", "journal"])
@pytest.mark.parametrize("suffix", [".csv", ".md"])
def test_load_report_sidecars_cannot_follow_symlinks_into_run_data(
        dataset, server, tmp_path, protected, suffix):
    from glamdb_workload.clickbench.ingest import load

    state = tmp_path / "run.json"
    initialize(dataset, state)
    root = dataset if protected == "dataset" else tmp_path / "run-artifacts"
    target = root / "protected.txt"
    target.write_text("original")
    (tmp_path / ("load" + suffix)).symlink_to(target)
    with pytest.raises(ValueError, match="outside the dataset and journal"):
        load(state, report=tmp_path / "load.json")
    assert target.read_text() == "original"
    assert not server.requests
    assert not (tmp_path / "load.json").exists()


def test_legacy_append_journal_hydrates_receipts_without_resending(dataset, server, tmp_path):
    import sqlite3

    from glamdb_workload.clickbench.ingest import load
    from glamdb_workload.clickbench.model import read_json, save_json

    state = tmp_path / "legacy.json"
    initialize(dataset, state)
    saved = read_json(state)
    saved["request_plan_version"] = 1
    save_json(state, saved)
    assert load(state)["status"] == "PASS"
    saved = read_json(state)
    saved.pop("append_plan")
    for key in ("collection_mode", "target_schema", "key_encoding", "request_plan_version"):
        saved.pop(key, None)
    save_json(state, saved)
    with sqlite3.connect(tmp_path / "legacy-artifacts" / "worker-000000.sqlite") as db:
        original = db.execute("SELECT operation_id,body FROM operations ORDER BY rowid").fetchall()
        db.execute("UPDATE operations SET outcome=?", ('{"server_outcome":"committed","phase":"publication"}',))
        db.execute("DROP TABLE clickbench_chunks")
        db.execute("DROP TABLE clickbench_meta")
    assert load(state, resume=True)["status"] == "PASS"
    assert len(server.requests) == 5
    with sqlite3.connect(tmp_path / "legacy-artifacts" / "worker-000000.sqlite") as db:
        assert db.execute("SELECT operation_id,body FROM operations ORDER BY rowid").fetchall() == original


def legacy_run(dataset, server, tmp_path, *, lose_reply=False):
    import sqlite3

    from glamdb_workload.clickbench.ingest import load
    from glamdb_workload.clickbench.model import read_json, save_json

    state = tmp_path / "old-run.json"
    initialize(dataset, state)
    saved = read_json(state)
    saved["request_plan_version"] = 1
    save_json(state, saved)
    server.lose_reply = lose_reply
    result = load(state)
    assert result["status"] == ("INCOMPLETE" if lose_reply else "PASS")
    saved = read_json(state)
    for key in ("append_plan", "collection_mode", "target_schema", "key_encoding",
                "request_plan_version", "confirmed_rows"):
        saved.pop(key, None)
    save_json(state, saved)
    with sqlite3.connect(tmp_path / "old-run-artifacts" / "worker-000000.sqlite") as db:
        db.execute("UPDATE operations SET outcome=? WHERE state='committed'",
                   ('{"server_outcome":"committed","phase":"publication"}',))
        db.execute("DROP TABLE clickbench_chunks")
        db.execute("DROP TABLE clickbench_meta")
    return state


def test_legacy_resolve_looks_up_original_requests_before_resume(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load, resolve

    state = legacy_run(dataset, server, tmp_path, lose_reply=True)
    original = server.requests[0]
    result = resolve(state)
    assert result["status"] == "PASS" and result["confirmed_rows"] == 7
    assert server.requests == [original]
    assert load(state, resume=True)["status"] == "PASS"
    assert len(server.requests) == 5
    assert server.requests[0].body == original.body


def test_legacy_verify_restores_plan_and_progress_without_resending(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import verify
    from glamdb_workload.clickbench.model import read_json

    state = legacy_run(dataset, server, tmp_path)
    originals = [(request.operation_id, request.body) for request in server.requests]
    result = verify(state, format="json")
    assert result["status"] == "PASS" and result["actual_rows"] == 31
    assert [(request.operation_id, request.body) for request in server.requests] == originals
    assert read_json(state)["load_complete"] is True


def test_failed_verification_revokes_completion_and_can_be_repeated(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load, status, verify
    from glamdb_workload.clickbench.model import read_json

    state = tmp_path / "invalidated.json"
    initialize(dataset, state)
    assert load(state)["status"] == "PASS"
    scan = server.scan_pages

    @contextmanager
    def corrupted(*args, **kwargs):
        with scan(*args, **kwargs) as pages:
            changed = list(pages)
            changed[0].rows[0]["CLID"] = 123456789
            yield iter(changed)

    server.scan_pages = corrupted
    assert verify(state, format="json")["status"] == "FAIL"
    assert read_json(state)["load_complete"] is False
    assert status(state)["status"] == "INCOMPLETE"
    server.scan_pages = scan
    assert verify(state, format="json")["status"] == "PASS"
    assert status(state)["status"] == "PASS"
    server.collection["current_snapshot_id"] = "999"
    with pytest.raises(ValueError, match="foreign|changed"):
        verify(state, format="json")
    assert read_json(state)["load_complete"] is False
    assert status(state)["status"] == "INCOMPLETE"


def test_changed_unloaded_part_blocks_resume_before_new_writes(dataset, server, tmp_path):
    from glamdb_workload.clickbench.ingest import load, resolve

    state = tmp_path / "changed-source.json"
    initialize(dataset, state)
    server.lose_reply = True
    assert load(state)["status"] == "INCOMPLETE"
    assert resolve(state)["status"] == "PASS"
    original_sends = len(server.requests)
    last = dataset / "part-000004.jsonl"
    last.write_bytes(last.read_bytes() + b" ")
    with pytest.raises(ValueError, match="part"):
        load(state, resume=True)
    assert len(server.requests) == original_sends


def test_retry_cannot_drop_snapshot_precondition(dataset, server, tmp_path):
    import hashlib
    import json
    import sqlite3

    from glamdb_workload.clickbench.ingest import load, resolve

    state = tmp_path / "precondition.json"
    initialize(dataset, state)
    server.lose_reply = True
    assert load(state)["status"] == "INCOMPLETE"
    with sqlite3.connect(tmp_path / "precondition-artifacts" / "worker-000000.sqlite") as db:
        operation, body = db.execute("SELECT operation_id,body FROM operations").fetchone()
        value = json.loads(body)
        del value["expected_snapshot"]
        changed = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
        db.execute("UPDATE operations SET body=?,body_hash=?,bytes=? WHERE operation_id=?",
                   (changed, hashlib.sha256(changed).hexdigest(), len(changed), operation))
    with pytest.raises(ValueError, match="source|request|journal"):
        resolve(state, retry=True)
    assert len(server.requests) == 1


def test_client_disk_shortage_blocks_before_first_write(dataset, server, tmp_path, monkeypatch):
    from glamdb_workload.clickbench.ingest import Blocked, load

    path = tmp_path / "disk.json"
    initialize(dataset, path)
    monkeypatch.setattr("shutil.disk_usage", lambda path: SimpleNamespace(free=1))
    with pytest.raises(Blocked, match="disk"):
        load(path)
    assert not server.requests


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
