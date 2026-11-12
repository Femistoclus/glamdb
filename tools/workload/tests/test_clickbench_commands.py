import json
import os
import subprocess
import sys
from types import ModuleType

import pytest


def test_prepare_command_preserves_every_column_without_a_server(tmp_path):
    from tools.workload.tests.test_clickbench_dataset import source_table

    result = subprocess.run(
        [sys.executable, "-B", "-m", "glamdb_workload", "bench", "clickbench", "prepare",
         "--source", str(source_table(tmp_path / "hits.parquet")), "--output", str(tmp_path / "data"),
         "--type-profile", "legacy-date", "--rows", "7", "--part-rows", "3"],
        capture_output=True, text=True, timeout=20,
        env={key: value for key, value in os.environ.items() if key != "GLAMDB_TOKEN"})
    assert result.returncode == 0, result.stderr
    manifest = json.loads((tmp_path / "data" / "manifest.json").read_text())
    assert manifest["rows"] == 7 and len(manifest["schema"]["fields"]) == 105
    assert manifest["profile"] == "prefix" and len(manifest["parts"]) == 3
    assert json.loads(result.stdout)["status"] == "PASS"


def test_native_date_prepare_command_retains_calendar_dates(tmp_path, capsys):
    from glamdb_workload.__main__ import main
    from tools.workload.tests.test_clickbench_dataset import source_table

    source = source_table(tmp_path / "hits.parquet")
    output = tmp_path / "native"
    assert main(["bench", "clickbench", "prepare", "--source", str(source),
                 "--output", str(output), "--type-profile", "native"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert next(f["type"] for f in manifest["schema"]["fields"] if f["name"] == "EventDate") == "date"
    row = json.loads((output / manifest["parts"][0]["path"]).read_bytes().splitlines()[0])
    assert row["EventDate"] == "2013-07-14"


def test_state_commands_cannot_silently_switch_endpoint_or_database():
    from glamdb_workload.__main__ import parser

    app = parser()
    for action in ("load", "resolve", "verify", "status", "size", "clean"):
        extra = ["--report", "sizes.json"] if action == "size" else []
        args = app.parse_args(["bench", "clickbench", action, "--state", "run.json", *extra])
        assert args.mode == "clickbench"
        with pytest.raises(SystemExit):
            app.parse_args(["bench", "clickbench", action, "--state", "run.json",
                            *extra, "--url", "https://another.example"])


def test_query_mode_does_not_replace_the_clickbench_command_router():
    from glamdb_workload.__main__ import parser

    args = parser().parse_args(["bench", "clickbench", "query", "--state", "run.json",
                                "--catalog", "cache", "--report", "queries.json", "--mode", "reduce"])
    assert args.mode == "clickbench"
    assert args.query_mode == "reduce"


@pytest.mark.parametrize("mode", [None, "append", "kv"])
def test_init_forwards_explicit_mode_and_preserves_an_omitted_mode(monkeypatch, capsys, mode):
    from glamdb_workload.__main__ import main
    from glamdb_workload.clickbench import ingest

    received = {}
    def initialize(dataset, state, **options):
        received.update(dataset=dataset, state=state, **options)
        return {"phase": "ready", "database_id": "database", "collection_id": "collection",
                "performance_eligible": True, "plan": {}, "collection_mode": mode or "append"}
    monkeypatch.setattr(ingest, "initialize", initialize)
    arguments = ["bench", "clickbench", "init", "--dataset", "data", "--state", "run.json",
                 "--url", "https://example.org", "--name", "hits", "--ca-file", "ca.pem",
                 "--timeout", "19", "--max-journal-bytes", "4096"]
    if mode is not None:
        arguments += ["--mode", mode]
    assert main(arguments) == 0
    assert received == {"dataset": "data", "state": "run.json", "url": "https://example.org",
                        "name": "hits", "limits": None, "max_journal_bytes": 4096,
                        "test_backend": None, "timeout": 19.0, "ca_file": "ca.pem", "mode": mode}
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "PASS"
    assert result["collection_mode"] == (mode or "append")


@pytest.mark.parametrize("resume", [False, True])
def test_load_omitted_controls_leave_the_saved_plan_unchanged(monkeypatch, capsys, resume):
    from glamdb_workload.__main__ import main
    from glamdb_workload.clickbench import ingest

    received = {}
    def load(state, **options):
        received.update(state=state, **options)
        return {"status": "PASS"}
    monkeypatch.setattr(ingest, "load", load)
    arguments = ["bench", "clickbench", "load", "--state", "run.json"]
    if resume:
        arguments += ["--resume"]
    assert main(arguments) == 0
    assert received == {"state": "run.json", "timeout": 30, "ca_file": None, "resume": resume,
                        "batch_rows": None, "batch_bytes": None, "workers": None, "report": None}
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


def test_load_forwards_bounded_worker_controls_and_optional_json_report(monkeypatch, capsys):
    from glamdb_workload.__main__ import main
    from glamdb_workload.clickbench import ingest

    received = {}
    def load(state, **options):
        received.update(state=state, **options)
        return {"status": "PASS"}
    monkeypatch.setattr(ingest, "load", load)
    assert main(["bench", "clickbench", "load", "--state", "run.json", "--batch-rows", "8000",
                 "--batch-bytes", "6000000", "--workers", "3", "--resume", "--report", "load.json",
                 "--timeout", "17.5", "--ca-file", "ca.pem"]) == 0
    assert received == {"state": "run.json", "timeout": 17.5, "ca_file": "ca.pem", "resume": True,
                        "batch_rows": 8000, "batch_bytes": 6000000, "workers": 3, "report": "load.json"}
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


@pytest.mark.parametrize("action,default", [("query", "both"), ("scan", "scan")])
def test_query_and_scan_defaults_keep_the_command_router_and_single_client(action, default):
    from glamdb_workload.__main__ import parser

    args = parser().parse_args(["bench", "clickbench", action, "--state", "run.json",
                                "--catalog", "cache", "--report", "queries.json"])
    assert args.mode == "clickbench"
    assert args.clickbench_command == action
    assert args.query_mode == default
    assert args.source is None
    assert (args.ids, args.repeats, args.users, args.processes) == ("all", 3, 1, 1)


@pytest.mark.parametrize("action,id_option,repeat_option", [
    ("query", "--ids", "--repeats"), ("query", "--queries", "--repeat"),
    ("scan", "--ids", "--repeats"), ("scan", "--queries", "--repeat"),
])
def test_query_aliases_and_client_counts_reach_the_campaign(
        monkeypatch, capsys, action, id_option, repeat_option):
    from glamdb_workload.__main__ import main

    received = {}
    def campaign(state, catalog, report, **options):
        received.update(state=state, catalog=catalog, report=report, **options)
        return {"status": "PASS"}
    module = ModuleType("glamdb_workload.clickbench.run")
    module.campaign = campaign
    monkeypatch.setitem(sys.modules, module.__name__, module)
    assert main(["bench", "clickbench", action, "--state", "run.json", "--catalog", "cache",
                 "--report", "queries.json", "--source", "hits.parquet", id_option, "1,20,43",
                 repeat_option, "2", "--mode", "both", "--warmup", "0", "--users", "4",
                 "--processes", "2", "--timeout", "17.5", "--ca-file", "ca.pem",
                 "--memory-bytes", "1000", "--temp-bytes", "2000", "--max-candidate-bytes", "3000",
                 "--max-result-rows", "4000", "--max-result-bytes", "5000"]) == 0
    assert received == {"state": "run.json", "catalog": "cache", "report": "queries.json",
                        "source": "hits.parquet", "ids": "1,20,43", "repeats": 2, "mode": "both",
                        "warmup": 0, "users": 4, "processes": 2, "timeout": 17.5, "ca_file": "ca.pem",
                        "memory_bytes": 1000, "temp_bytes": 2000, "max_candidate_bytes": 3000,
                        "max_result_rows": 4000, "max_result_bytes": 5000}
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


@pytest.mark.parametrize("action", ["query", "scan"])
def test_both_query_commands_require_the_pinned_catalog(action):
    from glamdb_workload.__main__ import parser

    with pytest.raises(SystemExit):
        parser().parse_args(["bench", "clickbench", action, "--state", "run.json",
                            "--report", "queries.json"])


@pytest.mark.parametrize("option", ["--batch-rows", "--batch-bytes", "--workers"])
def test_load_rejects_nonpositive_controls(option):
    from glamdb_workload.__main__ import parser

    with pytest.raises(SystemExit):
        parser().parse_args(["bench", "clickbench", "load", "--state", "run.json", option, "0"])


def test_load_execution_does_not_require_query_dependencies(tmp_path):
    script = """
import importlib.abc
import sys
class NoQueryDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'duckdb', 'sqlglot'}:
            raise ModuleNotFoundError('query dependency deliberately unavailable: ' + fullname)
sys.meta_path.insert(0, NoQueryDependencies())
from glamdb_workload.__main__ import main
assert main(['bench', 'clickbench', 'load', '--state', sys.argv[1]]) == 2
assert 'duckdb' not in sys.modules and 'sqlglot' not in sys.modules
print('load reached the saved-state boundary without query dependencies')
"""
    result = subprocess.run([sys.executable, "-B", "-c", script, str(tmp_path / "absent.json")],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "saved-state boundary without query dependencies" in result.stdout


def test_process_lock_excludes_another_writer_and_releases_on_exception(tmp_path):
    from glamdb_workload.clickbench.model import run_lock

    state = tmp_path / "run.json"
    with pytest.raises(RuntimeError):
        with run_lock(state):
            with pytest.raises(ValueError, match="already in use"):
                with run_lock(state):
                    pytest.fail("second writer entered")
            raise RuntimeError("simulated interrupted command")
    with run_lock(state):
        pass


def test_only_json_report_paths_are_accepted_before_network(tmp_path):
    from glamdb_workload.clickbench.measure import measure

    with pytest.raises(ValueError, match="json"):
        measure(tmp_path / "absent-state", tmp_path / "result.csv")
    state = tmp_path / "run.json"
    state.write_bytes(b"original")
    with pytest.raises(ValueError, match="overwrite"):
        measure(state, state)
    assert state.read_bytes() == b"original"
