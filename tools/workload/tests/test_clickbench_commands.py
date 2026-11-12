import json
import os
import subprocess
import sys

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
