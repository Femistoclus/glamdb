import json
import os
import subprocess
import sys


def test_module_generate_and_validation(tmp_path):
    schema = tmp_path / "schema.json"
    schema.write_text(
        json.dumps({"fields": [{"name": "key", "type": "string", "nullable": False}]})
    )
    args = [
        sys.executable,
        "-m",
        "glamdb_workload",
        "generate",
        "--schema",
        str(schema),
        "--rows",
        "5",
        "--seed",
        "42",
        "--output",
        str(tmp_path / "dataset"),
    ]
    result = subprocess.run(args, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "dataset/manifest.json").read_text())["rows"] == 5
    result = subprocess.run(args, text=True, capture_output=True)
    assert result.returncode != 0


def test_invalid_duration_and_remote_http_fail_before_report(tmp_path):
    args = [
        sys.executable,
        "-m",
        "glamdb_workload",
        "bench",
        "read",
        "--url",
        "http://example.org",
        "--database",
        "bad",
        "--collection",
        "bad",
        "--query",
        str(tmp_path / "query.json"),
        "--report",
        str(tmp_path / "run"),
        "--duration",
        "0s",
    ]
    result = subprocess.run(args, text=True, capture_output=True)
    assert result.returncode == 2
    assert not (tmp_path / "run").exists()
