"""Exercise the installed CLI boundary and failure exit statuses."""

import json
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]


def invoke(*arguments):
    return subprocess.run(
        [sys.executable, "-m", "glamdb_verify", *map(str, arguments)],
        check=False, capture_output=True, text=True, timeout=30,
    )


@pytest.mark.parametrize("name", ["int64-null-duplicates", "all-types-boundaries"])
def test_installed_cli_checks_golden_bytes(name):
    result = invoke("check-fixture", ROOT / "fixtures/golden" / f"{name}.json")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["verification_status"] == "PASS"


def test_cli_generator_roundtrip_and_invalid_fixture_exit(tmp_path):
    path = tmp_path / "fixture.json"
    generated = invoke("generate", "--seed", 17, "--rows", 16, "--output", path)
    assert generated.returncode == 0, generated.stderr
    assert generated.stdout == ""
    assert invoke("check-fixture", path).returncode == 0
    path.write_text("{}", encoding="utf-8")
    invalid = invoke("check-fixture", path)
    assert invalid.returncode == 1
    assert json.loads(invalid.stdout)["verification_status"] == "FAIL"


def test_cli_evidence_never_upgrades_unexecuted_power_report(tmp_path):
    report = tmp_path / "not-run.json"
    report.write_text(json.dumps({
        "evidence_version": 1, "evidence_kind": "real-power", "status": "NOT RUN",
        "environment": {}, "cycles": [],
    }), encoding="utf-8")
    result = invoke("check-evidence", report)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["power_durability"] == "NOT VALIDATED"


@pytest.mark.parametrize("command", ["schedule", "model"])
def test_cli_model_and_schedule_are_explicitly_synthetic(command):
    result = invoke(command, "--seed", 17, "--seeded", 100)
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["power_durability"] == "NOT VALIDATED"
    if command == "schedule":
        assert len(value["schedule"]) == 204
    else:
        assert value["verification_status"] == "PASS"


def test_cli_missing_input_is_a_failed_check(tmp_path):
    result = invoke("check-evidence", tmp_path / "absent.json")
    assert result.returncode == 1
    assert json.loads(result.stdout)["verification_status"] == "FAIL"
