import json
from pathlib import Path
import subprocess
import sys


def test_seeded_fixture_generation_reproducible_and_duplicate_preserving(verify):
    first = verify.generate_fixture(seed=17, count=8)
    assert first == verify.generate_fixture(seed=17, count=8)
    assert first != verify.generate_fixture(seed=18, count=8)
    assert first["rows"][0] == first["rows"][1]
    assert len(first["rows"]) == 8
    assert first["schema"]["fields"][0]["field_id"] == 1
    assert first["evidence_kind"] == "fixture"
    assert first["power_durability"] == "NOT VALIDATED"
    assert first["rows"][0]["n"] == "9007199254740993"
    assert verify.check_fixture(first) == []
    first["payload_sha256"] = "0" * 64
    assert verify.check_fixture(first)


def test_checked_in_literal_golden_vectors(verify):
    root = Path(__file__).resolve().parents[2] / "fixtures" / "golden"
    vectors = sorted(root.glob("*.json"))
    assert len(vectors) >= 2
    for path in vectors:
        vector = json.loads(path.read_text())
        assert verify.check_fixture(vector) == [], path.name
        assert path.with_suffix(".hex").read_text().strip() == vector["canonical_hex"]


def test_cli_generate_and_validate_round_trip(verify, tmp_path):
    output = tmp_path / "fixture.json"
    generated = subprocess.run([sys.executable, "-m", "glamdb_verify", "generate", "--seed", "17", "--rows", "8", "--output", str(output)], capture_output=True, text=True)
    assert generated.returncode == 0, generated.stderr
    check = subprocess.run([sys.executable, "-m", "glamdb_verify", "check-fixture", str(output)], capture_output=True, text=True)
    assert check.returncode == 0, check.stderr
    assert json.loads(check.stdout)["verification_status"] == "PASS"
    broken = json.loads(output.read_text())
    broken["rows"][0]["n"] = "7"
    output.write_text(json.dumps(broken))
    check = subprocess.run([sys.executable, "-m", "glamdb_verify", "check-fixture", str(output)], capture_output=True, text=True)
    assert check.returncode == 1
