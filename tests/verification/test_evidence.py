import json
import subprocess
import sys

import pytest


RECEIPT = {
    "collection_id": "00112233-4455-6677-8899-aabbccddeeff",
    "operation_id": "10112233-4455-6677-8899-aabbccddeeff",
    "payload_hash": "e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7",
    "hash_version": 1,
    "snapshot_id": "9007199254740993",
    "parent_snapshot_id": None,
    "rows_appended": 3,
    "data_files_added": 1,
    "compressed_data_bytes_added": 120,
    "snapshot_created_at_us": "0",
    "durability_profile": "LINUX-LOCAL-1",
    "receipt_version": 1,
}


def test_ledger_controller_is_separate_and_survives_reopen(verify, tmp_path):
    volume = tmp_path / "volume"
    volume.mkdir()
    controller = tmp_path / "controller"
    controller.mkdir()
    path = controller / "acks.jsonl"
    ledger = verify.AckLedger(path, test_volume=volume, evidence_kind="process-crash")
    ledger.record(RECEIPT)
    reopened = verify.AckLedger(path, test_volume=volume, evidence_kind="process-crash")
    assert reopened.receipts == [RECEIPT]
    assert verify.check_recovery(reopened.receipts, [RECEIPT]) == []
    with pytest.raises(ValueError, match="duplicate"):
        reopened.record(RECEIPT)
    with pytest.raises(ValueError):
        verify.AckLedger(path, test_volume=volume, evidence_kind="real-power")


@pytest.mark.parametrize("first_fence_fails", [False, True])
@pytest.mark.parametrize("reopen_fence_fails", [False, True])
def test_ledger_reopen_requires_directory_fence(verify, tmp_path, monkeypatch, first_fence_fails, reopen_fence_fails):
    from glamdb_verify import evidence

    volume = tmp_path / "volume"
    volume.mkdir()
    path = tmp_path / "acks.jsonl"
    if first_fence_fails:
        def fail_directory_sync(parent):
            raise OSError("injected initial directory sync failure")

        with monkeypatch.context() as fault:
            fault.setattr(evidence, "_sync_directory", fail_directory_sync)
            with pytest.raises(OSError, match="initial directory sync failure"):
                verify.AckLedger(path, test_volume=volume, evidence_kind="process-crash")
    else:
        verify.AckLedger(path, test_volume=volume, evidence_kind="process-crash")
    content = path.read_bytes()
    assert content == b'{"evidence_kind":"process-crash","ledger_version":1}\n'
    reopened = subprocess.run(
        [sys.executable, "-c", """
import json
import sys
from glamdb_verify import evidence

path, volume, fail = sys.argv[1:]
sync_directory = evidence._sync_directory
fences = []

def sync_parent(parent):
    if json.loads(fail):
        raise OSError("injected reopen directory sync failure")
    sync_directory(parent)
    fences.append(str(parent))

evidence._sync_directory = sync_parent
try:
    ledger = evidence.AckLedger(path, test_volume=volume, evidence_kind="process-crash")
except OSError as exc:
    print(json.dumps({"error": str(exc)}))
else:
    constructor_fences = list(fences)
    ledger.record(json.loads(sys.stdin.read()))
    print(json.dumps({"constructor_fences": constructor_fences, "receipts": ledger.receipts}))
""", str(path), str(volume), json.dumps(reopen_fence_fails)],
        input=json.dumps(RECEIPT), capture_output=True, text=True, timeout=30,
    )
    assert reopened.returncode == 0, reopened.stderr
    result = json.loads(reopened.stdout)
    if reopen_fence_fails:
        assert result == {"error": "injected reopen directory sync failure"}
        assert path.read_bytes() == content
    else:
        assert result == {"constructor_fences": [str(path.parent)], "receipts": [RECEIPT]}


def test_ledger_never_writes_to_cut_volume_or_alias(verify, tmp_path):
    volume = tmp_path / "volume"
    volume.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(volume, target_is_directory=True)
    for path in [volume / "ack.jsonl", alias / "ack.jsonl"]:
        with pytest.raises(ValueError):
            verify.AckLedger(path, test_volume=volume, evidence_kind="modeled-power")
    assert list(volume.iterdir()) == []


def test_recovery_detects_missing_duplicate_changed_and_malformed(verify):
    assert any("missing" in error for error in verify.check_recovery([RECEIPT], []))
    assert any("duplicate" in error for error in verify.check_recovery([RECEIPT], [RECEIPT, RECEIPT]))
    changed = dict(RECEIPT, rows_appended=2)
    assert any("changed" in error for error in verify.check_recovery([RECEIPT], [changed]))
    assert any("duplicate" in error for error in verify.check_recovery([RECEIPT, RECEIPT], [RECEIPT]))
    partial = dict(RECEIPT)
    del partial["payload_hash"]
    assert any("invalid" in error for error in verify.check_recovery([RECEIPT], [partial]))


@pytest.mark.parametrize("change", [
    {"receipt_version": 2}, {"hash_version": 2}, {"rows_appended": 0},
    {"snapshot_id": True}, {"snapshot_created_at_us": "1"},
    {"payload_hash": "a" * 63}, {"operation_id": "no-uuid"},
    {"committed_at": "0"},
])
def test_receipt_shape_versions_and_exact_integer_fields(verify, change):
    with pytest.raises(ValueError):
        verify.validate_receipt(dict(RECEIPT, **change))


def test_ledger_rejects_truncated_or_tampered_records(verify, tmp_path):
    volume = tmp_path / "volume"
    volume.mkdir()
    path = tmp_path / "ack.jsonl"
    ledger = verify.AckLedger(path, test_volume=volume, evidence_kind="modeled-power")
    ledger.record(RECEIPT)
    content = path.read_bytes()
    path.write_bytes(content[:-1])
    with pytest.raises(ValueError):
        verify.AckLedger(path, test_volume=volume, evidence_kind="modeled-power")
    records = [json.loads(line) for line in content.splitlines()]
    records[1]["receipt"]["rows_appended"] = 2
    path.write_text("".join(json.dumps(record) + "\n" for record in records))
    with pytest.raises(ValueError):
        verify.AckLedger(path, test_volume=volume, evidence_kind="modeled-power")


def test_disposable_directory_cleanup_never_removes_parent(verify, tmp_path):
    sentinel = tmp_path / "keep.txt"
    sentinel.write_text("keep")
    with verify.DisposableRun(tmp_path) as root:
        assert root.parent == tmp_path
        (root / "nested").mkdir()
        (root / "nested" / "data").write_text("synthetic")
    assert not root.exists()
    assert sentinel.read_text() == "keep"


def test_disposable_cleanup_rejects_replaced_directory_even_with_copied_marker(verify, tmp_path):
    run = verify.DisposableRun(tmp_path)
    root = run.__enter__()
    original = root.with_name(root.name + "-original")
    root.rename(original)
    root.mkdir()
    marker = ".glamdb-verification-disposable"
    (root / marker).write_text((original / marker).read_text())
    (root / "must-survive").write_text("replacement")
    with pytest.raises(ValueError):
        run.__exit__(None, None, None)
    assert (root / "must-survive").read_text() == "replacement"
    assert original.exists()


def test_schedule_has_boundary_coverage_and_reproducible_seed(verify):
    first = verify.fault_schedule(17, seeded=100)
    assert first == verify.fault_schedule(17, seeded=100)
    assert first != verify.fault_schedule(18, seeded=100)
    fixed = [entry for entry in first if entry["kind"] == "boundary"]
    seeded = [entry for entry in first if entry["kind"] == "seeded"]
    assert len(fixed) >= 100 and len(seeded) == 100
    points = {entry["cut_point"] for entry in fixed}
    assert {"data.file_sync.before", "manifest.directory_sync.after", "manifest_list.file_sync.after", "root.directory_sync.after", "sqlite.journal_unlink.after", "ack.after"} <= points


@pytest.mark.parametrize("kind", ["process-crash", "modeled-power"])
def test_nonreal_evidence_never_qualifies_power(verify, kind):
    evidence = {"evidence_version": 1, "evidence_kind": kind, "status": "PASS", "environment": {}, "cycles": [{"seed": 17, "cut_point": "ack.after", "acked": [RECEIPT], "recovered": [RECEIPT]}]}
    result = verify.check_evidence(evidence)
    assert result["power_durability"] == "NOT VALIDATED"
    assert result["verification_status"] == "PASS"
    evidence["power_durability"] = "PASS"
    assert verify.check_evidence(evidence)["verification_status"] == "FAIL"


def test_real_power_requires_environment_and_cycles(verify):
    evidence = {"evidence_version": 1, "evidence_kind": "real-power", "status": "PASS", "environment": {}, "cycles": []}
    result = verify.check_evidence(evidence)
    assert result["verification_status"] == "FAIL"
    assert result["power_durability"] == "NOT VALIDATED"
    assert result["errors"]


def test_evidence_failure_and_not_run_are_explicit(verify):
    evidence = {"evidence_version": 1, "evidence_kind": "process-crash", "status": "NOT RUN", "environment": {}, "cycles": []}
    assert verify.check_evidence(evidence)["verification_status"] == "NOT RUN"
    evidence["status"] = "PASS"
    evidence["cycles"] = [{"seed": 1, "cut_point": "ack.after", "acked": [RECEIPT], "recovered": []}]
    assert verify.check_evidence(evidence)["verification_status"] == "FAIL"


@pytest.mark.parametrize("evidence", [None, [], {}, {"evidence_version": 1, "evidence_kind": [], "status": "PASS", "cycles": [], "environment": {}}])
def test_malformed_evidence_returns_failure_instead_of_raising(verify, evidence):
    assert verify.check_evidence(evidence)["verification_status"] == "FAIL"


def test_ledger_rejects_bool_version_and_unrecognized_evidence_label(verify, tmp_path):
    volume = tmp_path / "volume"
    volume.mkdir()
    path = tmp_path / "ack.jsonl"
    path.write_text('{"ledger_version":true,"evidence_kind":"modeled-power"}\n')
    with pytest.raises(ValueError):
        verify.AckLedger(path, test_volume=volume, evidence_kind="modeled-power")
    with pytest.raises(ValueError):
        verify.AckLedger(tmp_path / "other.jsonl", test_volume=volume, evidence_kind=[])


def test_unacknowledged_complete_receipts_are_permitted(verify):
    other = dict(RECEIPT, operation_id="20112233-4455-6677-8899-aabbccddeeff", snapshot_id="9007199254740994", parent_snapshot_id="9007199254740993")
    assert verify.check_recovery([RECEIPT], [RECEIPT, other]) == []


def test_complete_real_receipt_still_requires_hardware_review(verify):
    environment = {"profile": "LINUX-LOCAL-1", "os": "Linux", "architecture": "x86_64", "filesystem": "ext4", "device": "synthetic-device-description", "firmware": "recorded", "kernel": "recorded", "flush_behavior": "recorded", "cache_policy": "recorded", "mount_options": "recorded", "controller_device": "independent-controller", "controller_volume_disjoint": True}
    cycles = [{"seed": i, "cut_point": "ack.after", "acked": [RECEIPT], "recovered": [RECEIPT]} for i in range(20)]
    result = verify.check_evidence({"evidence_version": 1, "evidence_kind": "real-power", "status": "PASS", "environment": environment, "cycles": cycles, "artifacts": [{"path": "synthetic-raw-image", "sha256": "a" * 64}]})
    assert result["verification_status"] == "PASS"
    assert result["power_durability"] == "REVIEW REQUIRED"
    assert result["acceptance_gate"] == "OPEN"


@pytest.mark.parametrize("seed,seeded", [(-1, 100), (True, 100), (4294967296, 100), (0, -1), (0, True)])
def test_fault_schedule_rejects_invalid_bounds(verify, seed, seeded):
    with pytest.raises(ValueError):
        verify.fault_schedule(seed, seeded=seeded)
