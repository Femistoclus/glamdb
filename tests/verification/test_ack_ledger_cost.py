"""AckLedger.record() validates only the new receipt unless the ledger file changed underneath it."""
import uuid

import glamdb_verify.evidence as evidence


def _receipt(index):
    return {"collection_id": "00112233-4455-6677-8899-aabbccddeeff", "operation_id": str(uuid.UUID(int=index + 1)),
            "payload_hash": "a" * 64, "hash_version": 1, "snapshot_id": str(9007199254740993 + index),
            "parent_snapshot_id": None, "rows_appended": "3", "data_files_added": "1",
            "compressed_data_bytes_added": "123", "snapshot_created_at_us": "1700000000000000",
            "durability_profile": "local-delete-extra-v1", "receipt_version": 1}


def test_record_cost_does_not_grow_with_ledger_length(tmp_path, monkeypatch):
    volume = tmp_path / "volume"
    volume.mkdir()
    controller = tmp_path / "controller"
    controller.mkdir()
    ledger = evidence.AckLedger(controller / "acks.jsonl", test_volume=volume, evidence_kind="process-crash")
    calls = []
    original = evidence.validate_receipt
    monkeypatch.setattr(evidence, "validate_receipt", lambda receipt: calls.append(1) or original(receipt))
    records = 200
    for index in range(records):
        ledger.record(_receipt(index))
    # A linear ledger needs O(1) receipt validations per acknowledgement. Current code needs ~n per call
    # (about 40,000 for 200 records).
    assert len(calls) <= 5 * records, len(calls)
    # Tamper/interrupt detection must remain: reopening validates everything once.
    reopened = evidence.AckLedger(controller / "acks.jsonl", test_volume=volume, evidence_kind="process-crash")
    assert len(reopened.receipts) == records
