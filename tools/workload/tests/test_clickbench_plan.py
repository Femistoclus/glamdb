from uuid import uuid4

import pytest

from glamdb_workload.ledger import Ledger
from tools.workload.tests.test_clickbench_dataset import source_table


def prepared(tmp_path):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    path = tmp_path / "data"
    prepare(source_table(tmp_path / "hits.parquet"), path, type_profile="native", part_rows=7)
    return ClickBenchDataset(path)


def state(data, mode="append"):
    return {"load_id": str(uuid4()), "collection_id": str(uuid4()),
            "dataset_hash": data.digest, "collection_mode": mode}


def test_kv_keys_preserve_duplicate_rows_and_source_identity(tmp_path):
    from glamdb_workload.clickbench.target import KEY_ENCODING, target_rows, target_schema

    data = prepared(tmp_path)
    rows = next(data.iter_parts())[1]
    keyed = target_rows(rows, 123, "kv")
    assert len(target_schema(data.schema, "kv").fields) == 106
    assert len(target_schema(data.schema, "append").fields) == 105
    assert len({row["key"] for row in keyed}) == len(rows)
    assert keyed[0]["key"] < keyed[-1]["key"]
    assert target_rows(rows[:1], 123, "kv") == keyed[:1]
    assert [{k: v for k, v in row.items() if k != "key"} for row in keyed] == rows
    assert rows[0] == rows[-1] and "key" not in rows[0]
    assert KEY_ENCODING == "source-row-ordinal-decimal-v1"


def test_frozen_plan_splits_wire_bytes_and_rejects_changed_resume(tmp_path):
    from glamdb_workload.clickbench.plan import freeze_plan, request_for

    data = prepared(tmp_path)
    saved = state(data, "kv")
    with Ledger(tmp_path / "plan.sqlite") as log:
        plan = freeze_plan(log, data, saved, batch_rows=7, batch_bytes=15000, workers=3)
        assert plan["rows"] == 31 and plan["operations"] > len(data.manifest["parts"])
        saved["append_plan"] = plan
        keys, ranges = set(), []
        for chunk in log.db.execute("SELECT * FROM clickbench_chunks ORDER BY ordinal"):
            request = request_for(data, saved, chunk, None)
            assert len(request.body) <= 15000 and request.kind == "upsert"
            assert request.to_dict()["expected_snapshot"] is None
            assert request.payload_hash == chunk["payload_hash"]
            assert chunk["worker"] == chunk["ordinal"] % 3
            rows = request.to_dict()["rows"]
            keys.update(row["key"] for row in rows)
            ranges.append((chunk["row_offset"], len(rows)))
        assert len(keys) == 31
        assert sum(n for _, n in ranges) == 31
        assert freeze_plan(log, data, saved, batch_rows=7, batch_bytes=15000, workers=3) == plan
        with pytest.raises(ValueError, match="plan"):
            freeze_plan(log, data, saved, batch_rows=7, batch_bytes=15000, workers=2)
        log.db.execute("UPDATE clickbench_chunks SET row_offset=row_offset+1 WHERE ordinal=0")
        with pytest.raises(ValueError, match="plan"):
            freeze_plan(log, data, saved, batch_rows=7, batch_bytes=15000, workers=3)


def test_extended_capacity_checks_history_and_journal_without_fixed_horizon():
    from glamdb_workload.clickbench.plan import capacity_preflight

    limits = {"max_collections": 1, "max_operations": 20000, "history": {
        "max_snapshots": 20000, "max_data_files": 20000, "max_manifests": 20000,
        "max_recovery_workspace_bytes": 512 * 1024**2}}
    plan = {"operations": 10000, "wire_bytes": 200000, "rows": 99997497}
    assert capacity_preflight(limits, plan, max_journal_bytes=300000)["operations_required"] == 10001
    limits["history"]["max_snapshots"] = 9999
    with pytest.raises(ValueError, match="max_snapshots"):
        capacity_preflight(limits, plan, max_journal_bytes=300000)
    limits["history"]["max_snapshots"] = 20000
    with pytest.raises(ValueError, match="journal"):
        capacity_preflight(limits, plan, max_journal_bytes=100)
