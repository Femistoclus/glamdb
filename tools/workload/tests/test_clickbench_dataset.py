from datetime import date, datetime, timezone
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def source_table(path, rows=31):
    from glamdb_workload.clickbench.schema import COLUMNS, STRING_COLUMNS, TIME_COLUMNS

    data = {}
    for index, name in enumerate(COLUMNS):
        if name in STRING_COLUMNS:
            data[name] = [("Привет 💗" if name == "Title" else "value").encode()] * rows
        elif name in TIME_COLUMNS:
            data[name] = [1373805240] * rows
        elif name == "EventDate":
            data[name] = [20130714] * rows
        else:
            data[name] = [9007199254740993 if name in {"WatchID", "UserID"} else index] * rows
    pq.write_table(pa.table(data), path)
    return path


@pytest.mark.parametrize("profile", ["legacy-date", "native"])
def test_prepare_keeps_all_105_columns_exact_values_and_duplicates(tmp_path, profile):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    source = source_table(tmp_path / "hits.parquet")
    output = tmp_path / "dataset"
    manifest = prepare(source, output, type_profile=profile, part_rows=7)
    assert manifest["rows"] == 31 and manifest["source"]["rows"] == 31
    assert manifest["complete"] is True and len(manifest["parts"]) == 5
    data = ClickBenchDataset(output)
    assert len(data.schema.fields) == 105
    assert data.schema.fields[0].name == "WatchID" and data.schema.fields[-1].name == "CLID"
    rows = [row for _, part in data.iter_parts() for row in part]
    assert len(rows) == 31 and rows[0] == rows[-1]
    assert rows[0]["UserID"] == 9007199254740993
    assert rows[0]["Title"] == "Привет 💗"
    expected = date(2013, 7, 14) if profile == "native" else datetime(2013, 7, 14, tzinfo=timezone.utc)
    assert rows[0]["EventDate"] == expected
    assert type(rows[0]["EventDate"]) is type(expected)
    assert next(f.type for f in data.schema.fields if f.name == "EventDate") == (
        "date" if profile == "native" else "timestamp_us_utc")
    assert "key" not in rows[0]
    assert all(item["rows"] <= 7 for item in manifest["parts"])


def test_prefix_and_empty_profiles_preserve_the_whole_schema(tmp_path):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    source = source_table(tmp_path / "hits.parquet")
    for count in (0, 9):
        output = tmp_path / f"dataset-{count}"
        manifest = prepare(source, output, type_profile="legacy-date", rows=count)
        assert manifest["rows"] == count and manifest["profile"] == "prefix"
        assert len(ClickBenchDataset(output).schema.fields) == 105


def test_changed_part_and_unsafe_paths_are_rejected(tmp_path):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    output = tmp_path / "dataset"
    prepare(source_table(tmp_path / "hits.parquet"), output, type_profile="legacy-date")
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    part = output / manifest["parts"][0]["path"]
    part.write_bytes(part.read_bytes() + b" ")
    with pytest.raises(ValueError, match="part"):
        list(ClickBenchDataset(output).iter_parts())
    manifest["parts"][0]["path"] = "../outside.jsonl"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        ClickBenchDataset(output)


def test_invalid_profile_and_overflow_do_not_silently_change_the_dataset(tmp_path):
    from glamdb_workload.clickbench.dataset import prepare

    source = source_table(tmp_path / "hits.parquet")
    with pytest.raises(ValueError, match="profile"):
        prepare(source, tmp_path / "invalid", type_profile="unknown")
    table = pq.read_table(source).set_column(9, "UserID", pa.array([2**63] * 31, type=pa.uint64()))
    pq.write_table(table, source)
    output = tmp_path / "overflow"
    with pytest.raises(ValueError):
        prepare(source, output, type_profile="legacy-date")
    assert not (output / "manifest.json").exists()


def test_byte_budget_and_source_schema_are_checked(tmp_path):
    from glamdb_workload.clickbench.dataset import prepare

    source = source_table(tmp_path / "hits.parquet")
    with pytest.raises(ValueError):
        prepare(source, tmp_path / "small", type_profile="legacy-date", part_bytes=10)
    table = pq.read_table(source).drop(["CLID"])
    pq.write_table(table, source)
    with pytest.raises(ValueError, match="105"):
        prepare(source, tmp_path / "narrow", type_profile="legacy-date")
