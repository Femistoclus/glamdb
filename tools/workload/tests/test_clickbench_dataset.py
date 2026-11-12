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


@pytest.mark.parametrize("profile", ["legacy-date", "native"])
def test_official_uint16_event_date_uses_epoch_days(tmp_path, profile):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    source = source_table(tmp_path / "hits.parquet", rows=3)
    table = pq.read_table(source)
    table = table.set_column(5, "EventDate", pa.array([15901, 0, 65535], type=pa.uint16()))
    table = table.set_column(9, "UserID", pa.array([-2461439046089301801] * 3, type=pa.int64()))
    pq.write_table(table, source)
    output = tmp_path / "dataset"
    prepare(source, output, type_profile=profile)
    rows = next(ClickBenchDataset(output).iter_parts())[1]
    expected = [date(2013, 7, 15), date(1970, 1, 1), date(2149, 6, 6)]
    if profile == "legacy-date":
        expected = [datetime(day.year, day.month, day.day, tzinfo=timezone.utc) for day in expected]
    assert [row["EventDate"] for row in rows] == expected
    assert rows[0]["UserID"] == -2461439046089301801


@pytest.mark.parametrize(("name", "array"), [
    ("Title", pa.array([17], type=pa.int32())),
    ("WatchID", pa.array([1.0], type=pa.float64())),
    ("UserID", pa.array([True], type=pa.bool_())),
    ("EventDate", pa.array([20130714.0], type=pa.float64())),
    ("EventTime", pa.array([b"1373805240"], type=pa.binary())),
])
def test_empty_prefix_still_checks_source_physical_types(tmp_path, name, array):
    from glamdb_workload.clickbench.dataset import prepare

    source = source_table(tmp_path / "hits.parquet", rows=1)
    table = pq.read_table(source)
    table = table.set_column(table.schema.get_field_index(name), name, array)
    pq.write_table(table, source)
    output = tmp_path / "dataset"
    with pytest.raises(ValueError, match="physical type"):
        prepare(source, output, type_profile="native", rows=0)
    assert not (output / "manifest.json").exists()


def test_logical_temporal_annotations_preserve_utc_and_exact_microseconds(tmp_path):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    source = source_table(tmp_path / "hits.parquet", rows=1)
    table = pq.read_table(source)
    table = table.set_column(4, "EventTime", pa.array([1373805240123456000],
                                                    type=pa.timestamp("ns", "Europe/Moscow")))
    table = table.set_column(5, "EventDate", pa.array([date(2013, 7, 14)], type=pa.date32()))
    pq.write_table(table, source)
    output = tmp_path / "exact"
    prepare(source, output, type_profile="native")
    row = next(ClickBenchDataset(output).iter_parts())[1][0]
    assert row["EventTime"] == datetime(2013, 7, 14, 12, 34, 0, 123456, tzinfo=timezone.utc)
    assert row["EventDate"] == date(2013, 7, 14)

    table = table.set_column(4, "EventTime", pa.array([1373805240123456001], type=pa.timestamp("ns")))
    pq.write_table(table, source)
    with pytest.raises(ValueError):
        prepare(source, tmp_path / "inexact", type_profile="native")
    assert not (tmp_path / "inexact" / "manifest.json").exists()


def test_prefix_keeps_source_order_across_row_groups_and_part_boundaries(tmp_path):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    source = source_table(tmp_path / "hits.parquet", rows=31)
    table = pq.read_table(source).set_column(0, "WatchID", pa.array(list(range(31)), type=pa.int64()))
    pq.write_table(table, source, row_group_size=4)
    output = tmp_path / "dataset"
    prepare(source, output, type_profile="native", rows=13, part_rows=3)
    assert [row["WatchID"] for _, part in ClickBenchDataset(output).iter_parts() for row in part] == list(range(13))


def test_read_part_validates_ordinal_and_reads_only_the_requested_part(tmp_path):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    output = tmp_path / "dataset"
    prepare(source_table(tmp_path / "hits.parquet", rows=9), output, type_profile="native", part_rows=4)
    data = ClickBenchDataset(output)
    (output / data.manifest["parts"][0]["path"]).unlink()
    assert len(data.read_part(1)) == 4
    assert len(data.read_part(2)) == 1
    for ordinal in (-1, 3, True, "1"):
        with pytest.raises(ValueError, match="ordinal"):
            data.read_part(ordinal)
    with pytest.raises(ValueError, match="part"):
        data.read_part(0)


@pytest.mark.parametrize("change", ["disk", "memory"])
def test_open_dataset_refuses_changed_manifest(tmp_path, change):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare

    output = tmp_path / "dataset"
    prepare(source_table(tmp_path / "hits.parquet", rows=1), output, type_profile="native")
    data = ClickBenchDataset(output)
    if change == "disk":
        manifest = output / "manifest.json"
        manifest.write_bytes(manifest.read_bytes() + b" ")
    else:
        data.manifest["rows"] = 2
    with pytest.raises(ValueError, match="manifest"):
        list(data.iter_parts())


def test_source_changed_during_conversion_is_not_published(tmp_path, monkeypatch):
    import glamdb_workload.clickbench.dataset as dataset

    source = source_table(tmp_path / "hits.parquet", rows=1)
    convert = dataset.convert_row

    def change_source(row, **options):
        result = convert(row, **options)
        with source.open("ab") as stream:
            stream.write(b"changed")
        return result

    monkeypatch.setattr(dataset, "convert_row", change_source)
    output = tmp_path / "dataset"
    with pytest.raises(ValueError, match="source changed"):
        dataset.prepare(source, output, type_profile="native")
    assert not (output / "manifest.json").exists()


def test_impossible_part_count_fails_before_creating_converted_parts(tmp_path, monkeypatch):
    import glamdb_workload.clickbench.dataset as dataset

    monkeypatch.setattr(dataset, "MAX_PARTS", 2)
    output = tmp_path / "dataset"
    with pytest.raises(ValueError, match="part-count"):
        dataset.prepare(source_table(tmp_path / "hits.parquet", rows=9), output,
                        type_profile="native", part_rows=4)
    assert not list(output.glob("part-*.jsonl"))


def test_impossible_converted_byte_budget_fails_before_creating_parts(tmp_path):
    from glamdb_workload.clickbench.dataset import prepare

    output = tmp_path / "dataset"
    with pytest.raises(ValueError, match="converted dataset byte budget"):
        prepare(source_table(tmp_path / "hits.parquet", rows=9), output,
                type_profile="native", part_rows=1, max_converted_bytes=5000)
    assert not list(output.glob("part-*.jsonl"))


def test_part_byte_limit_counts_toward_preflight_part_budget(tmp_path, monkeypatch):
    import glamdb_workload.clickbench.dataset as dataset

    monkeypatch.setattr(dataset, "MAX_PARTS", 2)
    output = tmp_path / "dataset"
    with pytest.raises(ValueError, match="part-count"):
        dataset.prepare(source_table(tmp_path / "hits.parquet", rows=9), output,
                        type_profile="native", part_bytes=5000)
    assert not list(output.glob("part-*.jsonl"))


def test_out_of_calendar_epoch_seconds_are_a_source_validation_error(tmp_path):
    from glamdb_workload.clickbench.dataset import prepare

    source = source_table(tmp_path / "hits.parquet", rows=1)
    table = pq.read_table(source).set_column(4, "EventTime", pa.array([2**63], type=pa.uint64()))
    pq.write_table(table, source)
    output = tmp_path / "dataset"
    with pytest.raises(ValueError, match="timestamp"):
        prepare(source, output, type_profile="native")
    assert not (output / "manifest.json").exists()


def test_source_batch_byte_budget_prevents_python_row_materialization(tmp_path, monkeypatch):
    import glamdb_workload.clickbench.dataset as dataset

    monkeypatch.setattr(dataset, "MAX_CONVERSION_BATCH_BYTES", 500)
    output = tmp_path / "dataset"
    with pytest.raises(ValueError, match="batch.*memory byte budget"):
        dataset.prepare(source_table(tmp_path / "hits.parquet", rows=1), output, type_profile="native")
    assert not list(output.glob("part-*.jsonl"))
