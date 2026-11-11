import copy
import hashlib
import json
from pathlib import Path

import pytest

CONFIG = {
    "schema": {
        "fields": [
            {"name": "key", "type": "string", "nullable": False},
            {"name": "i", "type": "int64", "nullable": False},
            {"name": "x", "type": "float64", "nullable": True},
            {"name": "b", "type": "bool", "nullable": False},
            {"name": "ts", "type": "timestamp_us_utc", "nullable": False},
            {"name": "s", "type": "string", "nullable": True},
        ]
    },
    "generation": {
        "fields": {
            "i": {"min": 9007199254740993, "max": 9007199254741000},
            "x": {"min": -2.5, "max": 2.5, "null_rate": 0.3},
            "ts": {
                "min": "2020-01-01T00:00:00.000001Z",
                "max": "2020-01-01T00:00:00.000009Z",
            },
            "s": {"length": 12, "alphabet": "Привет🎀", "null_rate": 0.4},
        },
        "key": {"profile": "duplicates", "distinct": 3, "prefix": "ключ-"},
    },
}


def test_seed_parts_and_exact_types(tmp_path):
    from glamdb_workload.dataset import Dataset, generate

    a, b = tmp_path / "first dir", tmp_path / "second"
    ma = generate(CONFIG, rows=103, seed=42, output=a, part_rows=11, locale="en")
    mb = generate(CONFIG, rows=103, seed=42, output=b, part_rows=11, locale="en")
    assert ma == mb
    assert ma["rows"] == 103 and ma["versions"]["mimesis"]
    assert len(ma["parts"]) == 10
    parts = list(Dataset(a).iter_parts())
    assert max(map(len, parts)) == 11
    rows = [r for part in parts for r in part]
    assert len(rows) == 103 and len({r["key"] for r in rows}) == 3
    assert all(type(r["i"]) is int and r["i"] > 2**53 for r in rows)
    from glamdb_sdk.codec import timestamp_us

    assert all(timestamp_us(r["ts"]) % 1000000 in range(1, 10) for r in rows)
    assert any(r["x"] is None for r in rows)
    assert any(r["s"] is not None and "🎀" in r["s"] for r in rows)
    for part in ma["parts"]:
        assert (
            hashlib.sha256((a / part["path"]).read_bytes()).hexdigest()
            == part["sha256"]
        )


@pytest.mark.parametrize(
    "change",
    [
        {"x": {"min": float("nan")}},
        {"i": {"max": 2**63}},
        {"b": {"null_rate": 0.1}},
        {"s": {"length": -1}},
        {"absent": {}},
    ],
)
def test_invalid_profiles_are_rejected(tmp_path, change):
    from glamdb_workload.dataset import generate

    config = copy.deepcopy(CONFIG)
    config["generation"]["fields"].update(change)
    with pytest.raises(ValueError):
        generate(config, rows=1, seed=1, output=tmp_path / "out")


def test_corruption_and_path_escape_rejected(tmp_path):
    from glamdb_workload.dataset import Dataset, generate

    out = tmp_path / "out"
    m = generate(CONFIG, rows=2, seed=1, output=out)
    path = out / m["parts"][0]["path"]
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash"):
        list(Dataset(out).iter_parts())
    m["parts"][0]["path"] = "../outside.jsonl"
    (out / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(ValueError, match="path"):
        Dataset(out)


def test_zero_rows_and_no_overwrite(tmp_path):
    from glamdb_workload.dataset import Dataset, generate

    out = tmp_path / "out"
    assert generate(CONFIG, rows=0, seed=1, output=out)["parts"] == []
    assert list(Dataset(out).iter_parts()) == []
    with pytest.raises((ValueError, FileExistsError)):
        generate(CONFIG, rows=1, seed=1, output=out)


def test_generation_does_not_materialize_the_rowset(tmp_path):
    import tracemalloc
    from glamdb_workload.dataset import generate

    config = {
        "schema": {
            "fields": [
                {"name": "key", "type": "string", "nullable": False},
                {"name": "text", "type": "string", "nullable": False},
            ]
        },
        "generation": {"fields": {"text": {"length": 1024}}},
    }
    tracemalloc.start()
    try:
        manifest = generate(
            config,
            rows=10000,
            seed=42,
            output=tmp_path / "bounded",
            part_rows=97,
            part_bytes=128 * 1024,
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert sum(part["bytes"] for part in manifest["parts"]) > 10 * 1024**2
    # Includes Mimesis startup allocations; the row data alone exceeds 10 MiB.
    assert peak < 8 * 1024**2, f"generator materialized rows: peak={peak}"
