from types import SimpleNamespace
from uuid import uuid4

import pytest


class MetadataDouble:
    """Literal metadata protocol fixture, not a physical storage measurement."""

    def __init__(self):
        self.calls = []

    def stats(self, collection, *, snapshot_id, **kwargs):
        assert snapshot_id == "7"
        return {"rows": "10", "data_files": "2", "compressed_data_bytes": "300",
                "metric_kinds": {"rows": "metadata", "compressed_data_bytes": "metadata"}}

    def files(self, collection, *, snapshot_id, cursor=None, **kwargs):
        self.calls.append((snapshot_id, cursor))
        return SimpleNamespace(items=({"file_id": "a" if cursor is None else "b",
                                       "compressed_bytes": "100" if cursor is None else "200",
                                       "record_count": "4" if cursor is None else "6"},),
                               next_cursor="page2" if cursor is None else None)

    def snapshots(self, collection, **kwargs):
        return SimpleNamespace(items=({"snapshot_id": "7"},), next_cursor=None)

    def file(self, collection, file_id, **kwargs):
        return {"row_groups": [{"compressed_bytes": "50", "uncompressed_bytes": "200"}]}


def test_parquet_sizes_are_distinct_from_unavailable_database_total():
    from glamdb_workload.clickbench.measure import collect_sizes

    client = MetadataDouble()
    state = {"collection_id": str(uuid4()), "snapshot_id": "7", "performance_eligible": True}
    result = collect_sizes(client, state, include_footers=True)
    assert result["parquet_file_bytes"] == 300
    assert result["database_file_bytes"] is None
    assert result["parquet_bytes_per_row"] == 30
    assert result["column_chunk_compression_ratio"] == 4
    assert client.calls == [("7", None), ("7", "page2")]
    assert result["unavailable"]["database_file_bytes"]


def test_repeated_metadata_cursor_and_membership_mismatch_are_errors():
    from glamdb_workload.clickbench.measure import collect_sizes

    client = MetadataDouble()
    client.files = lambda *args, **kwargs: SimpleNamespace(items=(), next_cursor="same")
    state = {"collection_id": str(uuid4()), "snapshot_id": "7", "performance_eligible": True}
    with pytest.raises(ValueError):
        collect_sizes(client, state)
    client = MetadataDouble()
    client.stats = lambda *args, **kwargs: {"rows": "11", "data_files": "2", "compressed_data_bytes": "300"}
    with pytest.raises(ValueError):
        collect_sizes(client, state)


def test_owned_filesystem_accounting_is_bounded_and_deduplicates_hardlinks(tmp_path):
    from glamdb_workload.clickbench.measure import filesystem_footprint

    import os

    root = tmp_path / str(uuid4())
    data = root / "warehouse" / "collection" / "data"
    metadata = root / "warehouse" / "collection" / "metadata"
    data.mkdir(parents=True)
    metadata.mkdir()
    (data / "a.parquet").write_bytes(b"x" * 100)
    os.link(data / "a.parquet", data / "same.parquet")
    (metadata / "v1.json").write_bytes(b"{}")
    (root / "catalog.sqlite").write_bytes(b"c" * 25)
    result = filesystem_footprint(root, expected_store_id=root.name)
    assert result["database_file_bytes"] == 127
    assert result["warehouse_data_file_bytes"] == 100
    assert result["iceberg_metadata_file_bytes"] == 2
    assert result["catalog_file_bytes"] == 25
    assert result["hardlink_paths"] == 1
    assert result["consistency"] != "atomic"
    with pytest.raises(ValueError):
        filesystem_footprint(root, expected_store_id=root.name, max_entries=1)


def test_symlink_and_wrong_store_identity_are_not_followed(tmp_path):
    from glamdb_workload.clickbench.measure import filesystem_footprint

    root = tmp_path / str(uuid4())
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_bytes(b"private")
    (root / "link").symlink_to(outside)
    with pytest.raises(ValueError):
        filesystem_footprint(root, expected_store_id=root.name)
    with pytest.raises(ValueError):
        filesystem_footprint(root, expected_store_id=str(uuid4()))
