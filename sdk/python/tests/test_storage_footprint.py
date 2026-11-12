import httpx
import pytest

from glamdb_sdk import Client, ProtocolError

STORE = "11111111-2222-4333-8444-555555555555"


def fixture(**changes):
    return {"version": 1, "store_id": STORE, "backend": "local", "mock": False,
            "consistency": "writer_quiescent", "database_file_bytes": "12",
            "database_allocated_bytes": "8192", "data_file_bytes": "5",
            "iceberg_metadata_file_bytes": "2", "catalog_file_bytes": "5",
            "other_file_bytes": "0", "shared_file_bytes": "0",
            "committed_data_file_bytes": "5", "referenced_metadata_file_bytes": "2",
            "unreferenced_warehouse_file_bytes": "0", "files": "3", "entries": "6", "hardlink_paths": "0", **changes}


def test_storage_footprint_is_scoped_and_identity_checked():
    calls = []
    def handle(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=fixture())
    with Client("https://example.test", "protocol-test-only", database_id=STORE,
                transport=httpx.MockTransport(handle)) as client:
        assert hasattr(client, "storage_footprint"), "SDK storage footprint method is missing"
        assert client.storage_footprint(store_id=STORE)["database_file_bytes"] == "12"
    assert calls == [f"/databases/{STORE}/storage"]


@pytest.mark.parametrize("changes", [
    {"store_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"}, {"mock": True},
    {"database_file_bytes": "-1"}, {"database_file_bytes": 12},
])
def test_storage_footprint_rejects_wrong_identity_and_inexact_values(changes):
    with Client("https://example.test", "protocol-test-only", database_id=STORE,
                transport=httpx.MockTransport(lambda request: httpx.Response(200, json=fixture(**changes)))) as client:
        assert hasattr(client, "storage_footprint"), "SDK storage footprint method is missing"
        with pytest.raises(ProtocolError):
            client.storage_footprint(store_id=STORE)
