import pytest
from uuid import uuid4
from apps.workbench_access.authorization import classify


def test_scan_is_read_and_append_is_write_and_unknown_is_denied():
    cid = str(uuid4())
    op = str(uuid4())
    assert classify("POST", f"/collections/{cid}/scan").action == "read"
    assert classify("POST", f"/collections/{cid}/append").action == "write"
    assert (
        classify("GET", f"/collections/{cid}/operations/{op}").action == "metadata"
    )
    assert classify("GET", "/collections:resolve").action == "metadata"
    assert classify("GET", f"/collections/{cid}/stats").metric == "statistics"
    for path in [
        "/../admin",
        "/collections/%2e%2e/scan",
        f"/collections/{cid}/scan/",
        f"/collections/{cid.upper()}/scan",
        "/future",
    ]:
        with pytest.raises(ValueError):
            classify("POST", path)
    with pytest.raises(ValueError):
        classify("DELETE", f"/collections/{cid}")


@pytest.mark.parametrize("suffix", ["scan/open", "scan/cancel"])
def test_native_scan_session_routes_use_read_policy(suffix):
    route = classify("POST", f"/collections/{uuid4()}/{suffix}")
    assert route.action == "read" and route.metric == "scan" and route.kind is None


def test_native_file_hash_is_metadata_but_arbitrary_paths_remain_denied():
    collection = uuid4()
    assert classify("GET", f"/collections/{collection}/files/{'a' * 64}").action == "metadata"
    for file_id in ["a" * 63, "g" * 64, "a/b", "../secret"]:
        with pytest.raises(ValueError):
            classify("GET", f"/collections/{collection}/files/{file_id}")


def test_write_settings_uses_database_write_policy_without_append_identity():
    route = classify("POST", f"/collections/{uuid4()}/write-settings")
    assert route.action == "write" and route.metric == "collection"
    assert route.kind is None


def test_storage_inventory_uses_metadata_policy_and_rejects_other_methods():
    route = classify("GET", "/storage")
    assert route.action == "metadata" and route.metric == "storage" and route.kind is None
    for method in ("POST", "DELETE"):
        with pytest.raises(ValueError):
            classify(method, "/storage")
