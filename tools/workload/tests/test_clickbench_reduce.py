from contextlib import contextmanager
import importlib.util

import pyarrow as pa
import pytest


class ArrowSource:
    def __init__(self, rows):
        self.table = pa.Table.from_pylist(rows)
        self.scans = 0
        self.closed = 0

    @contextmanager
    def scan_batches(self, collection, **kwargs):
        self.scans += 1
        projection = kwargs.get("projection", self.table.column_names)
        try:
            yield iter(self.table.select(projection).to_batches(max_chunksize=1))
        finally:
            self.closed += 1


def execute(client, sql, tmp_path, reference, *, mode="reduce", **kwargs):
    assert importlib.util.find_spec("glamdb_workload.clickbench.reduce"), "streamed ClickBench reducer is missing"
    from glamdb_workload.clickbench.reduce import execute_query
    from glamdb_workload.clickbench.queries import plan_query

    return execute_query(client, "11111111-2222-4333-8444-555555555555",
                         plan_query(sql, 18, snapshot="1"), mode=mode, workspace=tmp_path,
                         oracle_factory=lambda: reference.to_reader(max_chunksize=1), **kwargs)


def test_unordered_and_ordered_boundary_ties_accept_legal_different_rows(tmp_path):
    rows = [{"UserID": 2, "SearchPhrase": "b", "EventTime": 1},
            {"UserID": 1, "SearchPhrase": "a", "EventTime": 1},
            {"UserID": 3, "SearchPhrase": "c", "EventTime": 2}]
    client = ArrowSource(rows)
    reference = pa.Table.from_pylist(list(reversed(rows)))
    for sql in ("SELECT UserID, COUNT(*) FROM hits GROUP BY UserID LIMIT 1",
                "SELECT SearchPhrase FROM hits ORDER BY EventTime LIMIT 1"):
        assert execute(client, sql, tmp_path, reference)["status"] == "PASS"
    assert client.scans == 2 and client.closed == 2


def test_incorrect_result_and_float_tolerance(tmp_path):
    client = ArrowSource([{"UserID": 2}, {"UserID": 4}])
    good = pa.table({"UserID": [4, 2]})
    bad = pa.table({"UserID": [2, 8]})
    assert execute(client, "SELECT AVG(UserID) FROM hits", tmp_path, good)["status"] == "PASS"
    assert execute(client, "SELECT AVG(UserID) FROM hits", tmp_path, bad)["status"] == "FAIL"


def test_byte_length_and_every_input_batch_are_consumed_for_limit(tmp_path):
    client = ArrowSource([{"URL": "💗"}, {"URL": "a"}, {"URL": "аб"}])
    reference = client.table
    result = execute(client, "SELECT AVG(STRLEN(URL)) FROM hits", tmp_path, reference)
    assert result["status"] == "PASS" and result["sample"][0][0] == pytest.approx(3.0)
    result = execute(client, "SELECT URL FROM hits LIMIT 1", tmp_path, reference)
    assert result["candidate_rows"] == 3 and result["batches"] == 3


def test_scan_is_distinct_from_sql_and_budget_failure_closes_reader(tmp_path):
    client = ArrowSource([{"UserID": 2}, {"UserID": 4}])
    result = execute(client, "SELECT UserID FROM hits", tmp_path, client.table, mode="scan")
    assert result["status"] == "PASS" and result["sql_validation"] == "NOT_APPLICABLE"
    assert result["candidate_rows"] == 2
    with pytest.raises(ValueError, match="budget"):
        execute(client, "SELECT UserID FROM hits", tmp_path, client.table, max_candidate_bytes=1)
    assert client.closed == 2
