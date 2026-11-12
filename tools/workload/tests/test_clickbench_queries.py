import importlib.util

import pytest


def planner():
    assert importlib.util.find_spec("glamdb_workload.clickbench.queries"), "ClickBench query planner is missing"
    from glamdb_workload.clickbench.queries import plan_query
    return plan_query


def test_projection_is_complete_and_deduplicated_without_early_limit():
    plan = planner()("SELECT SUM(ResolutionWidth), SUM(ResolutionWidth+1) FROM hits", 30, snapshot="7")
    assert plan.projection == ("ResolutionWidth",)
    assert "limit" not in plan.scan
    wide = planner()("SELECT * FROM hits WHERE URL LIKE '%sample%' ORDER BY EventTime LIMIT 10", 24, snapshot="7")
    assert len(wide.projection) == 105 and wide.projection[-1] == "CLID"
    assert wide.predicate is None
    assert not wide.native_complete


def test_exact_int64_and_date_literals_are_typed_by_schema():
    plan = planner()("SELECT UserID FROM hits WHERE UserID = 435090932899640449", 20, snapshot="7")
    assert plan.predicate == {"op": "eq", "field": "UserID", "value": "435090932899640449"}
    assert plan.native_complete and plan.scan["snapshot_id"] == "7"
    date = planner()("SELECT URL FROM hits WHERE EventDate >= '2013-07-01' AND CounterID=62", 37, snapshot="8")
    assert date.predicate["op"] == "and"
    assert date.predicate["args"][0]["value"].startswith("2013-07-01T00:00:00")
    native = planner()("SELECT URL FROM hits WHERE EventDate >= '2013-07-01' AND CounterID=62",
                       37, snapshot="8", type_profile="native")
    assert native.predicate["args"][0]["value"] == {"type": "date", "value": "2013-07-01"}
    assert native.native_complete


def test_partial_and_is_safe_but_partial_or_and_not_are_not_pushed():
    make = planner()
    both = make("SELECT URL FROM hits WHERE CounterID=62 AND URL LIKE '%x%'", 22, snapshot="1")
    assert both.predicate == {"op": "eq", "field": "CounterID", "value": "62"}
    assert both.projection == ("CounterID", "URL")
    for where in ("CounterID=62 OR URL LIKE '%x%'", "NOT (CounterID=62 AND URL LIKE '%x%')"):
        assert make("SELECT URL FROM hits WHERE "+where, 22, snapshot="1").predicate is None
    full = make("SELECT URL FROM hits WHERE NOT(CounterID=62 OR CounterID IN (-1,6))", 22, snapshot="1")
    assert full.predicate["op"] == "not" and full.predicate["arg"]["op"] == "or"


def test_aliases_do_not_become_scan_columns_and_residual_sql_keeps_byte_length():
    plan = planner()("SELECT CounterID, AVG(STRLEN(URL)) AS l FROM hits GROUP BY CounterID ORDER BY l DESC LIMIT 25",
                     28, snapshot="1")
    assert plan.projection == ("CounterID", "URL")
    assert "STRLEN" in plan.sql.upper()
    assert "AVG" in plan.validation_sql.upper()
    assert plan.order_columns == 1 and plan.validation_policy == "ordered-with-legal-ties"
    unordered = planner()("SELECT UserID, COUNT(*) FROM hits GROUP BY UserID LIMIT 10", 18, snapshot="1")
    assert unordered.validation_policy == "unordered-subset"


def test_invalid_queries_and_unsafe_literals_are_rejected_or_left_residual():
    make = planner()
    for sql in ("DELETE FROM hits", "SELECT * FROM read_parquet('/private')", "SELECT * FROM other",
                "SELECT UnknownColumn FROM hits", "SELECT * FROM hits; SELECT * FROM hits"):
        with pytest.raises(ValueError):
            make(sql, 1, snapshot="1")
    unsupported = make("SELECT UserID FROM hits WHERE UserID = 9223372036854775808", 20, snapshot="1")
    assert unsupported.predicate is None
