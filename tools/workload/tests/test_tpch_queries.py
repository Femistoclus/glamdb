"""Independent expected literals and SQL semantics for the TPC-H scan plans."""

from datetime import date
from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace

import pyarrow as pa
import pytest
from glamdb_sdk import Field, Schema

from glamdb_workload import tpch_queries as queries


def stage(plan, alias):
    return next(item for item in plan["stages"] if item["alias"] == alias)


def leaves(predicate):
    if predicate["op"] in {"and", "or"}:
        return [p for arg in predicate["args"] for p in leaves(arg)]
    return [predicate]


def test_all_plans_are_explicit_source_scans_without_sql_top_n():
    plans = queries.query_plans()
    assert [p["query_id"] for p in plans] == list(range(1, 23))
    for plan in plans:
        assert plan["version"] == 1
        assert plan["spec_version"] == "3.0.1"
        assert plan["parameter_policy"] == "fixed-qualification"
        assert plan["stages"]
        assert plan["residual"]
        assert all(s["projection"] and "limit" not in s for s in plan["stages"])
        assert len({s["alias"] for s in plan["stages"]}) == len(plan["stages"])
        assert plan["reduction_supported"] == (plan["query_id"] in {1, 6})
    assert stage(plans[6], "n1")["predicate"] == {
        "op": "in", "field": "n_name", "values": ["FRANCE", "GERMANY"]
    }


def test_q6_legacy_literal_and_projection_contract():
    plan = queries.query_plan(6, profile="legacy-scaled")
    scan = plan["stages"][0]
    assert scan["projection"] == ["l_extendedprice", "l_discount"]
    assert scan["predicate"] == {
        "op": "and", "args": [
            {"op": "ge", "field": "l_shipdate", "value": "1994-01-01T00:00:00.000000Z"},
            {"op": "lt", "field": "l_shipdate", "value": "1995-01-01T00:00:00.000000Z"},
            {"op": "ge", "field": "l_discount", "value": "5"},
            {"op": "le", "field": "l_discount", "value": "7"},
            {"op": "lt", "field": "l_quantity", "value": "2400"},
        ]
    }


def test_native_predicates_have_typed_exact_decimal_and_date_literals():
    predicates = leaves(queries.query_plan(6)["stages"][0]["predicate"])
    assert predicates[0]["value"] == {"type": "date", "value": "1994-01-01"}
    assert predicates[2]["value"] == {
        "type": "decimal", "precision": 15, "scale": 2, "value": "0.05"
    }
    assert predicates[4]["value"]["value"] == "24.00"
    assert queries.query_plan(1)["stages"][0]["predicate"]["value"] == {
        "type": "date", "value": "1998-09-02"
    }


@pytest.mark.parametrize("sf,want", [("0.01", Fraction(1, 100)), ("10", Fraction(1, 100000)), ("3", Fraction(1, 30000))])
def test_q11_fraction_scales_exactly_even_for_nonterminating_decimal(sf, want):
    parameter = queries.query_plan(11, scale_factor=sf)["parameters"]["FRACTION"]
    assert Fraction(int(parameter["numerator"]), int(parameter["denominator"])) == want
    assert parameter["formula"] == "0.0001 / SF"


def test_outer_join_and_conditional_ratio_keep_required_candidate_rows():
    q13 = queries.query_plan(13)
    assert all(s["predicate"] is None for s in q13["stages"])
    assert stage(q13, "o")["projection"] == ["o_orderkey", "o_custkey", "o_comment"]
    assert any("LEFT OUTER JOIN" in text and "ON" in text for text in q13["residual"])
    q14 = queries.query_plan(14)
    assert stage(q14, "p")["predicate"] is None
    assert "p_type" in stage(q14, "p")["projection"]
    assert any("denominator" in text for text in q14["residual"])


def test_q19_part_predicate_preserves_brand_container_size_coupling():
    plan = queries.query_plan(19, profile="legacy-scaled")
    part = stage(plan, "p")["predicate"]
    assert part["op"] == "or"
    assert len(part["args"]) == 3
    first = leaves(part["args"][0])
    assert {"op": "eq", "field": "p_brand", "value": "Brand#12"} in first
    assert {"op": "le", "field": "p_size", "value": "5"} in first
    assert {"op": "in", "field": "p_container", "values": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]} in first
    middle = leaves(part["args"][1])
    assert {"op": "in", "field": "p_container", "values": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]} in middle
    assert any("coupled" in text and "l_quantity" in text for text in plan["residual"])
    assert "l_quantity" in stage(plan, "l")["projection"]


def test_reused_sources_have_their_own_correlated_aliases_and_inputs():
    assert {s["alias"] for s in queries.query_plan(21)["stages"]} >= {"l1", "l2", "l3"}
    assert stage(queries.query_plan(21), "l2")["projection"] == ["l_orderkey", "l_suppkey"]
    assert stage(queries.query_plan(22), "c_avg")["predicate"] == {
        "op": "gt", "field": "c_acctbal",
        "value": {"type": "decimal", "precision": 15, "scale": 2, "value": "0.00"}
    }
    assert stage(queries.query_plan(11), "ps_total")["projection"] == ["ps_suppkey", "ps_supplycost", "ps_availqty"]


@pytest.mark.parametrize("number,alias,field,op,value", [
    (2, "p", "p_size", "eq", "15"),
    (3, "c", "c_mktsegment", "eq", "BUILDING"),
    (3, "o", "o_orderdate", "lt", "1995-03-15T00:00:00.000000Z"),
    (4, "o", "o_orderdate", "lt", "1993-10-01T00:00:00.000000Z"),
    (5, "r", "r_name", "eq", "ASIA"),
    (7, "l", "l_shipdate", "le", "1996-12-31T00:00:00.000000Z"),
    (8, "p", "p_type", "eq", "ECONOMY ANODIZED STEEL"),
    (8, "r", "r_name", "eq", "AMERICA"),
    (10, "l", "l_returnflag", "eq", "R"),
    (10, "o", "o_orderdate", "lt", "1994-01-01T00:00:00.000000Z"),
    (11, "n", "n_name", "eq", "GERMANY"),
    (12, "l", "l_receiptdate", "lt", "1995-01-01T00:00:00.000000Z"),
    (14, "l", "l_shipdate", "lt", "1995-10-01T00:00:00.000000Z"),
    (15, "l", "l_shipdate", "lt", "1996-04-01T00:00:00.000000Z"),
    (16, "p", "p_brand", "ne", "Brand#45"),
    (17, "p", "p_brand", "eq", "Brand#23"),
    (17, "p", "p_container", "eq", "MED BOX"),
    (20, "n", "n_name", "eq", "CANADA"),
    (20, "l_sum", "l_shipdate", "lt", "1995-01-01T00:00:00.000000Z"),
    (21, "o", "o_orderstatus", "eq", "F"),
    (21, "n", "n_name", "eq", "SAUDI ARABIA"),
])
def test_qualification_filters_use_independently_expected_wire_literals(number, alias, field, op, value):
    plan = queries.query_plan(number, profile="legacy-scaled")
    assert {"op": op, "field": field, "value": value} in leaves(stage(plan, alias)["predicate"])


def test_unsupported_reductions_and_bad_query_selections_fail_explicitly():
    with pytest.raises(ValueError, match=r"unsupported reductions:.*2.*22"):
        queries.query_plans("all", mode="reduce")
    assert [p["query_id"] for p in queries.query_plans("6,1", mode="reduce")] == [6, 1]
    for value in ("", "0", "23", "1,1", "1,,6", "1.0"):
        with pytest.raises(ValueError):
            queries.query_plans(value)


def native_rows():
    return [
        dict(l_returnflag="N", l_linestatus="O", l_quantity=Decimal("10.00"), l_extendedprice=Decimal("100.01"), l_discount=Decimal("0.05"), l_tax=Decimal("0.02"), l_shipdate=date(1994, 1, 1)),
        dict(l_returnflag="N", l_linestatus="O", l_quantity=Decimal("20.00"), l_extendedprice=Decimal("200.03"), l_discount=Decimal("0.07"), l_tax=Decimal("0.04"), l_shipdate=date(1994, 12, 31)),
        dict(l_returnflag="A", l_linestatus="F", l_quantity=Decimal("24.00"), l_extendedprice=Decimal("50.00"), l_discount=Decimal("0.06"), l_tax=Decimal("0.00"), l_shipdate=date(1994, 8, 1)),
        dict(l_returnflag="R", l_linestatus="F", l_quantity=Decimal("1.00"), l_extendedprice=Decimal("9999999999999.99"), l_discount=Decimal("0.01"), l_tax=Decimal("0.08"), l_shipdate=date(1998, 9, 3)),
    ]


@pytest.mark.parametrize("profile", ["native", "legacy-scaled"])
def test_q1_exact_streamed_sums_averages_and_order(profile):
    rows = native_rows()[:3]
    if profile == "legacy-scaled":
        rows = [{k: int(v * 100) if isinstance(v, Decimal) else v for k, v in row.items()} for row in rows]
    reducer = queries.make_reducer(1, profile)
    reducer.add_rows(rows[:1])
    reducer.add_rows(rows[1:])
    assert reducer.finish() == [
        dict(l_returnflag="A", l_linestatus="F", sum_qty="24.00", sum_base_price="50.00", sum_disc_price="47.00", sum_charge="47.00", avg_qty="24.00", avg_price="50.00", avg_disc="0.06", count_order=1),
        dict(l_returnflag="N", l_linestatus="O", sum_qty="30.00", sum_base_price="300.04", sum_disc_price="281.04", sum_charge="290.38", avg_qty="15.00", avg_price="150.02", avg_disc="0.06", count_order=2),
    ]


def test_q6_uses_wide_accumulation_and_rounds_only_at_result_boundary():
    reducer = queries.make_reducer(6, "legacy-scaled")
    reducer.add_rows([{"l_extendedprice": 999999999999999, "l_discount": 7}] * 100)
    assert reducer.finish() == {"revenue": "69999999999999.93"}
    tiny = queries.make_reducer(6, "native")
    tiny.add_batch(pa.record_batch({"l_extendedprice": [Decimal("0.10")] * 2, "l_discount": [Decimal("0.05")] * 2}))
    assert tiny.finish() == {"revenue": "0.01"}


def test_sql_empty_input_and_independent_null_aggregates():
    assert queries.make_reducer(1, "native").finish() == []
    assert queries.make_reducer(6, "native").finish() == {"revenue": None}
    reducer = queries.make_reducer(1, "native")
    reducer.add_rows([dict(l_returnflag=None, l_linestatus="O", l_quantity=None, l_extendedprice=Decimal("1.00"), l_discount=None, l_tax=Decimal("0.00"))])
    assert reducer.finish() == [dict(l_returnflag=None, l_linestatus="O", sum_qty=None, sum_base_price="1.00", sum_disc_price=None, sum_charge=None, avg_qty=None, avg_price="1.00", avg_disc=None, count_order=1)]


def test_reference_evaluates_original_source_with_independent_where_and_sql_aggregation(monkeypatch):
    dataset = SimpleNamespace(profile="native", tables={"lineitem": SimpleNamespace(iter_parts=lambda: iter([native_rows()]))})
    # A reference that accidentally invokes the production reducer must fail.
    monkeypatch.setattr(queries, "make_reducer", lambda *args: pytest.fail("reference reused reducer"))
    results = queries.reference_results(dataset, queries.query_plans("1,6", mode="reduce"))
    assert results["6"] == {"revenue": "19.00"}
    assert [r["l_returnflag"] for r in results["1"]] == ["A", "N"]
    assert results["1"][1]["sum_charge"] == "290.38"


@pytest.mark.parametrize("profile", ["native", "legacy-scaled"])
def test_independent_candidate_counts_use_exact_source_dates_decimals_and_inclusive_bounds(profile):
    rows = native_rows()
    if profile == "native":
        schema = Schema([Field("l_shipdate", "date"), Field("l_discount", "decimal", precision=15, scale=2), Field("l_quantity", "decimal", precision=15, scale=2)])
    else:
        epoch = date(1970, 1, 1)
        rows = [{k: int(v * 100) if isinstance(v, Decimal) else (v - epoch).days * 86400000000 if isinstance(v, date) else v for k, v in row.items()} for row in rows]
        schema = Schema([Field("l_shipdate", "timestamp_us_utc"), Field("l_discount", "int64"), Field("l_quantity", "int64")])
    source = SimpleNamespace(schema=schema, iter_parts=lambda: iter([rows[:2], rows[2:]]))
    dataset = SimpleNamespace(profile=profile, tables={"lineitem": source})
    assert queries.reference_candidates(dataset, queries.query_plans("1,6", profile=profile)) == {"1": {"l": 3}, "6": {"l": 2}}


def test_candidate_reference_uses_sql_three_valued_not_in_null_and_empty_in():
    rows = [{"n": None}, {"n": 1}, {"n": 2}, {"n": 3}]
    source = SimpleNamespace(schema=Schema([Field("n", "int64", nullable=True)]), iter_parts=lambda: iter([rows]))
    predicates = {
        "true": None,
        "unknown_eq": {"op": "eq", "field": "n", "value": None},
        "not_unknown": {"op": "not", "arg": {"op": "eq", "field": "n", "value": None}},
        "in_with_null": {"op": "in", "field": "n", "values": ["1", None]},
        "not_in_with_null": {"op": "not", "arg": {"op": "in", "field": "n", "values": ["1", None]}},
        "null_or_eq": {"op": "or", "args": [{"op": "is_null", "field": "n"}, {"op": "eq", "field": "n", "value": "2"}]},
        "unknown_or_true": {"op": "or", "args": [{"op": "eq", "field": "n", "value": None}, {"op": "eq", "field": "n", "value": "1"}]},
        "unknown_and_false": {"op": "and", "args": [{"op": "eq", "field": "n", "value": None}, {"op": "eq", "field": "n", "value": "1"}]},
        "not_null": {"op": "is_not_null", "field": "n"},
        "empty_in": {"op": "in", "field": "n", "values": []},
        "not_empty_in": {"op": "not", "arg": {"op": "in", "field": "n", "values": []}},
    }
    plan = {"query_id": 1, "stages": [{"table": "test", "stage_id": name, "predicate": predicate} for name, predicate in predicates.items()]}
    assert queries.reference_candidates(SimpleNamespace(tables={"test": source}), [plan]) == {"1": {"true": 4, "unknown_eq": 0, "not_unknown": 0, "in_with_null": 1, "not_in_with_null": 0, "null_or_eq": 2, "unknown_or_true": 1, "unknown_and_false": 0, "not_null": 3, "empty_in": 0, "not_empty_in": 3}}


def test_candidate_reference_checks_q19_medium_bags_and_coupled_part_branches():
    source = SimpleNamespace(schema=Schema([Field("p_brand", "string"), Field("p_container", "string"), Field("p_size", "int64")]), iter_parts=lambda: iter([[dict(p_brand="Brand#23", p_container="MED BAG", p_size=10), dict(p_brand="Brand#12", p_container="SM BOX", p_size=5), dict(p_brand="Brand#23", p_container="SM BOX", p_size=5), dict(p_brand="Brand#12", p_container="SM BOX", p_size=6)]]))
    plan = queries.query_plan(19)
    plan["stages"] = [stage(plan, "p")]
    assert queries.reference_candidates(SimpleNamespace(tables={"part": source}), [plan]) == {"19": {"p": 2}}
