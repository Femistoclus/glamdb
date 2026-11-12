"""Logical constraints have independent hand-declared positive and negative inputs."""

from types import SimpleNamespace
import sqlite3

import pytest

from glamdb_workload.tpch_verify import verify_source


def source(**replace):
    rows = {
        'region': [{'r_regionkey': 0}],
        'nation': [{'n_nationkey': 1, 'n_regionkey': 0}],
        'supplier': [{'s_suppkey': 2, 's_nationkey': 1}],
        'customer': [{'c_custkey': 3, 'c_nationkey': 1}],
        'part': [{'p_partkey': 4}],
        'partsupp': [{'ps_partkey': 4, 'ps_suppkey': 2}],
        'orders': [{'o_orderkey': 65, 'o_custkey': 3}],
        'lineitem': [{'l_orderkey': 65, 'l_linenumber': 1, 'l_partkey': 4, 'l_suppkey': 2}],
    } | replace
    return SimpleNamespace(validate=lambda: None, tables={
        name: SimpleNamespace(manifest={'rows': len(values)}, iter_parts=lambda values=values: iter([values]))
        for name, values in rows.items()})


def test_full_verification_checks_sparse_keys_and_composite_reference(tmp_path):
    result = verify_source(source(), tmp_path)
    assert result == {'status': 'PASS', 'level': 'full', 'logical_keys': True,
                      'foreign_keys': True, 'rows': 8}


def test_duplicate_partsupp_bridge_is_invalid_even_when_full_rows_differ(tmp_path):
    data = source(partsupp=[{'ps_partkey': 4, 'ps_suppkey': 2, 'ps_availqty': 9},
                            {'ps_partkey': 4, 'ps_suppkey': 2, 'ps_availqty': 17}])
    with pytest.raises(ValueError, match='partsupp.*duplicate'):
        verify_source(data, tmp_path)


def test_lineitem_must_reference_a_part_supplier_pair_not_only_separate_ids(tmp_path):
    data = source(supplier=[{'s_suppkey': 2, 's_nationkey': 1}, {'s_suppkey': 7, 's_nationkey': 1}],
                  lineitem=[{'l_orderkey': 65, 'l_linenumber': 1, 'l_partkey': 4, 'l_suppkey': 7}])
    with pytest.raises(ValueError, match='lineitem.*partsupp'):
        verify_source(data, tmp_path)


def test_orphan_customer_reference_is_rejected(tmp_path):
    with pytest.raises(ValueError, match='orders.*customer'):
        verify_source(source(orders=[{'o_orderkey': 65, 'o_custkey': 999}]), tmp_path)


def test_multiset_oracle_detects_missing_duplicate_even_when_row_totals_match(tmp_path):
    from glamdb_sdk import Field, Schema
    from glamdb_workload.tpch_verify import compare_multiset
    schema = Schema([Field('id', 'int64')])
    with sqlite3.connect(tmp_path / 'rows.sqlite') as db:
        result = compare_multiset(schema, [[{'id': 1}, {'id': 1}, {'id': 2}]],
                                  [[{'id': 1}], [{'id': 2}, {'id': 2}]], db)
    assert result == {'expected_rows': 3, 'actual_rows': 3, 'missing_rows': 1,
                      'extra_rows': 1, 'status': 'FAIL'}


def test_multiset_oracle_accepts_unordered_pages_and_releases_scratch(tmp_path):
    from glamdb_sdk import Field, Schema
    from glamdb_workload.tpch_verify import compare_multiset
    schema = Schema([Field('id', 'int64')])
    with sqlite3.connect(tmp_path / 'rows.sqlite') as db:
        first = compare_multiset(schema, [[{'id': 1}, {'id': 1}, {'id': 2}]],
                                 [[{'id': 2}], [{'id': 1}, {'id': 1}]], db)
        second = compare_multiset(schema, [[{'id': 9}]], [[{'id': 9}]], db)
    assert first['status'] == second['status'] == 'PASS'
    assert second['expected_rows'] == second['actual_rows'] == 1
