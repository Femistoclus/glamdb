"""Independent source fixtures exercise exact TPC-H conversion and publication."""

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import io
from itertools import chain, zip_longest
import json
import shutil

import pytest

from glamdb_workload.tpch_dataset import _generator_provenance as source_provenance


def test_schema_and_native_conversion_keep_all_source_fields():
    from glamdb_sdk.codec import decode_rows
    from glamdb_workload.tpch_schema import TABLES, convert_row, table_schema

    names = {
        "region": "r_regionkey r_name r_comment",
        "nation": "n_nationkey n_name n_regionkey n_comment",
        "supplier": "s_suppkey s_name s_address s_nationkey s_phone s_acctbal s_comment",
        "customer": "c_custkey c_name c_address c_nationkey c_phone c_acctbal c_mktsegment c_comment",
        "part": "p_partkey p_name p_mfgr p_brand p_type p_size p_container p_retailprice p_comment",
        "partsupp": "ps_partkey ps_suppkey ps_availqty ps_supplycost ps_comment",
        "orders": "o_orderkey o_custkey o_orderstatus o_totalprice o_orderdate o_orderpriority o_clerk o_shippriority o_comment",
        "lineitem": "l_orderkey l_partkey l_suppkey l_linenumber l_quantity l_extendedprice l_discount l_tax l_returnflag l_linestatus l_shipdate l_commitdate l_receiptdate l_shipinstruct l_shipmode l_comment",
    }
    assert list(TABLES) == list(names)
    assert sum(len(table_schema(t, "native").fields) for t in TABLES) == 61
    for table, columns in names.items():
        fields = table_schema(table, "native").fields
        assert [f.name for f in fields] == columns.split()
        assert [f.field_id for f in fields] == list(range(1, len(fields) + 1))
        assert all(not f.nullable for f in fields)
    line = "1|2|3|4|17.00|9007199254740.91|0.06|0.02|R|F|1996-02-29|1996-03-01|1996-03-02|DELIVER IN PERSON|AIR|  keep this comment  |\n"
    wire = convert_row("lineitem", line, "native")
    assert wire["l_quantity"] == "17.00"
    assert wire["l_extendedprice"] == "9007199254740.91"
    assert wire["l_discount"] == "0.06"
    assert wire["l_shipdate"] == "1996-02-29"
    assert wire["l_comment"] == "  keep this comment  "
    row = decode_rows(table_schema("lineitem", "native"), [wire])[0]
    assert row["l_extendedprice"] == Decimal("9007199254740.91")
    assert row["l_shipdate"] == date(1996, 2, 29)


def test_legacy_conversion_scales_quantity_rates_and_money_without_float():
    from glamdb_sdk.codec import decode_rows
    from glamdb_workload.tpch_schema import convert_row, table_schema

    line = "1|2|3|4|17.00|9007199254740.91|0.06|0.02|R|F|1996-02-29|1996-03-01|1996-03-02|DELIVER IN PERSON|AIR|comment|\n"
    schema = table_schema("lineitem", "legacy-scaled")
    wire = convert_row("lineitem", line, "legacy-scaled")
    assert {name: wire[name] for name in ("l_quantity", "l_extendedprice", "l_discount", "l_tax")} == {
        "l_quantity": "1700", "l_extendedprice": "900719925474091", "l_discount": "6", "l_tax": "2"}
    row = decode_rows(schema, [wire])[0]
    assert row["l_shipdate"] == datetime(1996, 2, 29, tzinfo=timezone.utc)
    assert row["l_extendedprice"] == 900719925474091


@pytest.mark.parametrize("line", [
    "1|EUROPE|comment", "1|EUROPE|comment|extra|\n", "1.0|EUROPE|comment|\n",
    "1|EUROPE|comment|\r\n", "1|EUROPE|comment|\ntrailing",
])
def test_conversion_rejects_malformed_source_records(line):
    from glamdb_workload.tpch_schema import convert_row

    with pytest.raises(ValueError):
        convert_row("region", line, "native")


@pytest.mark.parametrize("value", ["1993-02-29", "1996-2-01", "1996-02-29T00:00:00Z"])
def test_conversion_rejects_invalid_dates(value):
    from glamdb_workload.tpch_schema import convert_row

    with pytest.raises(ValueError):
        convert_row("orders", f"1|2|O|123.45|{value}|1-URGENT|Clerk#000000001|0|comment|\n", "native")


@pytest.mark.parametrize("value", ["1.001", "nan", "10000000000000.00", "1e2"])
def test_conversion_rejects_inexact_or_out_of_range_decimals(value):
    from glamdb_workload.tpch_schema import convert_row

    with pytest.raises(ValueError):
        convert_row("partsupp", f"1|2|3|{value}|comment|\n", "legacy-scaled")


@pytest.fixture(scope="module")
def reference_process(tmp_path_factory):
    """A tiny external DBGEN protocol fixture; source pin checks are separate."""
    from glamdb_workload import tpch_dataset

    tmp_path = tmp_path_factory.mktemp("tpch-reference-process")
    program = tmp_path / "reference-process"
    program.write_text("""#!/usr/bin/env python3
import os, sys
from pathlib import Path
a=sys.argv
assert a[a.index('-s')+1] in ('0.01', '0.010')
code=a[a.index('-T')+1] if '-T' in a else None
parts=int(a[a.index('-C')+1]) if '-C' in a else 1
step=int(a[a.index('-S')+1]) if '-S' in a else 1
if code=='l':
 assert '-C' not in a and '-S' not in a
tables={None:('region','nation','supplier','customer','part','partsupp','orders','lineitem'), 'l':('region','nation'), 's':('supplier',), 'c':('customer',), 'p':('part','partsupp'), 'o':('orders','lineitem')}[code]
counts={'region':5,'nation':25,'supplier':100,'customer':1500,'part':2000,'partsupp':8000,'orders':15000,'lineitem':60000}
for t in tables:
 n=counts[t]
 chunks,chunk=(1,1) if t in ('region','nation') else (parts,step)
 # DBGEN partitions child rows by parent, with remainder in the final step.
 factor=4 if t in ('partsupp','lineitem') else 1
 n//=factor
 lo=n//chunks*(chunk-1)
 hi=n if chunk==chunks else n//chunks*chunk
 suffix='.'+str(chunk) if chunks>1 else ''
 with (Path(os.environ['DSS_PATH'])/(t+'.tbl'+suffix)).open('w') as f:
  for k in range(lo*factor,hi*factor):
   i=k+1
   v={'region':[k,'EUROPE',' keep comment '], 'nation':[k,'GERMANY',k%5,'comment'],
   'supplier':[i,'Supplier','Address',k%25,'10-000-000-0000','-123.45','comment'],
   'customer':[i,'Customer','Address',k%25,'10-000-000-0000','123.45','BUILDING','comment'],
   'part':[i,'part name','Manufacturer#1','Brand#11','SMALL BRASS',3,'SM BOX','123.45','comment'],
   'partsupp':[k//4+1,k%10+1,10,'123.45','comment'],
   'orders':[i*8-7,k%150+1,'O','123.45','1994-01-01','1-URGENT','Clerk#000000001',0,'comment'],
   'lineitem':[(k//4+1)*8-7,k//4%200+1,k%10+1,k%4+1,'17.00','123.45','0.06','0.02','N','O','1994-01-02','1994-01-03','1994-01-04','DELIVER IN PERSON','AIR','comment']}[t]
   f.write('|'.join(map(str,v))+'|\\n')
""")
    program.chmod(0o755)
    distributions = tmp_path / "dists.dss"
    distributions.write_bytes(b"independent fixture distributions")
    patch = pytest.MonkeyPatch()
    patch.setattr(tpch_dataset, "_generator_provenance", lambda dbgen, *a: {
        "package_version": "3.0.1", "reported_version": "3.0.0",
        "source_sha256": tpch_dataset.SOURCE_SHA256,
        "archive_sha256": tpch_dataset.ARCHIVE_SHA256,
        "distributions_sha256": tpch_dataset.DISTRIBUTIONS_SHA256,
        "executable_sha256": hashlib.sha256(dbgen.read_bytes()).hexdigest(),
        "rng": "reference default seeds; no updates",
    })
    yield program, distributions
    patch.undo()


def _generate_fixture(tmp_path, reference_process, **opts):
    from glamdb_workload.tpch_dataset import generate

    program, distributions = reference_process
    return generate(scale_factor="0.01", output=tmp_path, dbgen=program,
                    type_profile="native", distributions=distributions, **opts)


@pytest.fixture(scope="module")
def generated_native_dataset(tmp_path_factory, reference_process):
    output = tmp_path_factory.mktemp("tpch-fixtures") / "native"
    _generate_fixture(output, reference_process)
    return output


def _copy_dataset(original, output):
    shutil.copytree(original, output)
    return json.loads((output / "manifest.json").read_bytes())


def test_generation_partitions_one_global_scale_and_publishes_all_tables(tmp_path, reference_process):
    from glamdb_workload.tpch_dataset import TPCHDataset

    manifest = _generate_fixture(tmp_path / "out", reference_process, generator_parts=3, part_rows=73, part_bytes=4096)
    data = TPCHDataset(tmp_path / "out")
    assert manifest["complete"] is True
    assert manifest["scale_factor"] == "0.01"
    assert manifest["rows"] == 86630
    assert {t: d.manifest["rows"] for t, d in data.tables.items()} == {
        "region": 5, "nation": 25, "supplier": 100, "customer": 1500,
        "part": 2000, "partsupp": 8000, "orders": 15000, "lineitem": 60000}
    assert data.profile == "native" and data.scale_factor == "0.01"
    orders = [r["o_orderkey"] for p in data.tables["orders"].iter_parts() for r in p]
    assert orders == list(range(1, 120000, 8))
    assert all(len(p) <= 73 for d in data.tables.values() for p in d.iter_parts())
    assert all(p["bytes"] <= 4096 for d in data.tables.values() for p in d.manifest["parts"])
    for table, item in manifest["tables"].items():
        assert item["path"] == table
        assert item["manifest_sha256"] == hashlib.sha256((data.path / table / "manifest.json").read_bytes()).hexdigest()
    data.validate()


def test_partitioning_preserves_the_complete_rowset(tmp_path, reference_process, generated_native_dataset):
    from glamdb_workload.tpch_dataset import TPCHDataset

    _generate_fixture(tmp_path / "three", reference_process, generator_parts=3, part_rows=71)
    one, three = TPCHDataset(generated_native_dataset), TPCHDataset(tmp_path / "three")
    for table in one.tables:
        assert all(a == b for a, b in zip_longest(chain.from_iterable(one.tables[table].iter_parts()),
                                                 chain.from_iterable(three.tables[table].iter_parts())))


def test_dataset_checks_parts_again_and_refuses_changed_schema(tmp_path, generated_native_dataset):
    from glamdb_workload.tpch_dataset import TPCHDataset

    _copy_dataset(generated_native_dataset, tmp_path / "out")
    data = TPCHDataset(tmp_path / "out")
    part = data.path / "region" / data.tables["region"].manifest["parts"][0]["path"]
    saved = part.read_bytes()
    part.write_bytes(saved.replace(b"EUROPE", b"AFRICA"))
    with pytest.raises(ValueError, match="hash"):
        data.validate()
    part.write_bytes(saved)
    path = data.path / "region" / "manifest.json"
    table = json.loads(path.read_bytes())
    table["schema"]["fields"][0]["nullable"] = True
    path.write_text(json.dumps(table))
    manifest = json.loads((data.path / "manifest.json").read_bytes())
    manifest["tables"]["region"]["manifest_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    (data.path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="schema"):
        TPCHDataset(data.path)


@pytest.mark.parametrize("change", [
    lambda m: m.update(complete=False),
    lambda m: m["tables"].pop("lineitem"),
    lambda m: m["tables"]["region"].update(path="../region"),
    lambda m: m["tables"]["region"].update(rows=6),
    lambda m: m.update(dataset_id="sha256:" + "0" * 64),
])
def test_dataset_refuses_incomplete_or_unbound_manifest(tmp_path, generated_native_dataset, change):
    from glamdb_workload.tpch_dataset import TPCHDataset

    manifest = _copy_dataset(generated_native_dataset, tmp_path / "out")
    change(manifest)
    (tmp_path / "out" / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        TPCHDataset(tmp_path / "out")


def test_failed_generator_preserves_only_an_incomplete_manifest(tmp_path, reference_process):
    from glamdb_workload.tpch_dataset import TPCHDataset

    original, distributions = reference_process
    program = tmp_path / "broken-reference-process"
    program.write_text("#!/usr/bin/env python3\nraise SystemExit(9)\n")
    program.chmod(0o755)
    with pytest.raises(ValueError, match="generator"):
        _generate_fixture(tmp_path / "out", (program, distributions))
    assert json.loads((tmp_path / "out" / "manifest.json").read_bytes())["complete"] is False
    with pytest.raises(ValueError, match="complete"):
        TPCHDataset(tmp_path / "out")


@pytest.mark.parametrize("sf", ["0", "0.0001", "0.001", "0.0099", "1.5", "100001", "1e1", 0.1, True])
def test_generation_refuses_silent_reference_scale_rounding(tmp_path, reference_process, sf):
    from glamdb_workload.tpch_dataset import generate

    program, distributions = reference_process
    with pytest.raises(ValueError, match="scale"):
        generate(scale_factor=sf, output=tmp_path / "out", dbgen=program,
                 distributions=distributions, type_profile="native")
    assert not (tmp_path / "out").exists()


def test_generator_requires_pinned_unmodified_sources(tmp_path):
    program = tmp_path / "dbgen"
    program.write_text("#!/bin/sh\nexit 0\n")
    program.chmod(0o755)
    with pytest.raises(ValueError, match="source"):
        source_provenance(program, tmp_path / "dists.dss", None)
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize('change', [
    lambda m: m.update(version=True),
    lambda m: m.update(schema_version=True),
    lambda m: m['versions'].update(conversion=True),
    lambda m: m['generator'].update(source_files={'driver.c': 'a' * 64}),
    lambda m: m['generator'].update(rng='different seeds'),
])
def test_reader_refuses_ambiguous_versions_and_inconsistent_generator_metadata(tmp_path, generated_native_dataset, change):
    from glamdb_workload.tpch_dataset import TPCHDataset, _identity
    manifest = _copy_dataset(generated_native_dataset, tmp_path / 'out')
    change(manifest)
    manifest['dataset_id'] = _identity(manifest)
    (tmp_path / 'out' / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        TPCHDataset(tmp_path / 'out')


def test_reader_refuses_duplicate_json_keys_even_when_the_last_value_is_valid(tmp_path, generated_native_dataset):
    from glamdb_workload.tpch_dataset import TPCHDataset
    _copy_dataset(generated_native_dataset, tmp_path / 'out')
    path = tmp_path / 'out' / 'manifest.json'
    original = path.read_bytes()
    path.write_bytes(b'{"version":0,' + original[1:])
    with pytest.raises(ValueError, match='duplicate'):
        TPCHDataset(tmp_path / 'out')


def test_tiny_scale_with_reference_key_collisions_is_refused_before_generation(tmp_path, reference_process):
    from glamdb_workload.tpch_dataset import generate

    program, distributions = reference_process
    with pytest.raises(ValueError, match="0.001.*duplicate PARTSUPP"):
        generate(scale_factor="0.001", output=tmp_path / "out", dbgen=program,
                 distributions=distributions, type_profile="native")
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("change", [
    lambda m: m.update(source_bytes=m["source_bytes"] + 1),
    lambda m: m["versions"].update(conversion=2),
    lambda m: m["timings"].update(generator_seconds=float("nan")),
])
def test_dataset_rejects_inconsistent_source_or_conversion_metadata(tmp_path, generated_native_dataset, change):
    from glamdb_workload.tpch_dataset import TPCHDataset

    manifest = _copy_dataset(generated_native_dataset, tmp_path / "out")
    change(manifest)
    (tmp_path / "out" / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        TPCHDataset(tmp_path / "out")


def test_row_counts_do_not_use_the_callers_decimal_rounding_context():
    from decimal import localcontext
    from glamdb_workload.tpch_dataset import expected_counts

    with localcontext() as context:
        context.prec = 2
        assert expected_counts("33") == {
            "region": 5, "nation": 25, "supplier": 330000, "customer": 4950000,
            "part": 6600000, "partsupp": 26400000, "orders": 49500000}


def test_missing_dataset_part_is_a_validation_failure(tmp_path, generated_native_dataset):
    from glamdb_workload.tpch_dataset import TPCHDataset

    _copy_dataset(generated_native_dataset, tmp_path / "out")
    data = TPCHDataset(tmp_path / "out")
    (data.path / "region" / data.tables["region"].manifest["parts"][0]["path"]).unlink()
    with pytest.raises(ValueError, match="missing|unreadable"):
        data.validate()


def test_generation_does_not_reinitialize_reference_rng_for_each_table(tmp_path, reference_process):
    from glamdb_workload.tpch_dataset import TPCHDataset

    original, distributions = reference_process
    program = tmp_path / "record-reference-process"
    # The external fixture records invocations as an observable side effect.
    text = original.read_text().replace("a=sys.argv", "a=sys.argv\nwith (Path(__file__).parent/'calls').open('a') as log: log.write('call\\n')")
    program.write_text(text)
    program.chmod(0o755)
    _generate_fixture(tmp_path / "out", (program, distributions))
    assert (program.parent / "calls").read_text().splitlines() == ["call"]
    TPCHDataset(tmp_path / "out").validate()


@pytest.mark.parametrize("scale,part_rows", [("1", 1), ("100000", 10000)])
def test_metadata_budget_refuses_obviously_excessive_part_counts_before_dbgen(tmp_path, reference_process, scale, part_rows):
    from glamdb_workload.tpch_dataset import generate

    program, distributions = reference_process
    with pytest.raises(ValueError, match="metadata.*65536|65536.*metadata"):
        generate(scale_factor=scale, output=tmp_path / "out", dbgen=program,
                 distributions=distributions, type_profile="native", part_rows=part_rows)
    assert not (tmp_path / "out").exists()


def test_oversized_top_manifest_is_bounded_before_json_parsing(tmp_path, monkeypatch):
    from pathlib import Path
    from glamdb_workload import tpch_dataset

    limit = 1024 * 1024
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b"x" * (limit + 1))
    original_open = Path.open
    reads = []

    class BoundedFile(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= limit + 1, "metadata read must have a finite byte bound"
            reads.append(size)
            return super().read(size)

    def open_guard(path, *args, **kwargs):
        if path == manifest:
            return BoundedFile(b"x" * (limit + 1))
        return original_open(path, *args, **kwargs)

    def parse_guard(*args, **kwargs):
        raise AssertionError("oversized metadata reached JSON parsing")

    monkeypatch.setattr(Path, "open", open_guard)
    monkeypatch.setattr(tpch_dataset.json, "loads", parse_guard)
    with pytest.raises(ValueError, match="metadata.*limit|limit.*metadata"):
        tpch_dataset.TPCHDataset(tmp_path)
    assert reads


def test_generic_table_reader_limits_metadata_before_json_parsing(tmp_path, monkeypatch):
    from pathlib import Path
    from glamdb_workload.dataset import Dataset

    path = tmp_path / "manifest.json"
    path.write_bytes(b"x" * 4097)
    original_open = Path.open
    reads = []

    class BoundedFile(io.BytesIO):
        def read(self, size=-1):
            assert size == 4097, "generic reader must bound metadata before parsing"
            reads.append(size)
            return super().read(size)

    def open_guard(opened, *args, **kwargs):
        return BoundedFile(b"x" * 4097) if opened == path else original_open(opened, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_guard)
    with pytest.raises(ValueError, match="metadata.*limit|limit.*metadata"):
        Dataset(tmp_path, max_manifest_bytes=4096)
    assert reads == [4097]


def test_table_metadata_aggregate_budget_is_enforced_before_loading_all_tables(tmp_path, generated_native_dataset, monkeypatch):
    from glamdb_workload import tpch_dataset

    _copy_dataset(generated_native_dataset, tmp_path / "out")
    first_two_bytes = sum((tmp_path / "out" / t / "manifest.json").stat().st_size for t in ("region", "nation"))
    monkeypatch.setattr(tpch_dataset, "MAX_TOTAL_TABLE_MANIFEST_BYTES", first_two_bytes - 1)
    with pytest.raises(ValueError, match="metadata.*limit|limit.*metadata"):
        tpch_dataset.TPCHDataset(tmp_path / "out")


def test_flush_enforces_global_part_budget_and_leaves_generation_incomplete(tmp_path, reference_process, monkeypatch):
    from glamdb_workload import tpch_dataset

    # Row-only preflight fits20 parts; small byte caps require more parts.
    monkeypatch.setattr(tpch_dataset, "MAX_DATASET_PARTS", 20)
    with pytest.raises(ValueError, match="metadata.*part|part.*metadata"):
        _generate_fixture(tmp_path / "out", reference_process, part_bytes=1024)
    manifest = json.loads((tmp_path / "out" / "manifest.json").read_bytes())
    assert manifest["complete"] is False
    assert len(list((tmp_path / "out").glob("*/part-*.jsonl"))) == 20


@pytest.mark.parametrize("setting,limit", [("MAX_DATASET_PARTS", 13), ("MAX_TABLE_MANIFEST_BYTES", 1)])
def test_reader_enforces_its_part_and_individual_table_metadata_limits(tmp_path, generated_native_dataset, monkeypatch, setting, limit):
    from glamdb_workload import tpch_dataset

    _copy_dataset(generated_native_dataset, tmp_path / "out")
    monkeypatch.setattr(tpch_dataset, setting, limit)
    with pytest.raises(ValueError, match="metadata.*limit"):
        tpch_dataset.TPCHDataset(tmp_path / "out")
