"""The 61 required TPC-H fields and lossless reference DBGEN conversion."""

from functools import lru_cache

from glamdb_sdk import Field, Schema
from glamdb_sdk.codec import date_days, decimal_parts, encode_rows, int64


SCHEMA_VERSION = 1
PROFILES = ("native", "legacy-scaled")
# Order and field IDs are stable within each table; D means DECIMAL(15,2).
COLUMNS = {
    "region": (("r_regionkey", "I"), ("r_name", "S"), ("r_comment", "S")),
    "nation": (("n_nationkey", "I"), ("n_name", "S"), ("n_regionkey", "I"), ("n_comment", "S")),
    "supplier": (("s_suppkey", "I"), ("s_name", "S"), ("s_address", "S"), ("s_nationkey", "I"),
                 ("s_phone", "S"), ("s_acctbal", "D"), ("s_comment", "S")),
    "customer": (("c_custkey", "I"), ("c_name", "S"), ("c_address", "S"), ("c_nationkey", "I"),
                 ("c_phone", "S"), ("c_acctbal", "D"), ("c_mktsegment", "S"), ("c_comment", "S")),
    "part": (("p_partkey", "I"), ("p_name", "S"), ("p_mfgr", "S"), ("p_brand", "S"),
             ("p_type", "S"), ("p_size", "I"), ("p_container", "S"), ("p_retailprice", "D"), ("p_comment", "S")),
    "partsupp": (("ps_partkey", "I"), ("ps_suppkey", "I"), ("ps_availqty", "I"),
                 ("ps_supplycost", "D"), ("ps_comment", "S")),
    "orders": (("o_orderkey", "I"), ("o_custkey", "I"), ("o_orderstatus", "S"), ("o_totalprice", "D"),
               ("o_orderdate", "T"), ("o_orderpriority", "S"), ("o_clerk", "S"),
               ("o_shippriority", "I"), ("o_comment", "S")),
    "lineitem": (("l_orderkey", "I"), ("l_partkey", "I"), ("l_suppkey", "I"), ("l_linenumber", "I"),
                 ("l_quantity", "D"), ("l_extendedprice", "D"), ("l_discount", "D"), ("l_tax", "D"),
                 ("l_returnflag", "S"), ("l_linestatus", "S"), ("l_shipdate", "T"),
                 ("l_commitdate", "T"), ("l_receiptdate", "T"), ("l_shipinstruct", "S"),
                 ("l_shipmode", "S"), ("l_comment", "S")),
}
TABLES = tuple(COLUMNS)
PRIMARY_KEYS = {
    "region": ("r_regionkey",), "nation": ("n_nationkey",), "supplier": ("s_suppkey",),
    "customer": ("c_custkey",), "part": ("p_partkey",), "partsupp": ("ps_partkey", "ps_suppkey"),
    "orders": ("o_orderkey",), "lineitem": ("l_orderkey", "l_linenumber"),
}
FOREIGN_KEYS = (
    ("nation", ("n_regionkey",), "region", ("r_regionkey",)),
    ("supplier", ("s_nationkey",), "nation", ("n_nationkey",)),
    ("customer", ("c_nationkey",), "nation", ("n_nationkey",)),
    ("partsupp", ("ps_partkey",), "part", ("p_partkey",)),
    ("partsupp", ("ps_suppkey",), "supplier", ("s_suppkey",)),
    ("orders", ("o_custkey",), "customer", ("c_custkey",)),
    ("lineitem", ("l_orderkey",), "orders", ("o_orderkey",)),
    ("lineitem", ("l_partkey", "l_suppkey"), "partsupp", ("ps_partkey", "ps_suppkey")),
)


@lru_cache(maxsize=16)
def table_schema(table, profile):
    """Return the exact physical SDK schema for an explicitly selected profile."""
    if profile not in PROFILES:
        raise ValueError("type_profile must be native or legacy-scaled")
    if table not in COLUMNS:
        raise ValueError("unknown TPC-H table")
    types = {"I": "int64", "S": "string", "D": "decimal", "T": "date"}
    if profile == "legacy-scaled":
        types.update(D="int64", T="timestamp_us_utc")
    return Schema(Field(name, types[kind], nullable=False, field_id=index,
                        precision=15 if types[kind] == "decimal" else None,
                        scale=2 if types[kind] == "decimal" else None)
                  for index, (name, kind) in enumerate(COLUMNS[table], 1))


def logical_fields(table, profile):
    """Describe logical types and their exact physical conversion in manifests."""
    schema = table_schema(table, profile)
    descriptions = {"I": "int64", "S": "string", "D": "decimal(15,2)", "T": "date"}
    return [dict(field_id=f.field_id, name=name, logical_type=descriptions[kind], physical_type=f.type,
                 **({"precision": 15, "scale": 2,
                     "conversion": "exact decimal" if profile == "native" else "exact coefficient x100"}
                    if kind == "D" else
                    {"conversion": "strict calendar date" if profile == "native" else "UTC midnight microseconds"}
                    if kind == "T" else {"conversion": "preserve source UTF-8" if kind == "S" else "exact int64"}))
            for f, (name, kind) in zip(schema.fields, COLUMNS[table])]


def convert_row(table, source_line, profile):
    """Parse one LF-terminated DBGEN row without stripping source strings."""
    schema = table_schema(table, profile)
    if not isinstance(source_line, str) or not source_line.endswith("|\n") or "\r" in source_line:
        raise ValueError("DBGEN record must end in a pipe and LF")
    values = source_line[:-2].split("|")
    if len(values) != len(schema.fields) or any("\n" in v or "\0" in v for v in values):
        raise ValueError("DBGEN record has invalid field count or control character")
    row = {}
    for (name, kind), value in zip(COLUMNS[table], values):
        if kind == "I":
            value = int64(value)
        elif kind == "D":
            coefficient, normalized = decimal_parts(value, 15, 2)
            value = normalized if profile == "native" else int64(coefficient)
        elif kind == "T":
            days = date_days(value)
            if profile == "legacy-scaled":
                value = days * 86400000000
        row[name] = value
    return encode_rows(schema, [row])[0]
