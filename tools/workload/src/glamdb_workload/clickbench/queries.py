"""Pinned external query material and conservative SDK scan adaptation."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import urllib.request

from glamdb_sdk.predicates import Predicate, and_, col, or_
from glamdb_sdk.errors import ValidationError
import sqlglot
from sqlglot import exp

from .model import save_json
from .schema import COLUMNS, UPSTREAM, schema_for

MATERIAL = {
    "duckdb/queries.sql": "274ffe1c4f83baad2fc177bbb6773bbdab0db779faeb2970de8cc531292a5dc6",
    "duckdb/create.sql": "8065807651b163ab7006b8e451d820e2f6ddc757a6c6bd033800fe1dbc3f81e7",
    "LICENSE": "9d42e8bf72619767ff7534116151204a976a4894a5082ed45db977e63360e215",
}


def fetch_material(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    for name, digest in MATERIAL.items():
        url = f"https://raw.githubusercontent.com/ClickHouse/ClickBench/{UPSTREAM}/{name}"
        with urllib.request.urlopen(url, timeout=60) as response:
            if response.url != url:
                raise ValueError("query material redirected away from the pinned source")
            payload = response.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024 or hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("pinned query material checksum or byte budget differs")
        destination = output / name
        destination.parent.mkdir(exist_ok=True)
        destination.write_bytes(payload)
    save_json(output / "material.json", {"upstream": UPSTREAM, "sha256": MATERIAL,
                                        "license": "CC BY-NC-SA 4.0", "complete": True})
    return {"status": "PASS", "upstream": UPSTREAM, "cache": str(output.absolute()), "queries": 43}


class QueryCatalog:
    def __init__(self, directory):
        directory = Path(directory)
        for name, digest in MATERIAL.items():
            path = directory / name
            if path.is_symlink() or path.stat().st_size > 1024 * 1024:
                raise ValueError("invalid query material file")
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("pinned query material was changed")
        self.sql = tuple(statement.strip().rstrip(";") + ";" for statement in
                         (directory / "duckdb/queries.sql").read_text().splitlines() if statement.strip())
        if len(self.sql) != 43:
            raise ValueError("pinned catalog must contain exactly Q1..Q43")
        self.digest = MATERIAL["duckdb/queries.sql"]

    def plan(self, query_id, *, snapshot, type_profile="legacy-date"):
        if type(query_id) is not int or not 1 <= query_id <= 43:
            raise ValueError("ClickBench query ID must be in 1..43")
        return plan_query(self.sql[query_id - 1], query_id, snapshot=snapshot, type_profile=type_profile)


@dataclass(frozen=True)
class QueryPlan:
    query_id: int
    sql: str
    sql_hash: str
    projection: tuple
    predicate: dict | None
    snapshot_id: str | None
    native_complete: bool
    validation_sql: str
    reference_sql: str
    output_columns: int
    order_columns: int
    validation_policy: str
    limit: int | None
    offset: int
    order_by: str

    @property
    def scan(self):
        value = {"projection": self.projection, "snapshot_id": self.snapshot_id}
        if self.predicate is not None:
            value["predicate"] = self.predicate
        return value

    def describe(self):
        return {"query_id": self.query_id, "sql_sha256": self.sql_hash, "projection": list(self.projection),
                "predicate": self.predicate, "snapshot_id": self.snapshot_id,
                "native_complete": self.native_complete, "residual": "none" if self.native_complete else
                "original DuckDB SQL retained on the streamed candidates",
                "validation_policy": self.validation_policy, "early_limit": False}


def _literal(node, field):
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Neg):
        value = -int(node.this.this)
    elif isinstance(node, exp.Literal):
        value = node.this if node.is_string else int(node.this)
    else:
        raise ValueError("not a supported literal")
    if field.type == "timestamp_us_utc":
        if not isinstance(value, str):
            raise ValueError("timestamp comparison requires a calendar literal")
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
    return value


def _predicate(node, fields):
    """Return a safe necessary predicate and whether it covers the full tree."""
    if isinstance(node, exp.Paren):
        return _predicate(node.this, fields)
    if isinstance(node, (exp.And, exp.Or)):
        left, left_full = _predicate(node.this, fields)
        right, right_full = _predicate(node.expression, fields)
        if isinstance(node, exp.Or):
            return (or_(left, right), True) if left_full and right_full else (None, False)
        parts = tuple(part for part in (left, right) if part is not None)
        return (and_(*parts) if len(parts) > 1 else parts[0] if parts else None, left_full and right_full)
    if isinstance(node, exp.Not):
        value, complete = _predicate(node.this, fields)
        return (~value, True) if complete else (None, False)
    try:
        field = fields[node.this.name.lower()] if isinstance(node.this, exp.Column) else None
        if field is None:
            return None, False
        column = col(field.name, field.type)
        comparison = {exp.EQ: "eq", exp.NEQ: "ne", exp.LT: "lt", exp.LTE: "le", exp.GT: "gt", exp.GTE: "ge"}
        if type(node) in comparison:
            return getattr(column, comparison[type(node)])(_literal(node.expression, field)), True
        if isinstance(node, exp.In) and not node.args.get("query"):
            return column.in_(_literal(item, field) for item in node.expressions), True
        if isinstance(node, exp.Is) and isinstance(node.expression, exp.Null):
            return column.is_null(), True
    except (ValueError, TypeError, KeyError, OverflowError, ValidationError):
        pass
    return None, False


def _bound(node, name):
    if node is None:
        return None
    value = node.expression
    if not isinstance(value, exp.Literal) or value.is_string or not value.this.isdecimal():
        raise ValueError(f"{name} must be a constant nonnegative integer")
    return int(value.this)


def plan_query(sql, query_id, *, snapshot, type_profile="legacy-date"):
    if type(query_id) is not int or not 1 <= query_id <= 43:
        raise ValueError("query ID must be in 1..43")
    parsed = sqlglot.parse(sql, read="duckdb")
    if len(parsed) != 1 or not isinstance(parsed[0], exp.Select):
        raise ValueError("one SELECT query is required")
    tree = parsed[0]
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1 or tables[0].name.lower() != "hits" or tables[0].db or tables[0].catalog:
        raise ValueError("query must read exactly the benchmark hits table")
    if tree.args.get("joins") or tree.args.get("with_") or list(tree.find_all(exp.Subquery)):
        raise ValueError("joins, CTEs and subqueries are outside the pinned benchmark profile")
    fields = {field.name.lower(): field for field in schema_for(type_profile).fields}
    aliases = {expression.alias.lower(): expression.this for expression in tree.expressions
               if isinstance(expression, exp.Alias)}
    names = set()
    for column in tree.find_all(exp.Column):
        lower = column.name.lower()
        if lower in fields:
            names.add(fields[lower].name)
        elif lower not in aliases:
            raise ValueError("query references an unknown benchmark column")
    star = any(isinstance(expression, exp.Star) for expression in tree.expressions)
    projection = tuple(COLUMNS) if star else tuple(name for name in COLUMNS if name in names)
    if not projection:
        projection = ("WatchID",)  # Arrow consumers need one physical column to count rows.
    where = tree.args.get("where")
    predicate, complete = _predicate(where.this, fields) if where is not None else (None, True)
    order = tree.args.get("order")
    augmented = tree.copy()
    if star:
        augmented.set("expressions", [exp.column(name) for name in COLUMNS])
    sort_columns = []
    if order is not None:
        for ordinal, key in enumerate(order.expressions):
            expression = key.this
            if isinstance(expression, exp.Column) and expression.name.lower() in aliases:
                expression = aliases[expression.name.lower()]
            elif isinstance(expression, exp.Literal) and not expression.is_string:
                expression = tree.expressions[int(expression.this) - 1]
                expression = expression.this if isinstance(expression, exp.Alias) else expression
            augmented.append("expressions", exp.alias_(expression.copy(), f"__cb_order_{ordinal}"))
            sort_columns.append(expression)
    full = augmented.copy()
    full.set("limit", None)
    full.set("offset", None)
    full.set("order", None)
    limit, offset = _bound(tree.args.get("limit"), "LIMIT"), _bound(tree.args.get("offset"), "OFFSET") or 0
    simple = (complete and not tree.args.get("group") and not tree.args.get("having") and
              not tree.args.get("distinct") and order is None and limit is None and
              all(isinstance(expression, exp.Column) for expression in tree.expressions))
    return QueryPlan(query_id, sql, hashlib.sha256(sql.encode()).hexdigest(), projection,
                     predicate.to_dict() if predicate else None, snapshot, simple,
                     augmented.sql(dialect="duckdb"), full.sql(dialect="duckdb"),
                     len(COLUMNS) if star else len(tree.expressions), len(sort_columns),
                     "ordered-with-legal-ties" if order else "unordered-subset" if limit is not None else "multiset",
                     limit, offset, ", ".join(
                         f"c{(len(COLUMNS) if star else len(tree.expressions)) + index} " +
                         ("DESC" if key.args.get("desc") else "ASC") + " NULLS " +
                         ("FIRST" if key.args.get("nulls_first") else "LAST")
                         for index, key in enumerate(order.expressions if order else ())))
