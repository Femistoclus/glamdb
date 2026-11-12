"""Independent row-wise SQL three-valued predicate/projection/limit oracle."""

from .types import normalize_rows, normalize_schema, normalize_value

_COMPARISONS = {
    "eq": lambda a, b: a == b, "ne": lambda a, b: a != b,
    "lt": lambda a, b: a < b, "le": lambda a, b: a <= b,
    "gt": lambda a, b: a > b, "ge": lambda a, b: a >= b,
}


def _literal(field, value):
    if value is None:
        return None
    kind = field["type"]
    if isinstance(value, dict):
        if kind not in ('decimal', 'date'):
            raise ValueError('legacy scalar predicates require primitive literals')
        expected = {"type", "value", "precision", "scale"} if kind == "decimal" else {"type", "value"}
        if set(value) != expected or value["type"] != kind:
            raise ValueError("predicate literal shape/type differs from field")
        if kind == "decimal" and any(type(value[name]) is not int or value[name] != field[name] for name in ("precision", "scale")):
            raise ValueError("decimal predicate precision/scale differs from field")
        value = value["value"]
    elif kind in ("decimal", "date"):
        raise ValueError("decimal/date predicates require typed literals")
    return normalize_value(field, value, literal=True)


def _compile(predicate, fields, depth=0):
    if predicate is None and depth == 0:
        return lambda row: True
    if depth > 64 or not isinstance(predicate, dict):
        raise ValueError("predicate must be an object of depth <=64")
    op = predicate.get("op")
    if not isinstance(op, str):
        raise ValueError("predicate op required")
    if op == "true":
        if set(predicate) != {"op"}:
            raise ValueError("true predicate takes no arguments")
        return lambda row: True
    if op in ("and", "or"):
        args = predicate.get("args")
        if set(predicate) != {"op", "args"} or not isinstance(args, list) or not args:
            raise ValueError("and/or require nonempty args")
        children = [_compile(arg, fields, depth + 1) for arg in args]

        def combine(row):
            results = [child(row) for child in children]
            if op == "and":
                return False if False in results else None if None in results else True
            return True if True in results else None if None in results else False

        return combine
    if op == "not":
        if set(predicate) != {"op", "arg"}:
            raise ValueError("not requires arg")
        child = _compile(predicate["arg"], fields, depth + 1)

        def negate(row):
            result = child(row)
            return None if result is None else not result

        return negate
    name = predicate.get("field")
    if not isinstance(name, str) or name not in fields:
        raise ValueError("predicate references unknown field")
    field = fields[name]
    if op in ("is_null", "is_not_null"):
        if set(predicate) != {"op", "field"}:
            raise ValueError("NULL predicates require only field")
        return lambda row: (row[name] is None) if op == "is_null" else (row[name] is not None)
    if op == "in":
        values = predicate.get("values")
        if set(predicate) != {"op", "field", "values"} or not isinstance(values, list):
            raise ValueError("in requires values array")
        values = [_literal(field, value) for value in values]

        def contains(row):
            value = row[name]
            if value is None:
                return None
            if value in values:
                return True
            return None if None in values else False

        return contains
    if op not in _COMPARISONS or set(predicate) != {"op", "field", "value"}:
        raise ValueError("invalid comparison operator/shape")
    value = _literal(field, predicate["value"])
    compare = _COMPARISONS[op]
    return lambda row: None if row[name] is None or value is None else compare(row[name], value)


def scan_rows(schema: dict, rows: list[dict], projection, predicate=None, limit=None) -> list[dict]:
    schema = normalize_schema(schema)
    fields = {field["name"]: field for field in schema["fields"]}
    if projection is None:
        projection = list(fields)
    if not isinstance(projection, list) or any(not isinstance(name, str) or name not in fields for name in projection):
        raise ValueError("projection must contain known field names")
    if len(projection) != len(set(projection)):
        raise ValueError("projection contains duplicate fields")
    if limit is not None and (type(limit) is not int or limit < 0):
        raise ValueError("limit must be a nonnegative integer")
    matches = _compile(predicate, fields)
    rows = normalize_rows(schema, rows)
    if limit == 0:
        return []
    output = []
    for row in rows:
        if matches(row) is True:
            output.append({name: row[name] for name in projection})
            if limit is not None and len(output) == limit:
                break
    return output
