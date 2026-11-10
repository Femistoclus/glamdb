"""Typed builders for the public SQL-three-valued predicate tree."""

import json
from typing import Any, Iterable

from .codec import Field, Schema, encode_scalar
from .errors import ValidationError


class Predicate:
    def __init__(self, value: dict[str, Any]):
        self._encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._encoded)

    def __and__(self, other: "Predicate") -> "Predicate":
        return and_(self, other)

    def __or__(self, other: "Predicate") -> "Predicate":
        return or_(self, other)

    def __invert__(self) -> "Predicate":
        return Predicate({"op": "not", "arg": self.to_dict()})


class Column:
    def __init__(self, name: str, logical_type: str):
        # A predicate may compare NULL even if its source column is required.
        # Schema validates name/type; the special key requirement applies to
        # collection schemas, not the predicate literal's nullable allowance.
        validated = Schema([Field(name, logical_type, False)]).fields[0]
        self.field = Field(validated.name, validated.type, True, validated.field_id)

    def _compare(self, op: str, value: Any) -> Predicate:
        return Predicate({"op": op, "field": self.field.name, "value": encode_scalar(self.field, value)})

    def eq(self, value: Any) -> Predicate:
        return self._compare("eq", value)

    def ne(self, value: Any) -> Predicate:
        return self._compare("ne", value)

    def lt(self, value: Any) -> Predicate:
        return self._compare("lt", value)

    def le(self, value: Any) -> Predicate:
        return self._compare("le", value)

    def gt(self, value: Any) -> Predicate:
        return self._compare("gt", value)

    def ge(self, value: Any) -> Predicate:
        return self._compare("ge", value)

    def in_(self, values: Iterable[Any]) -> Predicate:
        if isinstance(values, (str, bytes)):
            raise ValidationError("IN values must be a sequence of typed literals")
        return Predicate({"op": "in", "field": self.field.name,
                          "values": [encode_scalar(self.field, value) for value in values]})

    def is_null(self) -> Predicate:
        return Predicate({"op": "is_null", "field": self.field.name})

    def is_not_null(self) -> Predicate:
        return Predicate({"op": "is_not_null", "field": self.field.name})


def col(name: str, logical_type: str) -> Column:
    return Column(name, logical_type)


def _combine(op: str, predicates: tuple[Predicate, ...]) -> Predicate:
    if not predicates or any(not isinstance(p, Predicate) for p in predicates):
        raise ValidationError(f"{op.upper()} requires at least one Predicate")
    return Predicate({"op": op, "args": [p.to_dict() for p in predicates]})


def and_(*predicates: Predicate) -> Predicate:
    return _combine("and", predicates)


def or_(*predicates: Predicate) -> Predicate:
    return _combine("or", predicates)
