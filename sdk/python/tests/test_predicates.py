import pytest

from glamdb_sdk import ValidationError, and_, col, or_


@pytest.mark.parametrize("op", ["eq", "ne", "lt", "le", "gt", "ge"])
def test_typed_comparisons_preserve_int64_wire_values(op):
    assert getattr(col("n", "int64"), op)(9007199254740993).to_dict() == {
        "op": op, "field": "n", "value": "9007199254740993"}


def test_boolean_expression_shape_nulls_and_empty_in_are_exact():
    a = col("a", "string")
    b = col("b", "bool")
    assert and_(a.in_([]), or_(a.is_null(), ~b.eq(True))).to_dict() == {
        "op": "and", "args": [
            {"op": "in", "field": "a", "values": []},
            {"op": "or", "args": [{"op": "is_null", "field": "a"},
                                    {"op": "not", "arg": {"op": "eq", "field": "b", "value": True}}]}]}
    assert a.is_not_null().to_dict() == {"op": "is_not_null", "field": "a"}
    assert (a.eq(None) | b.eq(False)).to_dict()["args"][0]["value"] is None


def test_predicate_literals_are_copied_and_type_checked():
    values = [1, 2]
    predicate = col("n", "int64").in_(values)
    values.append(3)
    raw = predicate.to_dict()
    raw["values"].append("4")
    assert predicate.to_dict()["values"] == ["1", "2"]
    with pytest.raises(ValidationError):
        col("b", "bool").eq(1)
