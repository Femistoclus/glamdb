"""Single-pass append preparation is byte-identical to the pre-plan implementation.

``legacy_prepare.py`` is a frozen copy of the old codec/request code and acts as
the oracle: for every input, ``payload_hash`` and ``body`` must be equal, and an
invalid input within the wire budget must raise the same exception type with
the same message. Canonical-valid requests exceeding the native HTTP wire
ceiling are now rejected locally, as covered by the boundary test below.
"""

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import random

import pytest

from glamdb_sdk import Field, Schema, ValidationError
from glamdb_sdk.codec import _canonical_row_size, encode_rows, format_timestamp, row_plan
from glamdb_sdk.models import ANY
from glamdb_sdk.request import AppendRequest, canonical_payload_hash, schema_fingerprint

_SPEC = importlib.util.spec_from_file_location("legacy_prepare", Path(__file__).with_name("legacy_prepare.py"))
legacy = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(legacy)

CID = "00112233-4455-6677-8899-aabbccddeeff"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
MIN_US, MAX_US = -62135596800000000, 253402300799999999


def outcome(call):
    try:
        return "ok", call()
    except Exception as exc:  # noqa: BLE001 - the exact type is compared
        return "error", type(exc), str(exc)


def new_prepare(schema, rows, expected=ANY):
    request = AppendRequest.prepare(CID, OP, schema, rows, expected)
    return request.collection_id, request.operation_id, request.payload_hash, request.body


def old_prepare(schema, rows, expected=ANY):
    return legacy.prepare(CID, OP, schema, rows, expected)


def assert_same(schema, make_rows, expected=ANY):
    """make_rows() is called once per implementation so generators are fresh."""
    old = outcome(lambda: old_prepare(schema, make_rows(), expected))
    new = outcome(lambda: new_prepare(schema, make_rows(), expected))
    assert new == old
    return new


# Field IDs deliberately differ from declaration order so wire order (declaration)
# and hash order (field ID) diverge.
ALL_TYPES = Schema([
    Field("key", "string", False, 7),
    Field("s", "string", True, 3),
    Field("b", "bool", True, 11),
    Field("i", "int64", True, 1),
    Field("f", "float64", True, 5),
    Field("t", "timestamp_us_utc", True, 2),
    Field("ri", "int64", False, 40),
    Field("rt", "timestamp_us_utc", False, 9),
])

GOLDEN_ROWS = [
    {"key": "a", "s": "é漢字🙂\0tab\t\"q\"\\", "b": True, "i": -(2**63), "f": -0.0,
     "t": "0001-01-01T00:00:00Z", "ri": "9223372036854775807", "rt": MAX_US},
    {"key": "b", "s": "", "b": False, "i": "-9223372036854775808", "f": 0,
     "t": "9999-12-31T23:59:59.999999Z", "ri": 2**63 - 1, "rt": MIN_US},
    {"key": "c", "s": None, "b": None, "i": None, "f": None, "t": None, "ri": 0, "rt": "1970-01-01T00:00:00.000000Z"},
    {"key": "d", "ri": "0", "rt": -1},  # missing nullable fields
    {"key": "e", "f": 1e308, "t": datetime(2020, 2, 29, 23, 59, 59, 999999,
                                          tzinfo=timezone(timedelta(hours=-5, minutes=-30))),
     "ri": -1, "rt": datetime(1970, 1, 1, tzinfo=timezone.utc)},
    {"key": "f", "f": 5e-324, "t": "2026-01-01T01:00:00+01:00", "ri": 9007199254740993,
     "rt": "2026-03-04T05:06:07.1-00:00"},
    {"key": "g", "f": 2**53 + 1, "t": "2026-03-04T05:06:07.123456+00:00", "ri": 42,
     "rt": "1969-12-31T23:59:59.999999Z"},
    {"key": "a", "s": "dup", "b": True, "i": 1, "f": 1.5, "t": 1_700_000_000_000_000, "ri": 1, "rt": 0},
]


def test_all_types_golden_vector_is_byte_identical_to_oracle():
    status, (_, _, payload_hash, body) = assert_same(ALL_TYPES, lambda: GOLDEN_ROWS)
    assert status == "ok"
    # Literal value (computed by the pre-plan code at 1f48601) pins the bytes
    # even if the oracle file were edited.
    assert payload_hash == "0398363fbb0a0104bf2853fa3219050cd125a41c49dd09b0f81003c9f0759aa2"
    rows = json.loads(body)["rows"]
    assert list(rows[0]) == ["key", "s", "b", "i", "f", "t", "ri", "rt"]
    assert rows[0]["f"] == 0.0 and str(rows[0]["f"]) == "0.0"
    assert rows[0]["t"] == "0001-01-01T00:00:00.000000Z" and rows[0]["rt"] == "9999-12-31T23:59:59.999999Z"
    assert rows[3] == {"key": "d", "s": None, "b": None, "i": None, "f": None, "t": None,
                       "ri": "0", "rt": "1969-12-31T23:59:59.999999Z"}
    assert rows[4]["t"] == "2020-03-01T05:29:59.999999Z"
    assert rows[5]["rt"] == "2026-03-04T05:06:07.100000Z"


@pytest.mark.parametrize("expected", [ANY, None, 0, "9223372036854775807", 2**63, "x", True])
def test_expected_snapshot_variants_match(expected):
    assert_same(ALL_TYPES, lambda: GOLDEN_ROWS[:2], expected)


def test_schema_fingerprint_matches_oracle_for_permuted_ids():
    for schema in (ALL_TYPES, Schema([Field("x", "int64")]),
                   Schema([Field(f"f{i}", "string", True, 1000 - i) for i in range(64)])):
        assert schema_fingerprint(schema) == legacy.schema_fingerprint(schema)


def test_equal_schemas_share_a_plan_and_different_ids_do_not():
    a = Schema([Field("x", "int64", True, 4), Field("y", "string", True, 2)])
    b = Schema([Field("x", "int64", True, 4), Field("y", "string", True, 2)])
    c = Schema([Field("x", "int64", True, 2), Field("y", "string", True, 4)])
    assert row_plan(a) is row_plan(b)
    assert row_plan(a) is not row_plan(c)
    assert schema_fingerprint(a) != schema_fingerprint(c)
    rows = [{"x": 1, "y": "z"}]
    assert new_prepare(a, rows) != new_prepare(c, rows)
    assert new_prepare(c, rows) == old_prepare(c, rows)


def test_input_mutation_after_prepare_does_not_change_request():
    rows = [dict(row) for row in GOLDEN_ROWS]
    request = AppendRequest.prepare(CID, OP, ALL_TYPES, rows)
    before = (request.payload_hash, request.body)
    for row in rows:
        row["key"] = "mutated"
        row.pop("ri", None)
    rows.append({"key": "late"})
    view = request.to_dict()
    view["rows"].clear()
    assert (request.payload_hash, request.body) == before
    assert before == old_prepare(ALL_TYPES, GOLDEN_ROWS)[2:]


def test_list_tuple_generator_and_custom_mapping_inputs_hash_identically():
    class Row(Mapping):
        def __init__(self, data):
            self.data = data

        def __getitem__(self, key):
            return self.data[key]

        def __iter__(self):
            return iter(self.data)

        def __len__(self):
            return len(self.data)

    results = {new_prepare(ALL_TYPES, make()) for make in (
        lambda: list(GOLDEN_ROWS), lambda: tuple(GOLDEN_ROWS), lambda: (row for row in GOLDEN_ROWS),
        lambda: iter(GOLDEN_ROWS), lambda: [Row(row) for row in GOLDEN_ROWS])}
    assert results == {old_prepare(ALL_TYPES, GOLDEN_ROWS)}


def test_sequence_growing_during_iteration_matches_oracle():
    def make():
        rows = []

        class Grow(dict):
            def get(self, key, default=None):
                if key == "key" and len(rows) < 6:
                    rows.append(Grow({"key": f"k{len(rows)}", "ri": 1, "rt": 2}))
                return super().get(key, default)

        rows.append(Grow({"key": "k", "ri": 1, "rt": 2}))
        return rows

    status, prepared = assert_same(ALL_TYPES, make)
    assert status == "ok" and len(json.loads(prepared[3])["rows"]) == 6


@pytest.mark.parametrize("make", [
    lambda: [],
    lambda: iter([]),
    lambda: [{"n": 1}] * 10000,
    lambda: [{"n": 1}] * 10001,
    lambda: ({"n": 1} for _ in range(10001)),
    lambda: [{"n": "x"}] + [{"n": 1}] * 10001,
], ids=["empty-list", "empty-iter", "10000", "10001-list", "10001-gen", "bad-first-of-10002"])
def test_row_count_bounds_match(make):
    assert_same(Schema([Field("n", "int64", True)]), make)


def test_generators_are_consumed_identically_up_to_each_limit():
    def counted(seen, rows):
        for row in rows:
            seen.append(1)
            yield row

    schema = Schema([Field("s", "string", False)])
    cases = [
        (schema, lambda: ({"s": "x" * 1024**2} for _ in range(20))),  # 16 MiB batch budget
        (Schema([Field("n", "int64", True)]), lambda: ({"n": i} for i in range(20000))),  # row count
        (schema, lambda: iter([{"s": "ok"}, {"s": 1}, {"s": "never"}])),  # invalid value
    ]
    for case_schema, make in cases:
        old_seen, new_seen = [], []
        old = outcome(lambda: old_prepare(case_schema, counted(old_seen, make())))
        new = outcome(lambda: new_prepare(case_schema, counted(new_seen, make())))
        assert new == old and new[0] == "error"
        assert len(new_seen) == len(old_seen)


def test_row_and_batch_byte_limits_match_at_the_boundary():
    schema = Schema([Field("a", "string"), Field("b", "string")] + [
        Field(f"n{index}", "int64", False, 100 + index) for index in range(62)])
    row = {"a": "x" * 1048576, "b": "y" * 1048008} | {f"n{index}": -1 for index in range(62)}
    assert assert_same(schema, lambda: [row])[0] == "ok"
    assert assert_same(schema, lambda: [row | {"b": row["b"] + "z"}])[0] == "error"
    # 8 exact-limit rows are 16 MiB; one more NULL-free byte overflows the batch.
    small = Schema([Field("s", "string", False)])
    exact = [{"s": "x" * (1024**2 - 5)}] * 16
    # The legacy canonical-only preparation admitted this, but JSON framing
    # exceeds the native HTTP wire ceiling even without scalar escaping.
    assert old_prepare(small, exact)
    with pytest.raises(ValidationError, match="16 MiB.*serialized"):
        new_prepare(small, exact)
    with pytest.raises(ValidationError, match="16 MiB canonical"):
        old_prepare(small, exact + [{"s": ""}])
    with pytest.raises(ValidationError, match="16 MiB.*serialized"):
        new_prepare(small, exact + [{"s": ""}])


INVALID_VALUES = {
    "string": [1, b"x", "\ud800", "x" * (1024**2 + 1), True],
    "bool": [1, 0, "true", None.__class__],
    "int64": [True, 1.0, 2**63, -(2**63) - 1, "1.0", "01", "-0", "1" * 21, "99999999999999999999", " 1", object()],
    "float64": [True, "1.0", float("nan"), float("inf"), -float("inf"), 10**400, object()],
    "timestamp_us_utc": [True, 1.0, MIN_US - 1, MAX_US + 1, 2**63, "20260101", "0", "2020-01-01",
                         "2020-01-01T00:00:00", "2020-01-01T00:00:00.0000001Z", "2020-01-01T00:00:00+24:00",
                         "2020-02-30T00:00:00Z", "0001-01-01T00:00:00+00:01", "9999-12-31T23:59:59.999999-00:01",
                         datetime(2020, 1, 1), "２０２０-01-01T00:00:00Z"],
}


@pytest.mark.parametrize("kind", sorted(INVALID_VALUES))
def test_each_invalid_value_raises_the_same_error(kind):
    field_name = "key" if kind == "string" else "v"
    fields = [Field(field_name, kind, False)] + ([] if kind == "string" else [Field("key", "string", False)])
    schema = Schema(fields)
    for value in INVALID_VALUES[kind] + ([] if kind == "string" else [None]):
        row = {field_name: value} if kind == "string" else {"v": value, "key": "k"}
        assert assert_same(schema, lambda: [row])[0] == "error", value
        assert outcome(lambda: encode_rows(schema, [row])) == outcome(lambda: legacy.encode_rows(schema, [row]))


@pytest.mark.parametrize("row", [
    None, [], "row", 1, {"key": "k", "extra": 1}, {}, {"key": None},
    {"key": "x" * 1025}, {"key": "x" * 1024, "a": None, "b": "nan"},
    {"key": 1, "a": "bad"},  # first failing field in declaration order wins
    {"a": "bad", "b": 1.5},  # missing required key, declared first
])
def test_row_shape_errors_and_their_order_match(row):
    schema = Schema([Field("key", "string", False, 9), Field("a", "int64", True, 1), Field("b", "float64", True, 4)])
    assert assert_same(schema, lambda: [{"key": "ok"}, row])[0] == "error"


def test_str_subclass_values_produce_identical_bytes():
    class Text(str):
        pass

    schema = Schema([Field("key", "string", False), Field("i", "int64", True), Field("t", "timestamp_us_utc", True)])
    rows = [{"key": Text("k"), "i": Text("123"), "t": Text("2020-01-01T00:00:00.000000Z")}]
    status, prepared = assert_same(schema, lambda: rows)
    assert status == "ok" and json.loads(prepared[3])["rows"] == [
        {"key": "k", "i": "123", "t": "2020-01-01T00:00:00.000000Z"}]


def test_public_codec_and_hash_helpers_still_match_oracle():
    encoded = encode_rows(ALL_TYPES, GOLDEN_ROWS)
    assert encoded == legacy.encode_rows(ALL_TYPES, GOLDEN_ROWS)
    assert [type(value) for row in encoded for value in row.values()] == [
        type(value) for row in legacy.encode_rows(ALL_TYPES, GOLDEN_ROWS) for value in row.values()]
    assert canonical_payload_hash(CID, ALL_TYPES, encoded) == legacy.canonical_payload_hash(CID, ALL_TYPES, encoded)
    assert [_canonical_row_size(ALL_TYPES, row) for row in encoded] == [
        legacy._canonical_row_size(ALL_TYPES, row) for row in encoded]
    assert [len(row_plan(ALL_TYPES).encode_row(row)[1]) for row in GOLDEN_ROWS] == [
        legacy._canonical_row_size(ALL_TYPES, row) for row in encoded]


def test_format_timestamp_matches_oracle_across_the_range():
    rng = random.Random(105)
    values = [MIN_US, MIN_US + 1, MAX_US, MAX_US - 1, -1, 0, 1, 951782400000000, 4107542400000000]
    values += [rng.randint(MIN_US, MAX_US) for _ in range(20000)]
    values += [rng.randint(-10**15, 10**15) for _ in range(5000)]
    for value in values:
        assert format_timestamp(value) == legacy.format_timestamp(value)


def _random_value(rng, kind, nullable):
    if nullable and rng.random() < 0.15:
        return None
    pick = rng.random()
    if kind == "string":
        alphabet = "aé漢🙂\0\"\\\n z"
        return "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))
    if kind == "bool":
        return rng.random() < 0.5
    if kind == "int64":
        number = rng.choice([-(2**63), 2**63 - 1, 0, -1, rng.randint(-(2**63), 2**63 - 1), rng.randint(-999, 999)])
        return str(number) if pick < 0.5 else number
    if kind == "float64":
        return rng.choice([-0.0, 0.0, 0, 1, rng.uniform(-1e300, 1e300), rng.random(), 5e-324,
                           rng.randint(-(2**60), 2**60)])
    micros = rng.choice([MIN_US, MAX_US, rng.randint(MIN_US, MAX_US), rng.randint(-10**12, 10**12)])
    if pick < 0.3:
        return micros
    if pick < 0.6:
        return legacy.format_timestamp(micros)
    hours = rng.randint(-23, 23)
    zone = timezone(timedelta(hours=hours, minutes=rng.choice([0, 30]) * (1 if hours >= 0 else -1)))
    moment = (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=micros)).astimezone(zone) \
        if MIN_US + 2 * 86400000000 < micros < MAX_US - 2 * 86400000000 else None
    if moment is None:
        return micros
    if pick < 0.8:
        return moment
    text = moment.isoformat(timespec=rng.choice(["seconds", "milliseconds", "microseconds"]))
    return text.replace("+00:00", "Z") if rng.random() < 0.5 else text


@pytest.mark.parametrize("seed", range(40))
def test_randomized_differential_against_oracle(seed):
    rng = random.Random(seed)
    kinds = ["string", "bool", "int64", "float64", "timestamp_us_utc"]
    count = rng.choice([1, 2, 5, 16, 64])
    ids = rng.sample(range(1, 10**6), count)
    fields = [Field(f"c{index}", rng.choice(kinds), rng.random() < 0.5, ids[index]) for index in range(count)]
    schema = Schema(fields)
    rows = []
    for _ in range(rng.choice([1, 3, 50])):
        row = {}
        for field in fields:
            value = _random_value(rng, field.type, field.nullable)
            if value is not None or rng.random() < 0.5:
                row[field.name] = value
        rows.append(row)
    if seed % 5 == 4:  # occasionally inject one invalid value
        victim = rng.choice(rows)
        victim[rng.choice(fields).name] = rng.choice([True, "bad", 1.5, None, 2**64])
    use_generator = seed % 2 == 1
    assert_same(schema, (lambda: (row for row in rows)) if use_generator else (lambda: rows))
    assert outcome(lambda: encode_rows(schema, rows)) == outcome(lambda: legacy.encode_rows(schema, rows))


class _View(Mapping):
    """Read-only non-dict Mapping: forces the declaration-order encoder."""

    def __init__(self, data):
        self.data = data

    def __getitem__(self, key):
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)


@pytest.mark.parametrize("seed", range(40))
def test_grouped_and_ordered_encoders_agree_with_oracle(seed):
    rng = random.Random(1000 + seed)
    kinds = ["string", "bool", "int64", "float64", "timestamp_us_utc"]
    fields = [Field(f"c{index}", rng.choice(kinds), rng.random() < 0.5, rng.randint(1, 10**6))
              for index in range(rng.choice([1, 7, 64]))]
    if len({field.field_id for field in fields}) != len(fields):
        return
    schema = Schema(fields)
    plan = row_plan(schema)
    for _ in range(20):
        row = {field.name: _random_value(rng, field.type, field.nullable) for field in fields}
        expected = outcome(lambda: legacy.encode_rows(schema, [row]))
        assert outcome(lambda: [plan.encode_row(row)[0]]) == expected
        assert outcome(lambda: [plan.encode_row(_View(row))[0]]) == expected
        if expected[0] == "ok":
            assert plan.encode_row(row) == plan.encode_row(_View(row)) == plan._encode_ordered(row)


VALID_TIMESTAMPS = [
    "2020-02-29T00:00:00.000000Z", "2000-12-31T23:59:59.999999Z", "1970-01-01T00:00:00.000001Z",
    "2020-01-01T00:00:00.5Z", "2020-01-01T00:00:00Z", "2020-01-01T12:00:00.000000+23:59",
    "0001-01-01T23:59:59.999999+23:59", "9999-12-31T00:00:00.000000-23:59",
    datetime(1, 1, 1, tzinfo=timezone.utc), datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
    datetime(1, 1, 1, 23, 0, tzinfo=timezone(timedelta(hours=23))),
    datetime(9999, 12, 31, 0, 30, tzinfo=timezone(timedelta(hours=-23, minutes=-29))),
    datetime(2020, 6, 1, 12, 0, 0, 1, tzinfo=timezone(timedelta(seconds=-37, microseconds=-5))),
    datetime(2020, 6, 1, 12, 0, 0, 1, fold=1, tzinfo=timezone(timedelta(hours=5))),
    MIN_US, MAX_US, 0, -1,
]
INVALID_TIMESTAMPS = [
    "2020-02-30T00:00:00.000000Z", "2021-02-29T00:00:00.000000Z", "0000-01-01T00:00:00.000000Z",
    "2020-01-01T24:00:00.000000Z", "2020-01-01T23:60:00.000000Z", "2020-01-01T23:59:60.000000Z",
    "2020-13-01T00:00:00.000000Z", "2020-01-01 00:00:00.000000Z", "2020-01-01T00:00:00.000000z",
    "2020-01-01T00:00:00,000000Z", "2020-01-01T00:00:00.000000Z\n", "2020-W01-1T00:00:00.000000Z",
    datetime(1, 1, 1, tzinfo=timezone(timedelta(minutes=1))),
    datetime(9999, 12, 31, 23, 59, tzinfo=timezone(timedelta(minutes=-1))),
    datetime(2020, 1, 1, tzinfo=None),
]


def test_timestamp_fast_path_boundaries_match_oracle():
    from zoneinfo import ZoneInfo

    class Stamp(datetime):
        pass

    schema = Schema([Field("t", "timestamp_us_utc", True)])
    extra = [datetime(2021, 3, 28, 2, 30, tzinfo=ZoneInfo("Europe/Berlin")),
             datetime(2021, 10, 31, 2, 30, fold=1, tzinfo=ZoneInfo("Europe/Berlin")),
             Stamp(2020, 1, 1, tzinfo=timezone.utc)]
    for value in VALID_TIMESTAMPS + extra:
        assert assert_same(schema, lambda: [{"t": value}])[0] == "ok", value
    for value in INVALID_TIMESTAMPS:
        assert assert_same(schema, lambda: [{"t": value}])[0] == "error", value


def test_int64_text_fast_path_boundaries_match_oracle():
    schema = Schema([Field("n", "int64", True)])
    valid = ["0", "-1", "9223372036854775807", "-9223372036854775808", "10", "-10", 0]
    invalid = ["+1", " 1", "1 ", "1_0", "١٢", "１２", "-0", "00", "0x10", "", "-", "9223372036854775808",
               "-9223372036854775809", "18446744073709551616", "1" * 21, "1e3"]
    for value in valid:
        assert assert_same(schema, lambda: [{"n": value}])[0] == "ok", value
    for value in invalid:
        assert assert_same(schema, lambda: [{"n": value}])[0] == "error", value


def test_float_and_bool_values_outside_the_fast_path_match_oracle():
    schema = Schema([Field("f", "float64", True), Field("b", "bool", True)])
    for value in [0, -0, 1, 2**53 + 1, 2**1023, -(2**63), 1e-310, -1e-310, -0.0]:
        assert assert_same(schema, lambda: [{"f": value, "b": False}])[0] == "ok", value
    assert assert_same(schema, lambda: [{"f": 2**1024}])[0] == "error"
