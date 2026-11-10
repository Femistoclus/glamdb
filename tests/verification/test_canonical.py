import pytest


SCHEMA = {"fields": [{"name": "n", "type": "int64", "nullable": True}]}
COLLECTION = "00112233-4455-6677-8899-aabbccddeeff"
SCHEMA_HEX = "434f4c554d4e41522d534348454d4100010001000100000001006e0301"
FINGERPRINT = "4078bb95b0753db6f11470b1507afe1936afc16845b96029fbf1105110301685"
APPEND_HEX = "434f4c554d4e41522d415050454e4400010000112233445566778899aabbccddeeff4078bb95b0753db6f11470b1507afe1936afc16845b96029fbf1105110301685030000000000000001010000000000200000010100000000002000"


def test_exact_independently_written_golden_bytes(verify):
    rows = [{"n": "9007199254740993"}, {"n": None}, {"n": "9007199254740993"}]
    assert verify.canonical_schema(SCHEMA).hex() == SCHEMA_HEX
    assert verify.schema_fingerprint(SCHEMA).hex() == FINGERPRINT
    assert verify.canonical_append(SCHEMA, COLLECTION, rows).hex() == APPEND_HEX
    assert verify.append_hash(SCHEMA, COLLECTION, rows) == "e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7"


def test_duplicates_and_row_order_are_hash_content(verify):
    one = verify.append_hash(SCHEMA, COLLECTION, [{"n": 1}])
    assert one != verify.append_hash(SCHEMA, COLLECTION, [{"n": 1}, {"n": 1}])
    assert verify.append_hash(SCHEMA, COLLECTION, [{"n": 1}, {"n": 2}]) != verify.append_hash(SCHEMA, COLLECTION, [{"n": 2}, {"n": 1}])
    assert one == verify.append_hash(SCHEMA, COLLECTION, [{"n": "1"}])


def test_type_markers_utf8_null_and_signed_zero(verify):
    schema = {"fields": [
        {"name": "s", "type": "string", "nullable": False},
        {"name": "b", "type": "bool", "nullable": False},
        {"name": "f", "type": "float64", "nullable": False},
        {"name": "t", "type": "timestamp_us_utc", "nullable": True},
    ]}
    row = {"s": "é", "b": True, "f": -0.0, "t": -1}
    assert verify.canonical_append(schema, COLLECTION, [row])[74:].hex() == "0102000000c3a9010101000000000000000001ffffffffffffffff"
    row["f"] = 0.0
    assert verify.canonical_append(schema, COLLECTION, [row])[74:].hex() == "0102000000c3a9010101000000000000000001ffffffffffffffff"
    row["s"] = "e\u0301"
    assert verify.canonical_append(schema, COLLECTION, [row])[74:81].hex() == "010300000065cc"


@pytest.mark.parametrize("rows", [[], [{"n": 1}] * 10001])
def test_append_rejects_empty_and_too_many_rows(verify, rows):
    with pytest.raises(ValueError):
        verify.canonical_append(SCHEMA, COLLECTION, rows)


def test_append_input_limit(verify):
    schema = {"fields": [{"name": "s", "type": "string", "nullable": False}]}
    with pytest.raises(ValueError):
        verify.canonical_append(schema, COLLECTION, [{"s": "x" * 1048576}] * 17)


def test_schema_id_is_not_part_of_fingerprint_and_chunking_is_irrelevant(verify):
    assert verify.schema_fingerprint(dict(SCHEMA, schema_id=42)) == verify.schema_fingerprint(SCHEMA)
    batches = [[{"n": "1"}], [{"n": 2}, {"n": None}]]
    flattened = [row for batch in batches for row in batch]
    assert verify.append_hash(SCHEMA, COLLECTION, flattened) == verify.append_hash(SCHEMA, COLLECTION, [{"n": 1}, {"n": "2"}, {}])
