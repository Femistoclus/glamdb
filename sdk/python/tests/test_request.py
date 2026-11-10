import pytest

from glamdb_sdk import Client, Field, Schema, ValidationError


def test_independent_canonical_hash_vector_preserves_duplicates_and_null():
    schema = Schema([Field("n", "int64", True)])
    with Client("http://127.0.0.1:1", "unused") as client:
        request = client.prepare_append("00112233-4455-6677-8899-aabbccddeeff", [
            {"n": "9007199254740993"}, {"n": None}, {"n": "9007199254740993"}],
            operation_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", schema=schema)
    assert request.payload_hash == "e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7"


@pytest.mark.parametrize("rows", [[], [{"n": 1}] * 10001])
def test_append_profile_row_bound_is_enforced_before_sending(rows):
    with Client("http://127.0.0.1:1", "unused") as client:
        with pytest.raises(ValidationError):
            client.prepare_append("00112233-4455-6677-8899-aabbccddeeff", rows,
                                  operation_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                                  schema=Schema([Field("n", "int64", True)]))


def test_decoded_input_byte_budget_stops_generator_before_copying_the_entire_input():
    seen = []
    def rows():
        for value in range(20):
            seen.append(value)
            yield {"s": "x" * 1024**2}
    with Client("http://127.0.0.1:1", "unused") as client:
        with pytest.raises(ValidationError, match="16 MiB"):
            client.prepare_append("00112233-4455-6677-8899-aabbccddeeff", rows(),
                                  operation_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                                  schema=Schema([Field("s", "string", False)]))
    assert len(seen) == 16  # 16 strings plus canonical presence/length bytes exceed 16 MiB


def test_all_types_against_independent_hand_written_golden_vector():
    # Same independent vector as fixtures/golden/all-types-boundaries.json.
    # Literal input and expected hashes keep this package test standalone.
    from glamdb_sdk.request import schema_fingerprint
    schema = Schema([Field("s", "string", False), Field("b", "bool", True), Field("i", "int64", True),
                     Field("f", "float64", True), Field("t", "timestamp_us_utc", True)])
    rows = [
        {"s": "é", "b": True, "i": "-9223372036854775808", "f": -0.0, "t": "0001-01-01T00:00:00Z"},
        {"s": "dup", "b": False, "i": "9223372036854775807", "f": 1.5, "t": "9999-12-31T23:59:59.999999Z"},
        {"s": "dup", "b": None, "i": None, "f": None, "t": None},
    ]
    with Client("http://127.0.0.1:1", "unused") as client:
        request = client.prepare_append("00112233-4455-6677-8899-aabbccddeeff", rows,
                                       operation_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", schema=schema)
    assert schema_fingerprint(schema).hex() == "24e0e6e92c6bf507bbdb0d552f86d237f3ddc945af2f6ae1f06edf4e22a75f3c"
    assert request.payload_hash == "72b0d7fbd4d8097a8a848de273fbf13cffa462d20be481e87d8c788f64feef1b"


def test_append_accepts_exact_canonical_row_limit_and_rejects_one_byte_over():
    schema = Schema([Field("a", "string"), Field("b", "string")] + [
        Field(f"n{index}", "int64") for index in range(62)])
    # 1048576 + 1048008 + 2 * 5 + 62 * 9 = 2097152 decoded bytes.
    row = {"a": "x" * 1048576, "b": "y" * 1048008} | {
        f"n{index}": 9223372036854775807 for index in range(62)}
    with Client("http://127.0.0.1:1", "unused") as client:
        request = client.prepare_append("00112233-4455-6677-8899-aabbccddeeff", [row],
                                       operation_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", schema=schema)
        expected = row | {f"n{index}": "9223372036854775807" for index in range(62)}
        assert request.to_dict()["rows"] == [expected]
        with pytest.raises(ValidationError, match="2 MiB"):
            client.prepare_append("00112233-4455-6677-8899-aabbccddeeff", [row | {"b": row["b"] + "z"}],
                                  operation_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", schema=schema)
