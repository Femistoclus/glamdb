"""The native 16 MiB wire ceiling is independent of canonical/response budgets."""

from dataclasses import replace
import json

import httpx
import pytest

from glamdb_sdk import ANY, AuthenticationError, Client, Field, Schema, ValidationError
from glamdb_sdk.request import AppendRequest

CID = "00112233-4455-6677-8899-aabbccddeeff"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
WIRE_LIMIT = 16 * 1024**2
PREFIX = b'{"operation_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","rows":['


def boundary_rows(target, char="x", wire_width=1, utf8_width=1, name="s", snapshot=ANY):
    """Hand-account JSON framing and a repeated scalar with known byte widths."""
    suffix = b"]}" if snapshot is ANY else (
        b'],"expected_snapshot":null}' if snapshot is None else
        b'],"expected_snapshot":"9223372036854775807"}')
    repeats = 1024**2 // utf8_width
    full_wire = repeats * wire_width
    count = (WIRE_LIMIT + full_wire - 1) // full_wire
    # {<quoted field name>:""}, plus commas between rows.
    row_frame = len(json.dumps(name, ensure_ascii=False).encode("utf-8")) + 5
    remaining = target - len(PREFIX) - len(suffix) - count * row_frame - (count - 1)
    last, padding = divmod(remaining - (count - 1) * full_wire, wire_width)
    rows = [{name: char * repeats}] * (count - 1) + [{name: char * last + "x" * padding}]
    return rows


@pytest.mark.parametrize("delta", [-1, 0, 1])
@pytest.mark.parametrize("char,wire_width,utf8_width", [
    ("x", 1, 1), ("é", 2, 2), ("🙂", 4, 4), ("\0", 6, 1),
    ("\x1f", 6, 1), ("\n", 2, 1), ('"', 2, 1), ("\\", 2, 1),
])
def test_preparation_wire_boundary_counts_utf8_and_json_escapes(delta, char, wire_width, utf8_width):
    rows = boundary_rows(WIRE_LIMIT + delta, char, wire_width, utf8_width)
    value = {"operation_id": OP, "rows": rows}
    expected = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    assert len(expected) == WIRE_LIMIT + delta
    if delta > 0:
        with pytest.raises(ValidationError, match="16 MiB.*serialized"):
            AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), rows)
    else:
        request = AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), rows)
        assert request.body == expected
        assert (request.collection_id, request.operation_id) == (CID, OP)


@pytest.mark.parametrize("delta", [-1, 0, 1])
@pytest.mark.parametrize("snapshot", [None, "9223372036854775807"])
def test_preparation_wire_boundary_includes_snapshot_precondition(delta, snapshot):
    rows = boundary_rows(WIRE_LIMIT + delta, snapshot=snapshot)
    if delta > 0:
        with pytest.raises(ValidationError, match="16 MiB.*serialized"):
            AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), rows, snapshot)
    else:
        request = AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), rows, snapshot)
        assert len(request.body) == WIRE_LIMIT + delta
        assert request.to_dict() == {"operation_id": OP, "rows": rows, "expected_snapshot": snapshot}


@pytest.mark.parametrize("delta", [-1, 0, 1])
@pytest.mark.parametrize("name", ['"' * 128, "🙂" * 32])
def test_preparation_wire_boundary_counts_long_escaped_field_names(delta, name):
    rows = boundary_rows(WIRE_LIMIT + delta, name=name)
    if delta > 0:
        with pytest.raises(ValidationError, match="16 MiB.*serialized"):
            AppendRequest.prepare(CID, OP, Schema([Field(name, "string")]), rows)
    else:
        request = AppendRequest.prepare(CID, OP, Schema([Field(name, "string")]), rows)
        assert len(request.body) == WIRE_LIMIT + delta
        assert request.to_dict()["rows"] == rows


def test_json_escaping_rejects_canonical_valid_append_during_preparation():
    # Three 1 MiB strings are 3,145,743 canonical bytes but escape to 18,874,459
    # wire bytes. Each value and row is independently within its decoded limit.
    rows = [{"s": "\0" * 1024**2}] * 3
    with pytest.raises(ValidationError, match="16 MiB.*serialized"):
        AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), rows)


def test_explicit_oversized_prepared_body_is_rejected_before_transport_and_token():
    original = AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), [{"s": "ok"}])
    request = replace(original, body=original.body + b" " * (WIRE_LIMIT + 1 - len(original.body)))
    sent, tokens = [], []

    def token():
        tokens.append(1)
        return "unused"

    def transport(outbound):
        sent.append(len(outbound.content))
        return httpx.Response(413, json={"error": {"code": "PAYLOAD_TOO_LARGE", "message": "too large"}})

    with Client("https://example.test", token_provider=token,
                transport=httpx.MockTransport(transport)) as client:
        with pytest.raises(ValidationError, match="16 MiB.*serialized"):
            client.append(request)
    assert sent == [] and tokens == []


def test_generator_stops_at_first_wire_overflow_instead_of_consuming_the_batch():
    seen = []

    def rows():
        for index in range(3):
            seen.append(index)
            yield {"s": "\0" * 1024**2}
        raise AssertionError("wire overflow must stop before the next generator item")

    with pytest.raises(ValidationError, match="16 MiB.*serialized"):
        AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), rows())
    assert seen == [0, 1, 2]


@pytest.mark.parametrize("snapshot", [None, "9223372036854775807"])
def test_generator_wire_stop_includes_precondition_before_fetching_another_row(snapshot):
    data = boundary_rows(WIRE_LIMIT + 1, snapshot=snapshot)

    def rows():
        yield from data
        raise AssertionError("precondition overflow must stop at the offending row")

    with pytest.raises(ValidationError, match="16 MiB.*serialized"):
        AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), rows(), snapshot)


def test_wide_nullable_schema_stops_before_repeated_names_exhaust_wire_budget():
    fields = [Field(f"{index:02d}" + "x" * 126, "string", True) for index in range(64)]
    row = {field.name: None for field in fields}
    # Each NULL costs one canonical byte; repeated 128-byte names dominate JSON.
    seen = []
    row_bytes = len(json.dumps(row, separators=(",", ":")).encode("utf-8"))

    def rows():
        for index in range(10000):
            seen.append(index)
            yield row

    with pytest.raises(ValidationError, match="16 MiB.*serialized"):
        AppendRequest.prepare(CID, OP, Schema(fields), rows())
    assert len(seen) == (WIRE_LIMIT - len(PREFIX) - 2 + 1) // (row_bytes + 1) + 1
    assert len(seen) < 10000


def test_client_preparation_overflow_does_not_send_any_http_request():
    sent = []

    def transport(request):
        sent.append(request)
        return httpx.Response(413, json={"error": {"code": "PAYLOAD_TOO_LARGE", "message": "too large"}})

    with Client("https://example.test", "unused", transport=httpx.MockTransport(transport)) as client:
        with pytest.raises(ValidationError, match="16 MiB.*serialized"):
            client.append(CID, [{"s": "\0" * 1024**2}] * 3,
                          operation_id=OP, schema=Schema([Field("s", "string")]))
    assert sent == []


@pytest.mark.parametrize("delta", [-1, 0])
def test_explicit_admitted_body_is_sent_unchanged_at_wire_boundary(delta):
    original = AppendRequest.prepare(CID, OP, Schema([Field("s", "string")]), [{"s": "ok"}])
    request = replace(original, body=original.body + b" " * (WIRE_LIMIT + delta - len(original.body)))
    sent = []

    def transport(outbound):
        sent.append(outbound.content)
        return httpx.Response(403, json={"error": {"code": "FORBIDDEN", "message": "denied"}})

    # A small response limit must not become an upload limit.
    with Client("https://example.test", "unused", max_json_bytes=64,
                transport=httpx.MockTransport(transport)) as client:
        for _ in range(2):
            with pytest.raises(AuthenticationError):
                client.append(request)
    assert sent == [request.body, request.body]
    assert (request.collection_id, request.operation_id, request.payload_hash) == (
        original.collection_id, original.operation_id, original.payload_hash)
