"""Reproducible bounded fixture generation and validation against literal vectors."""

from .canonical import append_hash, canonical_append, canonical_schema, schema_fingerprint
from .types import normalize_schema, wire_rows


def generate_fixture(*, seed=17, count=16) -> dict:
    if type(seed) is not int or not 0 <= seed <= 0xFFFFFFFF or type(count) is not int or not 1 <= count <= 10000:
        raise ValueError("seed must be uint32 and row count must be 1..10000")
    schema = normalize_schema({"fields": [
        {"name": "key", "type": "string", "nullable": False},
        {"name": "n", "type": "int64", "nullable": True},
        {"name": "t", "type": "timestamp_us_utc", "nullable": True},
        {"name": "f", "type": "float64", "nullable": True},
        {"name": "b", "type": "bool", "nullable": True},
        {"name": "note", "type": "string", "nullable": True},
    ]})
    rows = []
    first = {"key": "duplicate", "n": 9007199254740993, "t": -1, "f": -0.0, "b": True, "note": "common_ascii_prefix_longer_than_16_é"}
    state = seed
    for index in range(count):
        if index < 2:
            rows.append(dict(first))
            continue
        state = (1664525 * state + 1013904223) & 0xFFFFFFFF
        rows.append({
            "key": f"key-{state % 4}",
            "n": None if index % 5 == 0 else state - 2147483648,
            "t": None if index % 7 == 0 else (state - 2147483648) * 1000000 + index,
            "f": None if index % 3 == 0 else (state % 1000 - 500) / 4,
            "b": None if index % 4 == 0 else bool(state & 1),
            "note": None if index % 6 == 0 else f"common_ascii_prefix_longer_than_16_{state}_e\u0301",
        })
    collection = "00112233-4455-6677-8899-aabbccddeeff"
    return {
        "fixture_version": 1, "evidence_kind": "fixture", "power_durability": "NOT VALIDATED",
        "seed": seed, "collection_id": collection, "schema": schema, "rows": wire_rows(schema, rows),
        "schema_canonical_hex": canonical_schema(schema).hex(),
        "schema_fingerprint": schema_fingerprint(schema).hex(),
        "canonical_hex": canonical_append(schema, collection, rows).hex(),
        "payload_sha256": append_hash(schema, collection, rows),
    }


def check_fixture(fixture: dict) -> list[str]:
    if not isinstance(fixture, dict):
        return ["fixture must be an object"]
    errors = []
    if type(fixture.get("fixture_version")) is not int or fixture.get("fixture_version") != 1:
        errors.append("unsupported fixture_version")
    if fixture.get("evidence_kind") != "fixture" or fixture.get("power_durability") != "NOT VALIDATED":
        errors.append("fixtures cannot claim power durability evidence")
    try:
        schema, collection, rows = fixture["schema"], fixture["collection_id"], fixture["rows"]
        expected = {
            "schema_canonical_hex": canonical_schema(schema).hex(),
            "schema_fingerprint": schema_fingerprint(schema).hex(),
            "canonical_hex": canonical_append(schema, collection, rows).hex(),
            "payload_sha256": append_hash(schema, collection, rows),
        }
    except (KeyError, ValueError, TypeError) as exc:
        return errors + [f"invalid fixture inputs: {exc}"]
    for key, value in expected.items():
        if fixture.get(key) != value:
            errors.append(f"{key} mismatch")
    return errors
