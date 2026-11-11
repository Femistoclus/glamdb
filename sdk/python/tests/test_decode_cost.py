"""decode_rows validates and converts each wire value once: one RFC3339 parse per timestamp."""
import glamdb_sdk.codec as codec
from glamdb_sdk import Field, Schema


def test_each_wire_timestamp_text_is_parsed_once(monkeypatch):
    schema = Schema([Field("key", "string", False), Field("t", "timestamp_us_utc", True)])
    rows = [{"key": str(index), "t": codec.format_timestamp(1_700_000_000_000_000 + index)} for index in range(100)]
    rows.append({"key": "null", "t": None})
    text_parses = []
    original = codec.timestamp_us

    def counting(value):
        if isinstance(value, str):
            text_parses.append(1)
        return original(value)

    monkeypatch.setattr(codec, "timestamp_us", counting)
    decoded = codec.decode_rows(schema, rows)
    assert decoded[0]["t"].year == 2023 and decoded[-1]["t"] is None
    # Strict canonical-text validation needs one parse per non-null cell (plus int formatting).
    assert len(text_parses) == 100, len(text_parses)
