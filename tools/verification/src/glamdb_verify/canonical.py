"""RFC canonical encoding, independent of Arrow, Iceberg and engine code."""

import hashlib
import struct
import uuid

from .types import TYPE_IDS, normalize_rows, normalize_schema


def canonical_schema(schema: dict) -> bytes:
    fields = normalize_schema(schema)["fields"]
    output = bytearray(b"COLUMNAR-SCHEMA\0" + struct.pack("<HH", 1, len(fields)))
    for field in fields:
        name = field["name"].encode("utf-8")
        output.extend(struct.pack("<IH", field["field_id"], len(name)))
        output.extend(name)
        output.extend((TYPE_IDS[field["type"]], int(field["nullable"])))
    return bytes(output)


def schema_fingerprint(schema: dict) -> bytes:
    return hashlib.sha256(canonical_schema(schema)).digest()


def canonical_append(schema: dict, collection_id: str, rows: list[dict]) -> bytes:
    schema = normalize_schema(schema)
    if not isinstance(rows, list) or not 1 <= len(rows) <= 10000:
        raise ValueError("append requires 1..10000 rows")
    try:
        collection = uuid.UUID(collection_id)
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("collection_id must be a UUID string") from exc
    output = bytearray(b"COLUMNAR-APPEND\0" + struct.pack("<H", 1) + collection.bytes + schema_fingerprint(schema) + struct.pack("<Q", len(rows)))
    header_size = len(output)
    for input_row in rows:
        # A scan may contain many appends; keep this append's temporary copy bounded.
        row = normalize_rows(schema, [input_row])[0]
        for field in schema["fields"]:
            value = row[field["name"]]
            output.append(int(value is not None))
            if value is None:
                continue
            kind = field["type"]
            if kind == "string":
                encoded = value.encode("utf-8")
                output.extend(struct.pack("<I", len(encoded)))
                output.extend(encoded)
            elif kind == "bool":
                output.append(int(value))
            else:
                output.extend(struct.pack("<d" if kind == "float64" else "<q", value))
        if len(output) - header_size > 16777216:
            raise ValueError("append exceeds 16 MiB of canonical decoded values")
    return bytes(output)


def append_hash(schema: dict, collection_id: str, rows: list[dict]) -> str:
    return hashlib.sha256(canonical_append(schema, collection_id, rows)).hexdigest()
