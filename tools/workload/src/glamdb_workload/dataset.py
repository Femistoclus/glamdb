"""Bounded, versioned Mimesis datasets using the SDK's exact scalar codecs."""

import hashlib
import json
import math
import platform
from importlib.metadata import version
from pathlib import Path

from glamdb_sdk import Schema
from glamdb_sdk.codec import decode_rows, encode_rows, int64, timestamp_us
from glamdb_sdk.request import json_bytes

MAX_PART_BYTES = 8 * 1024**2


def write_json(path, value):
    """Replace a metadata file only after its complete JSON has been written."""
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(json_bytes(value) + b"\n")
    temporary.replace(path)


def _settings(config, schema):
    settings = config.get("generation", {})
    if set(settings) - {"fields", "key"}:
        raise ValueError("unknown generation option")
    fields = settings.get("fields", {})
    if set(fields) - {f.name for f in schema.fields if f.name != "key"}:
        raise ValueError("unknown field profile (key has a separate profile)")
    normalized = {}
    for field in schema.fields:
        if field.name == "key":
            continue
        opts = dict(fields.get(field.name, {}))
        allowed = {"null_rate"} | (
            {"min", "max"}
            if field.type in {"int64", "float64", "timestamp_us_utc"}
            else {"length", "alphabet"} if field.type == "string" else set()
        )
        if set(opts) - allowed:
            raise ValueError("unknown field generation option")
        null_rate = opts.setdefault("null_rate", 0)
        if (
            type(null_rate) not in (int, float)
            or not math.isfinite(null_rate)
            or not 0 <= null_rate <= 1
        ):
            raise ValueError("null_rate must be between zero and one")
        if not field.nullable and null_rate:
            raise ValueError("required field cannot generate NULL")
        if field.type == "int64":
            opts["min"] = int64(opts.get("min", -1000000))
            opts["max"] = int64(opts.get("max", 1000000))
        elif field.type == "timestamp_us_utc":
            opts["min"] = timestamp_us(opts.get("min", "2020-01-01T00:00:00.000000Z"))
            opts["max"] = timestamp_us(opts.get("max", "2021-01-01T00:00:00.000000Z"))
        elif field.type == "float64":
            for name, default in (("min", -1000.0), ("max", 1000.0)):
                value = opts.get(name, default)
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError("float bounds must be finite numbers")
                opts[name] = float(value)
            if not math.isfinite(opts["max"] - opts["min"]):
                raise ValueError("float range must be finite")
        elif field.type == "string":
            length = opts.setdefault("length", 24)
            alphabet = opts.setdefault(
                "alphabet",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            )
            if type(length) is not int or not 0 <= length <= 4096:
                raise ValueError("string length must be between zero and 4096")
            if (
                not isinstance(alphabet, str)
                or not alphabet
                or len(alphabet.encode("utf-8")) > 4096
            ):
                raise ValueError("alphabet must be nonempty bounded UTF-8")
        if "min" in opts and opts["min"] > opts["max"]:
            raise ValueError("minimum exceeds maximum")
        normalized[field.name] = opts
    key = dict(settings.get("key", {}))
    if set(key) - {"profile", "distinct", "prefix"}:
        raise ValueError("unknown key profile option")
    key.setdefault("profile", "unique")
    key.setdefault("distinct", 100)
    key.setdefault("prefix", "row-")
    if (
        key["profile"] not in {"unique", "duplicates"}
        or type(key["distinct"]) is not int
        or key["distinct"] < 1
    ):
        raise ValueError("invalid key profile")
    if not isinstance(key["prefix"], str) or len(key["prefix"].encode("utf-8")) > 900:
        raise ValueError("key prefix must fit the SDK key limit")
    return {"fields": normalized, "key": key}


def generate(
    config, *, rows, seed, output, part_rows=1000, locale="en", part_bytes=4 * 1024**2
):
    from mimesis import Field, Schema as MimesisSchema

    schema = Schema.from_dict(config.get("schema", config))
    if not any(f.name == "key" for f in schema.fields):
        raise ValueError("GlamDB dataset requires a key field")
    if type(rows) is not int or rows < 0 or type(seed) is not int:
        raise ValueError("rows must be nonnegative and seed must be an integer")
    if type(part_rows) is not int or not 1 <= part_rows <= 10000:
        raise ValueError("part_rows must be between one and 10000")
    if type(part_bytes) is not int or not 1024 <= part_bytes <= MAX_PART_BYTES:
        raise ValueError("part_bytes must be between 1024 and 8 MiB")
    settings = _settings(config, schema)
    field = Field(locale, seed=seed)
    index = 0

    def row():
        nonlocal index
        result = {}
        for spec in schema.fields:
            if spec.name == "key":
                profile = settings["key"]
                key = (
                    index % profile["distinct"]
                    if profile["profile"] == "duplicates"
                    else index
                )
                result[spec.name] = profile["prefix"] + str(key)
                continue
            opts = settings["fields"][spec.name]
            if (
                opts["null_rate"]
                and field("numeric.float_number", start=0.0, end=1.0)
                < opts["null_rate"]
            ):
                value = None
            elif spec.type in {"int64", "timestamp_us_utc"}:
                value = field(
                    "numeric.integer_number", start=opts["min"], end=opts["max"]
                )
            elif spec.type == "float64":
                value = field(
                    "numeric.float_number", start=opts["min"], end=opts["max"]
                )
            elif spec.type == "bool":
                value = field("development.boolean")
            else:
                value = "".join(
                    field(
                        "random.choices", population=opts["alphabet"], k=opts["length"]
                    )
                )
            result[spec.name] = value
        index += 1
        return encode_rows(schema, [result])[0]

    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "version": 1,
        "format": "glamdb-jsonl",
        "schema": schema.to_dict(),
        "rows": rows,
        "seed": seed,
        "locale": locale,
        "generation": settings,
        "part_rows": part_rows,
        "part_bytes": part_bytes,
        "versions": {
            "mimesis": version("mimesis"),
            "glamdb-sdk": version("glamdb-sdk"),
            "python": platform.python_version(),
        },
        "parts": [],
    }
    block = bytearray()
    count = 0

    def flush():
        nonlocal count
        name = f'part-{len(manifest["parts"]):08d}.jsonl'
        (output / name).write_bytes(block)
        manifest["parts"].append(
            {
                "path": name,
                "rows": count,
                "bytes": len(block),
                "sha256": hashlib.sha256(block).hexdigest(),
            }
        )
        block.clear()
        count = 0

    for value in MimesisSchema(row, iterations=rows) if rows else ():
        encoded = json_bytes(value) + b"\n"
        if len(encoded) > part_bytes:
            raise ValueError("generated row exceeds part_bytes; reduce string lengths")
        if count and (count == part_rows or len(block) + len(encoded) > part_bytes):
            flush()
        block.extend(encoded)
        count += 1
    if count:
        flush()
    write_json(output / "manifest.json", manifest)
    return manifest


class Dataset:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.manifest = json.loads((self.path / "manifest.json").read_bytes())
        m = self.manifest
        if m.get("version") != 1 or m.get("format") != "glamdb-jsonl":
            raise ValueError("unsupported dataset manifest")
        self.schema = Schema.from_dict(m["schema"])
        if (
            type(m["rows"]) is not int
            or m["rows"] < 0
            or not isinstance(m["parts"], list)
        ):
            raise ValueError("invalid manifest row count or parts")
        names = set()
        for part in m["parts"]:
            name = part["path"]
            if (
                not isinstance(name, str)
                or Path(name).name != name
                or name in names
                or name in {".", ".."}
            ):
                raise ValueError("unsafe or duplicate dataset part path")
            if (self.path / name).resolve().parent != self.path:
                raise ValueError("dataset part path escapes dataset")
            names.add(name)
            if (
                type(part["rows"]) is not int
                or not 1 <= part["rows"] <= 10000
                or type(part["bytes"]) is not int
                or not 0 < part["bytes"] <= MAX_PART_BYTES
            ):
                raise ValueError("invalid bounded part size")
        if sum(p["rows"] for p in m["parts"]) != m["rows"]:
            raise ValueError("manifest row count differs from parts")

    def iter_parts(self):
        for part in self.manifest["parts"]:
            with (self.path / part["path"]).open("rb") as source:
                content = source.read(MAX_PART_BYTES + 1)
            if (
                len(content) != part["bytes"]
                or hashlib.sha256(content).hexdigest() != part["sha256"]
            ):
                raise ValueError("dataset part size/hash mismatch")
            values = [json.loads(line) for line in content.splitlines()]
            if len(values) != part["rows"]:
                raise ValueError("dataset part row count mismatch")
            yield decode_rows(self.schema, values)
