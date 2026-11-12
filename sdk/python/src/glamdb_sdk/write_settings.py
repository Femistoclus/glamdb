"""Parquet write settings of data files, not HTTP Content-Encoding.

Settings are plain JSON objects in the wire shape. They choose the Parquet
compression codec/level, the dictionary policy and dictionary page size limit,
and per-column value encodings. Canonical form always has every option key::

    {"parquet": {"compression": {...} | None, "dictionary": str | None,
                 "dictionary_page_size_limit": "decimal" | None,
                 "columns": [{"field", "compression", "dictionary", "encoding"}, ...]}}

with columns in schema order, an explicit ``level`` (``None`` selects the codec
default) and ``None`` for every unset option. Field types, dictionary/encoding
conflicts and ranges are checked by the server. Settings reported by a server
may carry newer options this SDK does not know: unset (``None``) ones are
ignored, set ones cannot be represented and are refused.
"""

from typing import Any, Mapping

from .codec import MAX_SCHEMA_FIELDS, Schema, int64
from .errors import ValidationError

PARQUET_CODECS = ("uncompressed", "snappy", "gzip", "lz4_raw", "zstd")
DICTIONARY_POLICIES = ("auto", "enabled", "disabled")
PARQUET_ENCODINGS = ("plain", "delta_binary_packed", "delta_length_byte_array", "delta_byte_array",
                     "byte_stream_split", "rle")
_PARQUET_KEYS = ("compression", "dictionary", "dictionary_page_size_limit", "columns")
_COLUMN_KEYS = ("field", "compression", "dictionary", "encoding")


def _object(value: Any, keys: tuple[str, ...], label: str, reported: bool = False) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValidationError(f"{label} must be an object with only {', '.join(keys)}")
    unknown = [key for key in value if key not in keys]
    if unknown and not reported:
        raise ValidationError(f"{label} must be an object with only {', '.join(keys)}")
    if any(value[key] is not None for key in unknown):
        raise ValidationError(f"{label} sets options this SDK does not support: {', '.join(sorted(unknown))}")
    return value


def _compression(value: Any, label: str, reported: bool = False) -> dict[str, Any] | None:
    if value is None:
        return None
    value = _object(value, ("codec", "level"), label, reported)
    codec, level = value.get("codec"), value.get("level")
    if codec not in PARQUET_CODECS:
        raise ValidationError(f"{label} codec must be one of {', '.join(PARQUET_CODECS)}")
    # Ranges depend on the server build (capabilities["parquet"]["codecs"]);
    # the server checks them. Here a level only needs to be an int32.
    if level is not None and (type(level) is not int or not -(2**31) <= level < 2**31):
        raise ValidationError(f"{label} level must be an integer or None")
    return {"codec": codec, "level": level}


def _choice(value: Any, choices: tuple[str, ...], label: str) -> str | None:
    if value is not None and (not isinstance(value, str) or value not in choices):
        raise ValidationError(f"{label} must be one of {', '.join(choices)} or None")
    return value


def _page_size_limit(value: Any) -> str | None:
    if value is None:
        return None
    # The wire form is an int64 decimal string; the range (capabilities
    # parquet.dictionary.page_size_limit) is checked by the server.
    try:
        limit = int64(value)
    except ValidationError as exc:
        raise ValidationError("dictionary_page_size_limit must be an int64 or decimal string") from exc
    if limit <= 0:
        raise ValidationError("dictionary_page_size_limit must be positive")
    return str(limit)


def canonical_write_settings(value: Mapping[str, Any] | None, schema: Schema | None = None, *,
                             reported: bool = False) -> dict[str, Any]:
    """Validate the wire shape and return the canonical form.

    ``None``, ``{}`` and ``{"parquet": None}`` are the defaults. With a schema,
    columns must name its fields and are returned in schema order; without
    one, caller order is kept and the server checks the names. ``reported``
    accepts settings returned by a server, ignoring unknown unset options.
    """
    value = {} if value is None else _object(value, ("parquet",), "write_settings", reported)
    parquet = value.get("parquet")
    parquet = {} if parquet is None else _object(parquet, _PARQUET_KEYS, "write_settings.parquet", reported)
    compression = _compression(parquet.get("compression"), "Parquet compression", reported)
    dictionary = _choice(parquet.get("dictionary"), DICTIONARY_POLICIES, "Parquet dictionary policy")
    limit = _page_size_limit(parquet.get("dictionary_page_size_limit"))
    columns = parquet.get("columns")
    columns = [] if columns is None else columns
    if not isinstance(columns, (list, tuple)) or len(columns) > MAX_SCHEMA_FIELDS:
        raise ValidationError(f"Parquet column settings must be a list of at most {MAX_SCHEMA_FIELDS} entries")
    names = {field.name: index for index, field in enumerate(schema.fields)} if schema is not None else None
    seen: dict[str, dict[str, Any]] = {}
    for item in columns:
        item = _object(item, _COLUMN_KEYS, "Parquet column setting", reported)
        field = item.get("field")
        if not isinstance(field, str) or not field:
            raise ValidationError("Parquet column setting requires a field name")
        if names is not None and field not in names:
            raise ValidationError(f"Parquet column setting names an unknown field: {field}")
        if field in seen:
            raise ValidationError(f"Parquet column setting repeats field: {field}")
        column = {
            "field": field,
            "compression": _compression(item.get("compression"), f"Parquet compression of {field}", reported),
            "dictionary": _choice(item.get("dictionary"), DICTIONARY_POLICIES, f"Dictionary policy of {field}"),
            "encoding": _choice(item.get("encoding"), PARQUET_ENCODINGS, f"Parquet encoding of {field}"),
        }
        if column["compression"] is None and column["dictionary"] is None and column["encoding"] is None:
            raise ValidationError(f"Parquet column setting for {field} must set compression, dictionary or encoding")
        seen[field] = column
    ordered = list(seen.values()) if names is None else sorted(seen.values(), key=lambda item: names[item["field"]])
    return {"parquet": {"compression": compression, "dictionary": dictionary, "dictionary_page_size_limit": limit,
                        "columns": ordered}}


def request_write_settings(canonical: Mapping[str, Any]) -> dict[str, Any]:
    """Request form of canonical settings: unset options are omitted.

    Settings that only use compression then remain acceptable to servers that
    predate dictionary and encoding options.
    """
    parquet = {key: value for key, value in canonical["parquet"].items() if key != "columns" and value is not None}
    columns = [{key: value for key, value in column.items() if value is not None}
               for column in canonical["parquet"]["columns"]]
    return {"parquet": parquet | {"columns": columns}}


def write_settings_version(value: Any) -> int:
    """Parse the decimal-string version reported with a collection."""
    if not isinstance(value, str):
        raise ValidationError("write_settings_version must be a decimal string")
    version = int64(value)
    if version < 0 or str(version) != value:
        raise ValidationError("write_settings_version must be a canonical non-negative decimal string")
    return version
