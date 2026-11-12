"""Parquet write settings: value compression of data files, not HTTP Content-Encoding.

Settings are plain JSON objects in the wire shape. Canonical form always has
``{"parquet": {"compression": {...} | None, "columns": [...]}}`` with columns in
schema order and an explicit ``level`` (``None`` selects the codec default).
Settings reported by a server may carry newer options this SDK does not know:
unset (``None``) ones are ignored, set ones cannot be represented and are refused.
"""

from typing import Any, Mapping

from .codec import Schema, int64
from .errors import ValidationError

PARQUET_CODECS = ("uncompressed", "snappy", "gzip", "lz4_raw", "zstd")


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
    parquet = {} if parquet is None else _object(parquet, ("compression", "columns"), "write_settings.parquet",
                                                 reported)
    compression = _compression(parquet.get("compression"), "Parquet compression", reported)
    columns = parquet.get("columns")
    columns = [] if columns is None else columns
    if not isinstance(columns, (list, tuple)) or len(columns) > 64:
        raise ValidationError("Parquet column settings must be a list of at most 64 entries")
    names = {field.name: index for index, field in enumerate(schema.fields)} if schema is not None else None
    seen: dict[str, dict[str, Any]] = {}
    for item in columns:
        item = _object(item, ("field", "compression"), "Parquet column setting", reported)
        field = item.get("field")
        if not isinstance(field, str) or not field:
            raise ValidationError("Parquet column setting requires a field name")
        if names is not None and field not in names:
            raise ValidationError(f"Parquet column setting names an unknown field: {field}")
        if field in seen:
            raise ValidationError(f"Parquet column setting repeats field: {field}")
        column_compression = _compression(item.get("compression"), f"Parquet compression of {field}", reported)
        if column_compression is None:
            raise ValidationError(f"Parquet column setting for {field} must set compression")
        seen[field] = {"field": field, "compression": column_compression}
    ordered = list(seen.values()) if names is None else sorted(seen.values(), key=lambda item: names[item["field"]])
    return {"parquet": {"compression": compression, "columns": ordered}}


def write_settings_version(value: Any) -> int:
    """Parse the decimal-string version reported with a collection."""
    if not isinstance(value, str):
        raise ValidationError("write_settings_version must be a decimal string")
    version = int64(value)
    if version < 0 or str(version) != value:
        raise ValidationError("write_settings_version must be a canonical non-negative decimal string")
    return version
