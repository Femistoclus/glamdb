"""Strict version-3 mutation receipts. Existing append versions keep their profile."""
import re
from typing import Any, Mapping

from .codec import int64, timestamp_us
from .errors import ProtocolError


def validate_mutation_receipt(result: Mapping[str, Any]) -> None:
    def require(valid):
        if not valid:
            raise ProtocolError("invalid or incomplete KV mutation receipt")

    def number(name):
        value = result.get(name)
        require(isinstance(value, str) and re.fullmatch(r"0|-[1-9][0-9]*|[1-9][0-9]*", value, re.ASCII) is not None)
        return int64(value)

    kind = result.get("mutation_kind")
    require(kind in ("upsert", "delete", "compact") and type(result.get("snapshot_created")) is bool)
    values = {name: number(name) for name in (
        "snapshot_id", "rows_appended", "input_rows", "rows_affected", "rows_written", "rows_removed",
        "data_files_added", "data_files_removed", "compressed_data_bytes_added", "compressed_data_bytes_removed",
        "snapshot_created_at_us")}
    parent = result.get("parent_snapshot_id")
    require("parent_snapshot_id" in result)
    if parent is not None:
        number("parent_snapshot_id")
    profile = result.get("durability_profile")
    try:
        require(isinstance(profile, str) and 0 < len(profile.encode("utf-8")) <= 128)
    except UnicodeError as exc:
        raise ProtocolError("invalid KV receipt durability profile") from exc
    require(values["rows_appended"] == 0)
    for name in ("input_rows", "rows_written", "rows_removed"):
        require(0 <= values[name] <= 25600000)
    require(0 <= values["rows_affected"] <= values["input_rows"])
    for suffix in ("added", "removed"):
        files = values["data_files_" + suffix]
        require(0 <= files <= 256 and 0 <= values["compressed_data_bytes_" + suffix] <= files * 33554432)
    if kind == "compact":
        require(values["input_rows"] == 0)
    if not result["snapshot_created"]:
        require(values["snapshot_id"] == (0 if parent is None else int64(parent)))
        require(values["snapshot_created_at_us"] == 0)
        require(all(values[name] == 0 for name in ("rows_affected", "rows_written", "rows_removed",
            "data_files_added", "data_files_removed", "compressed_data_bytes_added", "compressed_data_bytes_removed")))
        return
    require(values["snapshot_id"] != 0 and str(values["snapshot_id"]) != parent)
    timestamp_us(values["snapshot_created_at_us"])
    require(values["snapshot_created_at_us"] % 1000 == 0)
    added, removed = values["data_files_added"], values["data_files_removed"]
    require(added > 0 or removed > 0)
    for suffix, rows in (("added", "rows_written"), ("removed", "rows_removed")):
        files = values["data_files_" + suffix]
        require((files == 0) == (values[rows] == 0) == (values["compressed_data_bytes_" + suffix] == 0))
        require(values[rows] <= files * 100000)
    if kind == "compact":
        require(values["input_rows"] == values["rows_affected"] == 0 and values["rows_written"] == values["rows_removed"])
        require(added >= 1 and removed >= added * 4)
    else:
        require(values["input_rows"] > 0 and values["rows_affected"] > 0)
        if kind == "delete":
            require(values["rows_removed"] - values["rows_written"] == values["rows_affected"])
        else:
            require(values["rows_written"] >= values["rows_removed"] and values["rows_written"] - values["rows_removed"] <= values["rows_affected"])
