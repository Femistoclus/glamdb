"""Physical ClickBench targets without changing the 105 logical source fields."""

from glamdb_sdk import AppendRequest, Field, MutationRequest, Schema
from glamdb_sdk.request import ANY

KEY_ENCODING = "source-row-ordinal-decimal-v1"


def collection_mode(saved):
    mode = saved.get("collection_mode", "append")
    if mode not in ("append", "kv"):
        raise ValueError("ClickBench collection mode must be append or kv")
    if mode == "kv" and saved.get("key_encoding", KEY_ENCODING) != KEY_ENCODING:
        raise ValueError("ClickBench KV key encoding differs from the original run")
    return mode


def target_schema(source, mode):
    if collection_mode({"collection_mode": mode}) == "append":
        return source
    return Schema([*source.fields, Field("key", "string", field_id=max(f.field_id for f in source.fields) + 1)])


def target_rows(rows, start, mode):
    if collection_mode({"collection_mode": mode}) == "append":
        return rows
    if type(start) is not int or start < 0 or start + len(rows) > 2**63:
        raise ValueError("ClickBench source row ordinal is outside int64")
    # Neither WatchID nor UserID is unique. A source ordinal preserves duplicate
    # full rows and stays stable across chunk sizes, workers and retries.
    return [dict(row, key=f"{start + offset:020d}") for offset, row in enumerate(rows)]


def prepare_request(saved, schema, operation, rows, start, expected_snapshot=ANY):
    mode = collection_mode(saved)
    if mode == "kv":
        return MutationRequest.prepare("upsert", saved["collection_id"], operation,
                                       target_schema(schema, mode), rows=target_rows(rows, start, mode),
                                       expected_snapshot=expected_snapshot)
    return AppendRequest.prepare(saved["collection_id"], operation, schema, rows, expected_snapshot)
