"""Run against a configured HTTP adapter; explicitly supply and retain both UUIDs."""

import argparse
import os
from uuid import UUID

from glamdb_sdk import Client, Field, ProtocolError, Schema, TransportError, col


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection_id", type=UUID)
    parser.add_argument("operation_id", type=UUID)
    parser.add_argument("--name", default="python-sdk-demo")
    args = parser.parse_args()
    token = os.environ.get("GLAMDB_TOKEN")
    if not token:
        parser.error("GLAMDB_TOKEN is required")
    schema = Schema([Field("key", "string", False), Field("n", "int64", True),
                     Field("at", "timestamp_us_utc", True)])
    print(f"collection_id={args.collection_id} operation_id={args.operation_id}", flush=True)
    with Client(os.environ.get("GLAMDB_URL", "http://127.0.0.1:8765"), token) as client:
        print("Capabilities:", client.capabilities())
        try:
            created = client.create_collection(args.name, schema, collection_id=args.collection_id)
        except (TransportError, ProtocolError):
            print("Create response unavailable; resolving exact name and checking original UUID/schema.")
            client.resolve_create(args.name, schema, collection_id=args.collection_id)
        else:
            if created.outcome != "committed":
                print("Create outcome:", created.raw)
                return 2
        request = client.prepare_append(args.collection_id, [
            {"key": "duplicate", "n": 9007199254740993, "at": "0001-01-01T00:00:00Z"},
            {"key": "duplicate", "n": None, "at": "9999-12-31T23:59:59.999999Z"},
        ], operation_id=args.operation_id, schema=schema)
        print("Prepared payload hash:", request.payload_hash, flush=True)
        try:
            outcome = client.append(request, deadline=60)
        except (TransportError, ProtocolError):
            print("Append response unavailable; retain original IDs/payload and use find_append_outcome.")
            return 2
        print("Append outcome:", outcome.raw)
        if outcome.outcome != "committed":
            return 2
        print("Filtered rows:", client.scan_json(args.collection_id, predicate=col("n", "int64").ge(9007199254740993)).rows)
        with client.scan_batches(args.collection_id) as stream:
            for batch in stream:
                print("Arrow batch:", batch.to_pylist())
        print("Fixed snapshot:", stream.snapshot_id, "integrity:", stream.integrity)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
