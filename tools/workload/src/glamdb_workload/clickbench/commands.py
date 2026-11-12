"""CLI11/Python command adapter; dependencies are imported only for execution."""

import json
from pathlib import Path


def positive(value):
    import argparse

    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def add_parser(benches):
    commands = benches.add_parser("clickbench").add_subparsers(dest="clickbench_command", required=True)
    fetch = commands.add_parser("fetch-queries")
    fetch.add_argument("--output", required=True)
    query = commands.add_parser("query")
    for name in ("state", "catalog", "report"):
        query.add_argument("--" + name, required=True)
    query.add_argument("--source")
    query.add_argument("--ids", default="all")
    query.add_argument("--mode", dest="query_mode", choices=("scan", "reduce", "both"), default="both")
    query.add_argument("--warmup", type=int, default=1)
    query.add_argument("--repeats", type=positive, default=3)
    query.add_argument("--timeout", type=float, default=120)
    query.add_argument("--ca-file")
    for name, default in (("memory-bytes", 512*1024**2), ("temp-bytes", 2*1024**3),
                          ("max-candidate-bytes", 8*1024**3), ("max-result-rows", 100000),
                          ("max-result-bytes", 64*1024**2)):
        query.add_argument("--"+name, type=positive, default=default)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--source", required=True)
    prepare.add_argument("--output", required=True)
    prepare.add_argument("--type-profile", required=True, choices=("legacy-date", "native"))
    prepare.add_argument("--rows", type=int)
    prepare.add_argument("--part-rows", type=positive, default=10000)
    prepare.add_argument("--part-bytes", type=positive, default=4 * 1024**2)
    prepare.add_argument("--max-source-bytes", type=positive, default=32 * 1024**3)
    prepare.add_argument("--max-converted-bytes", type=positive, default=8 * 1024**3)
    for action in ("init", "load", "status", "resolve", "verify", "size", "clean"):
        command = commands.add_parser(action)
        command.add_argument("--state", required=True)
        command.add_argument("--ca-file")
        command.add_argument("--timeout", type=float, default=120 if action == "verify" else 30)
        if action == "init":
            command.add_argument("--dataset", required=True)
            command.add_argument("--url", required=True)
            command.add_argument("--name", required=True)
            command.add_argument("--limits")
            command.add_argument("--max-journal-bytes", type=positive, default=1024**3)
            command.add_argument("--test-backend", choices=("mock",))
        elif action == "load":
            command.add_argument("--resume", action="store_true")
        elif action == "resolve":
            command.add_argument("--retry", action="store_true")
        elif action == "verify":
            command.add_argument("--format", choices=("arrow", "json"), default="arrow")
        elif action == "size":
            command.add_argument("--report", required=True)
            command.add_argument("--include-footers", action="store_true")
            command.add_argument("--owned-store-dir")


def run(args):
    from . import ingest

    try:
        if args.clickbench_command == "fetch-queries":
            from .queries import fetch_material

            result = fetch_material(args.output)
        elif args.clickbench_command == "query":
            from .run import campaign

            result = campaign(args.state, args.catalog, args.report, source=args.source, ids=args.ids,
                              mode=args.query_mode, warmup=args.warmup, repeats=args.repeats,
                              timeout=args.timeout, ca_file=args.ca_file, memory_bytes=args.memory_bytes,
                              temp_bytes=args.temp_bytes, max_candidate_bytes=args.max_candidate_bytes,
                              max_result_rows=args.max_result_rows, max_result_bytes=args.max_result_bytes)
        elif args.clickbench_command == "prepare":
            from .dataset import prepare

            manifest = prepare(args.source, args.output, type_profile=args.type_profile,
                               rows=args.rows, part_rows=args.part_rows, part_bytes=args.part_bytes,
                               max_source_bytes=args.max_source_bytes,
                               max_converted_bytes=args.max_converted_bytes)
            result = {"status": "PASS", "rows": manifest["rows"], "parts": len(manifest["parts"]),
                      "manifest": str(Path(args.output).absolute() / "manifest.json")}
        else:
            common = {"timeout": args.timeout, "ca_file": args.ca_file}
            if args.clickbench_command == "init":
                from .model import read_json

                state = ingest.initialize(
                    args.dataset, args.state, url=args.url, name=args.name,
                    limits=read_json(args.limits) if args.limits else None,
                    max_journal_bytes=args.max_journal_bytes, test_backend=args.test_backend, **common)
                result = {"status": "PASS", "phase": state["phase"], "database_id": state["database_id"],
                          "collection_id": state["collection_id"],
                          "performance_eligible": state["performance_eligible"], "plan": state["plan"]}
            elif args.clickbench_command == "size":
                from .measure import measure

                result = measure(args.state, args.report, owned_store_dir=args.owned_store_dir,
                                 include_footers=args.include_footers, **common)
            else:
                extra = {}
                if args.clickbench_command == "load":
                    extra["resume"] = args.resume
                elif args.clickbench_command == "resolve":
                    extra["retry"] = args.retry
                elif args.clickbench_command == "verify":
                    extra["format"] = args.format
                result = getattr(ingest, args.clickbench_command)(args.state, **common, **extra)
    except ingest.Blocked as error:
        result = {"status": "BLOCKED", "reason": str(error)}
    print(json.dumps(result, ensure_ascii=False))
    return {"PASS": 0, "FAIL": 1, "INCOMPLETE": 3, "BLOCKED": 3}[result["status"]]
