"""Internal argument adapter; the user-facing entry point is CLI11."""

import argparse
import importlib.util
import json
import math
import os
import platform
import re
import sys
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4


def positive(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def duration(value):
    if re.fullmatch(r"[1-9][0-9]*[smh]", value) is None:
        raise argparse.ArgumentTypeError(
            "use a positive whole duration, such as 60s, 5m or 1h"
        )
    return value


def parser():
    app = argparse.ArgumentParser(prog="glamdb-cli")
    commands = app.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate")
    for name in ("schema", "output"):
        generate.add_argument("--" + name, required=True)
    generate.add_argument("--rows", required=True, type=int)
    generate.add_argument("--seed", required=True, type=int)
    generate.add_argument("--locale", default="en")
    generate.add_argument("--part-rows", type=positive, default=1000)
    generate.add_argument("--part-bytes", type=positive, default=4 * 1024**2)
    benches = commands.add_parser("bench").add_subparsers(dest="mode", required=True)
    for mode in ("write", "read", "verify", "resolve"):
        command = benches.add_parser(mode)
        command.add_argument("--url", required=True)
        command.add_argument("--ca-file")
        command.add_argument("--timeout", type=float, default=30.0)
        if mode in {"verify", "resolve"}:
            command.add_argument("--run", required=True)
            if mode == "resolve":
                command.add_argument("--retry", action="store_true")
            else:
                command.add_argument(
                    "--format", choices=["json", "arrow"], default="json"
                )
            continue
        for name in ("database", "collection", "report"):
            command.add_argument("--" + name, required=True)
        command.add_argument("--users", type=positive, default=1)
        command.add_argument("--spawn-rate", type=float, default=1.0)
        command.add_argument("--duration", type=duration, default="60s")
        command.add_argument("--stop-timeout", type=int, default=3)
        command.add_argument("--max-journal-bytes", type=positive, default=1024**3)
        group = command.add_mutually_exclusive_group()
        group.add_argument("--master", action="store_true")
        group.add_argument("--worker", action="store_true")
        command.add_argument("--master-host", default="127.0.0.1")
        command.add_argument("--master-port", type=positive, default=5557)
        command.add_argument("--expect-workers", type=positive, default=1)
        if mode == "write":
            command.add_argument("--dataset", required=True)
        else:
            command.add_argument("--query", required=True)
            command.add_argument("--format", choices=["json", "arrow"], default="json")
    return app


def _extra(name, package):
    if importlib.util.find_spec(package) is None:
        raise ModuleNotFoundError(
            f"Install: python -m pip install -e 'tools/workload[{name}]'", name=package
        )


def _bench(args):
    from glamdb_sdk import Schema
    from glamdb_sdk.request import uuid_string
    from .dataset import Dataset, write_json
    from .ledger import Ledger
    from .runtime import (
        client_for,
        preflight,
        resolve_ledger,
        status_code,
        summarize,
        validate_url,
    )

    args.url = validate_url(args.url)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError("timeout must be positive finite seconds")
    if args.mode in {"verify", "resolve"}:
        run = Path(args.run).resolve()
        config = json.loads((run / "run.json").read_bytes())
        if config["url"] != args.url:
            raise ValueError(
                "endpoint differs from the original run; refusing to resolve on another server"
            )
        if config["mode"] != "write":
            raise ValueError("verify/resolve requires a write run")
        config.update(
            timeout=args.timeout, ca_file=args.ca_file or config.get("ca_file")
        )
        totals = summarize(run)
        expected = config["expect_workers"] if config.get("master") else 1
        if len(totals["journals"]) != expected:
            result = {
                "status": "INCOMPLETE",
                "reason": "collect every worker journal before resolving or verifying",
            }
        elif args.mode == "resolve":
            with client_for(config) as client:
                for name in totals["journals"]:
                    with Ledger(run / name) as log:
                        resolve_ledger(
                            client, log, retry=args.retry, deadline=args.timeout
                        )
            totals = summarize(run)
            result = {
                "status": (
                    "INCOMPLETE"
                    if totals["pending"]
                    else "FAIL" if totals["definitely_not_committed"] else "PASS"
                ),
                "operations": totals,
            }
        else:
            from .verification import verify_ledgers

            with client_for(config) as client:
                schema = client.get_schema(config["collection"], deadline=args.timeout)
                if schema != Schema.from_dict(config["target"]["schema"]):
                    raise ValueError("collection schema differs from the original run")
                result = verify_ledgers(
                    client,
                    run,
                    schema,
                    config["collection"],
                    format=args.format,
                    deadline=args.timeout,
                )
        write_json(run / (args.mode + ".json"), result)
        print(json.dumps(result, ensure_ascii=False))
        return status_code(result["status"])
    _extra("bench", "locust")
    if not math.isfinite(args.spawn_rate) or args.spawn_rate <= 0:
        raise ValueError("spawn-rate must be positive finite users per second")
    if not math.isfinite(args.stop_timeout) or not 0 <= args.stop_timeout <= 3:
        raise ValueError("stop-timeout must be between zero and three seconds")
    if not 1 <= args.master_port <= 65535:
        raise ValueError("master-port must fit a TCP port")
    config = vars(args).copy()
    config["database"] = uuid_string(args.database, "database")
    config["collection"] = uuid_string(args.collection, "collection")
    run = Path(args.report).resolve()
    config["report"] = str(run)
    config["ca_file"] = str(Path(args.ca_file).resolve()) if args.ca_file else None
    data = None
    if args.mode == "write":
        data = Dataset(args.dataset)
        # Validate every part outside the measured phase without retaining it.
        for _ in data.iter_parts():
            pass
        config["dataset"] = str(data.path)
        config["dataset_manifest"] = data.manifest
    else:
        config["query"] = json.loads(Path(args.query).read_bytes())
    if args.worker:
        original = json.loads((run / "run.json").read_bytes())
        for key in (
            "mode",
            "url",
            "database",
            "collection",
            "dataset_manifest",
            "query",
            "format",
        ):
            if config.get(key) != original.get(key):
                raise ValueError("worker parameters differ from the shared master run")
        if not original.get("master"):
            raise ValueError(
                "worker requires a master run in the shared report directory"
            )
        # Paths may differ across hosts; all processes must see the same files.
        config["run_id"] = original["run_id"]
        config["target"] = preflight(config, dataset=data)
        worker_config = run / ("launch-" + str(uuid4()) + ".json")
        write_json(worker_config, config)
        config_path = worker_config
    else:
        config["target"] = preflight(config, dataset=data)
        run.mkdir(parents=True, exist_ok=False)
        config["run_id"] = str(uuid4())
        config["client"] = {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "locust": version("locust"),
            "workload": version("glamdb-workload"),
            "pyarrow": version("pyarrow"),
        }
        config["measurement"] = (
            "SDK scenario; file reads, encoding and SQLite journal included in run throughput"
        )
        config["dataset_distribution"] = (
            "one complete dataset per worker; duplicate rows across workers are retained"
        )
        config_path = run / "run.json"
        write_json(config_path, config)
        write_json(
            run / "summary.json",
            {"status": "INCOMPLETE", "reason": "run has not finished"},
        )
    os.execv(
        sys.executable, [sys.executable, "-m", "glamdb_workload.load", str(config_path)]
    )


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == "generate":
            _extra("generate", "mimesis")
            from .dataset import generate

            result = generate(
                json.loads(Path(args.schema).read_bytes()),
                rows=args.rows,
                seed=args.seed,
                output=args.output,
                locale=args.locale,
                part_rows=args.part_rows,
                part_bytes=args.part_bytes,
            )
            print(
                json.dumps(
                    {
                        "rows": result["rows"],
                        "parts": len(result["parts"]),
                        "manifest": str(Path(args.output) / "manifest.json"),
                    }
                )
            )
            return 0
        return _bench(args)
    except ModuleNotFoundError as error:
        print(
            "Install dependencies: python -m pip install -e sdk/python -e 'tools/workload[generate,bench,arrow]'",
            file=sys.stderr,
        )
        return 4
    except KeyboardInterrupt:
        return 130
    except (ValueError, OSError, KeyError, TypeError) as error:
        # Never serialize credentials or server response/request text.
        print(
            f"Workload configuration failed ({type(error).__name__}): {error}",
            file=sys.stderr,
        )
        return 2
    except Exception as error:
        from .runtime import error_code

        print("Workload failed: " + error_code(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
