"""Standalone tooling: python -m glamdb_verify --help."""

import argparse
import json
from pathlib import Path
import sys

from .evidence import check_evidence, fault_schedule
from .fixtures import check_fixture, generate_fixture
from .model import run_power_model


def main(argv=None):
    parser = argparse.ArgumentParser(description="Independent GlamDB fixture/oracle/evidence tools")
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate", help="generate a deterministic synthetic fixture")
    generate.add_argument("--seed", type=int, default=17)
    generate.add_argument("--rows", type=int, default=16)
    generate.add_argument("--output", type=Path)
    fixture = commands.add_parser("check-fixture", help="check literal canonical bytes and hashes")
    fixture.add_argument("path", type=Path)
    evidence = commands.add_parser("check-evidence", help="check receipts and evidence-label discipline")
    evidence.add_argument("path", type=Path)
    schedule = commands.add_parser("schedule", help="emit cut points; this never cuts a disk")
    schedule.add_argument("--seed", type=int, default=17)
    schedule.add_argument("--seeded", type=int, default=100)
    model = commands.add_parser("model", help="exercise an ordering model and three broken variants")
    model.add_argument("--seed", type=int, default=17)
    model.add_argument("--seeded", type=int, default=100)
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            result = generate_fixture(seed=args.seed, count=args.rows)
        elif args.command == "schedule":
            result = {"evidence_kind": "schedule", "power_durability": "NOT VALIDATED", "schedule": fault_schedule(args.seed, seeded=args.seeded)}
        elif args.command == "model":
            result = run_power_model(seed=args.seed, seeded=args.seeded)
        elif args.command == "check-fixture":
            errors = check_fixture(json.loads(args.path.read_text(encoding="utf-8")))
            result = {"verification_status": "FAIL" if errors else "PASS", "power_durability": "NOT VALIDATED", "errors": errors}
        else:
            result = check_evidence(json.loads(args.path.read_text(encoding="utf-8")))
        encoded = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
        if args.command == "generate" and args.output:
            args.output.write_text(encoded, encoding="utf-8")
        else:
            sys.stdout.write(encoded)
        return 1 if result.get("verification_status") == "FAIL" else 0
    except (ValueError, OSError, TypeError) as exc:
        print(json.dumps({"verification_status": "FAIL", "power_durability": "NOT VALIDATED", "errors": [str(exc)]}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
