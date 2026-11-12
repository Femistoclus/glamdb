#!/usr/bin/env python3
"""Apply an optional source profile to the four-state alert configuration."""

import argparse
from copy import deepcopy
import json
from pathlib import Path


def extend(base, profile):
    result = deepcopy(base)
    result["selectors"].update(profile.get("selectors", {}))
    checks = {check["id"]: check for check in result["checks"]}
    if len(checks) != len(result["checks"]):
        raise ValueError("duplicate base check ID")
    for check in profile.get("checks", []):
        if check["id"] in checks:
            raise ValueError("profile check already exists: " + check["id"])
        checks[check["id"]] = deepcopy(check)
    for name, changes in profile.get("overrides", {}).items():
        if name not in checks:
            raise ValueError("override refers to an unknown check: " + name)
        if "id" in changes:
            raise ValueError("overrides cannot change check identity")
        checks[name].update(deepcopy(changes))
    result["checks"] = list(checks.values())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        config = extend(json.loads(args.base.read_text()), json.loads(args.profile.read_text()))
    except (KeyError, ValueError) as error:
        parser.exit(1, str(error) + "\n")
    args.output.write_text(json.dumps(config, indent=2) + "\n")


if __name__ == "__main__":
    main()
