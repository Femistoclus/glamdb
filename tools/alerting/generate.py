#!/usr/bin/env python3
"""Generate deterministic Prometheus rules from reviewable threshold configuration."""

import argparse
import json
import math
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[2]
STATES = ("OK", "WARN", "ALARM", "NODATA")
LABELS = ("cluster", "service", "namespace", "pod", "node")


def selector(labels):
    return ",".join(f"{k}={json.dumps(v)}" for k, v in sorted(labels.items()))


def rules(config):
    """One fixed inventory entry per check survives missing and stale inputs."""
    result = []
    seen = set()
    substitutions = config["selectors"]

    def expand(value):
        for key, replacement in substitutions.items():
            value = value.replace("$" + key, replacement)
        if "$" in value:
            raise ValueError("unknown selector placeholder")
        return value

    for check in config["checks"]:
        name = check["id"]
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name) or name in seen:
            raise ValueError("check IDs must be unique, bounded identifiers")
        seen.add(name)
        warn, alarm = check["warn"], check["alarm"]
        if not all(isinstance(x, (float, int)) and math.isfinite(x) for x in (warn, alarm)) or warn >= alarm:
            raise ValueError("finite warning threshold must be below alarm threshold")
        labels = {k: str(v) for k, v in config["scope"].items() if k in LABELS}
        labels.update(check.get("scope", {}))
        if set(labels) - set(LABELS):
            raise ValueError("unknown scope label")
        labels["check"] = name
        match = selector(labels)
        value = f"glamdb_alert_value{{{match}}}"
        state = f"glamdb_alert_state{{{match}}}"
        age = int(check.get("max_age_seconds", config["max_age_seconds"]))
        if not 15 <= age <= 3600 or not check.get("fresh"):
            raise ValueError("each check needs a bounded freshness window and raw source selectors")
        guards = [f"(time() - min(timestamp({expand(source)})) >= -5) and "
                  f"(time() - min(timestamp({expand(source)})) < {age})"
                  for source in check["fresh"]]
        guards += [f"({expand(x)})" for x in check.get("guards", [])]
        query = f"max({expand(check['query'])})"
        finite = f"({query} > -Inf) and ({query} < +Inf)"
        valid = f"({finite}) and on() (" + " and on() ".join(guards) + ")"
        result.append({"record": "glamdb_alert_value", "expr": valid, "labels": labels})
        # The comparisons are total only for a valid finite value; absent input
        # falls back to exactly one NODATA sample with the inventory's labels.
        result.append({"record": "glamdb_alert_state", "expr":
                       f"(({value} >= bool {warn}) + ({value} >= bool {alarm})) or on() vector(3)",
                       "labels": labels})
        for index, public_state in enumerate(STATES):
            alert_labels = {**labels, "state": public_state,
                            "notify_nodata": str(check.get("notify_nodata", False)).lower()}
            result.append({"alert": "GlamDBCheckState", "expr": f"{state} == {index}",
                           "for": config["hold"][public_state], "labels": alert_labels,
                           "annotations": {"summary": check["summary"],
                                           "description": check["description"],
                                           "runbook_url": config["runbook_url"],
                                           "observed_at": "{{ with query \"time()\" }}{{ . | first | value }}{{ end }}"}})
    return {"groups": [{"name": "glamdb-four-state", "interval": config["evaluation_interval"], "rules": result}]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "deploy/alerting/checks.json")
    parser.add_argument("--output", type=Path, default=ROOT / "deploy/alerting/generated/states.yml")
    parser.add_argument("--check", action="store_true", help="fail if generated rules differ")
    args = parser.parse_args()
    content = json.dumps(rules(json.loads(args.config.read_text())), indent=2) + "\n"
    if args.check:
        if not args.output.exists() or args.output.read_text() != content:
            parser.exit(1, "rules differ: run tools/alerting/generate.py\n")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content)


if __name__ == "__main__":
    main()
