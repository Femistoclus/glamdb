"""Run only the development adapter; no real store is opened or created."""

import argparse
import ipaddress
import os
from pathlib import Path

import uvicorn

from . import create_app


def main(argv=None):
    parser = argparse.ArgumentParser(description="Disposable GlamDB MOCK server; never production storage")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--ui-dir", type=Path, required=True)
    parser.add_argument("--theme-file", type=Path)
    parser.add_argument("--seed-demo", action="store_true")
    parser.add_argument("--enable-development-faults", action="store_true")
    parser.add_argument("--allow-non-loopback", action="store_true")
    args = parser.parse_args(argv)
    try:
        loopback = ipaddress.ip_address(args.host).is_loopback
    except ValueError:
        loopback = args.host.lower() == "localhost"
    if not loopback and not args.allow_non_loopback:
        parser.error("non-loopback binding requires explicit --allow-non-loopback")
    if not 1 <= args.port <= 65535:
        parser.error("port must be 1..65535")
    token = os.environ.get("GLAMDB_TOKEN", "")
    if not token:
        parser.error("GLAMDB_TOKEN is required")
    try:
        app = create_app(token=token, ui_dir=args.ui_dir, theme_file=args.theme_file,
                         seed_demo=args.seed_demo, enable_development_faults=args.enable_development_faults)
    except ValueError:
        parser.error("GLAMDB_TOKEN must be a nonempty bearer token without whitespace")
    uvicorn.run(app, host=args.host, port=args.port, access_log=False, log_level="warning")


if __name__ == "__main__":
    main()
