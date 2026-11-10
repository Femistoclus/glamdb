"""Child-process entry point: explicitly synthetic server, optionally under TLS."""

import argparse
import os
from pathlib import Path
import socket

import uvicorn

from glamdb_mock import create_app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fd", required=True, type=int)
    parser.add_argument("--ui-dir", required=True, type=Path)
    parser.add_argument("--certfile")
    parser.add_argument("--keyfile")
    args = parser.parse_args()
    app = create_app(token=os.environ["GLAMDB_TOKEN"], ui_dir=args.ui_dir,
                     enable_development_faults=True)
    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=0, access_log=False, log_level="warning",
        ssl_certfile=args.certfile, ssl_keyfile=args.keyfile,
        timeout_graceful_shutdown=2))
    # The test parent reserves this socket before spawning; there is no
    # choose-port/close/rebind race between concurrent test processes.
    with socket.socket(fileno=args.fd) as listener:
        server.run(sockets=[listener])


if __name__ == "__main__":
    main()
