"""Run the private OIDC workbench adapter behind OAuth2 Proxy."""

import argparse
import os
from pathlib import Path
import socket
import uvicorn
from .gateway import create_app
from .settings import Settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--operator-socket", required=True, type=Path)
    args = parser.parse_args()
    settings = Settings.load(args.config)
    args.operator_socket.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if args.operator_socket.exists() or args.operator_socket.is_symlink():
        parser.error(
            "operator socket already exists; verify the previous owning process has stopped before removing it"
        )
    os.umask(0o077)
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp.bind((args.host, args.port))
        tcp.listen(128)
        local.bind(str(args.operator_socket))
        os.chmod(args.operator_socket, 0o600)
        local.listen(16)
        config = uvicorn.Config(
            create_app(settings),
            access_log=False,
            log_level="warning",
            proxy_headers=False,
            limit_concurrency=settings.max_inflight * 2,
            timeout_keep_alive=5,
            timeout_graceful_shutdown=30,
        )
        uvicorn.Server(config).run(sockets=[tcp, local])
    finally:
        tcp.close()
        local.close()
        args.operator_socket.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
