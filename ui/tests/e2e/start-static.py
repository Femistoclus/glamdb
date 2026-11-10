"""Serve the exact packaged Pages output under its project path."""
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from functools import partial
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
directory = root / "artifacts/public-preview"
subprocess.run([sys.executable, str(root / "scripts/build-public-demo.py"),
                "--output", str(directory / "glamdb")], check=True)
class StaticHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass


handler = partial(StaticHandler, directory=str(directory))
server = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("GLAMDB_STATIC_PORT", "8768"))), handler)
server.serve_forever()
