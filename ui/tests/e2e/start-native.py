"""Disposable real native store for browser acceptance."""
import os
from pathlib import Path
import signal
import subprocess
import tempfile

root = Path(__file__).resolve().parents[3]
server = Path(os.environ['GLAMDB_NATIVE_SERVER']).resolve()
with tempfile.TemporaryDirectory(prefix='glamdb-browser-',
        dir=os.environ.get('GLAMDB_E2E_TEMP_ROOT')) as directory:
    process = subprocess.Popen([str(server), '--data-dir', str(Path(directory).resolve() / 'server'),
        '--port', os.environ.get('GLAMDB_E2E_PORT', '8766'), '--ui-root', str(root / 'ui')],
        env=dict(os.environ, GLAMDB_TOKEN='e2e-only-local-token'))
    signal.signal(signal.SIGTERM, lambda *_: process.terminate())
    signal.signal(signal.SIGINT, lambda *_: process.terminate())
    try:
        raise SystemExit(process.wait())
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=30)
