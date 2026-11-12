"""Disposable real native store for browser acceptance."""
import os
import json
from pathlib import Path
import signal
import subprocess
import tempfile

root = Path(__file__).resolve().parents[3]
server = Path(os.environ['GLAMDB_NATIVE_SERVER']).resolve()


def stop(*_):
    # Leave wait() so a child ignoring TERM still enters bounded cleanup.
    raise SystemExit(0)


with tempfile.TemporaryDirectory(prefix='glamdb-browser-',
        dir=os.environ.get('GLAMDB_E2E_TEMP_ROOT')) as directory:
    state_file = Path(os.environ['GLAMDB_E2E_STATE_FILE'])
    process = None
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps({'data_dir': str(Path(directory).resolve() / 'server')}))
        process = subprocess.Popen([str(server), '--data-dir', str(Path(directory).resolve() / 'server'),
            '--port', os.environ.get('GLAMDB_E2E_PORT', '8766'), '--ui-root', str(root / 'ui')],
            env=dict(os.environ, GLAMDB_TOKEN=os.environ.get('GLAMDB_E2E_TOKEN', 'e2e-only-local-token')))
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        raise SystemExit(process.wait())
    finally:
        # Repeated signals must not interrupt cleanup and strand the child.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        finally:
            state_file.unlink(missing_ok=True)
