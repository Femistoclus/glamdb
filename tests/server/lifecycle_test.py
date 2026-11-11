"""Asio lifecycle with a deterministic core publication barrier (test backend)."""
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve())) as directory:
    process = subprocess.Popen([sys.argv[1], str(Path(directory) / 'store')],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert select.select([process.stdout], [], [], 10)[0], 'no listening report'
        port = json.loads(process.stdout.readline())['port']
        body = json.dumps({'operation_id': '00000000-0000-0000-0000-000000000003',
                           'rows': [{'key': 'interrupted-client'}]}).encode()
        with socket.create_connection(('127.0.0.1', port)) as connection:
            connection.sendall((f'POST /collections/00112233-4455-6677-8899-aabbccddeeff/append HTTP/1.1\r\n'
                f'Host: localhost\r\nAuthorization: Bearer test-token\r\nContent-Type: application/json\r\n'
                f'Content-Length: {len(body)}\r\n\r\n').encode() + body)
            assert select.select([process.stdout], [], [], 10)[0], 'publication did not start'
            assert process.stdout.readline().strip() == 'publication'
        # Disconnect and stop while publication is blocked, before any ACK.
        process.terminate()
        try:
            process.wait(timeout=0.2)
        except subprocess.TimeoutExpired:
            pass
        else:
            raise AssertionError('server exited while a core publication was active')
        output, errors = process.communicate('\n', timeout=10)
        assert process.returncode == 0, (output, errors)
        outcome = json.loads(output)
        assert outcome['outcome'] == 'committed', outcome
        assert outcome['result']['rows_appended'] == '1', outcome
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
