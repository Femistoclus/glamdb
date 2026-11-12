"""Managed HTTP + real storage; deterministic SQLite VFS faults, not power-loss simulation."""
import http.client
import json
import os
import select
import socket
import struct
import subprocess
import sys
import tempfile
import unittest
import uuid
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from tls_proxy import tls_proxy

DRIVER = sys.argv.pop(1)
TOKEN = 'fault-test-token'


class NativeFaultHTTP(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='glamdb-http-fault-', dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve()))
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name) / 'server'
        self.start()
        self.addCleanup(self.stop)
        self.database = self.api('/databases', {'name': 'faults'})['database_id']
        self.base = '/databases/' + self.database

    def start(self):
        arguments = [DRIVER, str(self.directory)]
        many_readers = self._testMethodName == 'test_32_and_64_concurrent_snapshot_sessions_finish_without_busy_refusals'
        if hasattr(self, 'connection_limit') or hasattr(self, 'request_timeout_ms') or many_readers:
            arguments.append(str(getattr(self, 'connection_limit', 96)))
        if hasattr(self, 'request_timeout_ms') or many_readers:
            arguments.append(str(getattr(self, 'request_timeout_ms', 10000)))
        if many_readers:
            arguments.append('many-readers')
        self.process = subprocess.Popen(arguments, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        self.assertTrue(select.select([self.process.stdout], [], [], 20)[0], 'server startup timed out')
        line = self.process.stdout.readline()
        self.assertTrue(line, 'fault server did not report its address')
        self.port = json.loads(line)['port']

    def stop(self, *, terminate=True):
        if terminate and self.process.poll() is None:
            self.process.terminate()
        out, err = self.process.communicate(timeout=20)
        self.assertEqual(self.process.returncode, 0, out + err)
        self.log_records = [json.loads(line) for line in err.splitlines() if line.startswith('{')]

    def arm(self, target, commit, after):
        self.process.stdin.write(f'{target} {commit} {int(after)}\n')
        self.process.stdin.flush()
        self.assertTrue(select.select([self.process.stdout], [], [], 10)[0], 'fault control timed out')
        self.assertEqual(self.process.stdout.readline().strip(), 'armed')

    def wait_for_publication(self, count=1):
        self.process.stdin.write(f'wait {count}\n')
        self.process.stdin.flush()
        self.assertTrue(select.select([self.process.stdout], [], [], 12)[0])
        self.assertEqual(self.process.stdout.readline().strip(), 'paused')

    def control(self, command):
        self.process.stdin.write(command + '\n')
        self.process.stdin.flush()
        self.assertTrue(select.select([self.process.stdout], [], [], 10)[0], 'control timed out')
        return self.process.stdout.readline().strip()

    def request(self, path, body=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=15)
        connection.request('GET' if body is None else 'POST', path,
            None if body is None else json.dumps(body),
            {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, json.loads(data)

    def api(self, path, body=None, status=200):
        code, data = self.request(path, body)
        self.assertEqual(code, status, data)
        return data

    def collection(self):
        cid = str(uuid.uuid4())
        body = {'collection_id': cid, 'name': cid,
                'schema': {'fields': [{'name': 'key', 'type': 'string'}]}}
        self.api(self.base + '/collections', body)
        return self.base + '/collections/' + cid

    def test_discard_only_removes_uninitialized_reservations(self):
        self.arm('core', 1, False)
        self.api('/databases', {'name': 'abandoned'}, status=503)
        self.arm('core', 0, False)
        abandoned = next(item for item in self.api('/databases')['items'] if item['name'] == 'abandoned')
        path = '/databases/' + abandoned['database_id']
        self.assertTrue(self.api(path + '/discard', {})['discarded'])
        self.api(path, status=404)
        self.api(self.base + '/discard', {}, status=412)
        self.assertEqual(len(self.api('/databases')['items']), 1)
        self.stop()
        self.start()
        self.api(path, status=404)
        replacement = self.api('/databases', {'name': 'abandoned'})
        self.assertNotEqual(replacement['database_id'], abandoned['database_id'])

    def test_connection_ceiling_waits_for_capacity_without_reset(self):
        self.stop()
        self.connection_limit = 2
        self.start()
        streams = []
        try:
            for _ in range(2):
                stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                streams.append(stream)
                # Continue proves this socket was accepted and owns a body slot.
                stream.sendall((f'POST {self.base}/open HTTP/1.1\r\nHost: local\r\n'
                    f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                    'Content-Length: 2\r\nExpect: 100-continue\r\n\r\n').encode())
                response = b''
                while not response.endswith(b'\r\n\r\n'):
                    chunk = stream.recv(1)
                    self.assertTrue(chunk, 'listener closed before Continue')
                    response += chunk
                self.assertEqual(response, b'HTTP/1.1 100 Continue\r\n\r\n')
            pending = socket.create_connection(('127.0.0.1', self.port), timeout=5)
            streams.append(pending)
            pending.sendall(b'GET /healthz HTTP/1.1\r\nHost: local\r\n\r\n')
            self.assertFalse(select.select([pending], [], [], 0.1)[0], 'listener must wait instead of closing overflow')
            streams[0].close()
            response = http.client.HTTPResponse(pending)
            response.begin()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read())['status'], 'live')
            # A second complete body header fills the ceiling again. Shutdown
            # must cancel the paused accept timer and close both idle bodies.
            pending.close()
            stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
            streams.append(stream)
            stream.sendall((f'POST {self.base}/open HTTP/1.1\r\nHost: local\r\n'
                f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                'Content-Length: 2\r\nExpect: 100-continue\r\n\r\n').encode())
            response = b''
            while not response.endswith(b'\r\n\r\n'):
                chunk = stream.recv(1)
                self.assertTrue(chunk, 'listener closed before Continue')
                response += chunk
            self.assertEqual(response, b'HTTP/1.1 100 Continue\r\n\r\n')
            self.stop()
        finally:
            for stream in streams:
                stream.close()

    def test_slow_uploads_preserve_control_body_capacity(self):
        self.check_control_body_capacity(self.port)

    def test_https_slow_uploads_preserve_control_body_capacity(self):
        with tls_proxy(self.port, self.tmp.name) as (port, context):
            self.check_control_body_capacity(port, context)

    def check_control_body_capacity(self, port, context=None):
        path = self.collection()
        cursor = self.api(path + '/scan/open', {})['cursor']
        with ExitStack() as stack:
            def read_head(stream):
                head = b''
                while not head.endswith(b'\r\n\r\n'):
                    chunk = stream.recv(1)
                    self.assertTrue(chunk, 'listener closed before an admission response')
                    head += chunk
                return head

            def upload(target, token=TOKEN, wait=True):
                stream = stack.enter_context(socket.create_connection(('127.0.0.1', port), timeout=5))
                if context is not None:
                    stream = stack.enter_context(context.wrap_socket(stream, server_hostname='localhost'))
                authorization = '' if token is None else f'Authorization: Bearer {token}\r\n'
                stream.sendall((f'POST {target} HTTP/1.1\r\nHost: local\r\n{authorization}'
                    'Content-Type: application/json\r\nContent-Length: 2\r\nExpect: 100-continue\r\n\r\n').encode())
                return stream, read_head(stream) if wait else b''

            def waiting(target, token=TOKEN):
                stream, _ = upload(target, token, wait=False)
                stream.settimeout(0.1)
                with self.assertRaises(socket.timeout):
                    stream.recv(1)
                stream.settimeout(5)
                return stream

            ordinary = []
            for _ in range(12):
                stream, head = upload(path + '/append')
                self.assertIn(b' 100 ', head)
                ordinary.append(stream)
            queued_ordinary = waiting(path + '/append')
            # Public paths cannot borrow authenticated control capacity.
            queued_public = waiting('/assets/close', token=None)
            self.assertIn(b' 401 ', upload(self.base + '/open', token='wrong')[1])
            self.assertEqual(self.api('/healthz')['status'], 'live')
            self.api(path + '/scan/%63ancel', {'cursor': cursor})
            self.api(self.base + '/reconcile', {})

            protected = []
            for target in (self.base + '/open', self.base + '/%6fpen',
                           self.base.replace('/databases/', '/%64atabases/') + '/open', self.base + '/open'):
                stream, head = upload(target)
                self.assertIn(b' 100 ', head)
                protected.append(stream)
            queued_control = waiting(self.base + '/open')
            # A freed reserved reader must bypass the ordinary queue head.
            protected[0].sendall(b'{}')
            response = http.client.HTTPResponse(protected[0])
            response.begin()
            self.assertEqual(response.status, 200, response.read())
            self.assertIn(b' 100 ', read_head(queued_control))
            # Disconnects release ordinary readers to waiting ordinary bodies.
            ordinary[0].close()
            self.assertIn(b' 100 ', read_head(queued_ordinary))
            ordinary[1].close()
            self.assertIn(b' 100 ', read_head(queued_public))

    def test_discards_waiting_on_close_do_not_starve_control_requests(self):
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'held'}]}
        self.arm('pause', 1, False)
        with ThreadPoolExecutor(max_workers=4) as workers:
            append = workers.submit(self.api, path + '/append', body)
            self.wait_for_publication()
            close = workers.submit(self.api, self.base + '/close', {})
            discards = []
            try:
                deadline = time.monotonic() + 5
                while self.api(self.base)['state'] != 'closing':
                    self.assertLess(time.monotonic(), deadline)
                health = self.api(self.base + '/readyz', status=503)
                self.assertEqual(health['state'], 'stopping')
                self.assertFalse(health['read_available'])
                self.assertFalse(health['write_ready'])
                discards = [workers.submit(self.request, self.base + '/discard', {}) for _ in range(2)]
                # Exercise controls while both discard requests have time to
                # reach the server; they must never occupy the control workers.
                deadline = time.monotonic() + 0.3
                while time.monotonic() < deadline:
                    connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=0.5)
                    try:
                        connection.request('GET', '/databases', headers={'Authorization': 'Bearer ' + TOKEN})
                        response = connection.getresponse()
                        self.assertEqual(response.status, 200, response.read())
                    except TimeoutError:
                        self.fail('discard requests starved database descriptors behind close')
                    finally:
                        connection.close()
            finally:
                self.arm('release', 0, False)
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')
            self.assertEqual(close.result(timeout=10)['state'], 'closed')
            for discard in discards:
                self.assertIn(discard.result(timeout=10)[0], (412, 503))

    def test_listener_recovers_after_descriptor_exhaustion(self):
        self.arm('exhaust-fds', 0, False)
        try:
            with socket.create_connection(('127.0.0.1', self.port), timeout=5) as connection:
                connection.sendall(b'GET /healthz HTTP/1.1\r\nHost: local\r\n\r\n')
                # Let at least one accept report EMFILE before restoring capacity.
                time.sleep(0.3)
        finally:
            self.arm('release-fds', 0, False)
        self.assertEqual(self.api('/healthz')['status'], 'live')

    def test_blocked_database_does_not_starve_controls_or_other_database(self):
        path = self.collection()
        other = self.api('/databases', {'name': 'independent'})['database_id']
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'blocked'}]}
        self.arm('pause', 1, False)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append', body)
            self.wait_for_publication()
            try:
                self.assertEqual(self.api('/healthz')['status'], 'live')
                self.assertEqual(len(self.api('/databases')['items']), 2)
                self.assertEqual(self.api('/databases/' + other + '/collections')['items'], [])
                self.assertEqual(self.api(self.base)['state'], 'open')
                self.assertEqual(self.api(self.base + '/open', {})['state'], 'open')
            finally:
                self.arm('release', 0, False)
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')

    def test_explain_is_admitted_as_a_read_while_publication_is_blocked(self):
        path = self.collection()
        original = self.api(path + '/append',
            {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'original'}]})
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append',
                {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'paused'}]})
            self.wait_for_publication()
            try:
                explained = self.api(path + '/explain', {})
                self.assertEqual(explained['snapshot_id'], original['result']['snapshot_id'])
                self.assertEqual(explained['estimates']['rows_before_filter'], '1')
            finally:
                self.arm('release', 0, False)
            committed = append.result(timeout=10)
            self.assertEqual(committed['outcome'], 'committed')
        explained = self.api(path + '/explain', {})
        self.assertEqual(explained['snapshot_id'], committed['result']['snapshot_id'])
        self.assertEqual(explained['estimates']['rows_before_filter'], '2')

    def test_readiness_during_open_recovery_and_reconciliation(self):
        self.api(self.base + '/close', {})
        for action in ('open', 'reconcile'):
            with self.subTest(action=action):
                self.arm('pause', 1, False)
                with ThreadPoolExecutor(max_workers=1) as workers:
                    pending = workers.submit(self.api, self.base + '/' + action, {})
                    self.wait_for_publication()
                    try:
                        health = self.api(self.base + '/readyz', status=503)
                        self.assertEqual(health['state'], 'recovering')
                        self.assertFalse(health['read_available'])
                        self.assertFalse(health['write_ready'])
                        self.assertEqual(self.api(self.base + '/capabilities')['health'], health)
                        self.assertEqual(self.api('/healthz')['status'], 'live')
                    finally:
                        self.arm('release', 0, False)
                    pending.result(timeout=10)
                self.assertTrue(self.api(self.base + '/readyz')['write_ready'])

    def test_outcome_lookups_do_not_starve_controls_or_other_databases(self):
        path = self.collection()
        original = self.api(path + '/append',
            {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'original'}]})
        other = self.api('/databases', {'name': 'quiet'})['database_id']
        collection = str(uuid.uuid4())
        quiet = '/databases/' + other + '/collections/' + collection
        self.api('/databases/' + other + '/collections', {'collection_id': collection,
            'name': 'quiet', 'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})
        quiet_receipt = self.api(quiet + '/append',
            {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'quiet'}]})
        self.arm('pause', 2, False)  # Pause outer publication, holding the catalog mutex.
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append',
                {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'paused'}]})
            self.wait_for_publication()
            sockets = []
            try:
                operations = [original['operation_id']] + [str(uuid.uuid4()) for _ in range(7)]
                for operation in operations:
                    stream = socket.create_connection(('127.0.0.1', self.port), timeout=2)
                    sockets.append(stream)
                    stream.sendall((f'GET {path}/operations/{operation} HTTP/1.1\r\n'
                        f'Host: local\r\nAuthorization: Bearer {TOKEN}\r\n\r\n').encode())
                # Every lookup must refuse promptly while the publication
                # barrier remains held, including an older committed operation.
                for stream in sockets:
                    response = http.client.HTTPResponse(stream)
                    response.begin()
                    failure = json.loads(response.read())
                    self.assertEqual(response.status, 503, failure)
                    self.assertEqual(failure['error']['code'], 'OPERATION_IN_PROGRESS')
                    self.assertEqual(failure['error']['retry_hint'], 'SameIdAfterReconcile')
                    self.assertNotIn('outcome', failure)
                for target in ('/databases', self.base, '/databases/' + other,
                        quiet + '/operations/' + quiet_receipt['operation_id'],
                        self.base + '/readyz', self.base + '/capabilities'):
                    connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=1)
                    try:
                        connection.request('GET', target,
                            headers={'Authorization': 'Bearer ' + TOKEN})
                        response = connection.getresponse()
                        self.assertEqual(response.status, 200, response.read())
                    finally:
                        connection.close()
            finally:
                self.arm('release', 0, False)
                for stream in sockets:
                    stream.close()
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')
        self.assertEqual(self.api(path + '/operations/' + original['operation_id']), original)
        self.api(path + '/operations/' + operations[-1], status=404)

    def test_concurrent_appends_wait_without_fast_refusals(self):
        path = self.collection()
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append',
                {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'first'}]})
            self.wait_for_publication()
            streams = []
            try:
                for index in range(6):
                    body = json.dumps({'operation_id': str(uuid.uuid4()), 'rows': [{'key': str(index)}]})
                    stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                    self.addCleanup(stream.close)
                    streams.append(stream)
                    stream.sendall((f'POST {path}/append HTTP/1.1\r\nHost: local\r\n'
                        f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                        f'Content-Length: {len(body)}\r\n\r\n{body}').encode())
                self.assertFalse(select.select(streams, [], [], 0.25)[0], 'queued appends were refused before execution')
            finally:
                self.arm('release', 0, False)
            try:
                for stream in streams:
                    response = http.client.HTTPResponse(stream)
                    response.begin()
                    self.assertEqual(response.status, 200, response.read())
            finally:
                for stream in streams:
                    stream.close()
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')

    def test_half_closed_active_and_queued_appends_receive_committed_responses(self):
        path = self.collection()
        operations = [str(uuid.uuid4()), str(uuid.uuid4())]
        streams = []
        self.arm('pause', 2, False)
        try:
            try:
                for index, operation in enumerate(operations):
                    stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                    streams.append(stream)
                    body = json.dumps({'operation_id': operation, 'rows': [{'key': str(index)}]})
                    stream.sendall((f'POST {path}/append HTTP/1.1\r\nHost: local\r\n'
                        f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                        f'Connection: close\r\nContent-Length: {len(body)}\r\n\r\n{body}').encode())
                    if index == 0:
                        self.wait_for_publication()
                    stream.shutdown(socket.SHUT_WR)
                self.assertFalse(select.select(streams, [], [], 0.25)[0],
                    'half-closed requests lost their response socket before publication')
            finally:
                self.arm('release', 0, False)
            outcomes = []
            for stream, operation in zip(streams, operations):
                response = http.client.HTTPResponse(stream)
                response.begin()
                body = response.read()
                self.assertEqual(response.status, 200, body)
                outcome = json.loads(body)
                self.assertEqual(outcome['operation_id'], operation)
                self.assertEqual(outcome['outcome'], 'committed')
                outcomes.append(outcome)
            for outcome in outcomes:
                self.assertEqual(self.api(path + '/operations/' + outcome['operation_id']), outcome)
        finally:
            for stream in streams:
                stream.close()
        self.assertCountEqual(self.api(path + '/scan', {})['rows'], [{'key': '0'}, {'key': '1'}])

    def test_queued_append_reset_never_executes(self):
        path = self.collection()
        operation = str(uuid.uuid4())
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append',
                {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'first'}]})
            self.wait_for_publication()
            stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
            try:
                body = json.dumps({'operation_id': operation, 'rows': [{'key': 'disconnected'}]})
                stream.sendall((f'POST {path}/append HTTP/1.1\r\nHost: local\r\n'
                    f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                    f'Content-Length: {len(body)}\r\n\r\n{body}').encode())
                stream.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                stream.close()
                # Allow the I/O loop to observe the reset while publication waits.
                self.assertEqual(self.api(self.base)['state'], 'open')
                time.sleep(0.1)
            finally:
                stream.close()
                self.arm('release', 0, False)
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')
        self.api(path + '/operations/' + operation, status=404)

    def test_queue_timeout_never_executes_the_refused_append(self):
        self.stop()
        self.request_timeout_ms = 200
        self.start()
        path = self.collection()
        operation = str(uuid.uuid4())
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append',
                {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'first'}]})
            self.wait_for_publication()
            try:
                refused = self.api(path + '/append',
                    {'operation_id': operation, 'rows': [{'key': 'expired'}]}, status=408)
                self.assertIs(refused['execution_started'], False)
                self.assertEqual(refused['error']['retry_hint'], 'SameIdAfterReconcile')
            finally:
                self.arm('release', 0, False)
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')
        self.api(path + '/operations/' + operation, status=404)

    def test_closed_active_call_keeps_its_publisher_permit_until_completion(self):
        self.stop()
        self.request_timeout_ms = 200
        self.start()
        path = self.collection()
        first, second = str(uuid.uuid4()), str(uuid.uuid4())
        self.arm('pause', 2, False)
        stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
        try:
            body = json.dumps({'operation_id': first, 'rows': [{'key': 'held'}]})
            stream.sendall((f'POST {path}/append HTTP/1.1\r\nHost: local\r\n'
                f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                f'Content-Length: {len(body)}\r\n\r\n{body}').encode())
            self.wait_for_publication()
            stream.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            stream.close()
            refused = self.api(path + '/append',
                {'operation_id': second, 'rows': [{'key': 'expired'}]}, status=408)
            self.assertIs(refused['execution_started'], False)
        finally:
            stream.close()
            self.arm('release', 0, False)
        deadline = time.monotonic() + 3
        while self.request(path + '/operations/' + first)[0] != 200:
            self.assertLess(time.monotonic(), deadline)
        self.api(path + '/operations/' + second, status=404)

    def test_two_busy_stores_leave_other_stores_readers_and_lifecycle_available(self):
        paths = [self.collection()]
        first_base = self.base
        for name in ('second', 'third'):
            database = self.api('/databases', {'name': name})['database_id']
            self.base = '/databases/' + database
            paths.append(self.collection())
        self.base = first_base
        self.arm('pause', -2, False)
        with ThreadPoolExecutor(max_workers=4) as workers:
            appends = [workers.submit(self.api, path + '/append',
                {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'held'}]}) for path in paths[:2]]
            self.wait_for_publication(2)
            streams = []
            try:
                for path in paths[:2] * 2:
                    stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                    streams.append(stream)
                    body = json.dumps({'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'queued'}]})
                    stream.sendall((f'POST {path}/append HTTP/1.1\r\nHost: local\r\n'
                        f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                        f'Content-Length: {len(body)}\r\n\r\n{body}').encode())
                self.assertFalse(select.select(streams, [], [], 0.2)[0])
                self.assertEqual(self.api(paths[2] + '/append',
                    {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'free'}]})['outcome'], 'committed')
                self.assertEqual(self.api(paths[0] + '/scan', {})['rows'], [])
                self.assertEqual(len(self.api('/databases')['items']), 3)
                closes = [workers.submit(self.api, path.split('/collections/')[0] + '/close', {}) for path in paths[:2]]
                deadline = time.monotonic() + 3
                while any(self.api(path.split('/collections/')[0])['state'] != 'closing' for path in paths[:2]):
                    self.assertLess(time.monotonic(), deadline)
                third = paths[2].split('/collections/')[0]
                self.assertEqual(self.api(third + '/close', {})['state'], 'closed')
                self.assertEqual(self.api(third + '/open', {})['state'], 'open')
            finally:
                for stream in streams:
                    stream.close()
                self.arm('release', 0, False)
            for append in appends:
                self.assertEqual(append.result(timeout=10)['outcome'], 'committed')
            for close in closes:
                self.assertEqual(close.result(timeout=10)['state'], 'closed')

    def test_32_and_64_concurrent_snapshot_sessions_finish_without_busy_refusals(self):
        first_base = self.base
        paths = []
        for index in range(8):
            if index:
                database = self.api('/databases', {'name': 'readers-' + str(index)})['database_id']
                self.base = '/databases/' + database
            path = self.collection()
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'same'}] * 2})
            paths.append(path)
        self.base = first_base
        for count in (32, 64):
            with self.subTest(sessions=count), ThreadPoolExecutor(max_workers=count) as workers:
                barrier = threading.Barrier(count, timeout=30)
                def scan(index):
                    path = paths[index // 8]
                    opened = self.api(path + '/scan/open', {})
                    barrier.wait()
                    try:
                        page = self.api(path + '/scan', {'cursor': opened['cursor']})
                        self.assertEqual(page['rows'], [{'key': 'same'}] * 2)
                        self.assertEqual(page['snapshot_id'], opened['snapshot_id'])
                    finally:
                        self.api(path + '/scan/cancel', {'cursor': opened['cursor']})
                for future in [workers.submit(scan, index) for index in range(count)]:
                    future.result(timeout=40)

    def test_control_acknowledgements_after_startup(self):
        # The readiness record and control replies come from different threads.
        # TSan must see no shared stream-state race during this handshake.
        for _ in range(4):
            self.arm('core', 0, False)
            self.arm('registry', 0, False)
            self.arm('pause', 1, False)
            self.arm('release', 0, False)
        self.assertEqual(self.api(self.base + '/collections')['items'], [])

    def test_uncertain_create_commit_recovers_identity_without_duplicate_collection(self):
        for after in (False, True):
            with self.subTest(after_unlink=after):
                cid = str(uuid.uuid4())
                body = {'collection_id': cid, 'name': cid,
                        'schema': {'fields': [{'name': 'key', 'type': 'string'}]}}
                self.arm('core', 1, after)
                unknown = self.api(self.base + '/collections', body, status=503)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                self.assertEqual(unknown['recovery']['collection_id'], cid)
                self.assertEqual(unknown['recovery']['exact_name'], cid)
                self.assertTrue(unknown['recovery_required'])
                self.arm('core', 0, False)
                self.api(self.base + '/reconcile', {})
                created = self.api(self.base + '/collections', body)
                self.assertEqual(created['collection_id'], cid)
                self.assertEqual(self.api(self.base + '/collections', body), created)
        self.assertEqual(len(self.api(self.base + '/collections')['items']), 2)

    def test_logging_correlates_engine_commit_uncertainty_with_http_completion(self):
        path = self.collection()
        self.arm('core', 2, True)
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'private-log-marker'}]}
        unknown = self.api(path + '/append', body, status=503)
        self.assertEqual(unknown['outcome'], 'commit_unknown')
        self.arm('core', 0, False)
        self.api(self.base + '/reconcile', {})
        self.stop()
        recovery = [e for e in self.log_records if e['event'] == 'recovery_required']
        self.assertTrue(recovery, self.log_records)
        self.assertTrue(any(e['outcome'] == 'unknown' and e['operation'] == 'append' for e in recovery))
        requests = [e for e in self.log_records if e['event'] == 'request_completed']
        self.assertTrue(any(e['request_id'] == recovery[0]['request_id'] and int(e['status_code']) == 503
                            and e['outcome'] == 'error' for e in requests))
        self.assertNotIn('private-log-marker', json.dumps(self.log_records))
        self.assertNotIn(TOKEN, json.dumps(self.log_records))

    def test_uncertain_append_commit_and_recovery_fence_preserve_outcomes(self):
        path = self.collection()
        for after in (False, True):
            with self.subTest(after_unlink=after):
                body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': str(after)}]}
                self.arm('core', 2, after)
                unknown = self.api(path + '/append', body, status=503)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                self.assertEqual(unknown['phase'], 'catalog_commit')
                self.assertTrue(unknown['recovery']['publication_started'])
                lookup = path + '/operations/' + body['operation_id']
                lookup_unknown = self.api(lookup, status=503)
                self.assertEqual(lookup_unknown['outcome'], 'commit_unknown')
                self.assertEqual(lookup_unknown['recovery'], unknown['recovery'])
                self.assertEqual(self.api(path + '/append', body, status=503)['recovery'], unknown['recovery'])
                conflict = self.api(path + '/append', dict(body, rows=[{'key': 'different'}]), status=409)
                self.assertEqual(conflict['error']['code'], 'OPERATION_CONFLICT')
                blocked = self.api(path + '/append', {'operation_id': str(uuid.uuid4()),
                    'rows': [{'key': 'must-not-publish'}]}, status=503)
                self.assertEqual(blocked['error']['code'], 'RECOVERY_REQUIRED')
                self.arm('core', 1, True)
                self.api(self.base + '/reconcile', {}, status=503)
                self.api(lookup, status=503)
                self.arm('core', 0, False)
                self.api(self.base + '/reconcile', {})
                code, resolved = self.request(lookup)
                self.assertEqual(resolved['outcome'], 'committed' if after else 'definitely_not_committed')
                self.assertEqual(self.request(path + '/append', body), (code, resolved))
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'True'}])
        self.assertTrue(list(self.directory.rglob('*.parquet')), 'test must exercise native files')
        self.stop()
        self.start()
        self.assertEqual(self.request(lookup), (code, resolved))

    def test_binding_commit_fault_does_not_publish_or_invent_a_terminal_receipt(self):
        path = self.collection()
        for after in (False, True):
            with self.subTest(after_unlink=after):
                body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': str(after)}]}
                self.arm('core', 1, after)
                unknown = self.api(path + '/append', body, status=503)
                self.assertEqual(unknown['phase'], 'binding')
                health = self.api(self.base + '/readyz', status=503)
                self.assertEqual(health['state'], 'recovery_required')
                self.assertFalse(health['write_ready'])
                self.assertFalse(unknown['recovery']['publication_started'])
                self.arm('core', 0, False)
                self.api(self.base + '/reconcile', {})
                lookup = path + '/operations/' + body['operation_id']
                if after:
                    code, failure = self.request(lookup)
                    self.assertEqual(failure['outcome'], 'definitely_not_committed')
                    self.assertEqual(self.request(path + '/append', body), (code, failure))
                else:
                    self.api(lookup, status=404)
                    self.api(path + '/append', body)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'False'}])

    def test_lost_failure_recording_commit_retains_original_precondition_error(self):
        path = self.collection()
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'existing'}]})
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'rejected'}], 'expected_snapshot': None}
        self.arm('core', 2, True)
        unknown = self.api(path + '/append', body, status=503)
        self.assertEqual(unknown['phase'], 'recording_failure')
        self.assertEqual(unknown['recovery']['first_error']['code'], 'PRECONDITION_FAILED')
        self.assertFalse(unknown['recovery']['publication_started'])
        self.arm('core', 0, False)
        self.api(self.base + '/reconcile', {})
        failure = self.api(path + '/append', body, status=412)
        self.assertEqual(failure['outcome'], 'definitely_not_committed')
        self.assertEqual(failure['error'], unknown['recovery']['first_error'])
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id'], status=412), failure)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'existing'}])

    def test_native_producer_path_failure_and_lost_failure_record_are_recoverable(self):
        path = self.collection()
        cid = path.rsplit('/', 1)[1]
        data_dir = self.directory / 'databases' / self.database / 'warehouse' / cid / 'data'
        saved = Path(self.tmp.name) / 'saved-data'
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'never-published'}]}
        self.arm('pause', 1, False)
        self.arm('core', 2, True)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append', body, 503)
            self.wait_for_publication()
            # Preflight passed and binding is at COMMIT. A real ENOTDIR during
            # data-file production must survive a lost failure-record COMMIT.
            data_dir.rename(saved)
            data_dir.write_bytes(b'not a directory')
            try:
                self.arm('release', 0, False)
                unknown = append.result(timeout=10)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                self.assertEqual(unknown['phase'], 'recording_failure')
                self.assertEqual(unknown['recovery']['first_error']['code'], 'CORRUPTION')
                self.assertFalse(unknown['recovery']['publication_started'])
            finally:
                data_dir.unlink()
                saved.rename(data_dir)
                self.arm('core', 0, False)
        self.api(self.base + '/reconcile', {})
        failure = self.api(path + '/append', body, status=500)
        self.assertEqual(failure['outcome'], 'definitely_not_committed')
        self.assertEqual(failure['error'], unknown['recovery']['first_error'])
        self.assertEqual(self.api(path + '/scan', {})['rows'], [])
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'recovered'}]})
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id'], status=500), failure)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'recovered'}])

    def test_registry_open_and_close_commit_faults_recover_persisted_intent(self):
        for action in ('close', 'open'):
            for after in (False, True):
                with self.subTest(action=action, after_unlink=after):
                    self.api(self.base + '/open', {})
                    if action == 'open':
                        self.api(self.base + '/close', {})
                    self.arm('registry', 1, after)
                    self.api(self.base + '/' + action, {}, status=503)
                    self.api('/databases', {'name': 'blocked'}, status=503)
                    self.arm('registry', 0, False)
                    self.stop()
                    self.start()
                    expected = ('closed' if after else 'open') if action == 'close' else ('open' if after else 'closed')
                    self.assertEqual(self.api(self.base)['state'], expected)
                    self.assertEqual(len(self.api('/databases')['items']), 1)

    def test_registry_create_commit_faults_preserve_reservation_and_complete_store(self):
        for commit in (1, 2):
            for after in (False, True):
                with self.subTest(commit=commit, after_unlink=after):
                    database = str(uuid.uuid4())
                    body = {'name': database, 'database_id': database}
                    root = self.directory / 'databases' / database
                    self.arm('registry', commit, after)
                    self.api('/databases', body, status=503)
                    self.assertEqual(root.exists(), commit == 2)
                    identity = root / 'store.identity'
                    inode = identity.stat().st_ino if identity.exists() else None
                    self.api('/databases', body, status=503)
                    self.arm('registry', 0, False)
                    self.stop()
                    self.start()
                    created = self.api('/databases', body)
                    self.assertEqual(created['state'], 'open')
                    self.assertEqual(created['database_id'], database)
                    self.assertEqual(self.api('/databases', body), created)
                    if inode is not None:
                        self.assertEqual(identity.stat().st_ino, inode, 'complete store was replaced')

    def test_startup_store_failure_isolated_and_identity_never_replaced(self):
        path = self.collection()
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'kept'}]})
        other = self.api('/databases', {'name': 'unaffected'})['database_id']
        root = self.directory / 'databases' / self.database
        other_root = self.directory / 'databases' / other
        self.stop()
        saved = self.directory / 'saved'
        root.rename(saved)
        other_root.rename(root)
        saved.rename(other_root)
        self.start()
        self.assertEqual(self.api(self.base)['state'], 'error')
        self.assertEqual(self.api('/databases/' + other)['state'], 'error')
        new = self.api('/databases', {'name': 'healthy'})
        self.assertEqual(new['state'], 'open')
        error = self.api(self.base + '/open', {}, status=409)
        self.assertEqual(error['error']['code'], 'STORE_IDENTITY_MISMATCH')
        self.assertTrue(list(root.rglob('store.identity')))
        self.stop()
        root.rename(saved)
        other_root.rename(root)
        saved.rename(other_root)
        self.start()
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'kept'}])

    def test_shutdown_timeout_keeps_native_publication_and_ownership(self):
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'admitted'}, {'key': 'admitted'}]}
        self.assertEqual(self.control('owner ' + self.database), 'STATE_IN_USE')
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=1) as pool:
            append = pool.submit(self.api, path + '/append', body)
            try:
                self.wait_for_publication()
                self.assertEqual(self.control('shutdown ' + self.database), 'DEADLINE_EXCEEDED')
                self.assertEqual(self.control('owner ' + self.database), 'STATE_IN_USE')
                self.assertFalse(self.api(self.base + '/readyz', status=503)['write_ready'])
                self.process.terminate()
                try:
                    self.process.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    pass
                else:
                    self.fail('server exited during native publication')
                self.assertEqual(self.control('server-state'), 'running')
            finally:
                self.arm('release', 0, False)
            outcome = append.result(timeout=10)
            self.assertEqual(outcome['outcome'], 'committed')
            self.assertEqual(outcome['result']['rows_appended'], '2')
        self.stop(terminate=False)
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id']), outcome)
        self.assertEqual(self.api(path + '/append', body), outcome)
        self.assertEqual(self.api(path + '/scan', {})['rows'], body['rows'])

    def test_shutdown_reconciles_uncertain_native_publication_on_restart(self):
        for graceful in (True, False):
            with self.subTest(graceful=graceful):
                path = self.collection()
                body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'uncertain'}]}
                self.arm('core', 2, True)
                unknown = self.api(path + '/append', body, status=503)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                if graceful:
                    self.stop()
                else:
                    self.process.kill()
                    self.process.communicate(timeout=20)
                    self.assertEqual(self.process.returncode, -9)
                self.start()
                outcome = self.api(path + '/operations/' + body['operation_id'])
                self.assertEqual(outcome['outcome'], 'committed')
                self.assertEqual(self.api(path + '/append', body), outcome)
                self.assertEqual(self.api(path + '/scan', {})['rows'], body['rows'])

    def test_close_drains_native_publication_and_other_database_stays_available(self):
        path = self.collection()
        other = self.api('/databases', {'name': 'other'})['database_id']
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'admitted'}]}
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=2) as workers:
            append = workers.submit(self.api, path + '/append', body)
            self.wait_for_publication()
            close = workers.submit(self.api, self.base + '/close', {})
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    status, _ = self.request(self.base + '/collections')
                    if status == 429:
                        break
                    time.sleep(0.01)
                else:
                    self.fail('close did not reject new database work')
                self.assertFalse(close.done(), 'close released the store before publication finished')
                self.assertEqual(self.api('/databases/' + other + '/collections')['items'], [])
            finally:
                self.arm('release', 0, False)
            outcome = append.result(timeout=10)
            self.assertEqual(outcome['outcome'], 'committed')
            self.assertEqual(close.result(timeout=10)['state'], 'closed')
        self.api(self.base + '/open', {})
        self.assertEqual(self.api(path + '/operations/' + body['operation_id']), outcome)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'admitted'}])


if __name__ == '__main__':
    unittest.main()
