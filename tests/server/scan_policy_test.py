"""Configured scan admission against the real listener, storage and shared owner."""
import concurrent.futures
from collections import Counter
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

_test_arguments = sys.argv[4:]
from native_http_test import NativeHTTP, SERVER, UI
sys.argv[1:] = _test_arguments


class ScanPolicy(NativeHTTP):
    def validate_wire(self, name, value):
        try:
            from jsonschema import Draft202012Validator
        except ImportError:
            self.assertNotEqual(os.environ.get('GLAMDB_VALIDATE_SCHEMAS'), '1',
                                'required JSON Schema dependency is missing')
            return
        schema = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/v0.schema.json').read_text())
        Draft202012Validator(dict(schema, **{'$ref': '#/$defs/' + name})).validate(value)

    def read_page(self, path, body):
        # HTTP execution remains independently bounded. Retry only a refusal
        # that never entered the scan, always retaining the exact cursor body.
        for _ in range(100):
            code, _, raw = self.request(path + '/scan', body)
            value = json.loads(raw)
            if code == 503 and value.get('error', {}).get('code') == 'SERVER_BUSY':
                with self.refusal_lock:
                    self.refusals['SERVER_BUSY'] += 1
                time.sleep(.02)
                continue
            self.assertEqual(code, 200, raw)
            return value
        self.fail('bounded reader admission did not recover')

    def start(self):
        self.refusals = Counter()
        self.refusal_lock = threading.Lock()
        self.proc = subprocess.Popen([
            SERVER, '--data-dir', self.data_dir, '--port', '0', '--ui-root', UI,
            '--scan-live-per-database', '32', '--scan-completed-per-database', '2',
            '--scan-replay-bytes-per-database', str(32 * 16 * 1024 * 1024),
            '--resource-data-bytes', str(4 * 1024 ** 3),
            '--resource-metadata-bytes', str(24 * 1024 ** 3),
            '--scan-idle-seconds', '60', '--scan-completed-ttl-seconds', '60',
        ], env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertTrue(select.select([self.proc.stdout], [], [], 15)[0], 'service did not start')
        line = self.proc.stdout.readline()
        if not line:
            self.proc.wait(timeout=5)
            self.fail(self.proc.stderr.read())
        self.port = json.loads(line)['port']

    def test_thirty_two_live_readers_preserve_snapshot_replay_and_cleanup(self):
        path = self.collection()
        rows = [{'key': 'original', 'n': str(i), 'ts': None} for i in range(10000)]
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': rows})
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'tail'}]})
        baseline = self.api('/capabilities')['scan_sessions']['global']
        opened = [self.api(path + '/scan/open', {}) for _ in range(32)]
        occupied = self.api('/capabilities')
        self.validate_wire('capabilities', occupied)
        self.assertEqual(occupied['limits']['scan_sessions_per_database'], 32)
        self.assertEqual(occupied['scan_sessions']['global']['live_cursors'], 32)
        self.assertEqual(occupied['scan_sessions']['global']['replay_reserved_bytes'], 32 * 16 * 1024 * 1024)
        self.assertGreaterEqual(occupied['shared_resources']['memory_bytes'][0], 32 * 32 * 1024 * 1024)
        code, headers, raw = self.request(path + '/scan/open', {})
        self.assertEqual(code, 429, raw)
        self.assertEqual(json.loads(raw)['error']['reason'], 'database_live_limit')
        self.validate_wire('error', json.loads(raw))
        self.assertEqual(headers['Retry-After'], '1')
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'later'}]})
        latencies = []
        barrier = threading.Barrier(33)
        observations = []
        finished = threading.Event()

        def observe():
            while not finished.wait(.005):
                observations.append(self.api('/capabilities')['http_activity'])

        def first_page(item):
            barrier.wait(timeout=15)
            start = time.monotonic()
            body = {'cursor': item['cursor']}
            page = self.read_page(path, body)
            self.assertEqual(len(page['rows']), 10000)
            self.assertEqual(page['snapshot_id'], item['snapshot_id'])
            latencies.append(time.monotonic() - start)
            self.assertEqual(self.read_page(path, body), page)
            return page

        def publish():
            barrier.wait(timeout=15)
            return self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'during'}]})

        started = time.monotonic()
        sampler = threading.Thread(target=observe)
        sampler.start()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=33) as pool:
                readers = [pool.submit(first_page, item) for item in opened]
                publisher = pool.submit(publish)
                pages = [reader.result(timeout=90) for reader in readers]
                self.assertEqual(publisher.result(timeout=90)['outcome'], 'committed')
        finally:
            finished.set()
            sampler.join()
        self.assertTrue(observations)
        self.assertGreater(max(item['pending_calls'] for item in observations), 0)
        self.assertGreater(max(item['executing_calls'] for item in observations), 0)
        self.assertGreater(max(item['oldest_queue_age_ms'] for item in observations), 0)
        for page in pages:
            request = {'cursor': page['next_cursor']}
            last = self.api(path + '/scan', request)
            expected = rows + [{'key': 'tail', 'n': None, 'ts': None}]
            order = lambda row: (row['key'], row['n'] or '')
            self.assertEqual(sorted(page['rows'] + last['rows'], key=order), sorted(expected, key=order))
            self.assertEqual(last['snapshot_id'], opened[0]['snapshot_id'])
            self.assertIsNone(last['next_cursor'])
            self.assertEqual(self.api(path + '/scan', request), last)
        stats = self.api('/capabilities')['scan_sessions']
        self.assertEqual(stats['global']['live_cursors'], 0)
        self.assertEqual(stats['global']['completed_replays'], 2)
        self.assertEqual(stats['completed_evictions'], 30)
        self.api(path + '/scan', {'cursor': pages[0]['next_cursor']}, status=404)
        self.api('/databases/' + self.store_id + '/close', {})
        self.assertEqual(self.api('/capabilities')['scan_sessions']['global'], baseline)
        elapsed = time.monotonic() - started
        resources = self.api('/capabilities')['shared_resources']
        for peak, limit in zip(resources['memory_peak_bytes'], resources['memory_limits_bytes']):
            self.assertLessEqual(peak, limit)
        rss = next(line for line in Path('/proc/' + str(self.proc.pid) + '/status').read_text().splitlines()
                   if line.startswith('VmHWM:'))
        rss_bytes = int(rss.split()[1]) * 1024
        self.assertLess(rss_bytes, 1024 ** 3, 'fixed small-reader fixture exceeded its 1 GiB RSS envelope')
        print(json.dumps({'profile': '32-live-readers', 'successful_queries_per_second': 32 / elapsed,
                          'successful_page_seconds_max': max(latencies), 'occupancy': stats,
                          'pending_calls_peak': max(item['pending_calls'] for item in observations),
                          'oldest_queue_age_ms_peak': max(item['oldest_queue_age_ms'] for item in observations),
                          'http_refusals': dict(self.refusals),
                          'resources': resources, 'rss_peak_bytes': rss_bytes,
                          'fixture_rss_limit_bytes': 1024 ** 3}), flush=True)

    def test_global_sixty_four_clients_and_database_isolation(self):
        paths = [self.collection()]
        for index in range(2):
            database = self.api('/databases', {'name': 'other-' + str(index)})['database_id']
            cid = str(uuid.uuid4())
            base = '/databases/' + database + '/collections'
            self.api(base, {'collection_id': cid, 'name': 'rows', 'schema': {'fields': [
                {'name': 'key', 'type': 'string', 'nullable': False}]}})
            paths.append(base + '/' + cid)
        for path in paths[:2]:
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()),
                     'rows': [{'key': 'original-' + 'x' * 128}] * 10000})
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'tail'}]})
        held = [(path, self.api(path + '/scan/open', {})['cursor'])
                for path in paths[:2] for _ in range(32)]
        failure = self.api(paths[2] + '/scan/open', {}, status=429)
        self.validate_wire('error', failure)
        self.assertEqual(failure['error']['reason'], 'global_live_limit')
        stats = self.api('/capabilities')
        self.assertEqual(stats['scan_sessions']['global']['live_cursors'], 64)
        self.assertGreaterEqual(stats['http_activity']['connected_clients'], 1)
        self.assertEqual(stats['http_activity']['pending_calls'], 0)
        self.assertEqual(stats['http_activity']['oldest_queue_age_ms'], 0)
        self.assertLessEqual(stats['shared_resources']['memory_bytes'][0], 4 * 1024 ** 3)
        self.assertEqual(self.api('/databases/' + self.store_id + '/capabilities')
                         ['scan_sessions']['database']['live_cursors'], 32)
        path, cursor = held.pop()
        self.api(path + '/scan/cancel', {'cursor': cursor})
        extra = self.api(paths[2] + '/scan/open', {})['cursor']
        barrier = threading.Barrier(64)

        def scan(item):
            path, cursor = item
            barrier.wait(timeout=15)
            page = self.read_page(path, {'cursor': cursor})
            self.assertEqual(len(page['rows']), 10000)
            self.assertIsNotNone(page['next_cursor'])
            self.assertEqual(self.read_page(path, {'cursor': cursor}), page)

        def control():
            barrier.wait(timeout=15)
            self.assertEqual(self.api(paths[2] + '/scan/cancel', {'cursor': extra}), {'cancelled': True})
            self.assertTrue(self.api('/databases/' + self.store_id + '/readyz')['write_ready'])

        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            readers = [pool.submit(scan, item) for item in held]
            cancellation = pool.submit(control)
            for reader in readers:
                reader.result(timeout=90)
            cancellation.result(timeout=90)
        for path, cursor in held:
            self.api(path + '/scan/cancel', {'cursor': cursor})
        stats = self.api('/capabilities')['scan_sessions']
        self.assertEqual(stats['global']['live_cursors'], 0)
        self.assertEqual(stats['global']['replay_reserved_bytes'], 0)
        self.assertEqual(stats['rejections']['global_live_limit'], 1)
        print(json.dumps({'profile': '64-http-clients-three-databases', 'occupancy': stats,
                          'http_refusals': dict(self.refusals)}), flush=True)


for name in list(vars(NativeHTTP)):
    if name.startswith('test_'):
        setattr(ScanPolicy, name, None)
del NativeHTTP


class ScanConfiguration(unittest.TestCase):
    def test_infeasible_profile_is_rejected_before_creating_service_state(self):
        with tempfile.TemporaryDirectory(prefix='glamdb-scan-configuration-') as directory:
            service = Path(directory).resolve() / 'service'
            result = subprocess.run([
                SERVER, '--data-dir', str(service), '--port', '0',
                '--scan-live-per-database', '32',
                '--scan-replay-bytes-per-database', str(32 * 16 * 1024 * 1024),
            ], env=dict(os.environ, GLAMDB_TOKEN='configuration-test'),
                capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('profile exceeds shared', result.stderr)
            self.assertFalse(service.exists(), 'invalid configuration created persistent service state')


if __name__ == '__main__':
    unittest.main()
