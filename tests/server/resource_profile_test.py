"""Measured multi-database HTTP load and cold-process reopen with one owner."""
import concurrent.futures
import http.client
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

DRIVER = sys.argv.pop(1)
TOKEN = 'resource-profile-test'


class ResourceProfile(unittest.TestCase):
    def request(self, path, body=None, status=200):
        for attempt in range(100):
            connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=90)
            try:
                connection.request('GET' if body is None else 'POST', path,
                                   None if body is None else json.dumps(body),
                                   {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
                response = connection.getresponse()
                result = json.loads(response.read())
                if response.status == 503 and result.get('error', {}).get('code') == 'SERVER_BUSY':
                    with self.counter_lock:
                        self.refusals += 1
                    time.sleep(.01)
                    continue
                self.assertEqual(response.status, status, result)
                return result
            finally:
                connection.close()
        self.fail('server admission did not recover')

    def start(self, directory, *, saturated=False):
        self.process = subprocess.Popen([DRIVER, str(directory)] + (["full"] if saturated else []),
                                        env=dict(os.environ, GLAMDB_TOKEN=TOKEN),
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        readable, _, _ = select.select([self.process.stdout], [], [], 30)
        self.assertTrue(readable, 'profile service did not start')
        line = self.process.stdout.readline()
        self.assertTrue(line, self.process.stderr.read() if self.process.poll() is not None else '')
        self.port = json.loads(line)['port']

    def stop(self):
        self.process.terminate()
        out, err = self.process.communicate(timeout=90)
        self.assertEqual(self.process.returncode, 0, out + err)
        profile = json.loads(out.strip())
        self.assertEqual(profile['memory_remaining_bytes'], [0] * 5)
        self.assertEqual(profile['device_remaining_bytes'], 0)
        self.assertEqual(profile['inode_remaining'], 0)
        return profile

    def scan(self, base):
        rows = []
        body = {}
        while True:
            page = self.request(base + '/scan', body)
            rows.extend(page['rows'])
            if page['next_cursor'] is None:
                return rows
            body = {'cursor': page['next_cursor']}

    def test_combined_load_and_cold_reopen(self):
        self.refusals = 0
        self.counter_lock = threading.Lock()
        with tempfile.TemporaryDirectory(prefix='glamdb-resource-profile-') as temporary:
            directory = Path(temporary).resolve() / 'server'
            self.start(directory)
            try:
                databases = []
                for index in range(3):
                    database = str(uuid.uuid4())
                    collection = str(uuid.uuid4())
                    self.request('/databases', {'database_id': database, 'name': f'db-{index}'})
                    base = f'/databases/{database}/collections/{collection}'
                    self.request(f'/databases/{database}/collections', {
                        'collection_id': collection, 'name': 'events', 'schema': {'fields': [
                            {'name': 'key', 'type': 'string', 'nullable': False},
                            {'name': 'n', 'type': 'int64', 'nullable': False}]}})
                    rows = [{'key': f'row-{row % 17}-' + 'x' * 128, 'n': str(row)} for row in range(2000)]
                    operation = {'operation_id': str(uuid.uuid4()), 'rows': rows}
                    receipt = self.request(base + '/append', operation)
                    databases.append((database, base, rows, operation, receipt))

                stopped = threading.Event()
                physical = {'bytes': 0, 'inodes': 0, 'workspace_bytes': 0}

                def measure():
                    while not stopped.wait(.005):
                        allocated = inodes = workspace = 0
                        for parent, directories, files in os.walk(directory):
                            for name in directories + files:
                                path = Path(parent) / name
                                try:
                                    info = path.stat()
                                except FileNotFoundError:
                                    continue
                                size = max(info.st_size, info.st_blocks * 512)
                                allocated += size
                                inodes += 1
                                if name.startswith('.recovery-'):
                                    workspace += size
                        physical['bytes'] = max(physical['bytes'], allocated)
                        physical['inodes'] = max(physical['inodes'], inodes)
                        physical['workspace_bytes'] = max(physical['workspace_bytes'], workspace)

                sampler = threading.Thread(target=measure)
                sampler.start()

                def writer(item):
                    _, base, rows, _, _ = item
                    return self.request(base + '/append', {'operation_id': str(uuid.uuid4()), 'rows': rows})

                def reader(item):
                    _, base, rows, _, _ = item
                    actual = self.scan(base)
                    self.assertIn(len(actual), (2000, 4000))
                    self.assertEqual(actual[:2000], rows)
                    return len(actual)

                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                        jobs = [pool.submit(function, item) for item in databases for function in (writer, reader)]
                        for job in jobs:
                            job.result(timeout=120)
                    for database, base, rows, operation, receipt in databases:
                        self.assertEqual(self.scan(base), rows + rows)
                        self.assertEqual(self.request(base + '/append', operation), receipt)
                        self.request(f'/databases/{database}/reconcile', {})
                finally:
                    stopped.set()
                    sampler.join()
                warm = self.stop()
                self.start(directory)
                for _, base, rows, operation, receipt in databases:
                    self.assertEqual(self.scan(base), rows + rows)
                    self.assertEqual(self.request(base + '/append', operation), receipt)
                cold = self.stop()
                self.start(directory, saturated=True)
                for database, base, _, operation, receipt in databases:
                    self.assertEqual(self.request(base + '/operations/' + operation['operation_id']), receipt)
                    self.assertTrue(self.request(f'/databases/{database}/readyz')['write_ready'])
                    self.request(f'/databases/{database}/reconcile', {})
                protected = self.stop()
                print(json.dumps({'profile': 'three-database-http', 'warm': warm, 'cold_reopen': cold, 'ordinary_full': protected,
                                  'observed_physical_peak': physical, 'server_busy_refusals': self.refusals}, sort_keys=True), flush=True)
            finally:
                if self.process.poll() is None:
                    self.process.terminate()
                    self.process.communicate(timeout=90)


if __name__ == '__main__':
    unittest.main()
