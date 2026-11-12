"""Native HTTP coverage for persistent, validated engine resource limits."""
import http.client
import concurrent.futures
import json
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

SERVER = sys.argv[1]
del sys.argv[1]
CLI = sys.argv.pop(1) if len(sys.argv) > 1 and not sys.argv[1].startswith('-') else None
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
DEFAULTS = {'max_operations': 4096, 'max_collections': 8, 'max_catalog_bytes': 268435456,
            'max_pending_calls': 16, 'max_owned_input_bytes': 134217728,
            'writer_wait_ms': 5000, 'append_deadline_ms': 60000,
            'history': {'max_snapshots': 4096, 'max_manifests': 4096, 'max_data_files': 16384,
                        'max_root_bytes': 64 << 20, 'max_root_file_bytes': 16 << 20,
                        'max_manifest_bytes': 256 << 20, 'max_manifest_list_bytes': 512 << 20,
                        'max_footer_bytes': 256 << 20, 'max_commit_bytes': 32 << 20,
                        'max_recovery_workspace_bytes': 256 << 20}}


class DatabaseLimits(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve()))
        self.addCleanup(self.tmp.cleanup)
        self.directory = str(Path(self.tmp.name).resolve() / 'service')
        self.proc = None
        self.addCleanup(self.stop)
        self.start()

    def start(self):
        started = time.monotonic()
        self.proc = subprocess.Popen([SERVER, '--data-dir', self.directory, '--port', '0',
            '--log-success-sample-every', '1000000'],
            env=dict(os.environ, GLAMDB_TOKEN='limits-test'), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        self.assertTrue(select.select([self.proc.stdout], [], [], 15)[0])
        line = self.proc.stdout.readline()
        self.assertTrue(line, self.proc.stderr.read() if self.proc.poll() is not None else 'no server output')
        self.port = json.loads(line)['port']
        self.startup_ms = (time.monotonic() - started) * 1000

    def stop(self):
        if self.proc is not None:
            if self.proc.poll() is None:
                self.proc.terminate()
            out, err = self.proc.communicate(timeout=30)
            self.assertEqual(self.proc.returncode, 0, out + err)
            self.proc = None

    def api(self, path, body=None, status=200):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        headers = {'Authorization': 'Bearer limits-test', 'Content-Type': 'application/json'}
        connection.request('GET' if body is None else 'POST', path,
                           None if body is None else json.dumps(body), headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        self.assertEqual(response.status, status, data)
        return json.loads(data)

    def collection(self, base, name, status=200):
        return self.api(base + '/collections', {'collection_id': str(uuid.uuid4()), 'name': name,
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}}, status)

    def test_limits_apply_and_survive_closed_restart_and_explicit_reopen(self):
        limits = dict(DEFAULTS, max_collections=1, max_operations=1, writer_wait_ms=0)
        database = self.api('/databases', {'name': 'limited', 'limits': {
            'max_collections': 1, 'max_operations': 1, 'writer_wait_ms': 0}})
        self.assertEqual(database['limits'], limits)
        base = '/databases/' + database['database_id']
        first = self.collection(base, 'one')
        # Exhausted catalog capacity needs operator action: 507, not a 429 retry signal.
        rejected = self.collection(base, 'two', 507)
        self.assertEqual(rejected['error']['code'], 'RESOURCE_EXHAUSTED')
        self.assertEqual(rejected['error']['retry_hint'], 'OperatorAction')
        append = base + '/collections/' + first['collection_id'] + '/append'
        self.api(append, {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'one'}]})
        self.api(append, {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'two'}]}, 507)
        self.assertEqual(self.api('/databases', {'name': 'limited'})['limits'], limits)
        self.api('/databases', {'name': 'limited', 'limits': {'max_collections': 2}}, 409)
        self.api(base + '/open', {'limits': {'max_collections': 2}}, 412)
        self.assertEqual(self.api(base + '/close', {})['limits'], limits)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['state'], 'closed')
        self.assertEqual(self.api(base)['limits'], limits)
        changed = self.api(base + '/open', {'limits': {'max_collections': 2, 'max_operations': 2}})
        limits.update(max_collections=2, max_operations=2)
        self.assertEqual(changed['limits'], limits)
        self.collection(base, 'two')
        self.api(append, {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'two'}]})
        self.assertEqual(self.api(base + '/open', {'limits': limits})['limits'], limits)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['limits'], limits)
        self.assertEqual(self.api(base)['state'], 'open')

    def test_opt_in_operation_budget_survives_restart(self):
        database = self.api('/databases', {'name': 'large-history', 'limits': {
            'max_operations': 16384, 'history': {'max_snapshots': 12000,
                'max_manifests': 12000, 'max_manifest_list_bytes': 4 << 30}}})
        base = '/databases/' + database['database_id']
        self.assertEqual(database['limits']['max_operations'], 16384)
        first = self.collection(base, 'events')
        operation = str(uuid.uuid4())
        path = base + '/collections/' + first['collection_id'] + '/append'
        payload = {'operation_id': operation, 'rows': [{'key': 'duplicate'}, {'key': 'duplicate'}]}
        receipt = self.api(path, payload)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['limits']['max_operations'], 16384)
        self.assertEqual(self.api(base)['limits']['history']['max_snapshots'], 12000)
        self.assertEqual(self.api(base)['limits']['history']['max_manifest_list_bytes'], 4 << 30)
        self.assertEqual(self.api(path, payload), receipt)

    def test_history_capacity_is_aggregate_and_does_not_bind_refused_append(self):
        database = self.api('/databases', {'name': 'history-boundary', 'limits': {
            'max_operations': 16384, 'history': {'max_snapshots': 1}}})
        base = '/databases/' + database['database_id']
        first = self.collection(base, 'first')
        second = self.collection(base, 'second')
        path = base + '/collections/' + first['collection_id'] + '/append'
        payload = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'one'}]}
        original = self.api(path, payload)
        other = base + '/collections/' + second['collection_id'] + '/append'
        refused = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'two'}]}
        self.api(other, refused, 507)
        self.api(base + '/close', {})
        self.api(base + '/open', {'limits': {'history': {'max_snapshots': 2}}})
        self.api(other, refused)
        self.assertEqual(self.api(path, payload), original)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['limits']['history']['max_snapshots'], 2)

    def test_invalid_limits_do_not_reserve_names_or_change_open_database(self):
        bad = [None, [], {'unknown': 1}, {'max_operations': True}, {'max_operations': 1.0},
               {'max_operations': '1'}, {'max_operations': 0},
               {'max_collections': 65}, {'max_catalog_bytes': 65535},
               {'max_pending_calls': 17}, {'max_owned_input_bytes': 0},
               {'writer_wait_ms': -1}, {'writer_wait_ms': 5001},
               {'append_deadline_ms': 0}, {'append_deadline_ms': 60001},
               {'history': None}, {'history': []}, {'history': {'unknown': 1}},
               {'history': {'max_snapshots': True}}, {'history': {'max_snapshots': 0}},
               {'history': {'max_recovery_workspace_bytes': 8191}},
               {'history': {'max_root_file_bytes': (64 << 20) + 1}},
               {'history': {'max_manifest_list_bytes': 18446744073709551615}},
               {'max_operations': 18446744073709551615}]
        for limits in bad:
            with self.subTest(limits=limits):
                self.api('/databases', {'name': 'invalid', 'limits': limits}, 400)
                self.assertEqual(self.api('/databases')['items'], [])
        database = self.api('/databases', {'name': 'invalid'})
        self.assertEqual(database['limits'], DEFAULTS)
        base = '/databases/' + database['database_id']
        self.api(base + '/open', {'limits': {'writer_wait_ms': -1}}, 400)
        self.assertEqual(self.api(base)['state'], 'open')
        self.assertEqual(self.api(base)['limits'], DEFAULTS)
        self.collection(base, 'still-usable')

    def test_failed_open_preserves_requested_limits_for_recovery(self):
        database = self.api('/databases', {'name': 'reconfigure'})
        base = '/databases/' + database['database_id']
        self.collection(base, 'one')
        self.collection(base, 'two')
        self.api(base + '/close', {})
        self.api(base + '/open', {'limits': {'max_collections': 1}}, 400)
        failed = self.api(base)
        self.assertEqual(failed['state'], 'error')
        self.assertEqual(failed['limits']['max_collections'], 1)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['state'], 'error')
        self.assertEqual(self.api(base)['limits']['max_collections'], 1)
        self.api(base + '/close', {})
        self.api(base + '/open', {'limits': {'max_collections': 2}})
        self.assertEqual(len(self.api(base + '/collections')['items']), 2)

    def test_new_registry_has_complete_schema_without_migration(self):
        database = self.api('/databases', {'name': 'complete'})
        base = '/databases/' + database['database_id']
        self.api(base + '/close', {})
        self.stop()
        with sqlite3.connect(Path(self.directory) / 'registry.sqlite') as registry:
            self.assertEqual(registry.execute('PRAGMA user_version').fetchone()[0], 3)
            limits = registry.execute('SELECT limits FROM databases').fetchone()[0]
            self.assertEqual(json.loads(limits), DEFAULTS)
        self.start()
        self.assertEqual(self.api(base)['state'], 'closed')
        self.assertEqual(self.api(base)['limits'], DEFAULTS)

    def test_collection_profiles_preserve_every_identity_across_fresh_processes(self):
        from glamdb_sdk import Client
        tables = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/tpch-collections.json').read_text())['tables']
        reports = []
        for count in (8, 16, 64):
            with self.subTest(collections=count):
                database = self.api('/databases', {'name': f'profile-{count}',
                    'limits': {'max_collections': count}})
                database_id = database['database_id']
                base = '/databases/' + database_id
                expected = []
                started = time.monotonic()
                for index in range(count):
                    table = tables[index % 8]
                    schema = {'fields': [{'name': name, 'nullable': False,
                        'type': 'int64' if name in table['integers'] else 'float64' if name in table['decimals'] else 'string'}
                        for name in table['row']]}
                    definition = {'collection_id': str(uuid.uuid4()),
                        'name': f'dataset-{index // 8}/{table["name"]}', 'schema': schema}
                    info = self.api(base + '/collections', definition)
                    row = dict(table['row'])
                    comment = next(name for name in row if name.endswith('_comment'))
                    row[comment] += f' dataset-{index // 8}'
                    expected.append((definition, info, {'operation_id': str(uuid.uuid4()),
                        'rows': [row, dict(row)]}))
                create_ms = (time.monotonic() - started) * 1000
                exhausted = self.collection(base, 'no-slot', 507)
                self.assertEqual(exhausted['error']['code'], 'RESOURCE_EXHAUSTED')
                self.assertEqual(self.api(base + '/collections', expected[-1][0]), expected[-1][1])
                self.api(base + '/collections:resolve?name=no-slot', status=404)

                # Exercise concurrent exact-identity writes and full inventories.
                def append(entry):
                    definition, _, body = entry
                    return self.api(base + '/collections/' + definition['collection_id'] + '/append', body)
                def inventory(_):
                    return self.api(base + '/collections')['items']
                concurrent_started = time.monotonic()
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
                    futures, readers = [], []
                    for index, entry in enumerate(expected):
                        futures.append(workers.submit(append, entry))
                        if index % 4 == 0:
                            readers.append(workers.submit(inventory, index))
                    outcomes = [future.result() for future in futures]
                    for reader in readers:
                        self.assertEqual({item['collection_id'] for item in reader.result()},
                                         {entry[0]['collection_id'] for entry in expected})
                concurrent_ms = (time.monotonic() - concurrent_started) * 1000

                def verify():
                    capabilities = self.api(base + '/capabilities')
                    self.assertEqual(capabilities['limits']['max_collections'], count)
                    self.assertEqual(capabilities['limits']['max_supported_collections'], 64)
                    self.assertTrue(capabilities['features']['pagination'])
                    with Client(f'http://127.0.0.1:{self.port}', 'limits-test', database_id=database_id) as client:
                        items, cursor = [], None
                        while True:
                            page = client.list_collections(page_size=7, cursor=cursor)
                            items.extend(page.items)
                            cursor = page.next_cursor
                            if cursor is None:
                                break
                        self.assertEqual({item['collection_id'] for item in items},
                                         {entry[0]['collection_id'] for entry in expected})
                        self.assertEqual(len(items), count)
                        for entry, original in zip(expected, outcomes):
                            definition, info, body = entry
                            cid = definition['collection_id']
                            self.assertEqual(client.resolve_collection(definition['name'])['collection_id'], cid)
                            self.assertEqual(client.get_collection(cid)['schema'], info['schema'])
                            rows = self.api(base + '/collections/' + cid + '/scan', {})['rows']
                            self.assertEqual(rows, body['rows'])
                            self.assertEqual(self.api(base + '/collections/' + cid + '/append', body), original)
                    if CLI:
                        result = subprocess.run([CLI, 'collections', '--url', f'http://127.0.0.1:{self.port}',
                            '--database-id', database_id], env=dict(os.environ, GLAMDB_TOKEN='limits-test'),
                            capture_output=True, text=True, timeout=30)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(len(json.loads(result.stdout)['items']), count)
                    return capabilities['shared_resources']

                resources = verify()
                rss = {line.split(':', 1)[0]: line.split(':', 1)[1].strip()
                       for line in Path(f'/proc/{self.proc.pid}/status').read_text().splitlines()
                       if line.startswith(('VmRSS:', 'VmHWM:'))}
                catalog_bytes = (Path(self.directory) / 'databases' / database_id / 'catalog.sqlite').stat().st_size
                self.stop()
                self.start()
                recovery_ms = self.startup_ms
                self.assertEqual(self.api(base)['limits']['max_collections'], count)
                verify()
                self.api(base + '/close', {})
                self.assertEqual(self.api(base + '/capabilities')['limits']['max_collections'], count)
                if count > 8:
                    rejected = self.api(base + '/open', {'limits': {'max_collections': count - 1}}, 400)
                    self.assertEqual(rejected['error']['code'], 'CONFIG_TOO_SMALL')
                    self.assertEqual(self.api(base + '/capabilities')['limits']['max_collections'], count - 1)
                    self.api(base + '/close', {})
                    self.api(base + '/open', {'limits': {'max_collections': count}})
                    verify()
                    self.api(base + '/close', {})
                reports.append({'collections': count, 'create_ms': round(create_ms, 1),
                    'concurrent_append_inventory_ms': round(concurrent_ms, 1),
                    'fresh_process_recovery_ms': round(recovery_ms, 1), 'catalog_bytes': catalog_bytes,
                    **rss, **resources})
        print('COLLECTION_PROFILES ' + json.dumps(reports), flush=True)

    def test_collection_continuation_pins_membership_and_parameters(self):
        database = self.api('/databases', {'name': 'paging', 'limits': {'max_collections': 16}})
        base = '/databases/' + database['database_id']
        original = [self.collection(base, f'item-{index}') for index in range(9)]
        first = self.api(base + '/collections?page_size=4')
        self.assertIsInstance(first['next_cursor'], str)
        later = self.collection(base, 'created-after-first-page')
        path = base + '/collections?page_size=4&cursor=' + first['next_cursor']
        self.api(path.replace('page_size=4', 'page_size=3'), status=400)
        second = self.api(path)
        self.assertEqual(self.api(path), second)
        third = self.api(base + '/collections?page_size=4&cursor=' + second['next_cursor'])
        items = first['items'] + second['items'] + third['items']
        self.assertIsNone(third['next_cursor'])
        self.assertEqual([item['collection_id'] for item in items], [item['collection_id'] for item in original])
        self.assertNotIn(later['collection_id'], {item['collection_id'] for item in items})
        self.assertEqual(first['creation_high_water_mark'], third['creation_high_water_mark'])
        unread = self.api(base + '/collections?page_size=9')
        for _ in range(36):
            page = self.api(base + '/collections?page_size=9')
            completed = self.api(base + '/collections?page_size=9&cursor=' + page['next_cursor'])
            self.assertIsNone(completed['next_cursor'])
        continued = self.api(base + '/collections?page_size=9&cursor=' + unread['next_cursor'])
        self.assertEqual([item['collection_id'] for item in continued['items']], [later['collection_id']])
        self.assertEqual(len(self.api(base + '/collections')['items']), 10)
        self.api(base + '/close', {})
        self.api(base + '/open', {})
        self.api(path, status=400)

    def test_legacy_registry_without_history_budget_preserves_original_receipts(self):
        database = self.api('/databases', {'name': 'legacy'})
        base = '/databases/' + database['database_id']
        collection = self.collection(base, 'events')
        path = base + '/collections/' + collection['collection_id'] + '/append'
        payload = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'retained'}]}
        receipt = self.api(path, payload)
        self.stop()
        legacy = {key: value for key, value in DEFAULTS.items() if key != 'history'}
        with sqlite3.connect(Path(self.directory) / 'registry.sqlite') as registry:
            registry.execute('UPDATE databases SET limits=?', (json.dumps(legacy),))
        self.start()
        self.assertEqual(self.api(base)['limits'], DEFAULTS)
        self.assertEqual(self.api(path, payload), receipt)


if __name__ == '__main__':
    unittest.main()
