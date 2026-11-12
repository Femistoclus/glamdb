"""Snapshot archive over actual native HTTP storage and restart; no mock backend."""
import json
from pathlib import Path
import sys
import unittest
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
_test_arguments = sys.argv[4:]
from native_http_test import NativeHTTP, TOKEN
sys.argv[1:] = _test_arguments

ROOT = Path(__file__).resolve().parents[2]
WIRE = json.loads((ROOT / 'fixtures/http/v0.schema.json').read_text())
SCHEMA = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False},
                     {'name': 'n', 'type': 'int64', 'nullable': True}]}


class NativeSnapshotArchiveHTTP(NativeHTTP):
    def validate(self, kind, value):
        try:
            from jsonschema import Draft202012Validator
        except ImportError:
            # Shapes are still asserted field by field below.
            return
        Draft202012Validator(WIRE | {'$ref': '#/$defs/' + kind}).validate(value)

    def error(self, path, body=None, status=409):
        code, _, data = self.request(path, body)
        self.assertEqual(code, status, data)
        value = json.loads(data)
        self.validate('error', {'error': value['error']})
        return value['error']['code']

    @classmethod
    def without_identity(cls, value):
        """Identities name the current root, which every append changes."""
        if isinstance(value, dict):
            return {key: cls.without_identity(item) for key, item in value.items() if key != 'identity'}
        if isinstance(value, list):
            return [cls.without_identity(item) for item in value]
        return value

    def answers(self, path, snapshot):
        """Everything a selected snapshot answers, apart from current-root identity."""
        stats = self.api(f'{path}/stats?snapshot_id={snapshot}')
        # The retained count is reported only for the current view.
        stats.pop('retained_snapshots')
        files = self.api(f'{path}/files?snapshot_id={snapshot}&page_size=1000')
        details = [self.api(f"{path}/files/{item['file_id']}?snapshot_id={snapshot}") for item in files['items']]
        scan = self.api(path + '/scan', {'snapshot_id': snapshot})
        explain = self.api(path + '/explain', {'snapshot_id': snapshot})
        return self.without_identity({'stats': stats, 'files': files['items'], 'files_snapshot': files['snapshot_id'],
                                      'details': details, 'rows': scan['rows'], 'scan_snapshot': scan['snapshot_id'],
                                      'explain': explain})

    def history(self, path, page_size):
        listed, cursor, identity = [], None, None
        while True:
            page = self.api(f'{path}/snapshots?page_size={page_size}' + (f'&cursor={cursor}' if cursor else ''))
            self.validate('snapshotsPage', page)
            identity = identity or page['identity']
            self.assertEqual(page['identity'], identity)
            listed.extend(page['items'])
            cursor = page['next_cursor']
            if cursor is None:
                return listed

    def test_capabilities_creation_settings_and_kv_refusal(self):
        for capabilities in (self.api('/capabilities'), self.api(f'/databases/{self.store_id}/capabilities')):
            self.assertIs(capabilities['features']['snapshot_archive'], True)
        cid = str(uuid.uuid4())
        path = '/collections/' + cid
        body = {'collection_id': cid, 'name': 'archive', 'schema': SCHEMA, 'hot_snapshots': '3'}
        created = self.api('/collections', body)
        self.validate('collection', created)
        self.assertEqual(created['hot_snapshots'], '3')
        # An exact retry returns the collection; any other definition is refused.
        self.assertEqual(self.api('/collections', body), created)
        for other in ('4', None, 'omitted'):
            changed = {key: value for key, value in body.items() if key != 'hot_snapshots'}
            if other != 'omitted':
                changed['hot_snapshots'] = other
            self.assertEqual(self.error('/collections', changed), 'SNAPSHOT_ARCHIVE_MISMATCH')
        plain = str(uuid.uuid4())
        default = self.api('/collections', {'collection_id': plain, 'name': 'plain', 'schema': SCHEMA})
        self.assertIsNone(default['hot_snapshots'])
        self.assertEqual(self.api('/collections', {'collection_id': plain, 'name': 'plain', 'schema': SCHEMA,
                                                   'hot_snapshots': None}), default)
        for invalid in ('0', '1099511627777', 3, '+3', ''):
            self.assertEqual(self.error('/collections', {'collection_id': str(uuid.uuid4()), 'name': 'bad',
                                                         'schema': SCHEMA, 'hot_snapshots': invalid}, 400),
                             'INVALID_ARGUMENT')
            self.assertEqual(self.error(path + '/snapshot-archive', {'hot_snapshots': invalid}, 400),
                             'INVALID_ARGUMENT')
        self.assertEqual(self.error(path + '/snapshot-archive', {}, 400), 'INVALID_ARGUMENT')
        self.assertEqual(self.error(path + '/snapshot-archive/maintenance', {'hot_snapshots': '1'}, 400),
                         'INVALID_ARGUMENT')
        # Only GET and POST exist; the HTTP layer refuses other methods (with a body, before routing).
        code, _, _ = self.request(path + '/snapshot-archive', {'hot_snapshots': '1'}, method='PUT')
        self.assertIn(code, (400, 405))
        self.assertEqual(self.api(path)['hot_snapshots'], '3')
        self.assertEqual(self.error('/collections/' + str(uuid.uuid4()) + '/snapshot-archive', None, 404),
                         'COLLECTION_NOT_FOUND')

        updated = self.api(path + '/snapshot-archive', {'hot_snapshots': '1099511627776'})
        self.validate('collection', updated)
        self.assertEqual(updated['hot_snapshots'], '1099511627776')
        self.assertEqual(self.api(path + '/snapshot-archive', {'hot_snapshots': '1099511627776'}), updated)
        self.assertEqual(self.api(path), updated)
        # Creation keeps comparing the created value.
        self.assertEqual(self.api('/collections', body), updated)

        kv = {'collection_id': str(uuid.uuid4()), 'name': 'kv', 'mode': 'kv', 'schema': SCHEMA}
        self.assertEqual(self.error('/collections', dict(kv, hot_snapshots='2')), 'COLLECTION_MODE_MISMATCH')
        self.assertIsNone(self.api('/collections', kv)['hot_snapshots'])
        kv_path = '/collections/' + kv['collection_id']
        self.assertEqual(self.error(kv_path + '/snapshot-archive', {'hot_snapshots': '2'}),
                         'COLLECTION_MODE_MISMATCH')
        self.assertEqual(self.error(kv_path + '/snapshot-archive/maintenance', {}), 'COLLECTION_MODE_MISMATCH')
        self.assertIsNone(self.api(kv_path + '/snapshot-archive', {'hot_snapshots': None})['hot_snapshots'])
        status = self.api(kv_path + '/snapshot-archive')
        self.validate('snapshotArchive', status)
        self.assertEqual(status['archived_snapshots'], '0')

    def test_archived_snapshots_read_like_hot_ones_across_pages_maintenance_and_restart(self):
        cid = str(uuid.uuid4())
        path = '/collections/' + cid
        self.api('/collections', {'collection_id': cid, 'name': 'archive', 'schema': SCHEMA, 'hot_snapshots': '3'})
        snapshots, operations, hot = [], [], {}
        appends = 48
        for index in range(appends):
            operation = str(uuid.uuid4())
            rows = [{'key': 'duplicate', 'n': str(9007199254740993 + index)}, {'key': 'duplicate', 'n': None}]
            receipt = self.api(path + '/append', {'operation_id': operation, 'rows': rows})['result']
            snapshots.append(receipt['snapshot_id'])
            operations.append((operation, receipt))
            if index % 11 == 0:
                hot[index] = self.answers(path, receipt['snapshot_id'])
            status = self.api(path + '/snapshot-archive')
            self.validate('snapshotArchive', status)
            # The root keeps at most hot + min(hot, 16) snapshots.
            self.assertLessEqual(int(status['root_snapshots']), 6)
            self.assertEqual(int(status['root_snapshots']) + int(status['archived_snapshots']), index + 1)
            self.assertEqual(status['expired_snapshots'], '0')
        archived = int(self.api(path + '/snapshot-archive')['archived_snapshots'])
        self.assertGreaterEqual(archived, appends - 6)

        def check_history_and_reads():
            for page_size in (5, 1000):
                listed = self.history(path, page_size)
                self.assertEqual([item['snapshot_id'] for item in listed], snapshots)
                self.assertEqual([item['operation_id'] for item in listed], [op for op, _ in operations])
                self.assertEqual([item['receipt'] for item in listed], [receipt for _, receipt in operations])
            self.assertEqual(self.api(path + '/stats')['retained_snapshots'], str(appends))
            for index, expected in hot.items():
                with self.subTest(snapshot=index):
                    self.assertEqual(self.answers(path, snapshots[index]), expected)
                    self.assertEqual(len(expected['rows']), 2 * (index + 1))
            selected = self.api(f'{path}/snapshots?page_size=1000&snapshot_id={snapshots[2]}')
            self.assertEqual([item['snapshot_id'] for item in selected['items']], snapshots[:3])
            # An archived snapshot is never the current one for preconditions.
            stale = self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'x'}],
                                                'expected_snapshot': snapshots[0]}, status=412)
            self.assertEqual(stale['error']['code'], 'PRECONDITION_FAILED')
            # Retrying an archived snapshot's operation returns its original receipt.
            operation, receipt = operations[0]
            self.assertEqual(self.api(f'{path}/operations/{operation}')['result'], receipt)
            rows = [{'key': 'duplicate', 'n': '9007199254740993'}, {'key': 'duplicate', 'n': None}]
            self.assertEqual(self.api(path + '/append', {'operation_id': operation, 'rows': rows})['result'], receipt)

        check_history_and_reads()
        self.assertEqual(self.api(path + '/snapshot-archive', {'hot_snapshots': '1'})['hot_snapshots'], '1')
        before = self.api(path + '/snapshot-archive')
        self.assertEqual(before['archive_pending'], str(int(before['root_snapshots']) - 1))
        moved = self.api(path + '/snapshot-archive/maintenance', {})
        self.validate('snapshotArchiveMaintenance', moved)
        self.assertEqual(moved, {'hot_snapshots': '1', 'root_snapshots': '1', 'archived_snapshots': str(appends - 1),
                                 'expired_snapshots': '0', 'archive_pending': '0',
                                 'snapshots_archived': before['archive_pending']})
        repeated = self.api(path + '/snapshot-archive/maintenance', {})
        self.assertEqual(repeated, dict(moved, snapshots_archived='0'))
        check_history_and_reads()
        self.assertEqual(self.api(path + '/snapshot-archive', {'hot_snapshots': None})['hot_snapshots'], None)
        # Unsetting stops archiving; nothing returns to the root.
        extra = self.api(path + '/append', {'operation_id': str(uuid.uuid4()),
                                            'rows': [{'key': 'after-unset'}]})['result']
        status = self.api(path + '/snapshot-archive')
        self.assertEqual((status['root_snapshots'], status['archived_snapshots'], status['archive_pending']),
                         ('2', str(appends - 1), '0'))
        snapshots.append(extra['snapshot_id'])
        operations.append((extra['operation_id'], extra))
        appends += 1
        self.assertEqual(extra['operation_id'], operations[-1][0])

        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/snapshot-archive'), status)
        self.assertIsNone(self.api(path)['hot_snapshots'])
        check_history_and_reads()
        footprint = self.api(f'/databases/{self.store_id}/storage')
        self.assertGreater(int(footprint['referenced_metadata_file_bytes']), 0)

    def test_sdk_archive_methods_against_native_storage(self):
        from glamdb_sdk import Client, HTTPError, Schema
        schema = Schema.from_dict(SCHEMA)
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.store_id) as client:
            cid = str(uuid.uuid4())
            created = client.create_collection('sdk_archive', schema, collection_id=cid, hot_snapshots=2)
            self.assertEqual(created.info['hot_snapshots'], '2')
            ids = [client.append(cid, [{'key': str(index)}], operation_id=uuid.uuid4(), schema=schema)
                   .result['snapshot_id'] for index in range(9)]
            status = client.snapshot_archive(cid)
            self.assertEqual(int(status['root_snapshots']) + int(status['archived_snapshots']), 9)
            self.assertEqual([item['snapshot_id'] for item in client.snapshots(cid).items], ids)
            self.assertEqual([row['key'] for row in client.scan_json(cid, snapshot_id=ids[0]).rows], ['0'])
            self.assertEqual(client.update_snapshot_archive(cid, 1)['hot_snapshots'], '1')
            self.assertEqual(client.archive_snapshots(cid)['root_snapshots'], '1')
            refused = client.create_collection('sdk_archive', schema, collection_id=cid, hot_snapshots=1)
            self.assertEqual((refused.outcome, refused.error.code),
                             ('definitely_not_committed', 'SNAPSHOT_ARCHIVE_MISMATCH'))
            with self.assertRaises(HTTPError) as missing:
                client.update_snapshot_archive(str(uuid.uuid4()), 1)
            self.assertEqual(missing.exception.detail.code, 'COLLECTION_NOT_FOUND')


for name in list(vars(NativeHTTP)):
    if name.startswith('test_'):
        setattr(NativeSnapshotArchiveHTTP, name, None)
del NativeHTTP

if __name__ == '__main__':
    unittest.main()
