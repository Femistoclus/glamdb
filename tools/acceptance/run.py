#!/usr/bin/env python3
"""Installed native service, CLI, SDK and browser on one persistent A/B store."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import select
import subprocess
import sys
import time
import traceback
import uuid

if __package__:
    from .backup import copy_store, quiesced
    from .physical import inspect_store
else:
    from backup import copy_store, quiesced
    from physical import inspect_store
from glamdb_sdk import ANY, Client, Field, Schema


FORMAT = 'glamdb-native-acceptance-v1'
SCHEMA = Schema([Field('key', 'string'), Field('n', 'int64'), Field('label', 'string', True)])


def fixture_rows():
    a = [{'key': 'shared-' + str(i % 37), 'n': str(9007199254740993 + i),
          'label': None if i % 7 == 0 else 'lovely ✨ ' + str(i)} for i in range(1000)]
    return a, [dict(row) for row in a[:10]]


def multiset(rows):
    return Counter(json.dumps(dict(row, n=str(row['n'])), sort_keys=True, ensure_ascii=False) for row in rows)


def save(path, data, mode=0o600):
    with path.open('w') as stream:
        os.chmod(path, mode)
        json.dump(data, stream, indent=2, ensure_ascii=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.prefix = args.prefix.resolve(strict=True)
        self.directory = args.workspace
        if self.directory.exists():
            if self.directory.is_symlink() or self.directory.resolve() != self.directory:
                raise ValueError('workspace must be a canonical owned directory')
            state_path = self.directory / 'acceptance-state.json'
            self.state = json.loads(state_path.read_text())
            if self.state.get('format') != FORMAT:
                raise ValueError('existing directory is not an acceptance workspace')
            self.token = (self.directory / 'token').read_text().strip()
        else:
            if not self.directory.is_absolute() or self.directory.parent.resolve(strict=True) != self.directory.parent:
                raise ValueError('workspace requires a canonical existing parent')
            self.directory.mkdir(mode=0o700)
            self.state = {'format': FORMAT, 'store_id': str(uuid.uuid4()), 'collection_id': str(uuid.uuid4()),
                'operation_a': str(uuid.uuid4()), 'operation_b': str(uuid.uuid4()), 'operation_failed': str(uuid.uuid4())}
            self.token = secrets.token_hex(32)
            token_path = self.directory / 'token'
            token_path.write_text(self.token)
            os.chmod(token_path, 0o600)
            self.persist()
        if 'ui_collection_id' not in self.state:
            self.state['ui_collection_id'] = str(uuid.uuid4())
            self.persist()
        self.root = self.directory / 'store'
        self.admin = self.prefix / 'bin/glamdb-admin'
        self.server = self.prefix / 'bin/glamdb'
        self.cli = self.prefix / 'bin/glamdb-cli'
        for executable in (self.admin, self.server, self.cli):
            if not executable.is_file():
                raise ValueError('installed native executable missing: ' + str(executable))
        self.environment = dict(os.environ, GLAMDB_TOKEN=self.token, GLAMDB_E2E_PYTHON=sys.executable)
        self.process = None
        self.server_log = None
        self.commands = []
        self.report = {'format': FORMAT, 'backend': 'local', 'mock': False, 'status': 'RUNNING',
            'store_id': self.state['store_id'], 'collection_id': self.state['collection_id'],
            'prefix': str(self.prefix), 'workspace': str(self.directory), 'checks': {}, 'power_loss': 'NOT RUN'}
        repository = Path(__file__).resolve().parents[2]
        revision = subprocess.run(['git', '-C', str(repository), 'rev-parse', 'HEAD'],
            capture_output=True, text=True, timeout=10)
        dirty = subprocess.run(['git', '-C', str(repository), 'diff', '--quiet'], timeout=10)
        binaries = {}
        for executable in (self.admin, self.server, self.cli):
            with executable.open('rb') as stream:
                binaries[executable.name] = hashlib.file_digest(stream, 'sha256').hexdigest()
        ui_files = {}
        for asset in sorted((self.prefix / 'share/glamdb/ui').rglob('*')):
            if asset.is_file():
                with asset.open('rb') as stream:
                    ui_files[str(asset.relative_to(self.prefix))] = hashlib.file_digest(stream, 'sha256').hexdigest()
        import pyarrow
        import fastavro
        self.report['versions'] = {'python': sys.version, 'platform': platform.platform(),
            'node': subprocess.check_output([args.node, '--version'], text=True, timeout=10).strip(),
            'pyarrow': pyarrow.__version__, 'fastavro': fastavro.__version__,
            'source_revision': revision.stdout.strip() if revision.returncode == 0 else None,
            'uncommitted_tracked_changes': dirty.returncode != 0, 'binaries_sha256': binaries,
            'installed_ui_sha256': ui_files}

    def persist(self):
        save(self.directory / 'acceptance-state.json', self.state)

    def command(self, arguments, **kwargs):
        started = time.monotonic()
        result = subprocess.run([str(value) for value in arguments], env=self.environment,
            text=True, capture_output=True, timeout=90, **kwargs)
        self.commands.append({'command': [str(value) for value in arguments], 'exit_code': result.returncode,
            'duration_seconds': round(time.monotonic() - started, 3)})
        if result.returncode:
            raise RuntimeError('command failed: ' + str(arguments[0]) + ': ' + result.stderr[-4096:])
        return json.loads(result.stdout)

    def api(self, path, body=None, name='request'):
        arguments = [self.cli, 'request', '--url', self.url + path]
        if body is not None:
            payload = self.directory / (name + '.json')
            save(payload, body)
            arguments += ['--method', 'POST', '--body', payload]
        return self.command(arguments)

    def start(self, root=None):
        self.server_log = (self.directory / ('server-' + uuid.uuid4().hex[:8] + '.log')).open('w')
        self.process = subprocess.Popen([str(self.server), '--root', str(root or self.root),
            '--store-id', self.state['store_id'], '--port', '0'], env=self.environment,
            stdout=subprocess.PIPE, stderr=self.server_log, text=True)
        if not select.select([self.process.stdout], [], [], 30)[0]:
            raise RuntimeError('installed native service did not announce readiness')
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError('installed native service exited during startup; inspect server log')
        self.url = 'http://127.0.0.1:' + str(json.loads(line)['port'])
        with Client(self.url, self.token) as client:
            capabilities = client.capabilities()
            if capabilities.get('backend') != 'local' or capabilities.get('mock') is not False:
                raise RuntimeError('acceptance requires the real local backend; fallback is forbidden')
            if capabilities.get('store_id') != self.state['store_id']:
                raise RuntimeError('installed service opened another store')
            if not all(capabilities['features'].get(name) for name in ('stats', 'snapshots', 'files', 'explain')):
                raise RuntimeError('native metadata capabilities are missing')
        health = self.api('/readyz')
        assert health['write_ready'] is True

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
            self.process.communicate(timeout=45)
            if self.process.returncode:
                raise RuntimeError('native service failed during shutdown')
            self.process = None
        if self.server_log:
            self.server_log.close()
            self.server_log = None

    def seed(self):
        cid = self.state['collection_id']
        a, b = fixture_rows()
        with Client(self.url, self.token) as client:
            created = client.create_collection('acceptance_ab', SCHEMA, collection_id=cid)
            assert created.outcome == 'committed'
            ui_collection = client.create_collection('acceptance_ui', SCHEMA,
                collection_id=self.state['ui_collection_id'])
            assert ui_collection.outcome == 'committed'
            original = client.prepare_append(cid, a, operation_id=self.state['operation_a'], schema=SCHEMA)
            first = self.api('/collections/' + cid + '/append', json.loads(original.body), 'append-a')
            assert first['outcome'] == 'committed' and first['result']['rows_appended'] == '1000'
            second = client.append(cid, b, operation_id=self.state['operation_b'], schema=SCHEMA)
            assert second.outcome == 'committed' and second.result['rows_appended'] == '10'
            failed = client.append(cid, [{'key': 'must-not-appear', 'n': 0, 'label': None}],
                operation_id=self.state['operation_failed'], schema=SCHEMA, expected_snapshot=None)
            assert failed.outcome == 'definitely_not_committed' and failed.error.code == 'PRECONDITION_FAILED'
            self.state.update(receipt_a=first, receipt_b=dict(second.raw), failed_outcome=dict(failed.raw))
            self.persist()

    def views(self):
        return [('initial', None, 0, 0), ('a', int(self.state['receipt_a']['result']['snapshot_id']), 1000, 1),
            ('latest', ANY, 1010, 2)]

    def verify(self, stage):
        cid = self.state['collection_id']
        a, b = fixture_rows()
        evidence = {}
        retained = []
        with Client(self.url, self.token) as client:
            for name, selector, count, files in self.views():
                expected = [] if name == 'initial' else a if name == 'a' else a + b
                stats = client.stats(cid, snapshot_id=selector)
                assert stats['rows'] == str(count) and stats['data_files'] == str(files), stats
                result = client.scan_json(cid, snapshot_id=selector)
                assert multiset(result.rows) == multiset(expected)
                body = {} if selector is ANY else {'snapshot_id': None if selector is None else str(selector)}
                cli_rows = self.api('/collections/' + cid + '/scan', body, 'scan-' + name)
                assert multiset(cli_rows['rows']) == multiset(expected)
                with client.scan_batches(cid, snapshot_id=selector) as stream:
                    assert stream.schema.names == ['key', 'n', 'label']
                    batches = list(stream)
                    assert stream.integrity == 'verified' and stream.checksum_verified, stream.integrity
                    assert stream.snapshot_id == result.snapshot_id
                arrow_rows = [row for batch in batches for row in batch.to_pylist()]
                assert multiset(arrow_rows) == multiset(expected)
                retained.extend(batches[:1])
                inventory = client.files(cid, snapshot_id=selector)
                assert inventory.next_cursor is None and len(inventory.items) == files
                api_files = []
                for file in inventory.items:
                    details = client.file(cid, file['file_id'], snapshot_id=selector)
                    assert sum(int(group['record_count']) for group in details['row_groups']) == int(file['record_count'])
                    api_files.append({'file_id': file['file_id'], 'path': file['path'],
                        'record_count': file['record_count'], 'compressed_bytes': file['compressed_bytes'],
                        'footer_bytes': details['footer_bytes'], 'row_groups': details['row_groups']})
                plan = client.explain(cid, snapshot_id=selector)
                assert plan['snapshot_id'] == result.snapshot_id
                evidence[name] = {'rows': str(count), 'data_files': str(files), 'snapshot_id': result.snapshot_id,
                    'files': sorted(api_files, key=lambda item: item['file_id']),
                    'json': 'PASS', 'cli': 'PASS', 'arrow_schema_eos_checksum': 'PASS', 'metadata': 'PASS'}
                prior = self.report['checks'].get('before_restart', {}).get(name)
                if prior:
                    assert prior['files'] == evidence[name]['files'], 'file identities/details changed across restart/restore'
            history, cursor = [], None
            while True:
                page = client.snapshots(cid, page_size=1, cursor=cursor)
                history.extend(page.items)
                cursor = page.next_cursor
                if cursor is None:
                    break
            assert [row['snapshot_id'] for row in history] == [self.state['receipt_a']['result']['snapshot_id'],
                self.state['receipt_b']['result']['snapshot_id']]
            for operation, original in [('operation_a', 'receipt_a'), ('operation_b', 'receipt_b'),
                    ('operation_failed', 'failed_outcome')]:
                outcome = client.find_append_outcome(cid, self.state[operation])
                assert outcome is not None and dict(outcome.raw) == self.state[original]
        # Library-owned batches retain their buffers after stream/client teardown.
        assert retained and retained[0].column('n')[0].as_py() == 9007199254740993
        evidence['retained_arrow_batch'] = 'PASS'
        self.report['checks'][stage] = evidence

    def browser(self, stage):
        if self.args.no_browser:
            self.report['checks'][stage + '_browser'] = 'NOT RUN (explicit --no-browser)'
            return
        request = {'url': self.url, 'token_file': str(self.directory / 'token'),
            'collection_id': self.state['collection_id'], 'snapshot_a': self.state['receipt_a']['result']['snapshot_id'],
            'operation_id': self.state['operation_a'],
            'output': str(self.directory / ('browser-' + stage + '-' + uuid.uuid4().hex[:8]))}
        request.update(ui_collection_id=self.state['ui_collection_id'], write_ui=stage == 'before_restart')
        config = self.directory / ('browser-' + stage + '.json')
        save(config, request)
        arguments = [self.args.node, Path(__file__).with_name('browser.mjs'), config]
        if self.args.browser_path:
            arguments.append(self.args.browser_path)
        self.report['checks'][stage + '_browser'] = self.command(arguments)

    def offline(self):
        evidence = {}
        for name, selector, count, files in self.views():
            selection = 'latest' if selector is ANY else 'initial' if selector is None else str(selector)
            result = self.command([self.admin, 'read', '--root', self.root, '--store-id', self.state['store_id'],
                '--collection-id', self.state['collection_id'], '--snapshot', selection])
            assert len(result['rows']) == count
            evidence[name] = {'rows': str(count), 'snapshot_id': result['snapshot_id']}
        self.report['checks']['offline_facade'] = evidence

    def backup_restore(self):
        target = self.directory / ('restored-' + uuid.uuid4().hex[:8])
        cid, sid = self.state['collection_id'], self.state['store_id']
        selectors = ['latest', 'initial', self.state['receipt_a']['result']['snapshot_id']]
        with quiesced(self.admin, self.root, sid) as owner:
            competing = subprocess.run([str(self.admin), 'inspect-store', '--root', str(self.root), '--store-id', sid],
                capture_output=True, text=True, timeout=15)
            assert competing.returncode and 'STATE_IN_USE' in competing.stderr
            physical = inspect_store(self.root, sid, cid, selectors)
            for selector, name, count in zip(selectors, ['latest', 'initial', 'a'], [1010, 0, 1000]):
                assert physical['views'][selector]['rows'] == str(count)
                expected = self.report['checks']['before_restart'][name]['files']
                observed = sorted(physical['views'][selector]['files'], key=lambda item: item['file_id'])
                assert [file['file_id'] for file in expected] == [file['file_id'] for file in observed]
                for api, disk in zip(expected, observed):
                    assert (api['record_count'], api['compressed_bytes'], api['footer_bytes']) == (
                        disk['record_count'], disk['compressed_bytes'], disk['footer_bytes'])
                    if api['path'] is not None:
                        assert api['path'] == disk['location']
                    assert [int(group['record_count']) for group in api['row_groups']] == [
                        int(group['row_count']) for group in disk['row_groups']]
                    for group in api['row_groups']:
                        assert [field['field_id'] for field in group['field_metrics']] == [
                            field['field_id'] for field in disk['fields']]
            self.report['checks']['physical_chain'] = physical
            self.report['checks']['backup_copy'] = copy_store(self.root, target)
            if owner.poll() is not None:
                raise RuntimeError('source lease released before copy validation')
            self.command([self.admin, 'inspect-store', '--root', target, '--store-id', sid])
        unavailable = self.directory / ('source-unavailable-' + uuid.uuid4().hex[:8])
        self.root.rename(unavailable)
        try:
            assert not self.root.exists()
            self.start(target)
            self.verify('restore_source_unavailable')
            self.browser('restored')
            self.stop()
        finally:
            try:
                self.stop()
            finally:
                unavailable.rename(self.root)
        self.report['checks']['backup_lease'] = 'PASS: competing owner rejected while SQLite was closed'

    def warehouse_inventory(self):
        inventory = {}
        for path in sorted((self.root / 'warehouse').rglob('*')):
            if path.is_symlink():
                raise ValueError('symlink in acceptance warehouse')
            if path.is_file():
                with path.open('rb') as stream:
                    inventory[str(path.relative_to(self.root))] = hashlib.file_digest(stream, 'sha256').hexdigest()
            elif path.is_dir():
                inventory[str(path.relative_to(self.root)) + '/'] = None
            else:
                raise ValueError('special file in acceptance warehouse')
        return inventory

    def run(self):
        try:
            if not self.root.exists():
                self.command([self.admin, 'create-store', '--root', self.root, '--store-id', self.state['store_id']])
            self.start()
            self.seed()  # exact persisted IDs: repeated runs resolve the same operations, never reseed new ones
            self.verify('before_restart')
            self.browser('before_restart')
            self.stop()
            before_retry = self.warehouse_inventory()
            self.start()
            repeated = self.api('/collections/' + self.state['collection_id'] + '/append',
                json.loads((self.directory / 'append-a.json').read_text()), 'append-a-retry')
            assert repeated == self.state['receipt_a']
            self.verify('after_restart_exact_retry')
            self.browser('after_restart')
            self.stop()
            after_retry = self.warehouse_inventory()
            assert before_retry == after_retry, 'retry/restart changed physical files or created orphans'
            self.report['checks']['retry_warehouse_unchanged'] = {'status': 'PASS', 'paths_sha256': after_retry}
            self.offline()
            self.backup_restore()
            self.report['status'] = 'INCOMPLETE' if self.args.no_browser else 'PASS'
        except BaseException as error:
            self.report['status'] = 'FAIL'
            self.report['failure'] = {'type': type(error).__name__, 'message': str(error).replace(self.token, '[redacted]'),
                'traceback': traceback.format_exc().replace(self.token, '[redacted]')}
            raise
        finally:
            try:
                self.stop()
            finally:
                self.report['commands'] = self.commands
                save(self.directory / 'report.json', self.report)
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prefix', type=Path, required=True, help='Installed prefix with native service/CLI/admin/UI')
    parser.add_argument('--workspace', type=Path, required=True, help='New or previously marked persistent test root')
    parser.add_argument('--node', default='node')
    parser.add_argument('--browser-path')
    parser.add_argument('--no-browser', action='store_true', help='Explicitly incomplete backend-only diagnostic run')
    args = parser.parse_args()
    acceptance = None
    try:
        acceptance = Acceptance(args)
        report = acceptance.run()
    except Exception as error:
        message = str(error)
        if acceptance is not None:
            message = message.replace(acceptance.token, '[redacted]')
        print(type(error).__name__ + ': ' + message, file=sys.stderr)
        return 1
    print(json.dumps({'status': report['status'], 'backend': 'local', 'mock': False,
        'report': str(args.workspace / 'report.json'), 'rows': {'latest': 1010, 'snapshot_a': 1000, 'initial': 0}}))
    return 0 if report['status'] == 'PASS' else 2


if __name__ == '__main__':
    raise SystemExit(main())
