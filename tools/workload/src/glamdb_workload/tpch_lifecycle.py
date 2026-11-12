"""Explicit managed targets and immutable, bounded append/upsert plans."""

import hashlib
import hmac
import json
import os
from pathlib import Path
import platform
import resource
import time
from uuid import UUID, uuid4, uuid5

from glamdb_sdk import AppendOutcome, HTTPError, MutationRequest, OutcomeUnknownError, ProtocolError, TransportError
from glamdb_sdk.codec import decode_rows
from glamdb_sdk.errors import ValidationError
from glamdb_sdk.request import json_bytes

from .ledger import Ledger
from .runtime import add_stage, client_for, error_code, failure_kind, latency_report, stage_report
from .tpch_state import client_config, read_state, run_lock, write_state
from .tpch_target import KEY_ENCODING, collection_mode, prepare_request, target_schema


def _plan_digest(log):
    digest = hashlib.sha256()
    for row in log.db.execute('SELECT * FROM chunks ORDER BY ordinal'):
        digest.update(json_bytes(tuple(row)) + b'\n')
    return digest.hexdigest()


def freeze_plan(log, dataset, saved, *, batch_rows, batch_bytes, workers):
    if type(batch_rows) is not int or not 1 <= batch_rows <= 10000:
        raise ValueError('batch_rows must be between one and 10000')
    if type(batch_bytes) is not int or not 1 <= batch_bytes <= 16 * 1024**2:
        raise ValueError('batch_bytes must be between one and 16 MiB')
    if type(workers) is not int or not 1 <= workers <= 64:
        raise ValueError('workers must be between one and 64')
    log.db.executescript('''
        CREATE TABLE IF NOT EXISTS benchmark_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS chunks (
            ordinal INTEGER PRIMARY KEY, table_name TEXT NOT NULL, part_ordinal INTEGER NOT NULL,
            start_row INTEGER NOT NULL, end_row INTEGER NOT NULL, operation_id TEXT UNIQUE NOT NULL,
            rows INTEGER NOT NULL, wire_bytes INTEGER NOT NULL, payload_hash TEXT NOT NULL,
            worker INTEGER NOT NULL);
    ''')
    config = {'load_id': saved['load_id'], 'dataset_sha256': saved['dataset_sha256'],
              'collections': {name: info['collection_id'] for name, info in saved['tables'].items()},
              'batch_rows': batch_rows, 'batch_bytes': batch_bytes, 'workers': workers,
              'collection_mode': collection_mode(saved)}
    if collection_mode(saved) == 'kv':
        config['key_encoding'] = KEY_ENCODING
    old = log.db.execute("SELECT value FROM benchmark_meta WHERE key='plan'").fetchone()
    if old is not None:
        previous = json.loads(old[0])
        previous['config'].setdefault('collection_mode', 'append')
        if previous['config'] != config:
            raise ValueError('append plan differs from its immutable original configuration')
        if previous['summary'].get('sha256') != _plan_digest(log):
            raise ValueError('append plan checksum differs from its immutable original chunks')
        dataset.validate()
        return previous['summary']
    summary = {'operations': 0, 'rows': 0, 'wire_bytes': 0, 'tables': {}}
    with log.db:
        for table, source in dataset.tables.items():
            table_count = 0
            collection = saved['tables'][table]['collection_id']
            for part_ordinal, rows in enumerate(source.iter_parts()):
                start = 0
                while start < len(rows):
                    end = min(len(rows), start + batch_rows)
                    while True:
                        operation = str(uuid5(UUID(saved['load_id']),
                                              f'{collection}/{part_ordinal}/{start}:{end}'))
                        try:
                            # Longest signed snapshot identity bounds the actual request suffix.
                            request = prepare_request(saved, table, source.schema, operation,
                                                      rows[start:end], -9223372036854775808)
                            if len(request.body) <= batch_bytes:
                                break
                        except ValidationError:
                            if end - start == 1:
                                raise
                        if end - start == 1:
                            raise ValueError('one source row exceeds batch_bytes')
                        end = start + max(1, (end - start) // 2)
                    ordinal = summary['operations']
                    log.db.execute('INSERT INTO chunks VALUES (?,?,?,?,?,?,?,?,?,?)', (
                        ordinal, table, part_ordinal, start, end, operation, end - start,
                        len(request.body), request.payload_hash, ordinal % workers))
                    summary['operations'] += 1
                    summary['rows'] += end - start
                    summary['wire_bytes'] += len(request.body)
                    table_count += 1
                    start = end
            summary['tables'][table] = table_count
        summary['sha256'] = _plan_digest(log)
        log.db.execute("INSERT INTO benchmark_meta VALUES ('plan',?)",
                       (json.dumps({'config': config, 'summary': summary}),))
    return summary


def capacity_preflight(limits, plan, *, operations_used=0, snapshots_used=0):
    """A lower-bound preflight; byte-dependent physical capacity remains runtime admission."""
    if not isinstance(limits, dict) or not isinstance(limits.get('history'), dict):
        raise ValueError('server must advertise persisted operation and history limits (#156)')
    operations = operations_used + plan['operations']
    snapshots = snapshots_used + plan['operations']
    required = {'max_collections': 8, 'max_operations': operations}
    for key, value in required.items():
        if type(limits.get(key)) is not int or limits[key] < value:
            raise ValueError(f'insufficient {key}: requires at least {value}; no limits are raised automatically')
    for key, value in {'max_snapshots': snapshots, 'max_data_files': snapshots,
                       'max_manifests': snapshots,
                       'max_recovery_workspace_bytes': 8192 * (1 + 3 * snapshots)}.items():
        if type(limits['history'].get(key)) is not int or limits['history'][key] < value:
            raise ValueError(f'insufficient history.{key}: requires at least {value}')
    return {'operations_required': operations, 'snapshots_required': snapshots,
            'retained_manifests_required_at_least': snapshots,
            'recovery_workspace_bytes_required_at_least': 8192 * (1 + 3 * snapshots),
            'physical_byte_admission': 'server enforced; compressed sizes cannot be predicted exactly'}


def validate_saved_plan(log, dataset, saved):
    options = saved['append_plan']
    if not log.db.execute("SELECT 1 FROM benchmark_meta WHERE key='plan'").fetchone():
        raise ValueError('saved immutable append plan is missing from the journal')
    plan = freeze_plan(log, dataset, saved, batch_rows=options['batch_rows'],
                       batch_bytes=options['batch_bytes'], workers=options['workers'])
    if any(options.get(key) != value for key, value in plan.items()):
        raise ValueError('append plan differs from the original run state checksum or inventory')
    for row in log.db.execute('SELECT o.operation_id,o.collection_id,o.payload_hash,o.rows,o.bytes,'
                              'length(o.body) AS body_bytes,c.table_name,c.rows AS chunk_rows,'
                              'c.payload_hash AS chunk_hash FROM operations o '
                              'LEFT JOIN chunks c USING(operation_id)'):
        if (row['table_name'] not in saved['tables'] or row['rows'] != row['chunk_rows']
                or row['bytes'] != row['body_bytes'] or row['payload_hash'] != row['chunk_hash']
                or row['collection_id'] != saved['tables'][row['table_name']]['collection_id']):
            raise ValueError('journal accounting or identity differs from its immutable source chunk')
        request = log.request(row['operation_id'])  # Validate body digest before recovery/progress.
        if getattr(request, 'kind', 'append') != ('upsert' if collection_mode(saved) == 'kv' else 'append'):
            raise ValueError('journal mutation kind differs from the immutable load plan')
    return plan


def _dataset(saved):
    from .tpch_dataset import TPCHDataset
    data = TPCHDataset(saved['dataset'])
    if data._manifest_sha256 != saved['dataset_sha256']:
        raise ValueError('dataset manifest differs from the original run')
    if set(saved['tables']) != set(data.tables):
        raise ValueError('run target must contain all eight original tables')
    ids = set()
    for table, source in data.tables.items():
        info = saved['tables'][table]
        if info['schema'] != target_schema(source.schema, collection_mode(saved)).to_dict() or info['collection_id'] in ids:
            raise ValueError('run schema or collection assignment differs from its original dataset')
        ids.add(info['collection_id'])
    return data


def _capabilities(client, saved, *, deadline, writable=True):
    caps = client.capabilities(deadline=deadline)
    required = ('scan', 'scan_pagination', 'scan_cancel', 'stats', 'snapshots')
    required += ('key_value', 'upsert') if collection_mode(saved) == 'kv' else ('append',)
    if caps.get('mock') is not False or caps.get('backend') != 'local':
        raise ValueError('TPC-H requires the real native storage backend; no mock fallback')
    if caps.get('store_id') != saved['store_id']:
        raise ValueError('server store identity differs from the saved benchmark target')
    if any(caps.get('features', {}).get(key) is not True for key in required):
        raise ValueError('server lacks required write, scan or fixed-snapshot metadata capabilities')
    if writable and caps.get('health', {}).get('write_ready') is not True:
        raise ValueError('database is not write-ready; reopen/recovery is an explicit operator action')
    return caps


def _journal_path(state):
    return Path(state).resolve().with_suffix('.load.sqlite')


def _sync_progress(saved, log):
    """Recover the confirmed local prefix after a crash between receipt and state fsync."""
    counts = dict.fromkeys(saved['tables'], 0)
    snapshots = {table: info['initial_snapshot'] for table, info in saved['tables'].items()}
    for row in log.db.execute('SELECT c.table_name,o.rows,o.outcome FROM operations o '
                              'JOIN chunks c USING(operation_id) WHERE o.state=\'committed\' ORDER BY o.rowid'):
        receipt = json.loads(row['outcome'])['result']
        snapshots[row['table_name']] = receipt['snapshot_id']
        counts[row['table_name']] += row['rows']
    for table in saved['tables']:
        saved['tables'][table]['snapshot_id'] = snapshots[table]
        saved['tables'][table]['confirmed_rows'] = counts[table]
    saved['snapshots'] = snapshots
    return counts


def _check_heads(client, saved, data, *, deadline):
    stats = {}
    for table, source in data.tables.items():
        info = saved['tables'][table]
        if client.get_schema(info['collection_id'], deadline=deadline) != target_schema(source.schema, collection_mode(saved)):
            raise ValueError('server schema differs from the saved benchmark schema')
        if client.get_collection(info['collection_id'], deadline=deadline).get('mode', 'append') != collection_mode(saved):
            raise ValueError('server collection mode differs from the saved benchmark target')
        current = client.stats(info['collection_id'], deadline=deadline)
        if (current['snapshot_id'] != info['snapshot_id'] or
                int(current['rows']) != info.get('confirmed_rows', 0)):
            raise ValueError('unexpected foreign writes or unresolved outcomes changed the target snapshot')
        stats[table] = current
    return stats


def initialize(*, url, dataset, name, state, limits=None, table_prefix='', mode=None, timeout=30.0, ca_file=None):
    try:
        return _initialize(url=url, dataset=dataset, name=name, state=state, limits=limits,
                           table_prefix=table_prefix, mode=mode, timeout=timeout, ca_file=ca_file)
    except (TransportError, ProtocolError, HTTPError) as error:
        if isinstance(error, HTTPError) and error.status_code < 500:
            raise
        saved = read_state(state)
        return {'status': 'INCOMPLETE', 'phase': 'initialization_unresolved',
                'database_id': saved['database_id'], 'error_code': error_code(error),
                'reason': 'repeat explicit init with the saved original identities; no request was replayed'}


def _initialize(*, url, dataset, name, state, limits=None, table_prefix='', mode=None, timeout=30.0, ca_file=None):
    from .tpch_dataset import TPCHDataset
    from .tpch_verify import verify_source
    if not isinstance(name, str) or not name or not isinstance(table_prefix, str):
        raise ValueError('benchmark database name and optional table prefix must be strings')
    if mode is not None:
        collection_mode({'collection_mode': mode})
    data = TPCHDataset(dataset)
    state = Path(state).resolve()
    state.parent.mkdir(parents=True, exist_ok=True)
    digest = data._manifest_sha256
    requested = {'url': url.rstrip('/'), 'dataset': str(data.path), 'dataset_sha256': digest,
                 'name': name, 'requested_limits': limits, 'table_prefix': table_prefix}
    with run_lock(state):
        if state.exists():
            saved = read_state(state)
            if (any(saved.get(key) != value for key, value in requested.items())
                    or (mode is not None and mode != collection_mode(saved))):
                raise ValueError('init differs from the saved original target; use a new state for a new run')
        else:
            mode = mode or 'append'
            dbid = str(uuid4())
            saved = dict(requested, version=1, database_id=dbid, store_id=dbid, collection_mode=mode,
                         load_id=str(uuid4()), phase='initializing', load_complete=False,
                         tables={table: {'collection_id': str(uuid4()), 'name': table_prefix + table,
                                         'schema': target_schema(source.schema, mode).to_dict(), 'initial_snapshot': None,
                                         'snapshot_id': None, 'confirmed_rows': 0, 'created': False}
                                 for table, source in data.tables.items()},
                         snapshots=dict.fromkeys(data.tables),
                         client={'python': platform.python_version(), 'platform': platform.platform()})
            client_config(saved, timeout, ca_file)
            saved['source_validation'] = verify_source(data, state.parent)
            # Identity is durable before any lifecycle POST can become uncertain.
            write_state(state, saved)
        config = client_config(saved, timeout, ca_file)
        with client_for(config, scoped=False) as root:
            caps = root.capabilities(deadline=timeout)
            if any(caps.get('features', {}).get(key) is not True for key in ('database_management', 'database_drop')):
                raise ValueError('managed database creation and exact-identity cleanup (#153) are required')
            database = root.create_database(name, database_id=saved['database_id'], limits=limits, deadline=timeout)
            if database.get('database_id') != saved['database_id'] or database.get('state') != 'open':
                raise ValueError('created database identity/state differs from the explicit benchmark target')
            saved['database_limits'] = database['limits']
            if database['limits'].get('max_collections', 0) < 8:
                raise ValueError('TPC-H requires capacity for eight collections (#155)')
            write_state(state, saved)
        with client_for(config) as client:
            caps = _capabilities(client, saved, deadline=timeout)
            mode = collection_mode(saved)
            physical_types = {field.type for source in data.tables.values() for field in target_schema(source.schema, mode).fields}
            if not physical_types <= set(caps.get('schema_types', [])):
                raise ValueError('server does not support the explicitly selected type profile (#154)')
            saved['capabilities'] = caps
            for table, source in data.tables.items():
                info = saved['tables'][table]
                schema = target_schema(source.schema, mode)
                if info['schema'] != schema.to_dict():
                    raise ValueError('saved target schema differs from the original collection mode')
                if info['created']:
                    if (client.get_schema(info['collection_id'], deadline=timeout) != schema
                            or client.get_collection(info['collection_id'], deadline=timeout).get('mode', 'append') != mode):
                        raise ValueError('initialized collection schema changed')
                    continue
                outcome = client.create_collection(info['name'], schema, mode=mode,
                                                   collection_id=info['collection_id'], deadline=timeout)
                if outcome.outcome != 'committed':
                    saved['phase'] = 'initialization_' + outcome.outcome
                    write_state(state, saved)
                    return {'status': 'FAIL' if outcome.outcome == 'definitely_not_committed' else 'INCOMPLETE', 'phase': saved['phase'],
                            'database_id': saved['database_id'], 'table': table}
                info['created'] = True
                write_state(state, saved)
            saved['phase'] = 'initialized'
            write_state(state, saved)
        return {'status': 'PASS', 'phase': 'initialized', 'database_id': saved['database_id'],
                'collection_mode': collection_mode(saved),
                'tables': {name: info['collection_id'] for name, info in saved['tables'].items()},
                'type_profile': data.profile, 'scale_factor': data.scale_factor, 'state': str(state)}


def load_chunk(dataset, chunk):
    source = dataset.tables[chunk['table_name']]
    part = source.manifest['parts'][chunk['part_ordinal']]
    with (source.path / part['path']).open('rb') as input_file:
        content = input_file.read(part['bytes'] + 1)
    if len(content) != part['bytes'] or hashlib.sha256(content).hexdigest() != part['sha256']:
        raise ValueError('source part hash/size differs from the frozen append plan')
    rows = decode_rows(source.schema, [json.loads(line) for line in content.splitlines()])
    if len(rows) != part['rows'] or not 0 <= chunk['start_row'] < chunk['end_row'] <= len(rows):
        raise ValueError('saved chunk range differs from the immutable source part')
    selected = rows[chunk['start_row']:chunk['end_row']]
    if len(selected) != chunk['rows']:
        raise ValueError('saved chunk row count differs from its immutable range')
    return selected


def _receipt_matches_rows(request, result):
    rows = len(request.to_dict()['rows'])
    try:
        if not isinstance(result.get('snapshot_id'), str):
            return False
        if isinstance(request, MutationRequest):
            # Source primary keys are unique and the target starts empty. COW
            # may rewrite existing rows, but the live-row delta must equal input.
            return (result.get('receipt_version') == 3 and result.get('mutation_kind') == 'upsert'
                    and result.get('snapshot_created') is True
                    and int(result.get('input_rows', -1)) == rows
                    and int(result.get('rows_affected', -1)) == rows
                    and int(result['rows_written']) - int(result['rows_removed']) == rows)
        return int(result.get('rows_appended', -1)) == rows
    except (KeyError, TypeError, ValueError):
        return False


def record_outcome(log, request, outcome, *, authoritative=False):
    state = outcome.outcome
    code = outcome.error.code if outcome.error else None
    result = outcome.result
    if outcome.operation_id != request.operation_id:
        state, code = 'protocol_error', 'RECEIPT_IDENTITY_MISMATCH'
    elif state == 'committed':
        if not isinstance(result, dict) or not hmac.compare_digest(result.get('payload_hash', ''), request.payload_hash):
            state, code = 'conflict', 'OPERATION_CONFLICT'
        elif not _receipt_matches_rows(request, result):
            state, code = 'protocol_error', 'RECEIPT_ROW_COUNT_MISMATCH'
    elif state == 'definitely_not_committed' and not authoritative:
        # Admission can reject before binding an ID. A failed POST proves that
        # attempt failed, but only an outcome lookup proves a permanent receipt.
        state = 'failure_lookup_required'
    hint = outcome.error.retry_hint if outcome.error else None
    if not authoritative:
        log.db.execute('''CREATE TABLE IF NOT EXISTS attempt_outcomes (
            operation_id TEXT NOT NULL,attempt INTEGER NOT NULL,server_outcome TEXT NOT NULL,
            phase TEXT,error_code TEXT,retry_hint TEXT,result TEXT,
            PRIMARY KEY(operation_id,attempt))''')
        log.db.execute('INSERT INTO attempt_outcomes VALUES (?,(SELECT attempts FROM operations '
                       'WHERE operation_id=?),?,?,?,?,?) ON CONFLICT DO NOTHING',
                       (request.operation_id, request.operation_id, outcome.outcome, outcome.phase,
                        code, hint, None if result is None else json.dumps(result)))
    log.mark(request.operation_id, state, error_code=code,
             outcome={'server_outcome': outcome.outcome, 'phase': outcome.phase, 'result': result,
                      'error_code': code, 'retry_hint': hint})
    return state


def _send_once(client, log, request, *, deadline):
    log.attempted(request.operation_id)
    try:
        send = client.upsert if isinstance(request, MutationRequest) else client.append
        return record_outcome(log, request, send(request, deadline=deadline))
    except (TransportError, ProtocolError, HTTPError) as error:
        state = ('commit_unknown' if isinstance(error, OutcomeUnknownError) else
                 'transport_error' if isinstance(error, TransportError) else 'protocol_error')
        log.mark(request.operation_id, state, error_code=error_code(error))
        return state
    except BaseException:
        log.mark(request.operation_id, 'transport_error', error_code='INTERRUPTED')
        raise


def _locust_runtime():
    # This writer uses synchronous I/O under one publication gate. Locust owns
    # scheduling/statistics, but must not patch a caller's existing TLS objects.
    previous = os.environ.get('LOCUST_SKIP_MONKEY_PATCH')
    os.environ['LOCUST_SKIP_MONKEY_PATCH'] = '1'
    try:
        from locust import User, task
        from locust.env import Environment
        from locust.exception import StopUser
        return User, task, Environment, StopUser
    finally:
        if previous is None:
            os.environ.pop('LOCUST_SKIP_MONKEY_PATCH', None)
        else:
            os.environ['LOCUST_SKIP_MONKEY_PATCH'] = previous


def _execute_load(client, log, data, saved, path, config, workers):
    """Locust schedules disjoint durable chunks; one gate retains writer ordering."""
    import gevent
    from gevent.lock import Semaphore
    User, task, Environment, StopUser = _locust_runtime()

    gate = Semaphore(1)
    shared = {'next_worker': 0, 'active': workers, 'stopped': False, 'stages': {}, 'contention_seconds': 0.0}

    class Loader(User):
        def on_start(self):
            self.index = shared['next_worker']
            shared['next_worker'] += 1

        @task
        def publish(self):
            waiting = time.perf_counter()
            with gate:
                shared['contention_seconds'] += time.perf_counter() - waiting
                if shared['stopped']:
                    raise StopUser()
                chunk = log.db.execute('SELECT c.* FROM chunks c LEFT JOIN operations o USING(operation_id) '
                                       'WHERE c.worker=? AND o.operation_id IS NULL ORDER BY c.ordinal LIMIT 1',
                                       (self.index,)).fetchone()
                if chunk is None:
                    raise StopUser()
                began = time.perf_counter()
                try:
                    rows = load_chunk(data, chunk)
                    loaded = time.perf_counter()
                    info = saved['tables'][chunk['table_name']]
                    current = client.stats(info['collection_id'], deadline=config['timeout'])
                    if (current['snapshot_id'] != info['snapshot_id'] or
                            int(current['rows']) != info['confirmed_rows']):
                        raise ValueError('foreign writes changed a benchmark collection before append')
                    request = prepare_request(saved, chunk['table_name'], data.tables[chunk['table_name']].schema,
                                              chunk['operation_id'], rows, info['snapshot_id'])
                    if request.payload_hash != chunk['payload_hash'] or len(request.body) > saved['append_plan']['batch_bytes']:
                        raise ValueError('prepared body differs from the immutable append plan')
                    prepared = time.perf_counter()
                    log.prepare(request)
                    journaled = time.perf_counter()
                    state = _send_once(client, log, request, deadline=config['timeout'])
                    sent = time.perf_counter()
                    if state == 'committed':
                        receipt = json.loads(log.db.execute('SELECT outcome FROM operations WHERE operation_id=?',
                                                            (request.operation_id,)).fetchone()[0])['result']
                        info['snapshot_id'] = receipt['snapshot_id']
                        info['confirmed_rows'] += chunk['rows']
                        saved['snapshots'][chunk['table_name']] = receipt['snapshot_id']
                        write_state(path, saved)
                    else:
                        shared['stopped'] = True
                    ended = time.perf_counter()
                    for name, seconds in {'input_load': loaded - began, 'sdk_prepare': prepared - loaded,
                                          'journal': journaled - prepared + ended - sent,
                                          'http_send': sent - journaled, 'end_to_end': ended - began}.items():
                        add_stage(shared['stages'], name, seconds)
                    event_error = None if state == 'committed' else RuntimeError(state)
                    record = log.db.execute('SELECT error_code FROM operations WHERE operation_id=?',
                                             (request.operation_id,)).fetchone()
                    kind = 'SDK' if event_error is None else failure_kind(record[0] or state)
                    self.environment.events.request.fire(request_type=kind, name='tpch/' + getattr(request, 'kind', 'append'),
                                                          response_time=(sent - journaled) * 1000,
                                                          response_length=len(request.body), exception=event_error)
                except BaseException as error:
                    shared['stopped'] = True
                    shared['failure'] = error
                    raise StopUser() from error
            gevent.sleep(0)

        def on_stop(self):
            shared['active'] -= 1
            if shared['active'] == 0:
                gevent.spawn(self.environment.runner.quit)

    environment = Environment(user_classes=[Loader])
    runner = environment.create_local_runner()
    runner.start(workers, spawn_rate=workers)
    try:
        runner.greenlet.join()
    finally:
        runner.quit()
    if 'failure' in shared:
        raise shared['failure']
    return {'timing_stages': stage_report(shared['stages']), 'latency': latency_report(environment.stats),
            'writer_contention_seconds': shared['contention_seconds'], 'workers': workers,
            'publication_concurrency': 1, 'scheduler': 'Locust', 'assignment': 'global immutable chunks modulo worker count'}


def load_byte_metrics(log, data):
    parquet = 0
    kv = dict.fromkeys(('input_rows', 'rows_affected', 'rows_written', 'rows_removed',
                        'data_files_added', 'data_files_removed', 'compressed_data_bytes_removed'), 0)
    has_kv = False
    for row in log.db.execute("SELECT outcome FROM operations WHERE state='committed'"):
        result = json.loads(row['outcome'])['result']
        if result.get('mutation_kind') == 'upsert':
            has_kv = True
            for key in kv:
                kv[key] += int(result[key])
        value = result.get('compressed_data_bytes_added')
        if value is None:
            parquet = None
            break
        parquet += int(value)
    metrics = {'source_tbl_bytes': data.manifest['source_bytes'], 'dataset_jsonl_bytes': data.manifest['bytes'],
               'confirmed_wire_bytes': log.summary()['confirmed_bytes'], 'committed_parquet_bytes': parquet}
    if has_kv:
        metrics['kv_mutations'] = dict(kv, row_write_amplification=kv['rows_written'] / kv['input_rows'])
    return metrics


def load(state, *, batch_rows=10000, batch_bytes=8 * 1024**2, workers=1, resume=False, timeout=30.0, ca_file=None):
    from .tpch_verify import verify_remote, verify_source
    overall_started = time.perf_counter()
    path = Path(state).resolve()
    with run_lock(path):
        saved = read_state(path)
        config = client_config(saved, timeout, ca_file)
        data = _dataset(saved)
        if not all(info.get('created') for info in saved['tables'].values()):
            raise ValueError('finish explicit init before loading')
        if 'append_plan' in saved and not resume:
            raise ValueError('load already has a frozen plan; use --resume to continue the original load')
        with Ledger(_journal_path(path)) as log:
            planning_started = time.perf_counter()
            plan = freeze_plan(log, data, saved, batch_rows=batch_rows, batch_bytes=batch_bytes, workers=workers)
            if 'append_plan' in saved:
                validate_saved_plan(log, data, saved)
            saved['append_plan'] = dict(plan, batch_rows=batch_rows, batch_bytes=batch_bytes, workers=workers)
            planning_seconds = time.perf_counter() - planning_started
            totals = log.summary()
            if totals['pending']:
                return {'status': 'INCOMPLETE', 'reason': 'resolve saved original operations before resuming',
                        'operations': totals}
            if totals['definitely_not_committed']:
                return {'status': 'FAIL', 'reason': 'an original chunk has a permanent failed outcome', 'operations': totals}
            _sync_progress(saved, log)
            with client_for(config, scoped=False) as root:
                database = root.get_database(saved['database_id'], deadline=timeout)
                if database.get('state') != 'open':
                    raise ValueError('database must already be open; no automatic open/reconcile')
                saved['capacity_preflight'] = capacity_preflight(database['limits'], plan)
                saved['database_limits'] = database['limits']
            verify_source(data, path.parent)
            with client_for(config) as client:
                saved['capabilities'] = _capabilities(client, saved, deadline=timeout)
                _check_heads(client, saved, data, deadline=timeout)
                saved.update(phase='loading', load_complete=False)
                write_state(path, saved)
                preflight_seconds = time.perf_counter() - overall_started
                began, cpu = time.perf_counter(), time.process_time()
                before = log.summary()
                metrics = _execute_load(client, log, data, saved, path, config, workers)
                elapsed = time.perf_counter() - began
                totals = log.summary()
                metrics.update(load_byte_metrics(log, data), collection_mode=collection_mode(saved))
                loaded_rows = totals['confirmed_rows'] - before['confirmed_rows']
                loaded_bytes = totals['confirmed_bytes'] - before['confirmed_bytes']
                metrics.update(elapsed_seconds=elapsed, cpu_seconds=time.process_time() - cpu,
                               planning_seconds=planning_seconds, preflight_seconds=preflight_seconds,
                               peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                               journal_bytes=_journal_path(path).stat().st_size,
                               confirmed_rows_this_run=loaded_rows, confirmed_bytes_this_run=loaded_bytes,
                               rows_per_second=loaded_rows / elapsed if elapsed else None,
                               wire_bytes_per_second=loaded_bytes / elapsed if elapsed else None)
                saved['load_metrics'] = metrics
                _sync_progress(saved, log)
                if totals['pending'] or totals['committed'] != plan['operations']:
                    saved['phase'] = 'load_incomplete'
                    write_state(path, saved)
                    return {'status': 'FAIL' if totals['definitely_not_committed'] else 'INCOMPLETE',
                            'operations': totals, 'metrics': metrics}
                _check_heads(client, saved, data, deadline=timeout)
                verification_started = time.perf_counter()
                result = verify_remote(client, saved, data, log, path.parent, format='arrow', deadline=max(timeout, 300))
                metrics['verification_seconds'] = time.perf_counter() - verification_started
                metrics['command_elapsed_seconds'] = time.perf_counter() - overall_started
                saved['verification'] = result
                saved['load_complete'] = result['status'] == 'PASS'
                saved['phase'] = 'loaded' if saved['load_complete'] else 'verification_failed'
                write_state(path, saved)
                return {'status': result['status'], 'load_complete': saved['load_complete'], 'operations': totals,
                        'snapshots': saved['snapshots'], 'metrics': metrics, 'verification': result}


def resolve(state, *, retry=False, timeout=30.0, ca_file=None):
    path = Path(state).resolve()
    with run_lock(path):
        saved = read_state(path)
        config = client_config(saved, timeout, ca_file)
        data = _dataset(saved)
        if not _journal_path(path).is_file() or 'append_plan' not in saved:
            return {'status': 'INCOMPLETE', 'reason': 'no saved append plan to resolve'}
        with Ledger(_journal_path(path)) as log, client_for(config) as client:
            validate_saved_plan(log, data, saved)
            for row in log.operations(pending=True):
                request = log.request(row['operation_id'])
                chunk = log.db.execute('SELECT * FROM chunks WHERE operation_id=?', (request.operation_id,)).fetchone()
                if chunk is None:
                    raise ValueError('journal contains an operation outside the immutable append plan')
                body = request.to_dict()
                rebuilt = prepare_request(saved, chunk['table_name'], data.tables[chunk['table_name']].schema,
                                          request.operation_id, load_chunk(data, chunk), body.get('expected_snapshot'))
                if request != rebuilt or request.payload_hash != chunk['payload_hash']:
                    raise ValueError('saved retry body/hash differs from its immutable original source chunk')
                try:
                    outcome = client.find_append_outcome(request.collection_id, request.operation_id, deadline=timeout)
                    if isinstance(outcome, AppendOutcome):
                        record_outcome(log, request, outcome, authoritative=True)
                    elif outcome is None:
                        log.mark(request.operation_id, 'not_found')
                        if retry:
                            _send_once(client, log, request, deadline=timeout)
                    else:
                        log.mark(request.operation_id, 'in_progress')
                except (TransportError, ProtocolError, HTTPError) as error:
                    log.mark(request.operation_id, 'transport_error' if isinstance(error, TransportError) else 'protocol_error',
                             error_code=error_code(error))
            _sync_progress(saved, log)
            write_state(path, saved)
            totals = log.summary()
            return {'status': 'INCOMPLETE' if totals['pending'] else 'FAIL' if totals['definitely_not_committed'] else 'PASS',
                    'operations': totals, 'load_complete': saved['load_complete'], 'snapshots': saved['snapshots']}


def verify(state, *, format='arrow', level='full', timeout=300.0, ca_file=None):
    from .tpch_verify import verify_remote
    path = Path(state).resolve()
    with run_lock(path):
        saved = read_state(path)
        config = client_config(saved, timeout, ca_file)
        data = _dataset(saved)
        if not _journal_path(path).is_file() or 'append_plan' not in saved:
            return {'status': 'INCOMPLETE', 'reason': 'no completed append plan'}
        with Ledger(_journal_path(path)) as log, client_for(config) as client:
            validate_saved_plan(log, data, saved)
            _sync_progress(saved, log)
            result = verify_remote(client, saved, data, log, path.parent, format=format, level=level, deadline=timeout)
            saved['verification'] = result
            if result['status'] != 'PASS':
                saved['load_complete'] = False
            write_state(path, saved)
            return result


def status(state, *, timeout=30.0, ca_file=None):
    path = Path(state).resolve()
    saved = read_state(path)
    config = client_config(saved, timeout, ca_file)
    data = _dataset(saved)
    with client_for(config, scoped=False) as root:
        database = root.get_database(saved['database_id'], deadline=timeout)
    operations = None
    if _journal_path(path).is_file():
        with Ledger(_journal_path(path), readonly=True) as log:
            operations = log.summary()
    with client_for(config) as client:
        caps = client.capabilities(deadline=timeout)
    return {'status': 'PASS' if saved.get('load_complete') and not (operations or {}).get('pending') else 'INCOMPLETE',
            'collection_mode': collection_mode(saved),
            'phase': saved.get('phase'), 'database_id': saved['database_id'],
            'database': database, 'load_complete': saved.get('load_complete', False),
            'operations': operations, 'snapshots': saved.get('snapshots'),
            'tables': saved['tables'], 'capabilities': caps,
            'type_profile': data.profile, 'scale_factor': data.scale_factor}
