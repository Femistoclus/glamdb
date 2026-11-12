"""Disk-backed independent multiset, logical-key and receipt verification."""

import json
from pathlib import Path
import sqlite3
import tempfile

from glamdb_sdk import AppendOutcome, AppendRequest

from .verification import _row_key, scan_chunks

KEYS = {
    'region': ('r_regionkey',), 'nation': ('n_nationkey',),
    'supplier': ('s_suppkey',), 'customer': ('c_custkey',),
    'part': ('p_partkey',), 'partsupp': ('ps_partkey', 'ps_suppkey'),
    'orders': ('o_orderkey',), 'lineitem': ('l_orderkey', 'l_linenumber'),
}
REFERENCES = (
    ('nation', ('n_regionkey',), 'region'),
    ('supplier', ('s_nationkey',), 'nation'),
    ('customer', ('c_nationkey',), 'nation'),
    ('partsupp', ('ps_partkey',), 'part'),
    ('partsupp', ('ps_suppkey',), 'supplier'),
    ('orders', ('o_custkey',), 'customer'),
    ('lineitem', ('l_orderkey',), 'orders'),
    ('lineitem', ('l_partkey', 'l_suppkey'), 'partsupp'),
)


def _database(path):
    db = sqlite3.connect(path)
    db.execute('PRAGMA cache_size=-8192')
    db.execute('PRAGMA temp_store=FILE')
    db.execute('PRAGMA journal_mode=OFF')
    db.execute('PRAGMA synchronous=OFF')
    return db


def verify_source(dataset, directory, *, level='full'):
    """Keys and references are checked independently of generator RNG/partitioning."""
    if level not in {'rows', 'full'}:
        raise ValueError('verification level must be rows or full')
    if level == 'rows':
        dataset.validate()
        return {'status': 'PASS', 'level': level, 'rows': sum(d.manifest['rows'] for d in dataset.tables.values())}
    if set(dataset.tables) != set(KEYS):
        raise ValueError('logical verification requires all eight TPC-H tables')
    with tempfile.TemporaryDirectory(prefix='tpch-relations-', dir=directory) as temporary:
        db = _database(Path(temporary) / 'keys.sqlite')
        try:
            for table, primary in KEYS.items():
                columns = tuple(dict.fromkeys(primary + tuple(
                    field for source, fields, _ in REFERENCES if source == table for field in fields)))
                definitions = ','.join(name + ' INTEGER NOT NULL' for name in columns)
                db.execute(f'CREATE TABLE {table} ({definitions},PRIMARY KEY ({",".join(primary)})) WITHOUT ROWID')
                marks = ','.join('?' for _ in columns)
                try:
                    for rows in dataset.tables[table].iter_parts():
                        with db:
                            db.executemany(f'INSERT INTO {table} VALUES ({marks})',
                                           (tuple(row[name] for name in columns) for row in rows))
                except sqlite3.IntegrityError:
                    raise ValueError(f'TPC-H {table} has duplicate or NULL logical keys') from None
            for table, fields, parent in REFERENCES:
                condition = ' AND '.join(f'p.{pk}=c.{fk}' for fk, pk in zip(fields, KEYS[parent]))
                missing = db.execute(f'SELECT 1 FROM {table} c WHERE NOT EXISTS '
                                     f'(SELECT 1 FROM {parent} p WHERE {condition}) LIMIT 1').fetchone()
                if missing:
                    raise ValueError(f'TPC-H {table} has an invalid foreign key to {parent}')
        finally:
            db.close()
    return {'status': 'PASS', 'level': 'full', 'logical_keys': True, 'foreign_keys': True,
            'rows': sum(d.manifest['rows'] for d in dataset.tables.values())}


def verify_receipts(client, saved, dataset, log, *, deadline):
    total = log.summary()
    planned = log.db.execute('SELECT count(*) FROM chunks').fetchone()[0]
    expected = saved['append_plan']['operations']
    missing = log.db.execute('SELECT 1 FROM chunks c LEFT JOIN operations o USING(operation_id) '
                             'WHERE o.operation_id IS NULL LIMIT 1').fetchone()
    orphan = log.db.execute('SELECT 1 FROM operations o LEFT JOIN chunks c USING(operation_id) '
                            'WHERE c.operation_id IS NULL LIMIT 1').fetchone()
    if (total['pending'] or total['definitely_not_committed'] or planned != expected
            or total['committed'] != expected or total['unique_operations'] != expected
            or total['confirmed_rows'] != saved['append_plan']['rows'] or missing or orphan):
        return {'status': 'INCOMPLETE', 'operations': total}
    count = 0
    for record in log.db.execute('SELECT c.*,o.outcome FROM chunks c JOIN operations o USING(operation_id) ORDER BY ordinal'):
        request = log.request(record['operation_id'])
        body = request.to_dict()
        source = dataset.tables[record['table_name']]
        rebuilt = AppendRequest.prepare(request.collection_id, request.operation_id, source.schema,
                                        body['rows'], body.get('expected_snapshot'))
        if rebuilt != request or request.payload_hash != record['payload_hash']:
            raise ValueError('saved request identity/body/hash differs from the append plan')
        original = json.loads(record['outcome'])
        outcome = client.find_append_outcome(request.collection_id, request.operation_id, deadline=deadline)
        if not isinstance(outcome, AppendOutcome) or outcome.outcome != 'committed' or outcome.result != original['result']:
            return {'status': 'FAIL', 'reason': 'original durable receipt differs from server outcome',
                    'operation_id': request.operation_id}
        count += 1
    return {'status': 'PASS', 'original_receipts': count, 'operations': total}


def compare_multiset(schema, expected_chunks, actual_chunks, db):
    db.execute('CREATE TABLE counts (row BLOB PRIMARY KEY,n INTEGER NOT NULL) WITHOUT ROWID')
    expected = actual = extra = 0
    try:
        for rows in expected_chunks:
            with db:
                for row in rows:
                    db.execute('INSERT INTO counts VALUES (?,1) ON CONFLICT(row) DO UPDATE SET n=n+1',
                               (_row_key(schema, row),))
                    expected += 1
        for rows in actual_chunks:
            with db:
                for row in rows:
                    actual += 1
                    changed = db.execute('UPDATE counts SET n=n-1 WHERE row=? AND n>0', (_row_key(schema, row),))
                    extra += changed.rowcount == 0
        missing = db.execute('SELECT coalesce(sum(n),0) FROM counts').fetchone()[0]
    finally:
        db.execute('DROP TABLE counts')
    return {'expected_rows': expected, 'actual_rows': actual, 'missing_rows': missing,
            'extra_rows': extra, 'status': 'PASS' if not (missing or extra) else 'FAIL'}


def verify_remote(client, saved, dataset, log, directory, *, format='arrow', level='full', deadline=300):
    if format not in {'json', 'arrow'}:
        raise ValueError('verification format must be json or arrow')
    logical = verify_source(dataset, directory, level=level)
    receipts = verify_receipts(client, saved, dataset, log, deadline=deadline)
    if receipts['status'] != 'PASS':
        return receipts
    tables = {}
    with tempfile.TemporaryDirectory(prefix='tpch-verify-', dir=directory) as temporary:
        db = _database(Path(temporary) / 'rows.sqlite')
        try:
            for table, source in dataset.tables.items():
                info = saved['tables'][table]
                cid = info['collection_id']
                if client.get_schema(cid, deadline=deadline) != source.schema:
                    raise ValueError('server schema differs from the immutable dataset')
                current = compare_multiset(source.schema, source.iter_parts(),
                                           scan_chunks(client, cid, format=format,
                                                       query={'snapshot_id': saved['snapshots'][table]}, deadline=deadline), db)
                first = log.db.execute('SELECT o.operation_id,o.outcome FROM operations o JOIN chunks c '
                                       'USING(operation_id) WHERE c.table_name=? AND o.state=\'committed\' '
                                       'ORDER BY o.rowid LIMIT 1', (table,)).fetchone()
                if first is None:
                    return {'status': 'INCOMPLETE', 'reason': 'missing first committed snapshot receipt'}
                original = json.loads(first['outcome'])['result']
                first_id = original['snapshot_id']
                if first_id == saved['snapshots'][table]:
                    historical = dict(current, snapshot_id=first_id)
                else:
                    rows = log.request(first['operation_id']).to_dict()['rows']
                    historical = dict(compare_multiset(source.schema, [rows],
                                                       scan_chunks(client, cid, format=format,
                                                                   query={'snapshot_id': first_id}, deadline=deadline), db),
                                      snapshot_id=first_id)
                with client.scan_pages(cid, snapshot_id=info['initial_snapshot'], limit=1, deadline=deadline) as pages:
                    initial_empty = all(not page.rows for page in pages)
                tables[table] = dict(current, initial_empty=initial_empty, first_snapshot=historical,
                                     snapshot_id=saved['snapshots'][table],
                                     status='PASS' if current['status'] == historical['status'] == 'PASS' and initial_empty else 'FAIL')
        finally:
            db.close()
    return {'status': 'PASS' if all(t['status'] == 'PASS' for t in tables.values()) else 'FAIL',
            'format': format, 'level': level, 'tables': tables, 'source': logical, 'receipts': receipts}
