import test from 'node:test';
import assert from 'node:assert/strict';
import { formRows, sessionSnapshots } from '../../assets/model.js';

const schema = {
    fields: [
        { name: 'key', type: 'string', nullable: false },
        { name: 'n', type: 'int64', nullable: false },
        { name: 'x', type: 'float64', nullable: true },
        { name: 'active', type: 'bool', nullable: false },
        { name: 'at', type: 'timestamp_us_utc', nullable: true },
    ],
};
test('form values build exact typed JSON while preserving empty strings and NULL', () => {
    const rows = formRows(schema, [
        {
            key: '🎀',
            n: '9223372036854775807',
            x: '1.25',
            active: 'false',
            at: '2026-11-11T12:30:00.123456Z',
        },
        { key: '', n: '-9223372036854775808', x: null, active: 'true', at: null },
    ]);
    assert.deepEqual(rows, [
        {
            key: '🎀',
            n: '9223372036854775807',
            x: 1.25,
            active: false,
            at: '2026-11-11T12:30:00.123456Z',
        },
        { key: '', n: '-9223372036854775808', x: null, active: true, at: null },
    ]);
});
test('form rejects invalid values before posting a write', () => {
    for (const n of ['9223372036854775808', '1.2', ''])
        assert.throws(() => formRows(schema, [{ key: 'x', n, active: 'true', x: null, at: null }]));
    assert.throws(() => formRows(schema, [{ key: 'x', n: '1', active: 'yes', x: null, at: null }]));
    for (const x of ['Infinity', ' ', ''])
        assert.throws(() => formRows(schema, [{ key: 'x', n: '1', active: 'false', x, at: null }]));
});
test('observed history includes only committed writes for this collection, once per snapshot', () => {
    const operations = [
        {
            collectionId: 'a',
            outcome: 'committed',
            result: {
                snapshot_id: '9007199254740993',
                parent_snapshot_id: null,
                rows_appended: '2',
            },
        },
        { collectionId: 'a', outcome: 'commit_unknown', result: { snapshot_id: '3' } },
        { collectionId: 'b', outcome: 'committed', result: { snapshot_id: '4' } },
    ];
    assert.deepEqual(sessionSnapshots(operations, 'a'), [
        { snapshot_id: '9007199254740993', parent_snapshot_id: null, rows_added: '2' },
    ]);
});

test('session row totals require a complete confirmed parent chain', async () => {
    const { sessionRowCount } = await import('../../assets/model.js');
    const ops = [
        {
            collectionId: 'a',
            outcome: 'committed',
            result: { snapshot_id: '1', parent_snapshot_id: null, rows_appended: '2' },
        },
        {
            collectionId: 'a',
            outcome: 'committed',
            result: { snapshot_id: '2', parent_snapshot_id: '1', rows_appended: '3' },
        },
    ];
    assert.equal(sessionRowCount(ops, 'a', '2'), '5');
    assert.equal(sessionRowCount(ops, 'a', '3'), null);
    assert.equal(sessionRowCount(ops.slice(1), 'a', '2'), null);
});
