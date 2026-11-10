import test from 'node:test';
import assert from 'node:assert/strict';
import {
    selectionKey,
    selectView,
    buildPredicate,
    wireRows,
    displayValue,
    prepareMutation,
    escapeHTML,
    resetConnection,
} from '../../assets/model.js';

const fields = [
    { name: 'key', type: 'string', nullable: false },
    { name: 'age', type: 'int64', nullable: true },
    { name: 'active', type: 'bool', nullable: true },
    { name: 'at', type: 'timestamp_us_utc', nullable: true },
];

test('changing collection clears previous snapshot and query, preserving connection', () => {
    const old = {
        collectionId: 'a',
        snapshotId: '123',
        rows: [{ key: 'old' }],
        result: {},
        selectedFile: 'f',
        token: 'secret',
        epoch: 2,
    };
    const result = selectView(old, 'b');
    assert.equal(result.snapshotId, null);
    assert.deepEqual(result.rows, []);
    assert.equal(result.result, null);
    assert.equal(result.selectedFile, null);
    assert.equal(result.token, 'secret');
    assert.equal(result.epoch, 3);
    assert.equal(old.snapshotId, '123');
});
test('snapshot identity does not collapse initial and latest selectors', () => {
    assert.notEqual(selectionKey('a', null), selectionKey('a', 'initial'));
    assert.notEqual(selectionKey('a', '12'), selectionKey('b', '12'));
});
test('changing connection clears private data, selected view and query drafts', () => {
    const theme = { brand: { name: 'GlamDB' } };
    const previous = {
        api: {},
        token: 'private-token',
        base: 'https://old.example',
        capabilities: {},
        collections: [{}],
        collectionId: 'private-collection',
        snapshotId: 'old-snapshot',
        resolvedSnapshot: 'old-snapshot',
        collection: {},
        schema: {},
        stats: {},
        snapshots: [{}],
        files: [{}],
        rows: [{ key: 'private-row' }],
        result: { rows: [{ key: 'private-row' }] },
        plan: {},
        selectedFile: 'private-file',
        lastQuery: { snapshot_id: 'old-snapshot' },
        tab: 'data',
        clauses: [{}],
        join: 'or',
        negate: true,
        projection: ['private-column'],
        limit: 1,
        page: 4,
        operations: [{}],
        mutation: {},
        creation: {},
        epoch: 9,
        theme,
        themePreview: true,
    };
    const cleared = resetConnection(previous);
    for (const key of [
        'api',
        'capabilities',
        'collectionId',
        'snapshotId',
        'resolvedSnapshot',
        'collection',
        'schema',
        'stats',
        'result',
        'plan',
        'selectedFile',
        'lastQuery',
        'mutation',
        'creation',
    ])
        assert.equal(cleared[key], null, key);
    for (const key of [
        'collections',
        'snapshots',
        'files',
        'rows',
        'clauses',
        'projection',
        'operations',
    ])
        assert.deepEqual(cleared[key], [], key);
    assert.equal(cleared.token, '');
    assert.equal(cleared.tab, 'overview');
    assert.equal(cleared.limit, 1000);
    assert.equal(cleared.join, 'and');
    assert.equal(cleared.negate, false);
    assert.equal(cleared.page, 0);
    assert.equal(cleared.epoch, 10);
    assert.equal(cleared.theme, theme);
    assert.equal(cleared.themePreview, true);
    assert.equal(cleared.base, previous.base);
    assert.equal(previous.rows[0].key, 'private-row');
    assert.notEqual(resetConnection(cleared).rows, cleared.rows);
});
test('predicate builder preserves int64 precision and nested boolean choices', () => {
    assert.deepEqual(
        buildPredicate(
            [{ field: 'age', op: 'ge', value: '9007199254740993' }],
            'and',
            true,
            fields,
        ),
        { op: 'not', arg: { op: 'ge', field: 'age', value: '9007199254740993' } },
    );
    assert.deepEqual(
        buildPredicate(
            [
                { field: 'age', op: 'is_null', value: '' },
                { field: 'key', op: 'eq', value: 'a' },
            ],
            'or',
            false,
            fields,
        ),
        {
            op: 'or',
            args: [
                { op: 'is_null', field: 'age' },
                { op: 'eq', field: 'key', value: 'a' },
            ],
        },
    );
});
test('invalid integer, boolean operator and unknown field cannot submit', () => {
    for (const clause of [
        { field: 'age', op: 'eq', value: '2.1' },
        { field: 'age', op: 'eq', value: '9223372036854775808' },
        { field: 'active', op: 'gt', value: 'true' },
        { field: 'missing', op: 'eq', value: 'x' },
    ]) {
        assert.throws(() => buildPredicate([clause], 'and', false, fields));
    }
});
test('empty filter is true and IN preserves typed values', () => {
    assert.equal(buildPredicate([], 'and', false, fields), null);
    assert.deepEqual(
        buildPredicate(
            [{ field: 'age', op: 'in', value: '["1","9007199254740993"]' }],
            'and',
            false,
            fields,
        ),
        { op: 'in', field: 'age', values: ['1', '9007199254740993'] },
    );
});
test('wire rows retain duplicates and cannot silently round large integers', () => {
    const input = [
        { key: 'same', age: '9007199254740993' },
        { key: 'same', age: 2 },
    ];
    const result = wireRows({ fields }, input);
    assert.equal(result.length, 2);
    assert.equal(result[0].age, '9007199254740993');
    assert.equal(result[1].age, '2');
    assert.equal(result[0].active, null);
    assert.throws(() => wireRows({ fields }, [{ key: 'a', age: 9007199254740992 }]));
    assert.throws(() => wireRows({ fields }, [{ key: 'a', age: true }]));
    assert.throws(() => wireRows({ fields }, [{ key: 'a', surprise: 1 }]));
});
test('timestamp text is retained exactly, seven fractional digits rejected', () => {
    assert.equal(
        wireRows({ fields }, [{ key: 'a', at: '0001-01-01T00:00:00.000001Z' }])[0].at,
        '0001-01-01T00:00:00.000001Z',
    );
    assert.throws(() => wireRows({ fields }, [{ key: 'a', at: '2026-01-01T00:00:00.1234567Z' }]));
});
test('retry reuses the frozen operation and rejects edited unresolved payload', () => {
    const first = prepareMutation(null, { rows: [{ key: 'x' }] }, () => 'id-a');
    const retry = prepareMutation(first, { rows: [{ key: 'x' }] }, () => 'id-b');
    assert.equal(retry.operationId, 'id-a');
    assert.throws(() => prepareMutation(first, { rows: [{ key: 'different' }] }, () => 'id-b'));
    const next = prepareMutation(
        { ...first, terminal: true },
        { rows: [{ key: 'different' }] },
        () => 'id-b',
    );
    assert.equal(next.operationId, 'id-b');
});
test('display and escaped markup cannot turn dataset content into HTML', () => {
    assert.equal(displayValue('9007199254740993'), '9007199254740993');
    assert.equal(displayValue(null), 'NULL');
    assert.equal(escapeHTML('<img src=x onerror="x">'), '&lt;img src=x onerror=&quot;x&quot;&gt;');
});
