import test from 'node:test';
import assert from 'node:assert/strict';
import { BrowserDemoAPI, timestampMicros } from '../../assets/demo.js';

const cid = '11111111-1111-4111-8111-111111111111';
const op = '22222222-2222-4222-8222-222222222222';
const schema = {
    fields: [
        { name: 'key', type: 'string', nullable: false },
        { name: 'n', type: 'int64', nullable: true },
        { name: 'at', type: 'timestamp_us_utc', nullable: true },
    ],
};
async function fresh() {
    const api = new BrowserDemoAPI();
    await api.json('/v0/collections', {
        method: 'POST',
        body: { collection_id: cid, name: 'test', schema },
    });
    return api;
}
const append = (api, rows, id = op, extra = {}) =>
    api.json(`/v0/collections/${cid}/append`, {
        method: 'POST',
        body: { operation_id: id, rows, ...extra },
    });
const scan = (api, body = {}) =>
    api.json(`/v0/collections/${cid}/scan`, {
        method: 'POST',
        body: { projection: ['key', 'n', 'at'], ...body },
    });

test('public demo has isolated state and honest capabilities', async () => {
    const api = await fresh(),
        other = new BrowserDemoAPI();
    assert.equal((await other.json('/v0/collections')).items.length, 0);
    const caps = await api.json('/v0/capabilities');
    assert.equal(caps.browser_demo, true);
    assert.equal(caps.durable, false);
    assert.equal(caps.arrow_ipc, false);
});
test('duplicates, int64 beyond 2^53 and timestamp microseconds survive', async () => {
    const api = await fresh();
    await append(api, [
        { key: 'same', n: '9007199254740993', at: '0001-01-01T00:00:00.000001Z' },
        { key: 'same', n: null, at: '9999-12-31T23:59:59.999999Z' },
    ]);
    const result = await scan(api);
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].n, '9007199254740993');
    assert.equal(result.rows[0].at, '0001-01-01T00:00:00.000001Z');
    assert.equal(result.rows[1].at, '9999-12-31T23:59:59.999999Z');
});
test('original retry precedes changed precondition and different content conflicts', async () => {
    const api = await fresh(),
        rows = [{ key: 'a', n: '1', at: null }];
    const first = await append(api, rows);
    await append(api, [{ key: 'b', n: '2', at: null }], crypto.randomUUID());
    assert.deepEqual(await append(api, rows, op, { expected_snapshot: null }), first);
    await assert.rejects(
        () => append(api, [{ key: 'changed', n: '1', at: null }]),
        (e) => e.status === 409,
    );
    assert.equal((await scan(api)).rows.length, 2);
});
test('initial, historical, zero-limit and fixed pagination remain stable', async () => {
    const api = await fresh();
    const first = await append(api, [{ key: 'a', n: '1', at: null }]);
    const old = first.result.snapshot_id;
    await append(api, [{ key: 'b', n: '2', at: null }], crypto.randomUUID());
    assert.equal((await scan(api, { snapshot_id: 'initial' })).rows.length, 0);
    assert.equal((await scan(api, { snapshot_id: old })).rows.length, 1);
    const empty = await scan(api, { limit: 0 });
    assert.equal(empty.rows.length, 0);
    assert.equal(empty.schema.fields.length, 3);
    const page = await api.json(`/v0/collections/${cid}/snapshots?page_size=1`);
    await append(api, [{ key: 'c', n: '3', at: null }], crypto.randomUUID());
    const next = await api.json(
        `/v0/collections/${cid}/snapshots?page_size=1&cursor=${page.next_cursor}`,
    );
    assert.equal(next.items.length, 1);
    assert.equal(next.next_cursor, null);
    await assert.rejects(
        () => api.json(`/v0/collections/${cid}/snapshots?page_size=2&cursor=${page.next_cursor}`),
        (e) => e.status === 400,
    );
});
test('hidden predicate columns, SQL NULL and exact integer ordering', async () => {
    const api = await fresh();
    await append(api, [
        { key: 'a', n: '9007199254740993', at: null },
        { key: 'b', n: null, at: null },
        { key: 'c', n: '9007199254740992', at: null },
    ]);
    assert.deepEqual(
        (
            await scan(api, {
                projection: ['key'],
                predicate: { op: 'gt', field: 'n', value: '9007199254740992' },
            })
        ).rows,
        [{ key: 'a' }],
    );
    assert.deepEqual(
        (
            await scan(api, {
                projection: ['key'],
                predicate: { op: 'not', arg: { op: 'eq', field: 'n', value: '1' } },
            })
        ).rows,
        [{ key: 'a' }, { key: 'c' }],
    );
    assert.deepEqual(
        (await scan(api, { projection: ['key'], predicate: { op: 'is_null', field: 'n' } })).rows,
        [{ key: 'b' }],
    );
});
test('timestamp normalization validates calendar, offsets and UTC range', () => {
    assert.equal(timestampMicros('1970-01-01T00:00:00.000001Z'), 1n);
    assert.equal(timestampMicros('1969-12-31T23:59:59.999999Z'), -1n);
    assert.equal(timestampMicros('1970-01-01T01:00:00.000001+01:00'), 1n);
    for (const value of [
        '2025-02-29T00:00:00Z',
        '0001-01-01T00:00:00+01:00',
        '9999-12-31T23:59:59-01:00',
        '2000-01-01T00:00:00.0000001Z',
    ])
        assert.throws(() => timestampMicros(value));
});
test('definite failure is retained, invalid schema and collection conflicts reject', async () => {
    const api = await fresh();
    await append(api, [{ key: 'a', n: '1', at: null }]);
    const failed = await append(api, [{ key: 'b', n: '2', at: null }], crypto.randomUUID(), {
        expected_snapshot: null,
    });
    assert.equal(failed.outcome, 'definitely_not_committed');
    await assert.rejects(
        () =>
            api.json('/v0/collections', {
                method: 'POST',
                body: { collection_id: crypto.randomUUID(), name: 'test', schema },
            }),
        (e) => e.status === 409,
    );
    await assert.rejects(
        () =>
            api.json('/v0/collections', {
                method: 'POST',
                body: {
                    collection_id: crypto.randomUUID(),
                    name: 'bad',
                    schema: { fields: [{ name: 'x', type: 'bad' }] },
                },
            }),
        (e) => e.status === 422,
    );
});
test('resource admission does not partially change a view', async () => {
    const api = new BrowserDemoAPI([], { maxRows: 1 });
    await api.json('/v0/collections', {
        method: 'POST',
        body: { collection_id: cid, name: 'test', schema },
    });
    await append(api, [{ key: 'a', n: '1', at: null }]);
    const rejectedId = crypto.randomUUID();
    const rejection = await append(api, [{ key: 'b', n: '2', at: null }], rejectedId);
    assert.equal(rejection.outcome, 'definitely_not_committed');
    assert.deepEqual(await api.json(`/v0/collections/${cid}/operations/${rejectedId}`), rejection);
    assert.equal((await scan(api)).rows.length, 1);
});
test('estimated byte capacity keeps reads and original retries usable', async () => {
    const api = new BrowserDemoAPI([], { maxBytes: 20000 });
    await api.json('/v0/collections', {
        method: 'POST',
        body: { collection_id: cid, name: 'test', schema },
    });
    let lastSuccess, lastRows, lastId;
    for (let i = 0; i < 20; i++) {
        const id = crypto.randomUUID(),
            rows = [{ key: 'x'.repeat(500) + i, n: '1', at: null }];
        const result = await append(api, rows, id);
        if (result.outcome === 'definitely_not_committed') {
            assert.equal((await scan(api)).rows.length, i);
            assert.deepEqual(await append(api, lastRows, lastId), lastSuccess);
            return;
        }
        lastSuccess = result;
        lastRows = rows;
        lastId = id;
    }
    assert.fail('Expected bounded demo admission');
});
