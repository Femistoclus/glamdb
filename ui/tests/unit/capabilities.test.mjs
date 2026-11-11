import { test } from 'node:test';
import assert from 'node:assert/strict';
import { storageNotice, supportsFeature, validateQueryCapabilities } from '../../assets/model.js';

test('login storage notice follows the serving backend health', () => {
    const mock = { status: 'ok', backend: 'mock', mock: true, durability_validation: 'designed' };
    const native = {
        status: 'live',
        backend: 'local',
        mock: false,
        durability_validation: 'designed',
    };
    assert.equal(storageNotice(mock), 'Mock data is disposable. No storage durability is claimed.');
    assert.equal(
        storageNotice(native),
        'Persistent local storage. Power-loss durability is not yet qualified.',
    );
    const neutral = 'Storage durability depends on the server you connect to.';
    for (const health of [null, undefined, {}, [], { ...native, durability_validation: 'other' }])
        assert.equal(storageNotice(health), neutral);
    assert.equal(storageNotice({ ...native, backend: 'object_store' }), neutral);
});

test('missing native metadata stays unavailable while legacy mock remains compatible', () => {
    assert.equal(supportsFeature({ backend: 'local', features: { files: false } }, 'files'), false);
    assert.equal(supportsFeature({ mock: true }, 'files'), true);
});
test('unsupported predicates are rejected before submission', () => {
    const caps = { predicate_ops: ['true', 'eq'], features: { zero_column_projection: false } };
    assert.throws(
        () =>
            validateQueryCapabilities(caps, {
                projection: ['key'],
                predicate: { op: 'is_null', field: 'key' },
            }),
        /not supported/,
    );
    assert.throws(
        () => validateQueryCapabilities(caps, { projection: [], predicate: null }),
        /projection/,
    );
    validateQueryCapabilities(caps, {
        projection: ['key'],
        predicate: { op: 'eq', field: 'key', value: 'a' },
    });
});

test('unknown backends never implicitly enable mock metadata adapters', () => {
    for (const capabilities of [
        null,
        {},
        { backend: 'other' },
        { mock: false },
        { backend: 'local' },
    ])
        for (const feature of ['stats', 'snapshots', 'files', 'explain'])
            assert.equal(supportsFeature(capabilities, feature), false);
    assert.equal(supportsFeature({ mock: true, features: { files: false } }, 'files'), false);
    assert.equal(supportsFeature({ mock: false, features: { files: true } }, 'files'), true);
});

test('connected query maximum respects configured and backend bounds while retaining the demo ceiling', async () => {
    const model = await import('../../assets/model.js');
    assert.equal(typeof model.queryMaxRows, 'function');
    assert.equal(
        model.queryMaxRows(
            { backend: 'local', mock: false, limits: { scan_page_rows: 10000 } },
            {},
        ),
        10000,
    );
    assert.equal(
        model.queryMaxRows({ backend: 'local', limits: { scan_page_rows: 500 } }, {}),
        500,
    );
    assert.equal(
        model.queryMaxRows(
            { backend: 'local', limits: { scan_page_rows: 10000 } },
            { editor: { query_max_rows: 2048 } },
        ),
        2048,
    );
    assert.equal(model.queryMaxRows({ mock: true }, {}), 10000);
    assert.equal(
        model.queryMaxRows(
            { mock: true, backend: 'browser-demo', browser_demo: true },
            { editor: { query_max_rows: 10000 } },
        ),
        1000,
    );
});
