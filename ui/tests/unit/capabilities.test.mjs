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
