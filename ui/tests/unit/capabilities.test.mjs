import { test } from 'node:test';
import assert from 'node:assert/strict';
import { supportsFeature, validateQueryCapabilities } from '../../assets/model.js';

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
