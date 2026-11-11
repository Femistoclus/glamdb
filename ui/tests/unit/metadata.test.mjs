import test from 'node:test';
import assert from 'node:assert/strict';
import * as metadata from '../../assets/api.js';

const expected = { collectionId: 'collection-a', snapshotId: '9007199254740993' };
const envelope = {
    snapshot_id: '9007199254740993',
    identity: { collection_id: 'collection-a', current_snapshot_id: '9007199254740995' },
};

test('metadata validates the selected snapshot separately from the current root head', () => {
    assert.equal(typeof metadata.validateViewMetadata, 'function');
    assert.equal(metadata.validateViewMetadata(envelope, expected), envelope);
    assert.throws(
        () =>
            metadata.validateViewMetadata(
                { ...envelope, snapshot_id: '9007199254740995' },
                expected,
            ),
        /snapshot/i,
    );
    assert.throws(
        () =>
            metadata.validateViewMetadata(
                { ...envelope, identity: { collection_id: 'foreign' } },
                expected,
            ),
        /collection/i,
    );
    assert.equal(
        metadata.validateViewMetadata(
            { snapshot_id: null, identity: { collection_id: 'collection-a' } },
            { ...expected, snapshotId: 'initial' },
        ).snapshot_id,
        null,
    );
});

test('metadata pages retain exact snapshot identity and reject malformed or repeated continuations', async () => {
    assert.equal(typeof metadata.readMetadataPages, 'function');
    const calls = [];
    const api = {
        json: async (path) => {
            calls.push(path);
            return {
                ...envelope,
                items:
                    calls.length === 1
                        ? [{ snapshot_id: '9007199254740991' }]
                        : [{ snapshot_id: '9007199254740993' }],
                next_cursor: calls.length === 1 ? 'opaque/cursor' : null,
            };
        },
    };
    assert.deepEqual(
        await metadata.readMetadataPages(api, '/snapshots?snapshot_id=9007199254740993', {
            view: expected,
        }),
        [{ snapshot_id: '9007199254740991' }, { snapshot_id: '9007199254740993' }],
    );
    assert.ok(calls[1].includes('cursor=opaque%2Fcursor'));
    const repeated = { json: async () => ({ ...envelope, items: [], next_cursor: 'same' }) };
    await assert.rejects(
        metadata.readMetadataPages(repeated, '/files', { view: expected }),
        /repeated/i,
    );
    for (const page of [
        { ...envelope },
        { ...envelope, items: [], next_cursor: 7 },
        { ...envelope, snapshot_id: null, items: [] },
    ]) {
        await assert.rejects(
            metadata.readMetadataPages({ json: async () => page }, '/files', { view: expected }),
        );
    }
});

test('explicit legacy mock file descriptors use the selected identity instead of a file creation snapshot', () => {
    const file = {
        mock: true,
        snapshot_id: '1',
        identity: { collection_id: 'collection-a', snapshot_id: '2' },
    };
    assert.equal(
        metadata.validateViewMetadata(file, {
            collectionId: 'collection-a',
            snapshotId: '2',
            mock: true,
        }),
        file,
    );
    assert.throws(
        () =>
            metadata.validateViewMetadata(file, {
                collectionId: 'collection-a',
                snapshotId: '1',
                mock: false,
            }),
        /mock/i,
    );
});
