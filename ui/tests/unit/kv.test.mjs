import assert from 'node:assert/strict';
import { test } from 'node:test';
import { validateCommittedReceipt } from '../../assets/receipt.js';
import {
    compactionOptions,
    sessionSnapshots,
    sessionRowCount,
    verifyCreatedCollection,
    creationFingerprint,
    exportPending,
    importPending,
} from '../../assets/model.js';
const cid = '11111111-2222-4333-8444-555555555555';
const op = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const schema = { fields: [{ name: 'key', type: 'string', nullable: false }] };
const result = {
    collection_id: cid,
    operation_id: op,
    payload_hash: 'a'.repeat(64),
    hash_version: 1,
    receipt_version: 3,
    snapshot_id: '0',
    parent_snapshot_id: null,
    rows_appended: '0',
    data_files_added: '0',
    compressed_data_bytes_added: '0',
    snapshot_created_at_us: '0',
    durability_profile: 'local-fsync-v1',
    mutation_kind: 'delete',
    snapshot_created: false,
    input_rows: '2',
    rows_affected: '0',
    rows_written: '0',
    rows_removed: '0',
    data_files_removed: '0',
    compressed_data_bytes_removed: '0',
};
const payload = { outcome: 'committed', phase: 'complete', operation_id: op, result };
const path = `/collections/${cid}/operations/${op}`;
test('version 3 durable no-op is valid and contributes no snapshot', () => {
    validateCommittedReceipt(payload, 200, path);
    const operations = [{ collectionId: cid, outcome: 'committed', result }];
    assert.deepEqual(sessionSnapshots(operations, cid), []);
    assert.equal(sessionRowCount(operations, cid, '0'), null);
});
test('version 3 rejects coerced scalars and inconsistent no-op counters', () => {
    for (const change of [
        { input_rows: 2 },
        { snapshot_created: 0 },
        { rows_affected: '1' },
        { snapshot_id: '3' },
    ])
        assert.throws(() =>
            validateCommittedReceipt({ ...payload, result: { ...result, ...change } }, 200, path),
        );
});
test('immutable collection mode participates in create confirmation and frozen identity', () => {
    const body = { collection_id: cid, name: 'keys', schema, mode: 'kv' };
    assert.notEqual(creationFingerprint(body), creationFingerprint({ ...body, mode: 'append' }));
    assert.throws(() => verifyCreatedCollection({ ...body, mode: 'append' }, body));
});
test('pending upsert/delete/compact preserve operation kind across export and import', () => {
    const scope = {
        server_url: 'http://localhost:8080',
        database_id: null,
        store_id: null,
        backend: 'local',
        mock: false,
    };
    for (const [action, body] of [
        ['upsert', { rows: [{ key: 'a' }] }],
        ['delete', { keys: ['a', 'a'] }],
        ['compact', { max_files: '4', target_file_bytes: '1024' }],
    ]) {
        const draft = { collectionId: cid, operationId: op, schema, body, action, terminal: false };
        const imported = importPending(exportPending(scope, 'append', draft), scope);
        assert.equal(imported.draft.action, action);
        assert.deepEqual(imported.draft.body, body);
    }
});

import { readFileSync } from 'node:fs';
import { WorkbenchAPI } from '../../assets/api.js';
const fixture = JSON.parse(
    readFileSync(new URL('../../../fixtures/http/kv-v3.json', import.meta.url)),
);
for (const example of fixture.cases) {
    test(`shared v3 receipt preserves complete values: ${example.name}`, async () => {
        const client = new WorkbenchAPI(
            'http://localhost',
            'token',
            async () => new Response(JSON.stringify(example.response), { status: example.status }),
        );
        const path = `/collections/${fixture.collection_id}`;
        assert.deepEqual(
            await client.json(`${path}/${example.kind}`, { method: 'POST', body: example.body }),
            example.response,
        );
        assert.deepEqual(
            await client.json(`${path}/operations/${example.body.operation_id}`),
            example.response,
        );
    });
}
for (const invalid of fixture.invalid_receipts) {
    test(`shared v3 invalid receipt is rejected: ${invalid.name}`, async () => {
        const example = fixture.cases.find((case_) => case_.name === invalid.case);
        const value = structuredClone(example.response);
        if (invalid.payload_remove) delete value[invalid.payload_remove];
        else if (invalid.payload_changes) Object.assign(value, invalid.payload_changes);
        else if (invalid.remove) delete value.result[invalid.remove];
        else if (invalid.changes) Object.assign(value.result, invalid.changes);
        else value.result[invalid.field] = invalid.value;
        const client = new WorkbenchAPI(
            'http://localhost',
            'token',
            async () => new Response(JSON.stringify(value), { status: 200 }),
        );
        await assert.rejects(
            client.json(
                `/collections/${fixture.collection_id}/operations/${example.body.operation_id}`,
            ),
            /receipt/i,
        );
    });
}

test('compaction keeps an explicit exact snapshot precondition and rejects unknown options', () => {
    assert.deepEqual(compactionOptions({ expected_snapshot: '9007199254740993', max_files: '4' }), {
        max_files: '4',
        target_file_bytes: '8388608',
        expected_snapshot: '9007199254740993',
    });
    for (const value of [
        null,
        [],
        { unknown: 'ignored' },
        { expected_snapshot: 9007199254740993 },
        { expected_snapshot: '-0' },
    ])
        assert.throws(() => compactionOptions(value));
});
