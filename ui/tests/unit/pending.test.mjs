import test from 'node:test';
import assert from 'node:assert/strict';
import * as pending from '../../assets/model.js';

const scope = {
    server_url: 'http://localhost:8766',
    database_id: '00000000-0000-4000-8000-000000000001',
    store_id: '00000000-0000-4000-8000-000000000002',
    backend: 'local',
    mock: false,
};
const collectionId = '00000000-0000-4000-8000-000000000003';
const operationId = '00000000-0000-4000-8000-000000000004';
const schema = {
    fields: [
        { field_id: 1, name: 'key', type: 'string', nullable: false },
        { field_id: 2, name: 'n', type: 'int64', nullable: true },
    ],
};
const body = {
    rows: [
        { key: 'same', n: '9007199254740993' },
        { key: 'same', n: null },
    ],
    expected_snapshot: '9007199254740995',
};
const draft = {
    operationId,
    collectionId,
    schema,
    body,
    fingerprint: JSON.stringify(body),
    terminal: false,
    token: 'do-not-export-token',
    result: { token: 'do-not-export-token' },
};

test('pending append export roundtrip retains exact UUID, schema, rows and precondition without credentials', () => {
    assert.equal(typeof pending.exportPending, 'function', 'pending exports must be available');
    const exported = pending.exportPending(scope, 'append', draft);
    const saved = JSON.stringify(exported);
    assert.ok(!saved.includes('do-not-export-token'));
    const imported = pending.importPending(JSON.parse(saved), scope);
    assert.equal(imported.kind, 'append');
    assert.equal(imported.draft.operationId, operationId);
    assert.equal(imported.draft.collectionId, collectionId);
    assert.deepEqual(imported.draft.schema, schema);
    assert.deepEqual(imported.draft.body, body);
    assert.equal(imported.draft.fingerprint, JSON.stringify(body));
    assert.equal(imported.draft.terminal, false);
    assert.equal(imported.draft.outcome, 'unresolved');
});

test('pending create export retains exact collection ID, name and schema for explicit retry', () => {
    assert.equal(typeof pending.exportPending, 'function');
    const body = { collection_id: collectionId, name: 'original_name', schema };
    const exported = pending.exportPending(scope, 'create', { body, token: 'never-export' });
    const imported = pending.importPending(exported, scope);
    assert.equal(imported.kind, 'create');
    assert.deepEqual(imported.draft.body, body);
    assert.equal(imported.draft.fingerprint, JSON.stringify({ name: body.name, schema }));
    assert.ok(!JSON.stringify(exported).includes('never-export'));
});

test('pending import refuses a different database, store or backend rather than reassigning an ID', () => {
    assert.equal(typeof pending.exportPending, 'function');
    const exported = pending.exportPending(scope, 'append', draft);
    for (const changed of [
        { database_id: '00000000-0000-4000-8000-000000000099' },
        { store_id: '00000000-0000-4000-8000-000000000099' },
        { mock: true, backend: 'mock' },
        { server_url: 'http://localhost:8769' },
    ])
        assert.throws(
            () => pending.importPending(exported, { ...scope, ...changed }),
            /different|match/i,
        );
});

test('malformed or altered pending inputs never create replacement UUIDs', () => {
    assert.equal(typeof pending.exportPending, 'function');
    const exported = pending.exportPending(scope, 'append', draft);
    for (const change of [
        (value) => delete value.request.operation_id,
        (value) => (value.request.operation_id = 'not-a-uuid'),
        (value) => (value.request.body.rows[0].n = 9007199254740993),
        (value) => (value.request.body.expected_snapshot = 'latest'),
        (value) => (value.request.body.extra = true),
        (value) => (value.request.token = 'forbidden'),
        (value) => (value.version = 2),
    ]) {
        const invalid = structuredClone(exported);
        change(invalid);
        assert.throws(() => pending.importPending(invalid, scope));
    }
    assert.throws(
        () => pending.exportPending(scope, 'append', { ...draft, terminal: true }),
        /terminal|pending/i,
    );
});

test('pending schema preserves supported UTF-8 field names without imposing identifier syntax', () => {
    const namedSchema = {
        fields: [{ field_id: 1, name: 'длинное имя / value', type: 'string', nullable: false }],
    };
    const exported = pending.exportPending(scope, 'append', {
        ...draft,
        schema: namedSchema,
        body: { rows: [{ 'длинное имя / value': 'retained' }] },
    });
    assert.deepEqual(pending.importPending(exported, scope).draft.schema, namedSchema);
});

test('recovery file roundtrip accepts a valid wide append whose pretty envelope exceeds 16 MiB', async () => {
    assert.equal(typeof pending.serializePendingFile, 'function');
    assert.equal(typeof pending.readPendingFile, 'function');
    const fields = Array.from({ length: 64 }, (_, index) => ({
        field_id: index + 1,
        name: index === 0 ? 'key' : `f${index}`,
        type: 'string',
        nullable: false,
    }));
    const row = Object.fromEntries(fields.map((field) => [field.name, 'abcdefgh']));
    const wide = {
        ...draft,
        schema: { fields },
        body: {
            rows: Array.from({ length: 10000 }, () => ({ ...row })),
            expected_snapshot: body.expected_snapshot,
        },
    };
    assert.ok(
        Buffer.byteLength(JSON.stringify({ ...wide.body, operation_id: operationId })) <
            16 * 1024 * 1024,
    );
    const text = pending.serializePendingFile(scope, 'append', wide);
    assert.ok(Buffer.byteLength(text) > 16 * 1024 * 1024);
    assert.ok(!text.includes('do-not-export-token'));
    const imported = await pending.readPendingFile(new File([text], 'wide.json'), scope);
    assert.equal(imported.draft.operationId, operationId);
    assert.equal(imported.draft.collectionId, collectionId);
    assert.deepEqual(imported.draft.schema, wide.schema);
    assert.deepEqual(imported.draft.body, wide.body);
    assert.equal(imported.draft.fingerprint, JSON.stringify(wide.body));
});

test('recovery file export bounds the complete UTF-8 envelope, including scope and schema', () => {
    assert.equal(typeof pending.serializePendingFile, 'function');
    const maxBytes = 32 * 1024 * 1024;
    const minimal = {
        ...draft,
        schema: { fields: [{ ...schema.fields[0], name: 'label' }] },
        body: { rows: [{ label: '' }] },
    };
    const baseline = JSON.stringify(pending.exportPending(scope, 'append', minimal), null, 2);
    const padding = maxBytes - Buffer.byteLength(baseline);
    const atLimit = { ...minimal, body: { rows: [{ label: 'a'.repeat(padding) }] } };
    assert.equal(
        Buffer.byteLength(pending.serializePendingFile(scope, 'append', atLimit)),
        maxBytes,
    );
    assert.throws(
        () =>
            pending.serializePendingFile(
                { ...scope, backend: `${scope.backend}x` },
                'append',
                atLimit,
            ),
        /32 MiB/,
    );
    assert.throws(
        () =>
            pending.serializePendingFile(scope, 'append', {
                ...minimal,
                body: { rows: [{ label: 'é'.repeat(maxBytes / 2) }] },
            }),
        /32 MiB/,
    );
});

test('oversized recovery imports are rejected before reading or parsing the file', async () => {
    assert.equal(typeof pending.readPendingFile, 'function');
    let read = false;
    await assert.rejects(
        pending.readPendingFile(
            {
                size: 32 * 1024 * 1024 + 1,
                text() {
                    read = true;
                    throw new Error('must not read');
                },
            },
            scope,
        ),
        /32 MiB/,
    );
    assert.equal(read, false);
    // Validate encoded text as well, before JSON.parse, even for a custom file reader.
    await assert.rejects(
        pending.readPendingFile(
            {
                size: 1,
                async text() {
                    return 'é'.repeat(16 * 1024 * 1024 + 1);
                },
            },
            scope,
        ),
        /32 MiB/,
    );
});
