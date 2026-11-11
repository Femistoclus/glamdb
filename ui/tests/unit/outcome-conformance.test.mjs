import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { WorkbenchAPI } from '../../assets/api.js';
import { wireRows } from '../../assets/model.js';

const fixture = JSON.parse(
    readFileSync(new URL('../../../fixtures/http/outcomes-v0.json', import.meta.url)),
);
const cases = Object.fromEntries(fixture.cases.map((value) => [value.name, value]));

function invoke(client, example) {
    const request = fixture.appends[example.request];
    const path = `/collections/${fixture.collection_id}`;
    if (example.kind === 'create')
        return client.json('/collections', {
            method: 'POST',
            body: {
                collection_id: fixture.collection_id,
                name: fixture.exact_name,
                schema: fixture.schema,
            },
            allowOutcome: true,
        });
    if (example.kind === 'lookup')
        return client.json(`${path}/operations/${request.operation_id}`, { allowOutcome: true });
    return client.json(`${path}/append`, {
        method: 'POST',
        body: { operation_id: request.operation_id, rows: request.rows },
        allowOutcome: true,
    });
}

function clientFor(example, response = example.response) {
    return new WorkbenchAPI(
        'http://localhost',
        'token',
        async () => new Response(JSON.stringify(response), { status: example.status }),
    );
}

for (const example of fixture.cases) {
    test(`shared native outcome: ${example.name}`, async () => {
        assert.deepEqual(await invoke(clientFor(example), example), example.response);
    });
}

for (const invalid of fixture.invalid_recovery.filter(
    (value) => !value.clients || value.clients.includes('ui'),
)) {
    test(`invalid recovery fails explicitly: ${invalid.name}`, async () => {
        const example = cases[invalid.case];
        const response = structuredClone(example.response);
        const path = (invalid.remove ?? invalid.path).slice(1).split('/');
        let target = response;
        for (const part of path.slice(0, -1)) target = target[part];
        if (invalid.remove) delete target[path.at(-1)];
        else target[path.at(-1)] = invalid.value;
        await assert.rejects(invoke(clientFor(example, response), example), /recovery/i);
    });
}

for (const name of ['unknown_publication', 'blocked_lookup', 'lost_create']) {
    test(`recovery extension fields survive: ${name}`, async () => {
        const example = cases[name];
        const response = structuredClone(example.response);
        response.extension = { future: true };
        response.recovery.extension = { future: '9007199254740993' };
        assert.deepEqual(await invoke(clientFor(example, response), example), response);
    });
}

test('shared rows retain duplicate multiplicity, NULL, exact int64 and UTC microseconds', () => {
    for (const request of Object.values(fixture.appends))
        assert.deepEqual(wireRows(fixture.schema, request.rows), request.rows);
    assert.deepEqual(fixture.appends.a.rows[0], fixture.appends.a.rows[2]);
    assert.equal(fixture.appends.a.rows[1].n, null);
    assert.equal(fixture.appends.a.rows[0].n, '9007199254740993');
    assert.equal(fixture.appends.a.rows[0].ts, '2026-01-01T00:00:00.123456Z');
});

test('retry A after B returns the original complete receipt', async () => {
    const outcomes = [];
    for (const name of ['committed', 'committed_b', 'retry_a_after_b'])
        outcomes.push(await invoke(clientFor(cases[name]), cases[name]));
    assert.deepEqual(outcomes[0], outcomes[2]);
    assert.equal(outcomes[1].result.parent_snapshot_id, outcomes[0].result.snapshot_id);
});

test('create flow rejects incomplete recovery before interpreting the HTTP status', async () => {
    const example = cases.lost_create;
    const response = structuredClone(example.response);
    delete response.recovery;
    await assert.rejects(
        clientFor(example, response).json('/collections', {
            method: 'POST',
            body: {
                collection_id: fixture.collection_id,
                name: fixture.exact_name,
                schema: fixture.schema,
            },
        }),
        /recovery/i,
    );
});
