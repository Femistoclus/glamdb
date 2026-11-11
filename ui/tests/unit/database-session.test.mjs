import test from 'node:test';
import assert from 'node:assert/strict';
import { resetConnection, switchDatabase } from '../../assets/model.js';
import { WorkbenchAPI } from '../../assets/api.js';

test('database selection reads open state and only opens closed databases', async () => {
    const calls = [];
    const api = new WorkbenchAPI('http://localhost', 'secret', async (url, options) => {
        calls.push([url, options.method]);
        return new Response(
            JSON.stringify({ database_id: 'one', state: calls.length === 1 ? 'open' : 'closed' }),
        );
    });
    await api.selectDatabase('one');
    assert.deepEqual(calls, [['http://localhost/databases/one', 'GET']]);
    await api.selectDatabase('one');
    assert.deepEqual(calls.slice(1), [
        ['http://localhost/databases/one', 'GET'],
        ['http://localhost/databases/one/open', 'POST'],
    ]);
});

test('unresolved frozen writes survive database list/reconcile/switch but disconnect clears them', () => {
    const pending = {
        operationId: 'operation',
        collectionId: 'collection',
        terminal: false,
        body: { rows: [{ key: 'a' }] },
    };
    const a = { database_id: 'a' },
        b = { database_id: 'b' };
    let state = { ...resetConnection({}), database: a, operations: [pending], mutation: pending };
    state = switchDatabase(state, null);
    assert.deepEqual(state.operations, []);
    state = switchDatabase(state, b);
    assert.equal(state.mutation, null);
    state = switchDatabase(state, a);
    assert.equal(state.operations[0], pending);
    assert.equal(state.mutation, pending);
    assert.equal(state.mutation.operationId, 'operation');
    state = switchDatabase(resetConnection(state), a);
    assert.deepEqual(state.operations, []);
});

for (const state of ['opening', 'closing', 'reconciling', 'error']) {
    test(`database selection refuses ${state} without reopening it`, async () => {
        const calls = [];
        const api = new WorkbenchAPI('http://localhost', 'secret', async (url, options) => {
            calls.push(options.method);
            return new Response(JSON.stringify({ database_id: 'one', state }));
        });
        await assert.rejects(api.selectDatabase('one'), new RegExp(state));
        assert.deepEqual(calls, ['GET']);
    });
}
