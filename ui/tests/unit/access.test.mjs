import test from 'node:test';
import assert from 'node:assert/strict';
import {
    loadAccessSession,
    selectDatabase,
    signOutAccess,
    validateAccessSession,
} from '../../assets/access.js';

const ALPHA = '00112233-4455-6677-8899-aabbccddeeff';
const SHARED = '11112233-4455-6677-8899-aabbccddeeff';
const session = {
    version: 3,
    revision: 7,
    organization: {
        id: '22222222-3333-4444-8555-666666666666',
        name: 'Pink team',
        login: 'Organization account',
    },
    subject: '33333333-4444-4555-8666-777777777777',
    databases: [
        { id: ALPHA, name: 'Alpha' },
        { id: SHARED, name: 'Shared' },
    ],
    account_url: 'https://identity.example/realms/glamdb/account/',
    login_url: '/oauth2/start',
    logout_url: '/workbench/logout',
};

test('version three supports an authenticated control account without an organization', () => {
    const result = validateAccessSession({ ...session, organization: null, databases: [] });
    assert.equal(result.organization, null);
    assert.equal(result.subject, session.subject);
    for (const version of [1, 2])
        assert.throws(() => validateAccessSession({ ...session, version }), /session/i);
});

test('session validation retains only the organization and allowed database identities', () => {
    const input = structuredClone(session);
    input.backend = 'https://elsewhere.example';
    input.token = 'must-not-be-copied';
    input.organization.password = 'must-not-be-copied';
    input.databases[0].url = 'https://elsewhere.example';
    const result = validateAccessSession(input);
    assert.deepEqual(result, session);
    input.databases[0].name = 'Changed';
    assert.equal(result.databases[0].name, 'Alpha');
});

test('malformed sessions and unsafe or duplicate identities cannot select a database', () => {
    for (const input of [
        null,
        { ...session, version: 2 },
        { ...session, revision: -1 },
        { ...session, revision: 1.5 },
        { ...session, subject: 'not-a-uuid' },
        { ...session, organization: { ...session.organization, id: '../pink' } },
        { ...session, organization: { ...session.organization, name: '' } },
        { ...session, organization: { ...session.organization, login: 4 } },
        { ...session, databases: {} },
        { ...session, databases: [{ id: ALPHA.toUpperCase(), name: 'Alpha' }] },
        { ...session, databases: [{ id: [ALPHA], name: 'Alpha' }] },
        { ...session, databases: [{ id: 'alpha/../../beta', name: 'Alpha' }] },
        { ...session, databases: [{ id: ALPHA, name: null }] },
        { ...session, databases: [session.databases[0], session.databases[0]] },
        { ...session, login_url: 'https://elsewhere.example' },
        { ...session, logout_url: '/unsafe-logout' },
        { ...session, account_url: 'javascript:alert(1)' },
        { ...session, account_url: [session.account_url] },
        { ...session, account_url: 'https://user:password@identity.example/account/' },
    ])
        assert.throws(() => validateAccessSession(input), /session/i);
});

test('the access session is fetched without a script-managed authorization header', async () => {
    let received;
    const controller = new AbortController();
    const result = await loadAccessSession(async (url, options) => {
        received = { url, options };
        return Response.json(session);
    }, controller.signal);
    assert.deepEqual(result, session);
    assert.equal(received.url, '/workbench/session');
    assert.equal(received.options.cache, 'no-store');
    assert.equal(received.options.credentials, 'same-origin');
    assert.equal(received.options.signal, controller.signal);
    assert.equal(new Headers(received.options.headers).has('Authorization'), false);
    await assert.rejects(
        loadAccessSession(async () => new Response('Unavailable', { status: 503 })),
        /503/,
    );
    await assert.rejects(
        loadAccessSession(async () => Response.json({})),
        /session/i,
    );
});

test('switching databases aborts every request and removes all previous workspace data', async () => {
    const theme = { brand: { name: 'GlamDB' } };
    let finish;
    const first = selectDatabase({ theme, epoch: 3 }, session, ALPHA, {
        origin: 'https://workbench.example',
        fetchImpl: async () => new Promise((resolve) => (finish = resolve)),
    });
    const pending = first.api.json('/v0/collections');
    Object.assign(first, {
        capabilities: {},
        collections: [{ name: 'alpha-only' }],
        collectionId: 'same-id',
        snapshotId: 'old-snapshot',
        resolvedSnapshot: 'old-snapshot',
        collection: {},
        schema: { fields: [{ name: 'private-column' }] },
        stats: {},
        snapshots: [{}],
        files: [{}],
        rows: [{ key: 'private-row' }],
        result: {},
        plan: {},
        selectedFile: 'private-file',
        lastQuery: {},
        clauses: [{}],
        projection: ['private-column'],
        operations: [{}],
        mutation: {},
        creation: {},
        tab: 'data',
        page: 3,
    });
    const controllers = Array.from({ length: 3 }, () => new AbortController());
    let request;
    const second = selectDatabase(first, session, SHARED, {
        origin: 'https://workbench.example',
        controllers,
        fetchImpl: async (url, options) => {
            request = { url, options };
            return Response.json({ collections: [] });
        },
    });
    for (const controller of controllers) assert.equal(controller.signal.aborted, true);
    for (const key of [
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
        assert.equal(second[key], null, key);
    for (const key of [
        'collections',
        'snapshots',
        'files',
        'rows',
        'clauses',
        'projection',
        'operations',
    ])
        assert.deepEqual(second[key], [], key);
    assert.equal(second.theme, theme);
    assert.equal(second.epoch, 5);
    assert.equal(second.page, 0);
    assert.equal(second.tab, 'overview');
    assert.equal(second.token, '');
    assert.equal(second.databaseId, SHARED);
    assert.equal(second.base, `https://workbench.example/databases/${SHARED}`);
    assert.deepEqual(second.access, session);
    assert.notEqual(second.api, first.api);
    finish(Response.json({ collections: [{ name: 'alpha-only' }] }));
    await assert.rejects(pending, { name: 'AbortError' });
    await second.api.json('/v0/collections');
    assert.equal(request.url, `https://workbench.example/databases/${SHARED}/v0/collections`);
    assert.equal(new Headers(request.options.headers).has('Authorization'), false);
    assert.deepEqual(second.collections, []);
});

test('an unlisted database cannot be opened and an empty allowlist creates no client', () => {
    assert.throws(
        () => selectDatabase({}, session, 'beta', { origin: 'https://workbench.example' }),
        /access/i,
    );
    const empty = selectDatabase(
        { rows: [{ key: 'old' }], api: { close() {} }, epoch: 1 },
        { ...session, databases: [] },
        null,
        { origin: 'https://workbench.example' },
    );
    assert.equal(empty.api, null);
    assert.equal(empty.databaseId, null);
    assert.deepEqual(empty.rows, []);
    assert.deepEqual(empty.collections, []);
    assert.deepEqual(empty.access.databases, []);
});

test('logout sends CSRF once and refuses an arbitrary redirect from the server', async () => {
    const previous = globalThis.document;
    globalThis.document = { cookie: '__Host-glamdb-csrf=one-time-cookie' };
    const calls = [];
    try {
        const fetch = async (url, options) => {
            calls.push({ url, options });
            return Response.json({ signed_out: true, redirect: '/oauth2/sign_out?rd=%2F' });
        };
        assert.equal(await signOutAccess(fetch), '/oauth2/sign_out?rd=%2F');
        assert.equal(calls.length, 1);
        assert.equal(calls[0].url, '/workbench/logout');
        assert.equal(calls[0].options.method, 'POST');
        assert.equal(calls[0].options.credentials, 'same-origin');
        assert.equal(calls[0].options.redirect, 'error');
        assert.equal(new Headers(calls[0].options.headers).get('X-CSRFToken'), 'one-time-cookie');
        assert.equal(new Headers(calls[0].options.headers).has('Authorization'), false);
        await assert.rejects(
            signOutAccess(async () =>
                Response.json({ signed_out: true, redirect: 'https://elsewhere.example' }),
            ),
            /sign.out response/i,
        );
        await assert.rejects(
            signOutAccess(async () => new Response('', { status: 503 })),
            /503/,
        );
        globalThis.document.cookie = '';
        await assert.rejects(
            signOutAccess(async () => assert.fail('must not POST without CSRF')),
            /access session/i,
        );
    } finally {
        if (previous === undefined) delete globalThis.document;
        else globalThis.document = previous;
    }
});
