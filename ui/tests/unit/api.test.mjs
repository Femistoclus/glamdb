import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { brotliCompressSync, gzipSync } from 'node:zlib';
import { readFileSync } from 'node:fs';
import { WorkbenchAPI, APIError } from '../../assets/api.js';

const fixture = JSON.parse(
    readFileSync(new URL('../../../fixtures/http/outcomes-v0.json', import.meta.url)),
);
const unknown = fixture.cases.find((value) => value.name === 'unknown_publication').response;
const appendPath = `/collections/${fixture.collection_id}/append`;
const appendBody = { operation_id: fixture.appends.a.operation_id, rows: fixture.appends.a.rows };

async function withHTTPResponse({ body, headers = {}, status = 200 }, run) {
    const server = createServer((request, response) => {
        response.writeHead(status, {
            'Content-Type': 'application/json',
            'Content-Length': body.byteLength,
            Connection: 'close',
            ...headers,
        });
        response.end(body);
    });
    try {
        await new Promise((resolve, reject) => {
            server.once('error', reject);
            server.listen(0, '127.0.0.1', resolve);
        });
        return await run(`http://127.0.0.1:${server.address().port}`);
    } finally {
        if (server.listening) {
            await new Promise((resolve, reject) => {
                server.close((error) => (error ? reject(error) : resolve()));
                server.closeAllConnections();
            });
        }
    }
}

test('token is a header, never part of URL, and HTTP errors preserve code', async () => {
    let received;
    const client = new WorkbenchAPI(
        'http://localhost:8765',
        'private-token',
        async (url, options) => {
            received = { url, options };
            return new Response(
                JSON.stringify({ error: { code: 'NO_SUCH_COLLECTION', message: 'Missing' } }),
                { status: 404 },
            );
        },
    );
    await assert.rejects(
        client.json('/collections/x'),
        (error) => error instanceof APIError && error.code === 'NO_SUCH_COLLECTION',
    );
    assert.equal(received.options.headers.Authorization, 'Bearer private-token');
    assert.ok(!received.url.includes('private-token'));
});
test('gateway requests use browser credentials without an Authorization header', async () => {
    let received;
    const client = new WorkbenchAPI(
        'https://workbench.example/databases/alpha',
        'must-not-be-sent',
        async (url, options) => {
            received = { url, options };
            return Response.json({ collections: [] });
        },
        undefined,
        { gateway: true },
    );
    await client.json('/v0/collections', {
        headers: { authorization: 'must-not-be-sent-either' },
    });
    assert.equal(received.url, 'https://workbench.example/databases/alpha/v0/collections');
    assert.equal(received.options.credentials, 'same-origin');
    assert.equal(received.options.cache, 'no-store');
    assert.equal(received.options.redirect, 'error');
    assert.deepEqual(
        Object.keys(received.options.headers).filter(
            (name) => name.toLowerCase() === 'authorization',
        ),
        [],
    );
    assert.equal(client.token, '');
});
test('closing a client aborts requests and rejects a late successful response', async () => {
    let finish, signal;
    const client = new WorkbenchAPI('http://localhost', 'token', async (_url, options) => {
        signal = options.signal;
        return new Promise((resolve) => (finish = resolve));
    });
    const pending = client.json('/v0/collections');
    client.close();
    assert.equal(signal.aborted, true);
    finish(Response.json({ collections: [{ name: 'private' }] }));
    await assert.rejects(pending, { name: 'AbortError' });
    await assert.rejects(client.json('/v0/collections'), { name: 'AbortError' });
});
test('closing a client during body streaming cannot publish a completed export', async () => {
    let stream;
    const client = new WorkbenchAPI(
        'http://localhost',
        'token',
        async () =>
            new Response(
                new ReadableStream({
                    start(controller) {
                        stream = controller;
                    },
                }),
            ),
    );
    const pending = client.arrow('/v0/collections/a/scan', {});
    await Promise.resolve();
    client.close();
    stream.enqueue(new Uint8Array([1, 2, 3]));
    stream.close();
    await assert.rejects(pending, { name: 'AbortError' });
});
test('unknown outcome remains unresolved even with HTTP503', async () => {
    const client = new WorkbenchAPI(
        'http://localhost',
        'token',
        async () =>
            new Response(JSON.stringify(unknown), {
                status: 503,
            }),
    );
    const result = await client.json(appendPath, {
        method: 'POST',
        body: appendBody,
        allowOutcome: true,
    });
    assert.equal(result.outcome, 'commit_unknown');
    assert.equal(result.recovery_required, true);
});
test('oversized and malformed responses are errors, never empty success', async () => {
    const large = new WorkbenchAPI(
        'http://localhost',
        'token',
        async () => new Response('x'.repeat(32)),
        8,
    );
    await assert.rejects(large.json('/capabilities'), /too large/i);
    const invalid = new WorkbenchAPI(
        'http://localhost',
        'token',
        async () => new Response('not-json'),
    );
    await assert.rejects(invalid.json('/capabilities'), /JSON/);
});
test('credentials in base URL and insecure nonlocal HTTP are rejected', () => {
    assert.throws(() => new WorkbenchAPI('https://user:password@example.com', 't'));
    assert.throws(() => new WorkbenchAPI('http://example.com', 't'));
});

for (const [encoding, compress] of [
    ['gzip', gzipSync],
    ['br', brotliCompressSync],
]) {
    test(`real Fetch accepts ${encoding} JSON with the encoded Content-Length`, async () => {
        const payload = { collections: ['events'] };
        const decoded = Buffer.from(JSON.stringify(payload));
        const body = compress(decoded);
        assert.notEqual(body.byteLength, decoded.byteLength);
        await withHTTPResponse(
            { body, headers: { 'Content-Encoding': encoding } },
            async (base) => {
                const client = new WorkbenchAPI(base, 'token');
                assert.deepEqual(await client.json('/collections'), payload);
            },
        );
    });

    test(`real Fetch preserves an unknown append outcome in a ${encoding} response`, async () => {
        const body = compress(Buffer.from(JSON.stringify(unknown)));
        await withHTTPResponse(
            { body, headers: { 'Content-Encoding': encoding }, status: 503 },
            async (base) => {
                const client = new WorkbenchAPI(base, 'token');
                assert.deepEqual(
                    await client.json(appendPath, {
                        method: 'POST',
                        body: appendBody,
                        allowOutcome: true,
                    }),
                    unknown,
                );
            },
        );
    });
}

test('real Fetch enforces the decoded byte limit on compressed responses', async () => {
    const body = gzipSync(Buffer.from(JSON.stringify({ value: 'x'.repeat(512) })));
    assert.ok(body.byteLength < 64);
    await withHTTPResponse({ body, headers: { 'Content-Encoding': 'gzip' } }, async (base) => {
        const client = new WorkbenchAPI(base, 'token', globalThis.fetch.bind(globalThis), 64);
        await assert.rejects(client.json('/capabilities'), /too large/i);
    });
});

test('real Fetch accepts compressed overhead beyond the decoded byte allowance', async () => {
    const decoded = Buffer.from('{"ok":true}');
    const body = gzipSync(decoded);
    assert.ok(body.byteLength > decoded.byteLength);
    await withHTTPResponse({ body, headers: { 'Content-Encoding': 'gzip' } }, async (base) => {
        const client = new WorkbenchAPI(
            base,
            'token',
            globalThis.fetch.bind(globalThis),
            decoded.byteLength,
        );
        assert.deepEqual(await client.json('/capabilities'), { ok: true });
    });
});

test('uncompressed responses still reject a mismatched Content-Length', async () => {
    for (const encoding of [undefined, 'identity']) {
        const client = new WorkbenchAPI(
            'http://localhost',
            'token',
            async () =>
                new Response('{"ok":true}', {
                    headers: {
                        'Content-Length': '12',
                        ...(encoding ? { 'Content-Encoding': encoding } : {}),
                    },
                }),
        );
        await assert.rejects(client.json('/capabilities'), /interrupted/i);
    }
});

test('real Fetch rejects a truncated uncompressed response', async () => {
    await withHTTPResponse(
        { body: Buffer.from('{"ok":true}'), headers: { 'Content-Length': '12' } },
        async (base) => {
            const client = new WorkbenchAPI(base, 'token');
            await assert.rejects(client.json('/capabilities'), /interrupted|terminated/i);
        },
    );
});

test('Arrow downloads forward cancellation to the real HTTP request', async () => {
    await withHTTPResponse({ body: Buffer.from('arrow') }, async (base) => {
        const client = new WorkbenchAPI(base, 'token');
        await assert.rejects(
            client.arrow('/collections/a/scan', {}, { signal: AbortSignal.abort() }),
            { name: 'AbortError' },
        );
    });
});

test('unsafe gateway requests use only the current readable CSRF cookie', async () => {
    const oldDocument = globalThis.document;
    globalThis.document = { cookie: '__Host-glamdb-csrf=csrf-cookie-value; other=unrelated' };
    let received;
    try {
        const client = new WorkbenchAPI(
            'https://workbench.example/databases/00112233-4455-6677-8899-aabbccddeeff',
            '',
            async (url, options) => {
                received = { url, options };
                return Response.json({});
            },
            undefined,
            { gateway: true },
        );
        await client.json('/v0/collections', {
            method: 'POST',
            body: {},
            headers: { authorization: 'forged', 'x-csrftoken': 'forged' },
        });
        const headers = new Headers(received.options.headers);
        assert.equal(headers.get('X-CSRFToken'), 'csrf-cookie-value');
        assert.equal(headers.has('Authorization'), false);
        assert.equal(received.options.credentials, 'same-origin');
        assert.equal(received.options.redirect, 'error');
        assert.equal(received.options.body, '{}');
    } finally {
        if (oldDocument === undefined) delete globalThis.document;
        else globalThis.document = oldDocument;
    }
});

test('gateway mode rejects a cross-origin destination before exposing a CSRF cookie', () => {
    const previous = globalThis.location;
    globalThis.location = { origin: 'https://workbench.example' };
    try {
        assert.throws(
            () =>
                new WorkbenchAPI('https://elsewhere.example', '', async () => {}, undefined, {
                    gateway: true,
                }),
            /same.origin/i,
        );
    } finally {
        if (previous === undefined) delete globalThis.location;
        else globalThis.location = previous;
    }
});
