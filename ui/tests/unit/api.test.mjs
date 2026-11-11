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
