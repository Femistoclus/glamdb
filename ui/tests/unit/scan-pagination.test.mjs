import test from 'node:test';
import assert from 'node:assert/strict';
import { WorkbenchAPI } from '../../assets/api.js';

test('preview releases continuation and marks its partial result', async () => {
    const requests = [];
    const api = new WorkbenchAPI('http://localhost', 'secret', async (url, options) => {
        requests.push([url, JSON.parse(options.body)]);
        if (url.endsWith('/open')) return new Response(JSON.stringify({ cursor: 'first' }));
        return new Response(
            JSON.stringify(
                url.endsWith('/cancel')
                    ? { cancelled: true }
                    : { rows: [{ key: 'a' }], next_cursor: 'next' },
            ),
            { headers: { 'Content-Type': 'application/json' } },
        );
    });
    const result = await api.scanPreview('/collections/id/scan', { method: 'POST', body: {} });
    assert.equal(result.next_cursor, 'next');
    assert.deepEqual(requests[2], [
        'http://localhost/collections/id/scan/cancel',
        { cursor: 'first' },
    ]);
});

test('single-file Arrow export refuses a truncated paged result and releases cursor', async () => {
    let cancelled = false;
    const api = new WorkbenchAPI('http://localhost', 'secret', async (url) => {
        if (url.endsWith('/open')) return new Response(JSON.stringify({ cursor: 'first' }));
        if (url.endsWith('/cancel')) {
            cancelled = true;
            return new Response('{}');
        }
        return new Response(new Uint8Array([1, 2]), {
            headers: { 'X-GlamDB-Next-Cursor': 'next' },
        });
    });
    await assert.rejects(api.arrow('/collections/id/scan', {}), /multiple pages/);
    assert.equal(cancelled, true);
});

test('aborted first preview page cancels the known open cursor', async () => {
    const calls = [];
    const api = new WorkbenchAPI('http://localhost', 'secret', async (url, options) => {
        calls.push([url, JSON.parse(options.body)]);
        if (url.endsWith('/open')) return new Response(JSON.stringify({ cursor: 'first' }));
        if (url.endsWith('/cancel')) return new Response('{}');
        throw new DOMException('lost page', 'AbortError');
    });
    await assert.rejects(
        api.scanPreview('/collections/id/scan', { method: 'POST', body: { limit: 1 } }),
        /lost page/,
    );
    assert.deepEqual(calls, [
        ['http://localhost/collections/id/scan/open', { limit: 1 }],
        ['http://localhost/collections/id/scan', { cursor: 'first' }],
        ['http://localhost/collections/id/scan/cancel', { cursor: 'first' }],
    ]);
});

test('an explicit unpaged capability uses the legacy single-response scan', async () => {
    const paths = [];
    const api = new WorkbenchAPI('http://localhost', 'secret', async (url) => {
        paths.push(new URL(url).pathname);
        return new Response(
            JSON.stringify(
                url.endsWith('/capabilities')
                    ? { scan_pagination: false }
                    : { rows: [], next_cursor: null },
            ),
        );
    });
    await api.json('/capabilities');
    await api.scanPreview('/collections/id/scan', { method: 'POST', body: {} });
    assert.deepEqual(paths, ['/capabilities', '/collections/id/scan']);
});
