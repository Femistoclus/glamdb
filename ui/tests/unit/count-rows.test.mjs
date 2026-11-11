import test from 'node:test';
import assert from 'node:assert/strict';
import { WorkbenchAPI } from '../../assets/api.js';

test('count streams every page of the same view and cancels its cursor', async () => {
    const requests = [];
    const api = new WorkbenchAPI('http://localhost', 'token', async (url, options) => {
        const body = JSON.parse(options.body);
        requests.push([new URL(url).pathname, body]);
        if (url.endsWith('/open')) return Response.json({ cursor: 'first' });
        if (url.endsWith('/cancel')) return Response.json({ cancelled: true });
        return Response.json({
            snapshot_id: '9007199254740993',
            rows: body.cursor === 'first' ? [{ n: '1' }, { n: '2' }] : [{ n: '3' }],
            next_cursor: body.cursor === 'first' ? 'last' : null,
        });
    });
    assert.deepEqual(await api.countRows('/collections/c/scan', { projection: ['n'] }), {
        rows: '3',
        snapshot_id: '9007199254740993',
    });
    assert.deepEqual(
        requests.map((x) => x[0]),
        [
            '/collections/c/scan/open',
            '/collections/c/scan',
            '/collections/c/scan',
            '/collections/c/scan/cancel',
        ],
    );
    assert.deepEqual(requests[0][1], { projection: ['n'] });
});
test('count never presents mixed snapshots as a complete total', async () => {
    let cancelled = false;
    const api = new WorkbenchAPI('http://localhost', 'token', async (url, options) => {
        if (url.endsWith('/open')) return Response.json({ cursor: 'first' });
        if (url.endsWith('/cancel')) {
            cancelled = true;
            return Response.json({});
        }
        const first = JSON.parse(options.body).cursor === 'first';
        return Response.json({
            snapshot_id: first ? '1' : '2',
            rows: [{}],
            next_cursor: first ? 'last' : null,
        });
    });
    await assert.rejects(
        () => api.countRows('/collections/c/scan', { projection: ['key'] }),
        /view changed/,
    );
    assert.equal(cancelled, true);
});

test('cancelling a count releases its open scan and never returns a partial total', async () => {
    const controller = new AbortController();
    let cancelled = false;
    const api = new WorkbenchAPI('http://localhost', 'token', async (url) => {
        if (url.endsWith('/open')) return Response.json({ cursor: 'first' });
        if (url.endsWith('/cancel')) {
            cancelled = true;
            return Response.json({});
        }
        controller.abort();
        return Response.json({ snapshot_id: '1', rows: [{}], next_cursor: 'second' });
    });
    await assert.rejects(
        () =>
            api.countRows(
                '/collections/c/scan',
                { projection: ['key'] },
                { signal: controller.signal },
            ),
        { name: 'AbortError' },
    );
    assert.equal(cancelled, true);
});
test('a repeated scan cursor fails rather than counting a replay twice', async () => {
    const api = new WorkbenchAPI('http://localhost', 'token', async (url) =>
        Response.json(
            url.endsWith('/open')
                ? { cursor: 'first' }
                : url.endsWith('/cancel')
                  ? {}
                  : { snapshot_id: '1', rows: [{}], next_cursor: 'first' },
        ),
    );
    await assert.rejects(
        () => api.countRows('/collections/c/scan', { projection: ['key'] }),
        /repeated/,
    );
});
