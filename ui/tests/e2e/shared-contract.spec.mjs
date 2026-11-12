import { test as base, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import path from 'node:path';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const token = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';
const native = process.env.GLAMDB_TEST_BACKEND === 'native';
let databaseId;
let collectionId;
let pageErrors;
const headers = { Authorization: `Bearer ${token}` };
const python = process.env.GLAMDB_E2E_PYTHON;
const test = base.extend({
    sharedServer: [
        async ({}, use, worker) => {
            if (native) {
                await use(worker.project.use.baseURL);
                return;
            }
            // Keep the mock's eight-collection ceiling independent of other suites.
            const port = Number(process.env.GLAMDB_E2E_PORT ?? 8766) + 100 + worker.workerIndex;
            const child = spawn(
                python,
                ['-m', 'glamdb_mock', '--port', String(port), '--ui-dir', path.join(root, 'ui')],
                {
                    cwd: root,
                    stdio: 'ignore',
                    env: {
                        ...process.env,
                        GLAMDB_TOKEN: token,
                        PYTHONPATH: [
                            'tools/verification/src',
                            'apps/mock_server/src',
                            'sdk/python/src',
                        ]
                            .map((source) => path.join(root, source))
                            .join(path.delimiter),
                    },
                },
            );
            const ended = once(child, 'exit');
            const url = `http://127.0.0.1:${port}`;
            try {
                await expect
                    .poll(async () => {
                        if (child.exitCode !== null) throw new Error('Shared mock server exited');
                        try {
                            return (await fetch(`${url}/healthz`)).ok;
                        } catch {
                            return false;
                        }
                    })
                    .toBe(true);
                await use(url);
            } finally {
                if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
                await ended;
            }
        },
        { scope: 'worker' },
    ],
    baseURL: async ({ sharedServer }, use) => use(sharedServer),
});

test.beforeEach(async ({ page }) => {
    databaseId = undefined;
    pageErrors = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    const response = await page.request.get('/capabilities', { headers });
    expect(response.ok()).toBe(true);
    const capabilities = await response.json();
    expect(capabilities.backend).toBe(native ? 'local' : 'mock');
    expect(capabilities.mock).toBe(!native);
    expect(capabilities.durability_validation).toBe('designed');
});
test.afterEach(async ({ request }) => {
    expect(pageErrors, 'uncaught browser errors').toEqual([]);
    if (native && databaseId) {
        const database = await request.get(`/databases/${databaseId}`, { headers });
        if ((await database.json()).state === 'open') {
            const closed = await request.post(`/databases/${databaseId}/close`, {
                headers,
                data: {},
            });
            expect(closed.ok()).toBe(true);
        }
    }
});

async function login(page) {
    await page.goto('/');
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    if (native) {
        const name = `conformance_${Date.now()}`;
        await page.getByLabel('Database name').fill(name);
        await page.getByRole('button', { name: 'Create database', exact: true }).click();
        await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
        const response = await page.request.get('/databases', { headers });
        expect(response.ok()).toBe(true);
        const database = (await response.json()).items.find((item) => item.name === name);
        expect(database?.state).toBe('open');
        databaseId = database.database_id;
    }
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
    await nav(page, 'Data studio');
}

async function nav(page, name) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
}
async function createCollection(page, name) {
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill(name);
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('.breadcrumb strong')).toHaveText(name);
    const scope = native ? `/databases/${databaseId}` : '';
    const response = await page.request.get(
        `${scope}/collections:resolve?name=${encodeURIComponent(name)}`,
        { headers },
    );
    expect(response.ok()).toBe(true);
    collectionId = (await response.json()).collection_id;
}
async function appendRows(page, rows) {
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    const dialog = page.getByRole('dialog');
    await dialog.getByLabel('Rows · JSON array').fill(JSON.stringify(rows));
    await dialog.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(dialog.getByText('Append committed', { exact: true })).toBeVisible();
    await dialog.getByRole('button', { name: 'Close dialog' }).click();
}

test('create, append duplicate keys, precision, filter and historical view', async ({ page }) => {
    await login(page);
    await createCollection(page, `browser_${Date.now()}`);
    await appendRows(page, [
        {
            key: 'same',
            label: '<img src=x onerror="window.injected=true">',
            amount: '9007199254740993',
        },
        { key: 'same', label: 'Second', amount: '2' },
    ]);
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(2);
    await expect(page.getByRole('cell', { name: '9007199254740993', exact: true })).toBeVisible();
    await expect(page.getByRole('table', { name: 'Query results' }).locator('img')).toHaveCount(0);
    expect(await page.evaluate(() => window.injected)).toBeUndefined();
    const old = await page.locator('#snapshot-selector option').last().getAttribute('value');
    await appendRows(page, [{ key: 'next', label: 'Third', amount: '3' }]);
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(3);
    await page.locator('#snapshot-selector').selectOption(old);
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(2);
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter column 1').selectOption('amount');
    await page.getByLabel('Filter operator 1').selectOption('gt');
    await page.getByLabel('Filter value 1').fill('2');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(1);
    await expect(page.getByRole('cell', { name: '9007199254740993', exact: true })).toBeVisible();
    const download = page.waitForEvent('download');
    await page.getByRole('button', { name: 'Arrow', exact: true }).click();
    const arrow = await download;
    expect(arrow.suggestedFilename()).toMatch(/\.arrows$/);
    const arrowPath = await arrow.path();
    expect(await arrow.failure()).toBeNull();
    const rows = JSON.parse(
        execFileSync(
            python,
            [
                '-c',
                'import json,sys,pyarrow.ipc as ipc; rows=ipc.open_stream(sys.argv[1]).read_all().to_pylist(); print(json.dumps([dict(r, amount=str(r["amount"])) for r in rows]))',
                arrowPath,
            ],
            { cwd: root, encoding: 'utf8' },
        ),
    );
    expect(rows).toEqual([
        {
            key: 'same',
            label: '<img src=x onerror="window.injected=true">',
            amount: '9007199254740993',
        },
    ]);
    if (native) {
        const closed = await page.request.post(`/databases/${databaseId}/close`, {
            headers,
            data: {},
        });
        expect(closed.ok()).toBe(true);
        const { data_dir: dataDir } = JSON.parse(
            await readFile(process.env.GLAMDB_E2E_STATE_FILE, 'utf8'),
        );
        const evidence = JSON.parse(
            execFileSync(
                python,
                [
                    '-c',
                    'import json,sys; from tools.acceptance.physical import inspect_store; print(json.dumps(inspect_store(*sys.argv[1:])))',
                    `${dataDir}/databases/${databaseId}`,
                    databaseId,
                    collectionId,
                ],
                { cwd: root, encoding: 'utf8' },
            ),
        );
        expect(evidence.views.latest.rows).toBe('3');
        expect(evidence.views.latest.files).toHaveLength(2);
        expect(evidence.views.latest.manifests.length).toBeGreaterThan(0);
    }
});

async function downloadContents(page) {
    const pending = page.waitForEvent('download');
    await page.getByRole('button', { name: 'Arrow', exact: true }).click();
    const downloaded = await pending;
    expect(await downloaded.failure()).toBeNull();
    return JSON.parse(
        execFileSync(
            python,
            [
                '-c',
                'import json,sys,pyarrow.ipc as ipc; reader=ipc.open_stream(sys.argv[1]); rows=reader.read_all().to_pylist(); print(json.dumps(dict(names=reader.schema.names, rows=[dict(r, amount=None if r["amount"] is None else str(r["amount"])) for r in rows])))',
                await downloaded.path(),
            ],
            { cwd: root, encoding: 'utf8' },
        ),
    );
}

test('empty Arrow retains schema and latest preview download stays fixed across append', async ({
    page,
}) => {
    await login(page);
    await createCollection(page, `arrow_view_${Date.now()}`);
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByText('No rows match this query. The result schema is still available.'),
    ).toBeVisible();
    expect(await downloadContents(page)).toEqual({ names: ['key', 'label', 'amount'], rows: [] });
    await appendRows(page, [{ key: 'preview', label: 'fixed', amount: '9007199254740993' }]);
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(1);
    const selected = await page.locator('.results-panel .panel-header p code').textContent();
    const collection = `${native ? `/databases/${databaseId}` : ''}/collections/${collectionId}`;
    const appended = await page.request.post(`${collection}/append`, {
        headers,
        data: {
            operation_id: randomUUID(),
            rows: [{ key: 'after-preview', label: 'later', amount: '2' }],
        },
    });
    expect((await appended.json()).outcome).toBe('committed');
    const requests = [];
    page.on('request', (request) => {
        if (request.headers().accept === 'application/vnd.apache.arrow.stream')
            requests.push(request.postDataJSON());
    });
    expect((await downloadContents(page)).rows).toEqual([
        { key: 'preview', label: 'fixed', amount: '9007199254740993' },
    ]);
    expect(requests.some((body) => body.snapshot_id === selected)).toBe(true);
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(1);
});

for (const fault of ['checksum', 'missing_eos', 'snapshot']) {
    test(`invalid ${fault} in a real Arrow export never starts a download`, async ({ page }) => {
        await login(page);
        await createCollection(page, `arrow_fault_${Date.now()}`);
        await appendRows(page, [{ key: 'verified-only', label: 'verified', amount: '1' }]);
        await page.getByRole('button', { name: 'Run query', exact: true }).click();
        await expect(
            page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
        ).toHaveCount(1);
        const downloads = [];
        page.on('download', (download) => downloads.push(download));
        await page.route('**/collections/*/scan', async (route) => {
            if (route.request().headers().accept !== 'application/vnd.apache.arrow.stream') {
                await route.continue();
                return;
            }
            const response = await route.fetch();
            const metadata = response.headers();
            let bytes = await response.body();
            if (fault === 'checksum') metadata['x-content-sha256'] = '0'.repeat(64);
            else if (fault === 'snapshot') metadata['x-glamdb-snapshot-id'] = 'initial';
            else {
                bytes = bytes.subarray(0, -8);
                metadata['content-length'] = String(bytes.length);
                metadata['x-content-sha256'] = createHash('sha256').update(bytes).digest('hex');
            }
            await route.fulfill({ response, headers: metadata, body: bytes });
        });
        await page.getByRole('button', { name: 'Arrow', exact: true }).click();
        await expect(
            page.getByText(
                fault === 'checksum'
                    ? 'Arrow download checksum mismatch'
                    : fault === 'snapshot'
                      ? 'Arrow download returned a different snapshot'
                      : 'Invalid or incomplete Arrow IPC stream',
                { exact: true },
            ),
        ).toBeVisible();
        expect(downloads).toEqual([]);
        await page.unroute('**/collections/*/scan');
        expect((await downloadContents(page)).rows).toEqual([
            { key: 'verified-only', label: 'verified', amount: '1' },
        ]);
    });
}
