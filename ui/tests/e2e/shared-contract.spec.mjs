import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const token = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';
const native = process.env.GLAMDB_TEST_BACKEND === 'native';
let databaseId;
let collectionId;
let pageErrors;
const headers = { Authorization: `Bearer ${token}` };
const python = process.env.GLAMDB_E2E_PYTHON;

test.beforeEach(async ({ page }) => {
    pageErrors = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    const response = await page.request.get('/capabilities', { headers });
    expect(response.ok()).toBe(true);
    const capabilities = await response.json();
    expect(capabilities.backend).toBe(native ? 'local' : 'mock');
    expect(capabilities.mock).toBe(!native);
    expect(capabilities.durability_validation).toBe('designed');
});
test.afterEach(() => expect(pageErrors, 'uncaught browser errors').toEqual([]));

async function login(page) {
    await page.goto('/');
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    if (native) {
        const created = page.waitForResponse(
            (response) =>
                response.request().method() === 'POST' && response.url().endsWith('/databases'),
        );
        await page.getByLabel('Database name').fill(`conformance_${Date.now()}`);
        await page.getByRole('button', { name: 'Create database', exact: true }).click();
        databaseId = (await (await created).json()).database_id;
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
    const created = page.waitForResponse(
        (response) =>
            response.request().method() === 'POST' && response.url().endsWith('/collections'),
    );
    await page.getByRole('button', { name: 'Create collection' }).click();
    collectionId = (await (await created).json()).collection_id;
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('.breadcrumb strong')).toHaveText(name);
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
