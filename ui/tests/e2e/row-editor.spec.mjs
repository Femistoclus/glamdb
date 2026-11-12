import { test as base, expect } from '@playwright/test';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

// Each editor suite owns its disposable server and collection budget.
const root = fileURLToPath(new URL('../../../', import.meta.url));
const test = base.extend({
    editorServer: [
        async ({}, use) => {
            const socket = createServer();
            socket.listen(0, '127.0.0.1');
            await once(socket, 'listening');
            const port = socket.address().port;
            await new Promise((resolve) => socket.close(resolve));
            const python = process.env.GLAMDB_E2E_PYTHON ?? path.resolve('../.venv/bin/python');
            const child = spawn(
                python,
                ['-m', 'glamdb_mock', '--port', String(port), '--ui-dir', path.join(root, 'ui')],
                {
                    cwd: root,
                    stdio: 'ignore',
                    env: {
                        ...process.env,
                        GLAMDB_TOKEN: 'e2e-only-local-token',
                        PYTHONPATH: [
                            'tools/verification/src',
                            'apps/mock_server/src',
                            'sdk/python/src',
                        ]
                            .map((p) => path.join(root, p))
                            .join(path.delimiter),
                    },
                },
            );
            const ended = once(child, 'exit');
            const url = `http://127.0.0.1:${port}`;
            try {
                await expect
                    .poll(async () => {
                        if (child.exitCode !== null) throw new Error('Editor test server exited');
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
    baseURL: async ({ editorServer }, use) => use(editorServer),
});
test('separate read and write panels submit typed rows without a JSON editor', async ({ page }) => {
    await page.goto('/');
    await page.getByLabel('Access token').fill('e2e-only-local-token');
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('form_' + Date.now());
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    await expect(page.locator('#query-form')).toBeVisible();
    const write = page.locator('#write-form');
    await expect(write).toBeVisible();
    await expect(
        page.locator('#query-form').getByRole('button', { name: 'Append rows' }),
    ).toHaveCount(0);
    await expandRow(write, 0);
    await write.getByLabel('key row 1', { exact: true }).fill('form-🎀');
    await write.getByLabel('label row 1', { exact: true }).fill('a field value');
    await write.getByLabel('amount row 1 NULL', { exact: true }).uncheck();
    await write.getByLabel('amount row 1', { exact: true }).fill('9223372036854775807');
    let sent;
    await page.route('**/collections/*/append', async (route) => {
        sent = route.request().postDataJSON();
        await route.continue();
    });
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    expect(sent.rows).toEqual([
        { key: 'form-🎀', label: 'a field value', amount: '9223372036854775807' },
    ]);
    await page.locator('#run-query').click();
    await expect(page.getByRole('table', { name: 'Query results' })).toContainText(
        '9223372036854775807',
    );
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await page.screenshot({
        path: '../artifacts/ui-editor/read-write-desktop.png',
        fullPage: true,
    });
    await page.setViewportSize({ width: 390, height: 844 });
    await expect
        .poll(() => page.locator('#sidebar').evaluate((node) => node.getBoundingClientRect().right))
        .toBeLessThanOrEqual(0);
    await expect(write).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(
        390,
    );
    await page.screenshot({ path: '../artifacts/ui-editor/read-write-mobile.png', fullPage: true });
});

async function newCollection(page) {
    await page.goto('/');
    await page.getByLabel('Access token').fill('e2e-only-local-token');
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('typed_' + Date.now());
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    await expect(page.locator('#write-form')).toBeVisible();
}
test('form validates before sending and preserves rows while adding and removing fields', async ({
    page,
}) => {
    await newCollection(page);
    let writes = 0;
    page.on('request', (r) => {
        if (r.url().endsWith('/append')) writes++;
    });
    const write = page.locator('#write-form');
    await expandRow(write, 0);
    await write.getByLabel('key row 1', { exact: true }).fill('kept');
    await write.getByLabel('amount row 1 NULL', { exact: true }).uncheck();
    await write.getByLabel('amount row 1', { exact: true }).fill('9223372036854775808');
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByRole('alert')).toContainText('outside int64');
    expect(writes).toBe(0);
    await write.getByRole('button', { name: '+ Add row', exact: true }).click();
    await expect(write.getByLabel('key row 1', { exact: true })).toHaveValue('kept');
    await expandRow(write, 0);
    await expandRow(write, 1);
    await write.getByLabel('key row 2', { exact: true }).fill('removed');
    await write.getByRole('button', { name: 'Remove row 2', exact: true }).click();
    await expect(write.locator('[data-write-row]')).toHaveCount(1);
    await expandRow(write, 0);
    await write.getByLabel('amount row 1 NULL', { exact: true }).check();
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    expect(writes).toBe(1);
    await write.getByRole('button', { name: 'New rows', exact: true }).click();
    await expandRow(write, 0);
    await expect(write.getByLabel('key row 1', { exact: true })).toBeEditable();
    await expect(write.getByLabel('key row 1', { exact: true })).toHaveValue('');
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter column 1').selectOption('amount');
    await page.getByLabel('Filter value 1').fill('9223372036854775807');
    await page.getByText('Query JSON', { exact: true }).click();
    await expect(page.locator('#query-json')).toContainText('"value": "9223372036854775807"');
});
test('typed form freezes an unknown write and resolves it without submitting a second write', async ({
    page,
}) => {
    await newCollection(page);
    let writes = 0,
        operationId;
    await page.route('**/collections/*/append', async (route) => {
        writes++;
        operationId = route.request().postDataJSON().operation_id;
        await route.fetch();
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({ outcome: 'commit_unknown', recovery_required: true }),
        });
    });
    const write = page.locator('#write-form');
    await expandRow(write, 0);
    await write.getByLabel('key row 1', { exact: true }).fill('once');
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(
        write.getByText('Outcome unresolved · keep this operation ID', { exact: true }),
    ).toBeVisible();
    await expect(write.getByLabel('key row 1', { exact: true })).toBeDisabled();
    await write.getByRole('button', { name: 'Check outcome', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    await expect(write.locator('#write-outcome code')).toHaveText(operationId);
    expect(writes).toBe(1);
    await write.getByRole('button', { name: 'New rows', exact: true }).click();
    await expandRow(write, 0);
    await expect(write.getByLabel('key row 1', { exact: true })).toBeEditable();
});

test('runtime labels and editor spacing update without losing write drafts', async ({ page }) => {
    await newCollection(page);
    await expandRow(page.locator('#write-form'), 0);
    await page.getByLabel('key row 1', { exact: true }).fill('retained draft');
    await expect(page.getByLabel('Result limit')).toHaveValue('1000');
    await expect(page.getByLabel('Result limit')).toHaveAttribute('max', '10000');
    await page.getByLabel('Result limit').fill('1010');
    let queryLimit;
    await page.route('**/collections/*/scan', async (route) => {
        queryLimit = route.request().postDataJSON().limit;
        await route.continue();
    });
    await page.locator('#run-query').click();
    await expect.poll(() => queryLimit).toBe(1010);
    await page.route('**/workbench/config', async (route) => {
        const response = await route.fetch();
        const config = await response.json();
        config.labels.read_title = 'Read with a bow';
        config.labels.write_title = 'Write with sparkle';
        config.sizing.editor_gap = '25px';
        config.editor = { query_max_rows: 512 };
        await route.fulfill({ response, json: config });
    });
    await expect(page.locator('#read-title')).toHaveText('Read with a bow');
    await expect(page.locator('#write-title')).toHaveText('Write with sparkle');
    await expect(page.locator('.data-editors')).toHaveCSS('column-gap', '25px');
    await expect(page.getByLabel('Result limit')).toHaveAttribute('max', '512');
    await expect(page.getByLabel('Result limit')).toHaveValue('512');
    await expect(page.getByLabel('key row 1', { exact: true })).toHaveValue('retained draft');
});

test('inline retry preserves the original batch with multiline string values', async ({ page }) => {
    await newCollection(page);
    const requests = [];
    await page.route('**/collections/*/append', async (route) => {
        requests.push(route.request().postDataJSON());
        if (requests.length > 1) {
            await route.continue();
            return;
        }
        await route.fetch();
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({ outcome: 'commit_unknown', recovery_required: true }),
        });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    const dialog = page.getByRole('dialog');
    await dialog
        .getByLabel('Rows · JSON array')
        .fill(
            JSON.stringify([{ key: 'multiline', label: 'line 1\nline 2\r\nline 3', amount: null }]),
        );
    await dialog.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(dialog.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await dialog.getByRole('button', { name: 'Close dialog' }).click();
    await page.locator('nav').getByRole('button', { name: 'Schema', exact: true }).click();
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    const write = page.locator('#write-form');
    await write.getByRole('button', { name: 'Retry same operation', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    expect(requests).toHaveLength(2);
    expect(requests[1]).toEqual(requests[0]);
});

async function expandRow(form, index) {
    const row = form.locator('[data-write-row]').nth(index);
    if (!(await row.evaluate((node) => node.open))) await row.locator('summary').click();
}
