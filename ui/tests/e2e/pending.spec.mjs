import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';

const token = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';
const root = fileURLToPath(new URL('../../../', import.meta.url));
let server, origin;
// Keep these extra recovery writes within a separate disposable mock process;
// they must not consume the normal browser suite's finite collection budget.
test.beforeAll(async () => {
    const reservation = createServer();
    reservation.listen(0, '127.0.0.1');
    await once(reservation, 'listening');
    const port = reservation.address().port;
    await new Promise((resolve) => reservation.close(resolve));
    origin = `http://127.0.0.1:${port}`;
    server = spawn(
        process.env.GLAMDB_E2E_PYTHON ?? '../.venv/bin/python',
        ['tests/e2e/start-server.py'],
        {
            cwd: `${root}ui`,
            env: { ...process.env, GLAMDB_E2E_PORT: String(port) },
            stdio: 'ignore',
        },
    );
    for (let attempt = 0; attempt < 200; attempt++) {
        if (server.exitCode !== null)
            throw new Error(`Disposable recovery mock exited: ${server.exitCode}`);
        try {
            if ((await fetch(`${origin}/healthz`)).ok) return;
        } catch {
            /* Wait for the isolated child to bind. */
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error('Disposable recovery mock did not become ready');
});
test.afterAll(async () => {
    if (server && server.exitCode === null) {
        server.kill('SIGTERM');
        await once(server, 'exit');
    }
});
async function connect(page) {
    await page.goto(origin);
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.locator('.breadcrumb')).toBeVisible();
}
async function create(page, name) {
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill(name);
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
}
async function recoveryFile(page) {
    const downloaded = page.waitForEvent('download');
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Export pending request', exact: true })
        .click();
    return readFile(await (await downloaded).path());
}

test('typed unresolved append exports and imports after reload with its exact original request', async ({
    page,
}) => {
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await connect(page);
    const name = `recovery_${Date.now()}`;
    await create(page, name);
    let original;
    let writes = 0;
    await page.route('**/collections/*/append', async (route) => {
        writes++;
        const body = route.request().postDataJSON();
        if (writes === 1) original = body;
        else expect(body).toEqual(original);
        await route.abort('failed');
    });
    const form = page.locator('#write-form');
    await form.locator('[data-write-row] summary').first().click();
    await form.getByLabel('key row 1', { exact: true }).fill('retained-key');
    await form.getByLabel('label row 1', { exact: true }).fill('original payload');
    await form.getByLabel('amount row 1 NULL', { exact: true }).uncheck();
    await form.getByLabel('amount row 1', { exact: true }).fill('9007199254740993');
    await form.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(form.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    const exported = await recoveryFile(page);
    expect(exported.toString()).not.toContain(token);
    const document = JSON.parse(exported);
    expect(document.request.operation_id).toBe(original.operation_id);
    expect(document.request.body.rows).toEqual(original.rows);
    expect(Object.hasOwn(document.request.body, 'expected_snapshot')).toBe(false);
    await page.reload();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await page.locator('.collection-list button').filter({ hasText: name }).click();
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    await page.getByLabel('Recovery JSON file').setInputFiles({
        name: 'pending.json',
        mimeType: 'application/json',
        buffer: Buffer.from(JSON.stringify(document)),
    });
    await page.getByRole('button', { name: 'Import pending request', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(form.getByText(original.operation_id, { exact: true })).toBeVisible();
    await expect(form.getByLabel('key row 1', { exact: true })).toBeDisabled();
    expect(writes).toBe(1);
    await form.getByRole('button', { name: 'Retry same operation', exact: true }).click();
    await expect.poll(() => writes).toBe(2);
    expect(errors).toEqual([]);
    expect(await page.evaluate(() => ({ ...localStorage, ...sessionStorage }))).toEqual({});
});

test('pretty recovery files above 16 MiB import and export without submitting a write', async ({
    page,
}) => {
    test.setTimeout(60000);
    await connect(page);
    const name = `large_recovery_${Date.now()}`;
    await create(page, name);
    let writes = 0;
    await page.route('**/collections/*/append', async (route) => {
        writes++;
        await route.abort('failed');
    });
    const form = page.locator('#write-form');
    await form.locator('[data-write-row] summary').first().click();
    await form.getByLabel('key row 1', { exact: true }).fill('large-recovery');
    await form.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(form.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    const document = JSON.parse(await recoveryFile(page));
    // Each value and row fit native limits. Only the pretty recovery envelope
    // exceeds the 16 MiB HTTP body limit.
    const originalRow = document.request.body.rows[0];
    document.request.body.rows = Array.from({ length: 16 }, (_, index) => ({
        ...originalRow,
        key: `large-recovery-${index}`,
        label: '',
    }));
    document.request.body.expected_snapshot = '9007199254740995';
    const base = Buffer.byteLength(
        JSON.stringify({
            ...document.request.body,
            operation_id: document.request.operation_id,
        }),
    );
    const labelBytes = Math.floor((16 * 1024 * 1024 - 1024 - base) / 16);
    expect(labelBytes).toBeLessThanOrEqual(1024 * 1024);
    for (const row of document.request.body.rows) row.label = 'a'.repeat(labelBytes);
    const buffer = Buffer.from(JSON.stringify(document, null, 2));
    expect(
        Buffer.byteLength(
            JSON.stringify({
                ...document.request.body,
                operation_id: document.request.operation_id,
            }),
        ),
    ).toBeLessThan(16 * 1024 * 1024);
    expect(buffer.length).toBeGreaterThan(16 * 1024 * 1024);
    await page.reload();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await page.locator('.collection-list button').filter({ hasText: name }).click();
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    await page.getByLabel('Recovery JSON file').setInputFiles({
        name: 'large-pending.json',
        mimeType: 'application/json',
        buffer,
    });
    await page.getByRole('button', { name: 'Import pending request', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(form.getByText(document.request.operation_id, { exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    const exported = await recoveryFile(page);
    expect(exported.length).toBeGreaterThan(16 * 1024 * 1024);
    expect(JSON.parse(exported)).toEqual(document);
    expect(exported.toString()).not.toContain(token);
    expect(writes).toBe(1);
});

test('valid unknown create envelope is exported before disconnect and retried with its original schema', async ({
    page,
}) => {
    await connect(page);
    const fixture = JSON.parse(
        await readFile(new URL('../../../fixtures/http/outcomes-v0.json', import.meta.url), 'utf8'),
    );
    let original;
    let posts = 0;
    await page.route('**/collections', async (route) => {
        if (route.request().method() !== 'POST') return route.continue();
        posts++;
        const body = route.request().postDataJSON();
        if (posts === 1) original = body;
        else expect(body).toEqual(original);
        if (posts === 3)
            return route.fulfill({
                status: 500,
                contentType: 'application/json',
                body: JSON.stringify({
                    outcome: 'definitely_not_committed',
                    error: {
                        code: 'TEST_REJECTED',
                        message: 'creation resolved as not committed',
                        retry_hint: 'FixInput',
                    },
                }),
            });
        const unknown = structuredClone(
            fixture.cases.find((item) => item.name === 'lost_create').response,
        );
        unknown.recovery.collection_id = body.collection_id;
        unknown.recovery.exact_name = body.name;
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify(unknown),
        });
    });
    await page.route('**/collections:resolve?*', (route) =>
        route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({
                error: { code: 'RECOVERY_REQUIRED', message: 'reconcile first' },
            }),
        }),
    );
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill(`pending_create_${Date.now()}`);
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.locator('#modal-error')).toContainText('Creation is unresolved');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.getByRole('button', { name: 'Disconnect', exact: true }).click();
    const exported = await recoveryFile(page);
    expect(exported.toString()).not.toContain(token);
    expect(JSON.parse(exported).request.body).toEqual(original);
    await page.getByRole('button', { name: 'Disconnect and clear', exact: true }).click();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    await page
        .getByLabel('Recovery JSON file')
        .setInputFiles({ name: 'create.json', mimeType: 'application/json', buffer: exported });
    await page.getByRole('button', { name: 'Import pending request', exact: true }).click();
    await expect(page.getByLabel('Collection name')).toHaveValue(original.name);
    await expect(page.getByLabel('Collection name')).toHaveAttribute('readonly', '');
    expect(posts).toBe(1);
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect.poll(() => posts).toBe(2);
    await expect(
        page.getByRole('button', { name: 'Create collection', exact: true }),
    ).toBeEnabled();
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.locator('#modal-error')).toHaveText('creation resolved as not committed');
    await expect(page.getByLabel('Collection name')).not.toHaveAttribute('readonly', '');
    await expect(
        page
            .getByRole('dialog')
            .getByRole('button', { name: 'Export pending request', exact: true }),
    ).toHaveCount(0);
});

test('definite non-2xx create outcome permits corrected input while committed creation survives refresh failure', async ({
    page,
}) => {
    await connect(page);
    let posts = 0;
    let created = false;
    await page.route('**/collections', async (route) => {
        if (route.request().method() === 'POST') {
            posts++;
            if (posts === 1)
                return route.fulfill({
                    status: 500,
                    contentType: 'application/json',
                    body: JSON.stringify({
                        outcome: 'definitely_not_committed',
                        error: {
                            code: 'TEST_REJECTED',
                            message: 'terminal create rejection',
                            retry_hint: 'FixInput',
                        },
                    }),
                });
            const response = await route.fetch();
            created = true;
            return route.fulfill({ response });
        }
        return route.continue();
    });
    await page.route('**/collections?page_size=*', (route) =>
        created
            ? route.fulfill({
                  status: 503,
                  contentType: 'application/json',
                  body: JSON.stringify({ error: { message: 'metadata refresh unavailable' } }),
              })
            : route.continue(),
    );
    await page.route('**/collections:resolve?*', (route) =>
        route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({ error: { message: 'cannot resolve' } }),
        }),
    );
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill(`definite_create_${Date.now()}`);
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.locator('#modal-error')).toHaveText('terminal create rejection');
    await expect(page.getByLabel('Collection name')).not.toHaveAttribute('readonly', '');
    await page.getByLabel('Collection name').fill(`committed_create_${Date.now()}`);
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(
        page.getByRole('dialog').getByText('Collection created', { exact: true }),
    ).toBeVisible();
    await expect(page.getByRole('button', { name: 'Created', exact: true })).toBeDisabled();
    await expect(page.locator('#modal-error')).toContainText('metadata refresh unavailable');
    expect(posts).toBe(2);
});
