import { test, expect } from '@playwright/test';

test.skip(!process.env.GLAMDB_NATIVE_SERVER, 'native server profile only');
test('native typed write scan and metadata fallbacks', async ({ page }) => {
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await page.goto('/');
    await page.getByLabel('Access token').fill('e2e-only-local-token');
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(
        page.getByText('No databases yet. Create your first database above.'),
    ).toBeVisible();
    await page.getByLabel('Database name').fill('browser_database');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('native_browser');
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('.breadcrumb strong')).toHaveText('native_browser');
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    const write = page.locator('#write-form');
    await write.getByLabel('key row 1', { exact: true }).fill('persisted-browser-row');
    await write.getByLabel('label row 1', { exact: true }).fill('native');
    await write.getByLabel('amount row 1 NULL', { exact: true }).uncheck();
    await write.getByLabel('amount row 1', { exact: true }).fill('9223372036854775807');
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    await page.locator('#run-query').click();
    await expect(page.getByText('persisted-browser-row', { exact: true })).toBeVisible();
    await page.locator('nav').getByRole('button', { name: 'Overview', exact: true }).click();
    await page.getByRole('button', { name: 'Count rows', exact: true }).click();
    await expect(page.locator('.metrics .metric').first().locator('strong')).toHaveText('1');
    await expect(page.getByRole('button', { name: 'Count rows', exact: true })).toBeVisible();
    await page.locator('nav').getByRole('button', { name: 'Snapshots', exact: true }).click();
    await expect(
        page.getByRole('heading', { name: 'Observed in this session', exact: true }),
    ).toBeVisible();
    await expect(
        page.getByRole('table', { name: 'Snapshot history' }).locator('tbody tr'),
    ).toHaveCount(1);
    await page.locator('nav').getByRole('button', { name: 'Storage', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Storage receipts' }).locator('tbody tr'),
    ).toHaveCount(1);
    await page.locator('nav').getByRole('button', { name: 'Explain', exact: true }).click();
    await expect(page.getByText('This server does not yet expose this capability.')).toBeVisible();
    await page.getByRole('button', { name: 'Databases', exact: true }).click();
    const row = page.getByRole('row').filter({ hasText: 'browser_database' });
    await row.getByRole('button', { name: 'Close database' }).click();
    await expect(row.getByRole('cell', { name: 'closed', exact: true })).toBeVisible();
    await row.getByRole('button', { name: 'Open database' }).click();
    await expect(page.locator('.breadcrumb strong')).toHaveText('native_browser');
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    await page.locator('#run-query').click();
    await expect(page.getByText('persisted-browser-row', { exact: true })).toBeVisible();
    let release, seen;
    const held = new Promise((resolve) => (release = resolve));
    const captured = new Promise((resolve) => (seen = resolve));
    await page.route('**/databases/*/collections/*/scan', async (route) => {
        const response = await route.fetch();
        seen();
        await held;
        try {
            await route.fulfill({ response });
        } catch {
            /* Obsolete request was aborted. */
        }
    });
    await page.locator('#run-query').click();
    await captured;
    await page.getByRole('button', { name: 'Databases', exact: true }).click();
    await page.getByLabel('Database name').fill('empty_database');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await expect(page.locator('.breadcrumb strong')).toHaveText('Your collections');
    release();
    await page.unrouteAll({ behavior: 'wait' });
    await expect(page.getByText('persisted-browser-row', { exact: true })).not.toBeVisible();
    expect(errors).toEqual([]);
});

test('the latest database selection wins when opens finish out of order', async ({ page }) => {
    const headers = { Authorization: 'Bearer e2e-only-local-token' };
    const a = await (
        await page.request.post('/databases', { headers, data: { name: 'race_a' } })
    ).json();
    const b = await (
        await page.request.post('/databases', { headers, data: { name: 'race_b' } })
    ).json();
    for (const db of [a, b])
        await page.request.post(`/databases/${db.database_id}/close`, { headers, data: {} });
    await page.goto('/');
    await page.getByLabel('Access token').fill('e2e-only-local-token');
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.getByRole('heading', { name: 'Your databases' })).toBeVisible();
    await page.evaluate(async () => {
        const { WorkbenchAPI } = await import('/assets/api.js');
        const original = WorkbenchAPI.prototype.json;
        window.openReplies = 0;
        WorkbenchAPI.prototype.json = async function (path, options) {
            try {
                return await original.call(this, path, options);
            } finally {
                if (path.endsWith('/open')) window.openReplies++;
            }
        };
    });
    let releaseA, releaseB, seenA, seenB;
    const heldA = new Promise((resolve) => (releaseA = resolve));
    const heldB = new Promise((resolve) => (releaseB = resolve));
    const capturedA = new Promise((resolve) => (seenA = resolve));
    const capturedB = new Promise((resolve) => (seenB = resolve));
    await page.route('**/databases/*/open', async (route) => {
        const response = await route.fetch();
        if (route.request().url().includes(a.database_id)) {
            seenA();
            await heldA;
        } else {
            seenB();
            await heldB;
        }
        try {
            await route.fulfill({ response });
        } catch {
            /* Obsolete selection was aborted. */
        }
    });
    try {
        await page
            .getByRole('row')
            .filter({ hasText: 'race_a' })
            .getByRole('button', { name: 'Open database' })
            .click();
        await capturedA;
        await page
            .getByRole('row')
            .filter({ hasText: 'race_b' })
            .getByRole('button', { name: 'Open database' })
            .click();
        await capturedB;
        releaseA();
        await expect.poll(() => page.evaluate(() => window.openReplies)).toBe(1);
        await expect(page.getByRole('heading', { name: 'Your databases' })).toBeVisible();
        releaseB();
        await expect(page.locator('.workspace-tag strong')).toHaveText('race_b');
        expect(a.database_id).not.toBe(b.database_id);
    } finally {
        releaseA();
        releaseB();
        await page.unrouteAll({ behavior: 'wait' });
    }
});

test('unknown append survives database switching and reconciliation with its original identity', async ({
    page,
}) => {
    await page.goto('/');
    await page.getByLabel('Access token').fill('e2e-only-local-token');
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await page.getByLabel('Database name').fill('pending_database');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('pending_collection');
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    let operationId,
        writes = 0;
    await page.route('**/databases/*/collections/*/append', async (route) => {
        writes++;
        operationId = route.request().postDataJSON().operation_id;
        await route.fetch();
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({
                // Deliberately incomplete: report the protocol failure and keep
                // the original identity available for authoritative lookup.
                outcome: 'commit_unknown',
                recovery_required: true,
                error: { code: 'OUTCOME_UNKNOWN', message: 'Receipt lost after commit' },
            }),
        });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    const dialog = page.getByRole('dialog');
    await dialog
        .getByLabel('Rows · JSON array')
        .fill('[{"key":"exactly-once","label":"retained","amount":"1"}]');
    await dialog.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(dialog.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await expect(dialog.locator('#modal-error')).toContainText('recovery context');
    await dialog.getByRole('button', { name: 'Close dialog' }).click();
    await page.getByRole('button', { name: 'Databases', exact: true }).click();
    await page.getByLabel('Database name').fill('another_pending_workspace');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'Databases', exact: true }).click();
    const row = page.getByRole('row').filter({ hasText: 'pending_database' });
    let opens = 0;
    page.on('request', (request) => {
        if (new URL(request.url()).pathname.endsWith('/open')) opens++;
    });
    await row.getByRole('button', { name: 'Use database' }).click();
    await page.locator('nav').getByRole('button', { name: 'Operations', exact: true }).click();
    await expect(page.getByText(operationId, { exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Reconcile database', exact: true }).click();
    await page.getByRole('button', { name: 'Check outcome', exact: true }).click();
    await expect(page.locator('.operations-card strong')).toHaveText('committed');
    await expect(page.getByText(operationId, { exact: true })).toBeVisible();
    expect(writes).toBe(1);
    expect(opens).toBe(0);
    await page.getByLabel('Operation ID').fill(operationId);
    await page.getByRole('button', { name: 'Look up operation', exact: true }).click();
    const lookup = page.getByRole('dialog');
    await expect(lookup.getByRole('heading', { name: 'Operation outcome' })).toBeVisible();
    await expect(lookup.locator('pre')).toContainText('"outcome": "committed"');
    await lookup.getByRole('button', { name: 'Close dialog' }).click();
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    await page.locator('#run-query').click();
    await expect(page.getByText('exactly-once', { exact: true })).toBeVisible();
});
