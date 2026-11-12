import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';

const token = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';

test.afterEach(async ({ request }) => {
    // The disposable server is shared; release engine reservations between scenarios.
    const headers = { Authorization: `Bearer ${token}` };
    const response = await request.get('/databases', { headers });
    expect(response.ok()).toBe(true);
    const { items } = await response.json();
    for (const database of items) {
        if (database.state !== 'open') continue;
        const closed = await request.post(`/databases/${database.database_id}/close`, {
            headers,
            data: {},
        });
        expect(closed.ok()).toBe(true);
        expect((await closed.json()).state).toBe('closed');
    }
});
async function connect(page) {
    await page.goto('/');
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.getByRole('heading', { name: 'Your databases' })).toBeVisible();
}
async function useDatabase(page, name) {
    await page
        .getByRole('row')
        .filter({ hasText: name })
        .getByRole('button', { name: 'Use database', exact: true })
        .click();
    await expect(page.locator('.workspace-tag strong')).toHaveText(name);
}
async function navigate(page, name) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
}

test('native typed write and every metadata view survive browser reload', async ({
    page,
}, info) => {
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    const capabilities = await (
        await page.request.get('/capabilities', {
            headers: { Authorization: `Bearer ${token}` },
        })
    ).json();
    expect(capabilities.backend).toBe('local');
    expect(capabilities.mock).toBe(false);
    for (const feature of ['stats', 'snapshots', 'files', 'explain'])
        expect(capabilities.features[feature]).toBe(true);
    await connect(page);
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
    await navigate(page, 'Data studio');
    await expect(page.getByLabel('Result limit')).toHaveValue('1000');
    await expect(page.getByLabel('Result limit')).toHaveAttribute('max', '10000');
    await page.getByLabel('Result limit').fill('1010');
    const write = page.locator('#write-form');
    await write.locator('[data-write-row] summary').first().click();
    await write.getByLabel('key row 1', { exact: true }).fill('persisted-browser-row');
    await write.getByLabel('label row 1', { exact: true }).fill('native');
    await write.getByLabel('amount row 1 NULL', { exact: true }).uncheck();
    await write.getByLabel('amount row 1', { exact: true }).fill('9223372036854775807');
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    const operationId = await write.locator('#write-outcome code').textContent();
    await expect(page.locator('#snapshot-selector option')).toHaveCount(3);
    const snapshotA = await page.locator('#snapshot-selector option').last().getAttribute('value');
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page
        .getByRole('dialog')
        .getByLabel('Rows · JSON array')
        .fill('[{"key":"second-browser-row","label":"B","amount":"2"}]');
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(
        page.getByRole('dialog').getByText('Append committed', { exact: true }),
    ).toBeVisible();
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.locator('#run-query').click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(2);
    await expect(page.getByText('persisted-browser-row', { exact: true })).toBeVisible();
    await navigate(page, 'Overview');
    await expect(page.locator('.metrics .metric').first().locator('strong')).toHaveText('2');
    await expect(page.getByRole('button', { name: 'Count rows', exact: true })).toHaveCount(0);
    await navigate(page, 'Snapshots');
    await expect(
        page.getByRole('heading', { name: 'Snapshot history', exact: true }),
    ).toBeVisible();
    await expect(
        page.getByRole('table', { name: 'Snapshot history' }).locator('tbody tr'),
    ).toHaveCount(2);
    await navigate(page, 'Storage');
    await expect(page.getByRole('table', { name: 'Data files' }).locator('tbody tr')).toHaveCount(
        2,
    );
    await page.getByRole('button', { name: 'Inspect', exact: true }).first().click();
    const details = page.getByRole('dialog');
    await expect(
        details.getByRole('table', { name: 'Parquet row groups' }).locator('tbody tr'),
    ).toHaveCount(1);
    await expect(
        details.getByRole('table', { name: 'Parquet field statistics' }).locator('tbody tr'),
    ).toHaveCount(3);
    await expect(details.locator('.file-summary')).toContainText('Footer bytes');
    await expect(
        details
            .getByRole('table', { name: 'Parquet field statistics' })
            .getByRole('row')
            .filter({ hasText: 'amount' }),
    ).toContainText('int64');
    await expect(details.locator('details.raw-metadata')).not.toHaveAttribute('open', '');
    await expect(details.locator('pre')).toContainText('"synthetic": false');
    await expect(details.locator('pre')).toContainText('"row_groups": [');
    await expect(details.locator('pre')).toContainText('"physical_file_exists": true');
    const lower = await details
        .getByRole('table', { name: 'Parquet field statistics' })
        .getByRole('row')
        .filter({ hasText: 'amount' })
        .getByRole('cell')
        .nth(4)
        .locator('code')
        .textContent();
    expect(['2', '9223372036854775807']).toContain(lower);
    await page.screenshot({ path: info.outputPath('native-file-details-desktop.png') });
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(details.locator('.file-summary')).toBeInViewport({ ratio: 1 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
        true,
    );
    await page.screenshot({ path: info.outputPath('native-file-details-mobile.png') });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await details.getByRole('button', { name: 'Close dialog' }).click();
    await navigate(page, 'Explain');
    await page.getByRole('button', { name: 'Refresh plan', exact: true }).click();
    const plan = page.locator('.panel').filter({
        has: page.getByRole('heading', { name: 'Query plan', exact: true }),
    });
    await expect(plan.getByText('Native scan', { exact: true })).toBeVisible();
    await expect(page.locator('.mock-banner')).toHaveCount(0);
    await page.reload();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await useDatabase(page, 'browser_database');
    await expect(page.locator('.metrics .metric').first().locator('strong')).toHaveText('2');
    await navigate(page, 'Snapshots');
    await expect(
        page.getByRole('table', { name: 'Snapshot history' }).locator('tbody tr'),
    ).toHaveCount(2);
    await navigate(page, 'Storage');
    await expect(page.getByRole('table', { name: 'Data files' }).locator('tbody tr')).toHaveCount(
        2,
    );
    await page.locator('#snapshot-selector').selectOption(snapshotA);
    await expect(page.getByRole('table', { name: 'Data files' }).locator('tbody tr')).toHaveCount(
        1,
    );
    await navigate(page, 'Overview');
    await expect(page.locator('.metrics .metric').first().locator('strong')).toHaveText('1');
    await page.locator('#snapshot-selector').selectOption('initial');
    await expect(page.locator('.metrics .metric').first().locator('strong')).toHaveText('0');
    await navigate(page, 'Storage');
    await expect(page.getByRole('table', { name: 'Data files' }).locator('tbody tr')).toHaveCount(
        0,
    );
    await page.locator('#snapshot-selector').selectOption('latest');
    await expect(page.getByRole('table', { name: 'Data files' }).locator('tbody tr')).toHaveCount(
        2,
    );
    await navigate(page, 'Schema');
    await expect(
        page.getByRole('table', { name: 'Collection schema' }).locator('tbody tr'),
    ).toHaveCount(3);
    await navigate(page, 'Operations');
    await expect(
        page.getByRole('table', { name: 'Published snapshot operations' }).locator('tbody tr'),
    ).toHaveCount(2);
    await expect(page.getByRole('table', { name: 'Published snapshot operations' })).toContainText(
        operationId,
    );
    await page.getByRole('button', { name: 'Look up outcome', exact: true }).first().click();
    await expect(page.getByRole('dialog').locator('pre')).toContainText('"outcome": "committed"');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await expect(
        page.getByText('No appends submitted in this browser session.', { exact: false }),
    ).toBeVisible();
    await page.getByLabel('Operation ID').fill(operationId);
    await page.getByRole('button', { name: 'Look up operation', exact: true }).click();
    await expect(page.getByRole('dialog').locator('pre')).toContainText('"outcome": "committed"');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.getByRole('button', { name: 'Databases', exact: true }).click();
    const row = page.getByRole('row').filter({ hasText: 'browser_database' });
    await row.getByRole('button', { name: 'Close database' }).click();
    await expect(row.getByRole('cell', { name: 'closed', exact: true })).toBeVisible();
    await row.getByRole('button', { name: 'Open database' }).click();
    await expect(page.locator('.breadcrumb strong')).toHaveText('native_browser');
    await navigate(page, 'Data studio');
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
    const headers = { Authorization: `Bearer ${token}` };
    const a = await (
        await page.request.post('/databases', { headers, data: { name: 'race_a' } })
    ).json();
    const b = await (
        await page.request.post('/databases', { headers, data: { name: 'race_b' } })
    ).json();
    for (const db of [a, b])
        await page.request.post(`/databases/${db.database_id}/close`, { headers, data: {} });
    await page.goto('/');
    await page.getByLabel('Access token').fill(token);
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
    await page.getByLabel('Access token').fill(token);
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
    await page.route('**/databases', (route) =>
        route.fulfill({
            status: 503,
            json: { error: { code: 'SERVER_BUSY', message: 'Temporary database list failure' } },
        }),
    );
    await page.getByRole('button', { name: 'Databases', exact: true }).click();
    await expect(page.getByText('Temporary database list failure')).toBeVisible();
    await expect(
        page.getByRole('button', { name: 'Refresh databases', exact: true }),
    ).toBeVisible();
    await expect(page.getByLabel('Access token')).toHaveCount(0);
    await page.unroute('**/databases');
    await page.getByRole('button', { name: 'Refresh databases', exact: true }).click();

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
    await expect(
        page.locator('.operations-card').getByText(operationId, { exact: true }),
    ).toBeVisible();
    await page.getByRole('button', { name: 'Reconcile database', exact: true }).click();
    await expect(page.locator('#toast')).toContainText('Database reconciled.');
    await page.getByRole('button', { name: 'Check outcome', exact: true }).click();
    await expect(page.locator('.operations-card strong')).toHaveText('committed');
    await expect(
        page.locator('.operations-card').getByText(operationId, { exact: true }),
    ).toBeVisible();
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

test('native lost receipt exports and imports after reload without changing the frozen request', async ({
    page,
}) => {
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await connect(page);
    await page.getByLabel('Database name').fill('reload_pending_database');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('reload_pending_collection');
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    const bodies = [];
    page.on('request', (request) => {
        if (new URL(request.url()).pathname.endsWith('/append'))
            bodies.push(request.postDataJSON());
    });
    await page.route('**/databases/*/collections/*/append', async (route) => {
        await route.fetch();
        await route.abort('failed');
    });
    const write = page.locator('#write-form');
    await write.locator('[data-write-row] summary').first().click();
    await write.getByLabel('key row 1', { exact: true }).fill('native-recovery-row');
    await write.getByLabel('label row 1', { exact: true }).fill('original payload');
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    const downloaded = page.waitForEvent('download');
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Export pending request', exact: true })
        .click();
    const recovery = await readFile(await (await downloaded).path());
    const exported = JSON.parse(recovery);
    expect(recovery.toString()).not.toContain(token);
    expect(exported.scope.backend).toBe('local');
    expect(exported.scope.mock).toBe(false);
    expect(exported.scope.store_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(exported.request.operation_id).toBe(bodies[0].operation_id);
    await page.unrouteAll({ behavior: 'wait' });
    await page.reload();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await useDatabase(page, 'reload_pending_database');
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    await page.getByLabel('Recovery JSON file').setInputFiles({
        name: 'native-pending.json',
        mimeType: 'application/json',
        buffer: recovery,
    });
    await page.getByRole('button', { name: 'Import pending request', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(write.locator('#write-outcome code')).toHaveText(bodies[0].operation_id);
    expect(bodies).toHaveLength(1);
    await write.getByRole('button', { name: 'Retry same operation', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    expect(bodies).toHaveLength(2);
    expect(bodies[1]).toEqual(bodies[0]);
    await navigate(page, 'Overview');
    await expect(page.locator('.metrics .metric').first().locator('strong')).toHaveText('1');
    await navigate(page, 'Storage');
    await expect(page.getByRole('table', { name: 'Data files' }).locator('tbody tr')).toHaveCount(
        1,
    );
    await navigate(page, 'Snapshots');
    await expect(
        page.getByRole('table', { name: 'Snapshot history' }).locator('tbody tr'),
    ).toHaveCount(1);
    expect(errors).toEqual([]);
});

test('native creation resolution clears cached pending requests after a database switch', async ({
    page,
}) => {
    await connect(page);
    await page.getByLabel('Database name').fill('create_resolution_database');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    let originalId;
    await page.route('**/databases/*/collections', async (route) => {
        if (route.request().method() !== 'POST') return route.continue();
        originalId = route.request().postDataJSON().collection_id;
        await route.fetch();
        await route.abort('failed');
    });
    await page.route('**/databases/*/collections:resolve?*', (route) =>
        route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({ error: { message: 'resolution temporarily unavailable' } }),
        }),
    );
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('resolved_collection');
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.locator('#modal-error')).toContainText('Creation is unresolved');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.getByRole('button', { name: 'Databases', exact: true }).click();
    await useDatabase(page, 'create_resolution_database');
    await page.unrouteAll({ behavior: 'wait' });
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await expect(page.getByLabel('Collection name')).toHaveValue('resolved_collection');
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('.collection-list button')).toHaveCount(1);
    await expect(page.locator('.collection-list button')).toHaveAttribute('data-id', originalId);
    await page.getByRole('button', { name: 'Disconnect', exact: true }).click();
    await expect(page.getByLabel('Access token')).toBeVisible();
    await expect(page.getByRole('dialog')).not.toBeVisible();
});

test('native decimal and date creation, exact write and typed filters survive reload', async ({
    page,
}) => {
    await connect(page);
    await page.getByLabel('Database name').fill('scalar_browser_database');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    const dialog = page.getByRole('dialog');
    await dialog.getByLabel('Collection name').fill('scalar_browser');
    await dialog.getByLabel('Column name 2', { exact: true }).fill('day');
    await dialog.getByLabel('Column type 2', { exact: true }).selectOption('date');
    await dialog.getByLabel('Column type 3', { exact: true }).selectOption('decimal');
    await dialog.getByLabel('Decimal precision 3').fill('38');
    await dialog.getByLabel('Decimal scale 3').fill('4');
    const createRequest = page.waitForRequest(
        (request) =>
            request.method() === 'POST' && new URL(request.url()).pathname.endsWith('/collections'),
    );
    await dialog.getByRole('button', { name: 'Create collection', exact: true }).click();
    expect((await createRequest).postDataJSON().schema.fields[2]).toEqual({
        name: 'amount',
        type: 'decimal',
        nullable: true,
        precision: 38,
        scale: 4,
    });
    await expect(dialog).not.toBeVisible();
    await navigate(page, 'Data studio');
    const write = page.locator('#write-form');
    await write.locator('[data-write-row] summary').first().click();
    const amount = '9999999999999999999999999999999999.9999';
    await write.getByLabel('key row 1', { exact: true }).fill('exact-money');
    await write.getByLabel('day row 1', { exact: true }).fill('0001-01-01');
    await write.getByLabel('amount row 1 NULL', { exact: true }).uncheck();
    await write.getByLabel('amount row 1', { exact: true }).fill(amount);
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    await page.locator('#run-query').click();
    const results = page.getByRole('table', { name: 'Query results' });
    await expect(results).toContainText(amount);
    await expect(results).toContainText('0001-01-01');
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter column 1').selectOption('amount');
    await page.getByLabel('Filter value 1').fill(amount);
    const scanRequest = page.waitForRequest(
        (request) =>
            request.method() === 'POST' && new URL(request.url()).pathname.endsWith('/scan/open'),
    );
    await page.locator('#run-query').click();
    expect((await scanRequest).postDataJSON().predicate.value).toEqual({
        type: 'decimal',
        precision: 38,
        scale: 4,
        value: amount,
    });
    await expect(results.locator('tbody tr')).toHaveCount(1);
    await page.reload();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await useDatabase(page, 'scalar_browser_database');
    await navigate(page, 'Data studio');
    await page.locator('#run-query').click();
    await expect(results).toContainText(amount);
    await expect(results).toContainText('0001-01-01');
    await navigate(page, 'Schema');
    await expect(
        page
            .getByRole('table', { name: 'Collection schema' })
            .getByRole('row')
            .filter({ hasText: 'amount' }),
    ).toContainText('decimal(38,4)');
});

test('native KV controls replace full rows, preserve misses and omit no-op snapshots', async ({
    page,
}) => {
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await connect(page);
    await page.getByLabel('Database name').fill('browser_kv_database');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('browser_keys');
    await page.getByLabel('Collection mode', { exact: true }).selectOption('kv');
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('.page-footer')).toContainText('KV mode');
    await navigate(page, 'Data studio');
    await expect(
        page.locator('#write-form').getByRole('button', { name: 'Upsert rows', exact: true }),
    ).toBeVisible();
    const batch = async (rows) => {
        await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
        const dialog = page.getByRole('dialog');
        await dialog.getByLabel('Rows · JSON array', { exact: true }).fill(JSON.stringify(rows));
        await dialog.getByRole('button', { name: 'Upsert rows', exact: true }).click();
        await expect(dialog.getByText('Upsert committed', { exact: true })).toBeVisible();
        await page.getByRole('button', { name: 'Close dialog' }).click();
    };
    await batch([
        { key: 'same', label: 'first', amount: '1' },
        { key: 'same', label: 'last', amount: '9007199254740993' },
    ]);
    await expect(page.locator('#snapshot-selector option')).toHaveCount(3);
    const snapshot = await page.locator('#snapshot-selector option').last().getAttribute('value');
    await page.getByRole('button', { name: 'Lookup keys', exact: true }).click();
    const lookup = page.getByRole('dialog');
    await lookup.getByLabel('Keys · JSON array', { exact: true }).fill('["same","missing","same"]');
    await lookup.getByRole('button', { name: 'Lookup keys', exact: true }).click();
    await expect(lookup.locator('#lookup-result')).toContainText('9007199254740993');
    const result = JSON.parse(await lookup.locator('#lookup-result').textContent());
    expect(result.rows.map((row) => row?.key ?? null)).toEqual(['same', null, 'same']);
    expect(result.rows[0].label).toBe('last');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.getByRole('button', { name: 'Delete keys', exact: true }).click();
    const deletion = page.getByRole('dialog');
    await deletion.getByLabel('Keys · JSON array', { exact: true }).fill('["missing","missing"]');
    await deletion.getByRole('button', { name: 'Delete keys', exact: true }).click();
    await expect(deletion.getByText('Delete committed', { exact: true })).toBeVisible();
    await expect(deletion.locator('#append-outcome')).toContainText('No new snapshot');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await expect(page.locator('#snapshot-selector option')).toHaveCount(3);
    await page.getByRole('button', { name: 'Compact files', exact: true }).click();
    const compact = page.getByRole('dialog');
    await compact.getByRole('button', { name: 'Compact files', exact: true }).click();
    await expect(compact.getByText('Compaction committed', { exact: true })).toBeVisible();
    await expect(compact.locator('#append-outcome')).toContainText('No new snapshot');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await batch([{ key: 'same', label: 'replacement' }]);
    await page.locator('#run-query').click();
    const current = page.getByRole('table', { name: 'Query results' });
    await expect(current.locator('tbody tr')).toHaveCount(1);
    await expect(current).toContainText('replacement');
    await expect(current).toContainText('NULL');
    await page.getByLabel('Snapshot', { exact: true }).selectOption(snapshot);
    await page.locator('#run-query').click();
    await expect(page.getByRole('table', { name: 'Query results' })).toContainText('last');
    await navigate(page, 'Snapshots');
    await page.getByLabel('Snapshot', { exact: true }).selectOption('latest');
    await expect(
        page.getByRole('table', { name: 'Snapshot history' }).locator('tbody tr'),
    ).toHaveCount(2);
    await expect(page.getByRole('table', { name: 'Snapshot history' })).toContainText('written /');
    expect(errors).toEqual([]);
});

test('native KV no-op receipt exports, imports and retries after reload without a new snapshot', async ({
    page,
}) => {
    await connect(page);
    await page.getByLabel('Database name').fill('browser_kv_recovery');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('recover_keys');
    await page.getByLabel('Collection mode', { exact: true }).selectOption('kv');
    await page.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    const bodies = [];
    page.on('request', (request) => {
        if (new URL(request.url()).pathname.endsWith('/delete'))
            bodies.push(request.postDataJSON());
    });
    await page.route('**/databases/*/collections/*/delete', async (route) => {
        const response = await route.fetch();
        const outcome = await response.json();
        expect(outcome.result.snapshot_created).toBe(false);
        await route.abort('failed');
    });
    await page.getByRole('button', { name: 'Delete keys', exact: true }).click();
    const dialog = page.getByRole('dialog');
    await dialog.getByLabel('Keys · JSON array', { exact: true }).fill('["absent","absent"]');
    await dialog.getByRole('button', { name: 'Delete keys', exact: true }).click();
    await expect(dialog.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    const download = page.waitForEvent('download');
    await dialog.getByRole('button', { name: 'Export pending request', exact: true }).click();
    const recovery = await readFile(await (await download).path());
    const exported = JSON.parse(recovery);
    expect(exported.request.action).toBe('delete');
    expect(exported.request.body.keys).toEqual(['absent', 'absent']);
    expect(exported.request.operation_id).toBe(bodies[0].operation_id);
    expect(recovery.toString()).not.toContain(token);
    await page.unrouteAll({ behavior: 'wait' });
    await page.reload();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await useDatabase(page, 'browser_kv_recovery');
    await page.getByRole('button', { name: 'Pending requests', exact: true }).click();
    await page
        .getByLabel('Recovery JSON file')
        .setInputFiles({ name: 'kv-pending.json', mimeType: 'application/json', buffer: recovery });
    await page.getByRole('button', { name: 'Import pending request', exact: true }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    expect(bodies).toHaveLength(1);
    await page.getByRole('button', { name: 'Review or retry same operation', exact: true }).click();
    await dialog.getByRole('button', { name: 'Retry same operation', exact: true }).click();
    await expect(dialog.getByText('Delete committed', { exact: true })).toBeVisible();
    await expect(dialog.locator('#append-outcome')).toContainText('No new snapshot');
    expect(bodies).toHaveLength(2);
    expect(bodies[1]).toEqual(bodies[0]);
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await navigate(page, 'Snapshots');
    await expect(
        page.getByRole('table', { name: 'Snapshot history' }).locator('tbody tr'),
    ).toHaveCount(0);
    await navigate(page, 'Operations');
    await expect(page.locator('.operations-card')).toContainText('No new snapshot');
    await expect(
        page.getByRole('table', { name: 'Published snapshot operations' }).locator('tbody tr'),
    ).toHaveCount(0);
});

test('native snapshot selection keeps the same read and write forms and their drafts', async ({
    page,
}, info) => {
    await connect(page);
    await page.getByLabel('Database name').fill('compact_native');
    await page.getByRole('button', { name: 'Create database', exact: true }).click();
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    await page.getByLabel('Collection name').fill('compact_rows');
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await navigate(page, 'Data studio');
    const write = page.locator('#write-form');
    await write.locator('[data-write-row] summary').first().click();
    await write.getByLabel('key row 1', { exact: true }).fill('saved-row');
    await write.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(write.getByText('Append committed', { exact: true })).toBeVisible();
    const snapshot = await page.locator('#snapshot-selector option').last().getAttribute('value');
    await write.getByRole('button', { name: 'New rows', exact: true }).click();
    await write.locator('[data-write-row] summary').first().click();
    await write.getByLabel('key row 1', { exact: true }).fill('unsent-draft');
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter value 1').fill('saved-row');
    await page.getByLabel('Result limit').fill('7');
    await page.locator('#query-form').evaluate((node) => (globalThis.originalReadForm = node));
    await write.evaluate((node) => (globalThis.originalWriteForm = node));
    for (const [selected, count] of [
        ['initial', 0],
        [snapshot, 1],
        ['latest', 1],
    ]) {
        await page.getByLabel('Snapshot', { exact: true }).selectOption(selected);
        await expect
            .poll(() =>
                page
                    .locator('#query-form')
                    .evaluate((node) => node === globalThis.originalReadForm),
            )
            .toBe(true);
        await expect
            .poll(() => write.evaluate((node) => node === globalThis.originalWriteForm))
            .toBe(true);
        await expect(page.getByLabel('Result limit')).toHaveValue('7');
        await expect(page.getByLabel('Filter value 1')).toHaveValue('saved-row');
        await expect(write.getByLabel('key row 1', { exact: true })).toHaveValue('unsent-draft');
        await page.locator('#run-query').click();
        await expect(page.locator('.read-panel .results-panel')).toBeVisible();
        await expect(page.locator('.read-panel tbody tr')).toHaveCount(count);
        await expect(page.locator('.mock-banner')).toHaveCount(0);
    }
    await expect(write.getByLabel('key row 1', { exact: true })).toHaveValue('unsent-draft');
    await page.getByRole('button', { name: 'Switch to dark theme' }).click();
    await page.getByRole('button', { name: 'Collapse sidebar' }).click();
    await expect(page.locator('.sidebar')).toHaveCSS('width', '64px');
    await page.screenshot({
        path: info.outputPath('native-compact-data-dark.png'),
        fullPage: true,
    });
});
