import { test, expect } from '@playwright/test';

const token = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';
const apiRoute = (url) => /^\/(?:collections(?:[/:]|$)|capabilities$)/.test(url.pathname);
let errors = [];
test.beforeEach(async ({ page }) => {
    errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
});
test.afterEach(() => expect(errors).toEqual([]));
async function login(page) {
    await page.goto('/');
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
}
async function nav(page, name) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
}

async function disconnect(page) {
    await page.getByRole('button', { name: 'Disconnect', exact: true }).click();
    const clear = page.getByRole('button', { name: 'Disconnect and clear', exact: true });
    if (await clear.isVisible()) await clear.click();
}

test('reconnect uses a new session without previous rows snapshot or query', async ({ page }) => {
    let secondSession = false;
    const secondRequests = [];
    await page.route(apiRoute, async (route) => {
        if (!secondSession) {
            await route.continue();
            return;
        }
        expect(route.request().headers().authorization).toBe('Bearer second-session-only-token');
        secondRequests.push(new URL(route.request().url()));
        const response = await route.fetch({
            headers: { ...route.request().headers(), authorization: `Bearer ${token}` },
        });
        if (new URL(route.request().url()).pathname === '/collections') {
            const body = await response.json();
            body.items = body.items.filter((c) => c.name === 'pink_inventory');
            body.next_cursor = null;
            const headers = response.headers();
            delete headers['content-length'];
            await route.fulfill({ response, headers, json: body });
        } else await route.fulfill({ response });
    });
    await login(page);
    await nav(page, 'Data studio');
    const oldSnapshot = await page
        .locator('#snapshot-selector option')
        .nth(2)
        .getAttribute('value');
    await page.locator('#snapshot-selector').selectOption(oldSnapshot);
    await page.getByLabel('Result limit').fill('1');
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter value 1').fill('dream-001');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(page.getByRole('cell', { name: 'dream-001', exact: true })).toBeVisible();
    await disconnect(page);
    await expect(page.getByLabel('Access token')).toHaveValue('');
    secondSession = true;
    await page.getByLabel('Access token').fill('second-session-only-token');
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
    await expect(page.locator('.breadcrumb strong')).toHaveText('pink_inventory');
    expect(
        secondRequests
            .filter((u) => u.pathname.endsWith('/stats') || u.pathname.endsWith('/files'))
            .every((u) => u.searchParams.get('snapshot_id') !== oldSnapshot),
    ).toBe(true);
    await nav(page, 'Data studio');
    await expect(page.getByRole('table', { name: 'Query results' })).toHaveCount(0);
    await expect(page.locator('[data-condition]')).toHaveCount(0);
    await expect(page.getByLabel('Result limit')).toHaveValue('1000');
    await expect(page.locator('#snapshot-selector')).toHaveValue('latest');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(page.getByRole('cell', { name: 'pink-001', exact: true })).toBeVisible();
    await expect(page.getByRole('cell', { name: 'dream-001', exact: true })).toHaveCount(0);
});

for (const change of ['collection', 'snapshot', 'connection']) {
    test(`file details response is ignored after ${change} changes`, async ({ page }) => {
        await login(page);
        await nav(page, 'Storage');
        await page.evaluate(async () => {
            const { WorkbenchAPI } = await import('/assets/api.js');
            const original = WorkbenchAPI.prototype.json;
            window.detailsFinished = 0;
            WorkbenchAPI.prototype.json = async function (path, options) {
                try {
                    return await original.call(this, path, options);
                } finally {
                    if (/\/files\//.test(path)) window.detailsFinished++;
                }
            };
        });
        let release, seen;
        const held = new Promise((resolve) => (release = resolve)),
            captured = new Promise((resolve) => (seen = resolve));
        await page.route('**/collections/*/files/*?*', async (route) => {
            const response = await route.fetch();
            seen();
            await held;
            try {
                await route.fulfill({ response });
            } catch {
                /* Aborted obsolete reads do not render. */
            }
        });
        try {
            await page.getByRole('button', { name: 'Inspect', exact: true }).first().click();
            await captured;
            if (change === 'collection') {
                await page
                    .locator('.collection-list button')
                    .filter({ hasText: 'pink_inventory' })
                    .click();
                await expect(page.locator('.breadcrumb strong')).toHaveText('pink_inventory');
            } else if (change === 'snapshot') {
                await page.locator('#snapshot-selector').selectOption('initial');
                await expect(
                    page.getByRole('table', { name: 'Data files' }).locator('tbody tr'),
                ).toHaveCount(0);
            } else {
                await disconnect(page);
                await page.getByLabel('Access token').fill(token);
                await page.getByRole('button', { name: 'Open my workspace' }).click();
                await expect(
                    page.getByRole('heading', { name: 'A little overview' }),
                ).toBeVisible();
            }
            release();
            await expect.poll(() => page.evaluate(() => window.detailsFinished)).toBe(1);
            await expect(page.getByRole('dialog')).not.toBeVisible();
        } finally {
            release();
        }
    });
}

test('a late post-create listing cannot restore the previous session', async ({ page }) => {
    let created = false,
        secondSession = false,
        release,
        seen;
    const held = new Promise((resolve) => (release = resolve));
    const captured = new Promise((resolve) => (seen = resolve));
    await page.route(apiRoute, async (route) => {
        const request = route.request();
        const collectionList = new URL(request.url()).pathname === '/collections';
        if (collectionList && request.method() === 'POST') {
            created = true;
            await route.fulfill({ json: request.postDataJSON() });
            return;
        }
        const response = await route.fetch({
            headers: { ...request.headers(), authorization: `Bearer ${token}` },
        });
        if (collectionList && created && !secondSession) {
            seen();
            await held;
        } else if (collectionList && secondSession) {
            const body = await response.json();
            body.items = body.items.filter((item) => item.name === 'pink_inventory');
            body.next_cursor = null;
            const headers = response.headers();
            delete headers['content-length'];
            await route.fulfill({ response, headers, json: body });
            return;
        }
        await route.fulfill({ response });
    });
    await login(page);
    await page.evaluate(async (oldToken) => {
        const { WorkbenchAPI } = await import('/assets/api.js');
        const original = WorkbenchAPI.prototype.json;
        window.oldListFinished = 0;
        WorkbenchAPI.prototype.json = async function (path, options) {
            try {
                return await original.call(this, path, options);
            } finally {
                if (this.token === oldToken && path.startsWith('/collections?'))
                    window.oldListFinished++;
            }
        };
    }, token);
    try {
        await page.getByRole('button', { name: 'New collection', exact: true }).click();
        await page.getByLabel('Collection name').fill('previous_session_create');
        await page.getByRole('button', { name: 'Create collection', exact: true }).click();
        await captured;
        await page.getByRole('button', { name: 'Close dialog' }).click();
        await disconnect(page);
        secondSession = true;
        await page.getByLabel('Access token').fill('second-session-only-token');
        await page.getByRole('button', { name: 'Open my workspace' }).click();
        await expect(page.locator('.breadcrumb strong')).toHaveText('pink_inventory');
        release();
        await expect.poll(() => page.evaluate(() => window.oldListFinished)).toBe(1);
        await expect(page.locator('.collection-list button')).toHaveCount(1);
        await expect(page.locator('.collection-list')).not.toContainText('dreamhouse_events');
        await expect(page.locator('.breadcrumb strong')).toHaveText('pink_inventory');
        await expect(page.getByRole('dialog')).not.toBeVisible();
        await expect(page.locator('#toast')).toBeEmpty();
    } finally {
        release();
    }
});

test('a late create rejection cannot clear the next session pending identity', async ({ page }) => {
    let first = true,
        release,
        seen;
    const held = new Promise((resolve) => (release = resolve));
    const captured = new Promise((resolve) => (seen = resolve));
    await page.route('**/collections', async (route) => {
        if (route.request().method() !== 'POST') {
            await route.continue();
            return;
        }
        if (first) {
            first = false;
            seen();
            await held;
            await route.fulfill({ status: 409, json: { error: { message: 'Name conflict' } } });
        } else {
            await route.fulfill({ status: 503, json: { error: { message: 'Response lost' } } });
        }
    });
    await login(page);
    await page.evaluate(async () => {
        const { WorkbenchAPI } = await import('/assets/api.js');
        const original = WorkbenchAPI.prototype.json;
        window.oldCreateFinished = 0;
        WorkbenchAPI.prototype.json = async function (path, options) {
            try {
                return await original.call(this, path, options);
            } finally {
                if (options?.body?.name === 'previous_session_create') window.oldCreateFinished++;
            }
        };
    });
    try {
        await page.getByRole('button', { name: 'New collection', exact: true }).click();
        await page.getByLabel('Collection name').fill('previous_session_create');
        await page.getByRole('button', { name: 'Create collection', exact: true }).click();
        await captured;
        await page.getByRole('button', { name: 'Close dialog' }).click();
        await disconnect(page);
        await page.getByLabel('Access token').fill(token);
        await page.getByRole('button', { name: 'Open my workspace' }).click();
        await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
        await page.getByRole('button', { name: 'New collection', exact: true }).click();
        await page.getByLabel('Collection name').fill('next_session_pending');
        await page.getByRole('button', { name: 'Create collection', exact: true }).click();
        await expect(page.locator('#modal-error')).toContainText('Creation is unresolved');
        await page.getByRole('button', { name: 'Close dialog' }).click();
        release();
        await expect.poll(() => page.evaluate(() => window.oldCreateFinished)).toBe(1);
        await page.getByRole('button', { name: 'New collection', exact: true }).click();
        await expect(page.getByLabel('Collection name')).toHaveValue('next_session_pending');
        await expect(page.getByLabel('Collection name')).toHaveAttribute('readonly', '');
    } finally {
        release();
    }
});

test('disconnect cancels the previous session Arrow export without a download', async ({
    page,
}) => {
    const downloads = [];
    page.on('download', (download) => downloads.push(download.suggestedFilename()));
    await login(page);
    await nav(page, 'Data studio');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Query results' })).toBeVisible();
    await page.evaluate(async () => {
        const { WorkbenchAPI } = await import('/assets/api.js');
        const original = WorkbenchAPI.prototype.arrow;
        window.exportAborted = false;
        WorkbenchAPI.prototype.arrow = async function (...args) {
            try {
                return await original.apply(this, args);
            } catch (error) {
                window.exportAborted = error.name === 'AbortError';
                throw error;
            }
        };
    });
    let release, seen;
    const held = new Promise((resolve) => (release = resolve));
    const captured = new Promise((resolve) => (seen = resolve));
    await page.route('**/collections/*/scan', async (route) => {
        const response = await route.fetch();
        seen();
        await held;
        try {
            await route.fulfill({ response });
        } catch {
            /* The export belongs to the disconnected session. */
        }
    });
    try {
        await page.getByRole('button', { name: 'Arrow', exact: true }).click();
        await captured;
        await disconnect(page);
        await expect(page.getByLabel('Access token')).toHaveValue('');
        await expect.poll(() => page.evaluate(() => window.exportAborted)).toBe(true);
        release();
        expect(downloads).toEqual([]);
        await expect(page.locator('#toast')).toBeEmpty();
    } finally {
        release();
    }
});

test('an old operation error cannot show private text after disconnect', async ({ page }) => {
    await page.route('**/collections/*/append', (route) =>
        route.fulfill({
            status: 503,
            json: { outcome: 'commit_unknown', recovery_required: true },
        }),
    );
    await login(page);
    await nav(page, 'Data studio');
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(page.locator('#append-outcome')).toContainText('Outcome unresolved');
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await nav(page, 'Operations');
    await page.evaluate(async () => {
        const { WorkbenchAPI } = await import('/assets/api.js');
        const original = WorkbenchAPI.prototype.json;
        window.operationFinished = 0;
        WorkbenchAPI.prototype.json = async function (path, options) {
            try {
                return await original.call(this, path, options);
            } finally {
                if (path.includes('/operations/')) window.operationFinished++;
            }
        };
    });
    let release, seen;
    const held = new Promise((resolve) => (release = resolve));
    const captured = new Promise((resolve) => (seen = resolve));
    await page.route('**/collections/*/operations/*', async (route) => {
        seen();
        await held;
        try {
            await route.fulfill({
                status: 503,
                json: { error: { message: 'previous-account-private-operation' } },
            });
        } catch {
            /* The disconnected request may already be aborted. */
        }
    });
    try {
        await page.getByRole('button', { name: 'Check outcome', exact: true }).click();
        await captured;
        await disconnect(page);
        release();
        await expect.poll(() => page.evaluate(() => window.operationFinished)).toBe(1);
        await expect(page.locator('#toast')).toBeEmpty();
        await expect(page.getByLabel('Access token')).toHaveValue('');
    } finally {
        release();
    }
});
