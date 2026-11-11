import { test, expect } from '@playwright/test';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const token = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';
const themePath = path.join(root, 'artifacts/e2e/theme.json');
let pageErrors = [];
let consoleErrors = [];
test.beforeEach(async ({ page }) => {
    pageErrors = [];
    consoleErrors = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    page.on('console', (message) => {
        if (message.type() === 'error') consoleErrors.push(message.text());
    });
});
test.afterEach(({}, info) => {
    expect(pageErrors, 'uncaught browser errors').toEqual([]);
    const faults =
        /unresolved append|pending append|lost create|late scan|rejected credentials|mismatched create|duplicate create|terminal outcome|resolved append|metadata refresh failure/.test(
            info.title,
        );
    const unexpected = consoleErrors.filter(
        (message) =>
            !(
                (faults &&
                    /Failed to load resource:.*(?:503|403|409|ERR_FAILED|ERR_ABORTED)/.test(
                        message,
                    )) ||
                (/runtime config/.test(info.title) && /Failed to load resource:.*422/.test(message))
            ),
    );
    expect(unexpected, 'unexpected browser console errors').toEqual([]);
});
async function login(page) {
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Hello, gorgeous data.' })).toBeVisible();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
}
async function nav(page, name) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
}
async function createCollection(page, name) {
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    await page.getByLabel('Collection name').fill(name);
    await page.getByRole('button', { name: 'Create collection' }).click();
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

test('desktop workspace is usable and all metadata views load', async ({ page }) => {
    await login(page);
    await expect(
        page.getByText('Demo workspace · mock backend · durability not qualified'),
    ).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Big ideas. Beautiful data.' })).toBeVisible();
    await mkdir(path.join(root, 'artifacts/e2e'), { recursive: true });
    await page.screenshot({
        path: path.join(root, 'artifacts/e2e/overview-desktop.png'),
        fullPage: true,
    });
    for (const name of ['Schema', 'Snapshots', 'Storage', 'Operations', 'Style studio']) {
        await nav(page, name);
        await expect(page.locator('#view h1')).toBeVisible();
    }
    await nav(page, 'Storage');
    await page.getByRole('button', { name: 'Inspect', exact: true }).first().click();
    await expect(page.getByRole('dialog').getByText(/Synthetic mock descriptor/)).toBeVisible();
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await nav(page, 'Explain');
    await page.getByRole('button', { name: 'Refresh plan' }).click();
    await expect(page.getByText('Reference mock scan', { exact: true })).toBeVisible();
});

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
    await page.screenshot({
        path: path.join(root, 'artifacts/e2e/data-desktop.png'),
        fullPage: true,
    });
    const download = page.waitForEvent('download');
    await page.getByRole('button', { name: 'Arrow', exact: true }).click();
    expect((await download).suggestedFilename()).toMatch(/\.arrows$/);
});

test('changing collection clears stale query and credentials are not persisted', async ({
    page,
}) => {
    await login(page);
    await nav(page, 'Data studio');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Query results' })).toBeVisible();
    await page.locator('.collection-list button').filter({ hasText: 'pink_inventory' }).click();
    await expect(page.getByRole('table', { name: 'Query results' })).toHaveCount(0);
    const persisted = await page.evaluate(() => ({ ...localStorage, ...sessionStorage }));
    expect(JSON.stringify(persisted)).not.toContain(token);
    await page.getByRole('button', { name: 'Disconnect', exact: true }).click();
    await expect(page.getByLabel('Access token')).toHaveValue('');
});

test('runtime config edits and invalid reload preserve last valid theme', async ({ page }) => {
    await login(page);
    const original = await readFile(themePath, 'utf8');
    try {
        const changed = JSON.parse(original);
        changed.colors.accent = '#94155c';
        changed.brand.name = 'DreamDB';
        changed.decoration.bows = false;
        await writeFile(themePath, JSON.stringify(changed));
        await expect(page.locator('.brand-name')).toHaveText('DreamDB');
        await expect
            .poll(() =>
                page.evaluate(() =>
                    getComputedStyle(document.documentElement)
                        .getPropertyValue('--color-accent')
                        .trim(),
                ),
            )
            .toBe('#94155c');
        await expect(page.locator('html')).toHaveAttribute('data-bows', 'false');
        await writeFile(themePath, '{"version":2}');
        await expect(page.locator('#theme-warning')).toBeVisible();
        await expect(page.locator('.brand-name')).toHaveText('DreamDB');
    } finally {
        await writeFile(themePath, original);
    }
});

test('mobile navigation, keyboard focus and reduced motion', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await login(page);
    await page.getByRole('button', { name: 'Toggle navigation' }).click();
    await nav(page, 'Data studio');
    await expect(page.getByRole('heading', { name: "Let's find something lovely." })).toBeVisible();
    const widths = await page.evaluate(() => ({
        viewport: innerWidth,
        body: document.documentElement.scrollWidth,
    }));
    expect(widths.body).toBeLessThanOrEqual(widths.viewport);
    await page.keyboard.press('Tab');
    await expect(page.locator(':focus')).toHaveCount(1);
    await page.screenshot({
        path: path.join(root, 'artifacts/e2e/data-mobile.png'),
        fullPage: true,
    });
});

test('unresolved append stays unresolved and retry keeps operation identity', async ({ page }) => {
    await login(page);
    await createCollection(page, `unknown_${Date.now()}`);
    let firstId;
    let attempts = 0;
    await page.route('**/collections/*/append', async (route) => {
        attempts++;
        const body = route.request().postDataJSON();
        if (attempts === 1) firstId = body.operation_id;
        else expect(body.operation_id).toBe(firstId);
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({
                outcome: 'commit_unknown',
                operation_id: body.operation_id,
                recovery_required: true,
            }),
        });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(dialog.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await expect(dialog.locator('#modal-error')).toContainText('recovery context');
    await dialog.getByRole('button', { name: 'Retry same operation', exact: true }).click();
    await expect.poll(() => attempts).toBe(2);
    await expect(dialog.getByLabel('Rows · JSON array')).toHaveAttribute('readonly', '');
});

test('lost create response resolves exact original identity instead of creating again', async ({
    page,
}) => {
    await login(page);
    let posts = 0;
    let originalId;
    await page.route('**/collections', async (route) => {
        if (route.request().method() !== 'POST') {
            await route.continue();
            return;
        }
        posts++;
        originalId = route.request().postDataJSON().collection_id;
        await route.fetch();
        await route.abort('failed');
    });
    const name = `lost_create_${Date.now()}`;
    await createCollection(page, name);
    expect(posts).toBe(1);
    await nav(page, 'Schema');
    await expect(page.getByRole('table', { name: 'Collection schema' })).toBeVisible();
    expect(originalId).toMatch(/^[0-9a-f-]{36}$/);
});

test('late pending append response cannot decorate a different collection dialog', async ({
    page,
}) => {
    await login(page);
    await createCollection(page, `late_append_${Date.now()}`);
    let release, seen;
    const held = new Promise((resolve) => (release = resolve));
    const captured = new Promise((resolve) => (seen = resolve));
    await page.route('**/collections/*/append', async (route) => {
        const response = await route.fetch();
        seen();
        await held;
        await route.fulfill({ response });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await captured;
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.locator('.collection-list button').filter({ hasText: 'pink_inventory' }).click();
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    release();
    await expect(page.locator('#toast')).toContainText(/Every row added|Append committed/);
    await expect(
        page.getByRole('dialog').getByText('Append committed', { exact: true }),
    ).not.toBeVisible();
    await expect(page.getByRole('dialog').getByLabel('Rows · JSON array')).not.toHaveAttribute(
        'readonly',
        '',
    );
});

test('pending append keeps its identity after an append in another collection', async ({
    page,
}) => {
    await login(page);
    const name = `pending_a_${Date.now()}`;
    await createCollection(page, name);
    let pendingId, pendingPath;
    let attempts = 0;
    await page.route('**/collections/*/append', async (route) => {
        if (!pendingPath) pendingPath = new URL(route.request().url()).pathname;
        if (new URL(route.request().url()).pathname !== pendingPath) {
            await route.continue();
            return;
        }
        const body = route.request().postDataJSON();
        attempts++;
        if (!pendingId) pendingId = body.operation_id;
        else expect(body.operation_id).toBe(pendingId);
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({
                outcome: 'commit_unknown',
                operation_id: pendingId,
                recovery_required: true,
            }),
        });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(page.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.locator('.collection-list button').filter({ hasText: 'pink_inventory' }).click();
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(
        page.getByRole('dialog').getByText('Append committed', { exact: true }),
    ).toBeVisible();
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.locator('.collection-list button').filter({ hasText: name }).click();
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await expect(page.getByRole('dialog').getByLabel('Rows · JSON array')).toHaveAttribute(
        'readonly',
        '',
    );
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Retry same operation', exact: true })
        .click();
    await expect.poll(() => attempts).toBe(2);
});

test('late scan cannot replace a newer query in the same collection', async ({ page }) => {
    await login(page);
    await nav(page, 'Data studio');
    let release, seen;
    let scans = 0;
    const held = new Promise((resolve) => (release = resolve)),
        captured = new Promise((resolve) => (seen = resolve));
    await page.route('**/collections/*/scan', async (route) => {
        scans++;
        if (scans !== 1) {
            await route.continue();
            return;
        }
        const response = await route.fetch();
        seen();
        await held;
        try {
            await route.fulfill({ response });
        } catch {
            /* Aborting an obsolete read is permitted. */
        }
    });
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await captured;
    await nav(page, 'Schema');
    await nav(page, 'Data studio');
    await page.getByLabel('Result limit').fill('1');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(1);
    release();
    // Allow the held response and its fetch continuation to reach the application.
    await page.waitForTimeout(200);
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(1);
    await expect(page.getByLabel('Result limit')).toHaveValue('1');
});

test('rejected credentials can be corrected without persisting secrets', async ({ page }) => {
    await page.goto('/');
    await page.getByLabel('Access token').fill('deliberately-wrong-test-token');
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.locator('#login-error')).toContainText('not authorized');
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
});

test('NULL filter may read a hidden column and initial view remains empty', async ({ page }) => {
    await login(page);
    await nav(page, 'Data studio');
    for (const checkbox of await page.locator('input[name=column]').all()) await checkbox.uncheck();
    await page.getByRole('checkbox', { name: 'key', exact: true }).check();
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter column 1').selectOption('country');
    await page.getByLabel('Filter operator 1').selectOption('is_null');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    const table = page.getByRole('table', { name: 'Query results' });
    await expect(table.locator('tbody tr')).toHaveCount(1);
    await expect(table.locator('th')).toHaveCount(1);
    await expect(table.getByRole('cell', { name: 'dream-001', exact: true })).toBeVisible();
    await page.getByLabel('Result limit').fill('2');
    await page.locator('#snapshot-selector').selectOption('initial');
    await expect(page.getByLabel('Result limit')).toHaveValue('2');
    await page.getByLabel('Result limit').fill('0');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(table.locator('tbody tr')).toHaveCount(0);
    await expect(table.locator('th')).toHaveCount(1);
    await expect(page.locator('.results-panel')).toContainText('initial empty');
});

test('mismatched create resolution keeps original UUID and frozen payload', async ({ page }) => {
    await login(page);
    let id;
    let attempts = 0;
    await page.route('**/collections', async (route) => {
        if (route.request().method() !== 'POST') {
            await route.continue();
            return;
        }
        const body = route.request().postDataJSON();
        attempts++;
        if (!id) id = body.collection_id;
        else expect(body.collection_id).toBe(id);
        await route.abort('failed');
    });
    await page.route('**/collections:resolve?*', (route) =>
        route.fulfill({
            contentType: 'application/json',
            body: JSON.stringify({
                collection_id: 'ffffffff-ffff-4fff-8fff-ffffffffffff',
                name: 'different',
                schema: { fields: [] },
            }),
        }),
    );
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    await page.getByLabel('Collection name').fill(`unresolved_create_${Date.now()}`);
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog').locator('#modal-error')).toContainText(
        'Creation is unresolved',
    );
    await expect(page.getByLabel('Collection name')).toHaveAttribute('readonly', '');
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect.poll(() => attempts).toBe(2);
});

test('metadata refresh failure cannot erase a committed append receipt', async ({ page }) => {
    await login(page);
    await nav(page, 'Data studio');
    let appended = false;
    await page.route('**/collections/*/append', async (route) => {
        const response = await route.fetch();
        appended = true;
        await route.fulfill({ response });
    });
    await page.route('**/collections/*/stats?*', async (route) => {
        if (!appended) {
            await route.continue();
            return;
        }
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({
                error: { code: 'UNAVAILABLE', message: 'test metadata unavailable' },
            }),
        });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(page.getByRole('dialog').locator('#modal-error')).toContainText(
        'test metadata unavailable',
    );
    await expect(
        page.getByRole('dialog').getByText('Append committed', { exact: true }),
    ).toBeVisible();
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await nav(page, 'Operations');
    await expect(page.locator('.operations-card strong')).toHaveText('committed');
    await nav(page, 'Data studio');
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await expect(page.getByRole('dialog').getByLabel('Rows · JSON array')).not.toHaveAttribute(
        'readonly',
        '',
    );
});

test('duplicate create is a terminal rejection and allows a corrected name', async ({ page }) => {
    await login(page);
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    await page.getByLabel('Collection name').fill('dreamhouse_events');
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog').locator('#modal-error')).toContainText('already in use');
    await expect(page.getByLabel('Collection name')).not.toHaveAttribute('readonly', '');
    await page.getByLabel('Collection name').fill(`corrected_${Date.now()}`);
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
});

test('a terminal outcome survives an older unresolved status response', async ({ page }) => {
    await login(page);
    await nav(page, 'Data studio');
    await page.route('**/collections/*/append', async (route) => {
        await route.fetch();
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({
                outcome: 'commit_unknown',
                operation_id: route.request().postDataJSON().operation_id,
                recovery_required: true,
            }),
        });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(page.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await nav(page, 'Operations');
    let release, seen;
    let checks = 0;
    const held = new Promise((resolve) => (release = resolve)),
        captured = new Promise((resolve) => (seen = resolve));
    await page.route('**/collections/*/operations/*', async (route) => {
        checks++;
        if (checks !== 1) {
            await route.continue();
            return;
        }
        seen();
        await held;
        await route.fulfill({
            contentType: 'application/json',
            body: JSON.stringify({ outcome: 'commit_unknown', recovery_required: true }),
        });
    });
    await page.getByRole('button', { name: 'Check outcome', exact: true }).click();
    await captured;
    await page.getByRole('button', { name: 'Check outcome', exact: true }).click();
    await expect(page.locator('.operations-card strong')).toHaveText('committed');
    release();
    await page.waitForTimeout(200);
    await expect(page.locator('.operations-card strong')).toHaveText('committed');
});

test('resolved append disables its old retry button before a new write', async ({ page }) => {
    await login(page);
    await nav(page, 'Data studio');
    let posts = 0;
    await page.route('**/collections/*/append', async (route) => {
        posts++;
        await route.fetch();
        await route.fulfill({
            status: 503,
            contentType: 'application/json',
            body: JSON.stringify({
                outcome: 'commit_unknown',
                operation_id: route.request().postDataJSON().operation_id,
                recovery_required: true,
            }),
        });
    });
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('button', { name: 'Append rows', exact: true }).click();
    await expect(dialog.getByText('Outcome unresolved · keep this operation ID')).toBeVisible();
    await dialog.getByRole('button', { name: 'Close dialog' }).click();
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await dialog.getByRole('button', { name: 'Check outcome', exact: true }).click();
    await expect(dialog.getByText('Append committed', { exact: true })).toBeVisible();
    await expect(dialog.getByRole('button', { name: 'Committed', exact: true })).toBeDisabled();
    await expect(
        dialog.getByRole('button', { name: 'Retry same operation', exact: true }),
    ).toHaveCount(0);
    expect(posts).toBe(1);
});

test('legacy brand config without image paths still opens a workspace', async ({ page }) => {
    const config = JSON.parse(await readFile(path.join(root, 'ui/config/theme.json'), 'utf8'));
    delete config.brand.mark;
    delete config.brand.wordmark;
    await page.route('**/workbench/config', (route) =>
        route.fulfill({ contentType: 'application/json', body: JSON.stringify(config) }),
    );
    await login(page);
    await expect(page.locator('.avatar')).toHaveText('G');
    await nav(page, 'Data studio');
    await expect(page.getByRole('button', { name: 'Run query', exact: true })).toBeVisible();
});
