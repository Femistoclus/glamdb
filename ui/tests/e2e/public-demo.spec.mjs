import { test, expect } from '@playwright/test';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const url =
    process.env.GLAMDB_PUBLIC_URL ??
    `http://127.0.0.1:${process.env.GLAMDB_STATIC_PORT ?? '8768'}/glamdb/`;
const root = fileURLToPath(new URL('../../../', import.meta.url));
let errors, apiRequests;
test.beforeEach(async ({ page }) => {
    errors = [];
    apiRequests = [];
    page.on('pageerror', (error) => errors.push(error.message));
    page.on('console', (message) => {
        if (message.type() === 'error') errors.push(message.text());
    });
    page.on('request', (request) => {
        if (
            /^\/(?:collections(?:[/:]|$)|capabilities(?:\/|$))/.test(
                new URL(request.url()).pathname,
            )
        )
            apiRequests.push(request.url());
    });
});
test.afterEach(() => {
    expect(errors).toEqual([]);
    expect(apiRequests).toEqual([]);
});
async function open(page) {
    await page.goto(url);
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
}
async function nav(page, name) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
}

test('public prefix loads both supplied logos and needs no token or API', async ({ page }) => {
    await open(page);
    await expect(page.getByLabel('Access token')).toHaveCount(0);
    await expect(
        page.getByText('Public demo · your data stays in this tab · reload to reset'),
    ).toBeVisible();
    for (const selector of ['.brand-wordmark', '.brand-mark']) {
        const image = page.locator(selector);
        await expect(image).toBeVisible();
        await expect
            .poll(() => image.evaluate((node) => node.complete && node.naturalWidth > 0))
            .toBe(true);
    }
    expect(await page.locator('#brand-favicon').getAttribute('href')).toContain(
        '/assets/brand/mark.png',
    );
    for (const name of ['Schema', 'Snapshots', 'Storage', 'Explain', 'Operations']) {
        await nav(page, name);
        await expect(page.locator('#view h1')).toBeVisible();
    }
    await nav(page, 'Overview');
    await page.screenshot({
        path: path.join(root, 'artifacts/e2e/public-desktop.png'),
        fullPage: true,
    });
});

test('public create append precision filter history and JSON export', async ({ page }) => {
    await open(page);
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    await page.getByLabel('Collection name').fill('public_test');
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    async function append(rows) {
        await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
        await page.getByLabel('Rows · JSON array').fill(JSON.stringify(rows));
        await page
            .getByRole('dialog')
            .getByRole('button', { name: 'Append rows', exact: true })
            .click();
        await expect(page.getByText('Append committed', { exact: true })).toBeVisible();
        await page.getByRole('button', { name: 'Close dialog' }).click();
    }
    await append([
        { key: 'same', label: 'First', amount: '9007199254740993' },
        { key: 'same', label: 'Second', amount: '2' },
    ]);
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(page.getByRole('cell', { name: '9007199254740993', exact: true })).toBeVisible();
    const old = await page.locator('#snapshot-selector option').last().getAttribute('value');
    await append([{ key: 'other', label: 'Later', amount: '3' }]);
    await page.locator('#snapshot-selector').selectOption(old);
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter column 1').selectOption('amount');
    await page.getByLabel('Filter operator 1').selectOption('gt');
    await page.getByLabel('Filter value 1').fill('2');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect(
        page.getByRole('table', { name: 'Query results' }).locator('tbody tr'),
    ).toHaveCount(1);
    const downloaded = page.waitForEvent('download');
    await page.getByRole('button', { name: 'JSON', exact: true }).click();
    expect((await downloaded).suggestedFilename()).toBe('public_test.json');
    await nav(page, 'Storage');
    await page.getByRole('button', { name: 'Inspect', exact: true }).first().click();
    await expect(page.getByRole('dialog')).toContainText('Synthetic mock descriptor');
});

test('public visitors are isolated and reset restores sample collections', async ({
    page,
    browser,
}) => {
    await open(page);
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    await page.getByLabel('Collection name').fill('only_this_visitor');
    await page.getByRole('button', { name: 'Create collection' }).click();
    await expect(page.locator('.breadcrumb strong')).toHaveText('only_this_visitor');
    const context = await browser.newContext(),
        other = await context.newPage();
    try {
        await open(other);
        await expect(other.locator('.collection-list')).not.toContainText('only_this_visitor');
    } finally {
        await context.close();
    }
    expect(await page.evaluate(() => ({ ...localStorage, ...sessionStorage }))).toEqual({});
    await page.getByRole('button', { name: 'Reset demo', exact: true }).click();
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
    await expect(page.locator('.collection-list button')).toHaveCount(2);
});

test('public mobile navigation and theme toggle fit the viewport', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await open(page);
    await page.getByRole('button', { name: 'Toggle navigation' }).click();
    await page.getByRole('button', { name: 'Switch to dark theme' }).click();
    await expect(page.locator('html')).toHaveAttribute('data-appearance', 'dark');
    await nav(page, 'Data studio');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
        true,
    );
    await page.screenshot({
        path: path.join(root, 'artifacts/e2e/public-mobile.png'),
        fullPage: true,
    });
});

test('local admission rejection allows a smaller corrected append', async ({ page }) => {
    await open(page);
    await nav(page, 'Data studio');
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    const sample = {
        key: 'duplicate',
        event: 'demo',
        country: null,
        revenue: null,
        active: true,
        occurred_at: '2026-01-01T00:00:00Z',
    };
    await page
        .getByLabel('Rows · JSON array')
        .fill(JSON.stringify(Array.from({ length: 10000 }, () => sample)));
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(page.getByText('Definitely not committed', { exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Close dialog' }).click();
    await page.getByRole('button', { name: 'Batch editor', exact: true }).click();
    await page.getByLabel('Rows · JSON array').fill(JSON.stringify([sample]));
    await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Append rows', exact: true })
        .click();
    await expect(page.getByText('Append committed', { exact: true })).toBeVisible();
});
