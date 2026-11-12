import { test, expect } from '@playwright/test';

const url = `http://127.0.0.1:${process.env.GLAMDB_STATIC_PORT ?? '8768'}/glamdb/`;
test('compact overview, persistent appearance and collapsible navigation', async ({ page }) => {
    await page.goto(url);
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
    await expect(page.locator('.hero')).toHaveCount(0);
    await expect(
        page.locator('nav').getByRole('button', { name: 'Style studio', exact: true }),
    ).toHaveCount(0);
    await expect(page.getByRole('table', { name: 'Overview schema' })).toBeVisible();
    await page.getByRole('button', { name: 'Switch to dark theme' }).click();
    await expect(page.locator('html')).toHaveAttribute('data-appearance', 'dark');
    await page.getByRole('button', { name: 'Collapse sidebar' }).click();
    await expect(page.locator('.sidebar')).toHaveCSS('width', '64px');
    await page.reload();
    await expect(page.locator('html')).toHaveAttribute('data-appearance', 'dark');
    await expect(page.locator('.sidebar')).toHaveCSS('width', '64px');
    await page.getByRole('button', { name: 'Expand sidebar' }).click();
    await page.screenshot({ path: '../artifacts/compact-ui/overview-dark.png', fullPage: true });
});

test('snapshot changes preserve read/write drafts and results belong to Read rows', async ({
    page,
}) => {
    await page.goto(url);
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    await page.getByRole('button', { name: '+ Add a condition', exact: true }).click();
    await page.getByLabel('Filter column 1').selectOption('country');
    await page.getByLabel('Filter value 1').fill('US');
    const read = page.locator('#query-form');
    await read.evaluate((node) => (globalThis.originalReadForm = node));
    await read.locator('input[name=limit]').fill('7');
    await page.locator('[data-write-row] summary').first().click();
    await page.getByLabel('key row 1', { exact: true }).fill('keep-my-draft');
    await page.getByLabel('Snapshot', { exact: true }).selectOption('initial');
    await expect
        .poll(() => read.evaluate((node) => node === globalThis.originalReadForm))
        .toBe(true);
    await expect(read.locator('input[name=limit]')).toHaveValue('7');
    await expect(page.getByLabel('Filter value 1')).toHaveValue('US');
    await expect(page.getByLabel('key row 1', { exact: true })).toHaveValue('keep-my-draft');
    await page.locator('#run-query').click();
    await expect(
        page.locator('.read-panel').getByRole('table', { name: 'Query results' }),
    ).toBeVisible();
    await expect(
        page.locator('.write-panel').getByRole('table', { name: 'Query results' }),
    ).toHaveCount(0);
    await page.getByRole('button', { name: '+ Add row', exact: true }).click();
    await expect(page.locator('[data-write-row]')).toHaveCount(2);
    await expect(page.locator('[data-write-row]').nth(1)).toHaveJSProperty('open', false);
    await page.getByLabel('Snapshot', { exact: true }).selectOption('latest');
    await page.locator('#run-query').click();
    await expect(page.locator('.read-panel tbody tr')).not.toHaveCount(0);
    await page.screenshot({ path: '../artifacts/compact-ui/data-light.png', fullPage: true });
});
