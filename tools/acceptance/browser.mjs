import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { chromium } from '../../ui/node_modules/playwright/index.mjs';

const config = JSON.parse(await readFile(process.argv[2], 'utf8'));
const token = (await readFile(config.token_file, 'utf8')).trim();
const output = config.output;
await mkdir(output, { recursive: true, mode: 0o700 });
const browser = await chromium.launch({
    headless: true,
    ...(process.argv[3] ? { executablePath: process.argv[3] } : {}),
});
const browserVersion = browser.version();
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
const page = await context.newPage();
page.setDefaultTimeout(7000);
const errors = [];
page.on('pageerror', (error) => errors.push(error.message));
await context.tracing.start({ screenshots: true, snapshots: true, sources: false });
const screenshots = [];
async function screenshot(name) {
    const target = path.join(output, name + '.png');
    await page.screenshot({ path: target, fullPage: true });
    screenshots.push(target);
}
async function navigate(name) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
}
async function metricRows(expected) {
    await page.waitForFunction((value) => document.querySelector('.metrics .metric strong')?.textContent === value, expected);
}
async function connect() {
    await page.goto(config.url + '/');
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace', exact: true }).click();
    await page.locator('.collection-list').getByRole('button', { name: 'acceptance_ab', exact: true }).click();
    await metricRows('1,010');
}
let failure;
try {
    await connect();
    await screenshot('overview-latest');
    if (config.write_ui) {
        const response = await page.request.get(config.url + '/collections/' + config.ui_collection_id + '/stats', {
            headers: { Authorization: 'Bearer ' + token },
        });
        assert.equal(response.status(), 200);
        const statistics = await response.json();
        await page.locator('.collection-list').getByRole('button', { name: 'acceptance_ui', exact: true }).click();
        await metricRows(statistics.rows === '0' ? '0' : '1');
        if (statistics.rows === '0') {
            await navigate('Data studio');
            const form = page.locator('#write-form');
            await form.getByLabel('key row 1', { exact: true }).fill('browser-written');
            await form.getByLabel('n row 1', { exact: true }).fill('9223372036854775807');
            await form.getByLabel('label row 1 NULL', { exact: true }).uncheck();
            await form.getByLabel('label row 1', { exact: true }).fill('Native UI write');
            await form.getByRole('button', { name: 'Append rows', exact: true }).click();
            await form.getByText('Append committed', { exact: true }).waitFor();
            await screenshot('typed-native-write');
        }
        await page.locator('.collection-list').getByRole('button', { name: 'acceptance_ab', exact: true }).click();
        await navigate('Overview');
        await metricRows('1,010');
    }
    await navigate('Schema');
    assert.equal(await page.locator('#view').getByText('n', { exact: true }).count() > 0, true);
    await screenshot('schema');
    await navigate('Snapshots');
    await page.getByRole('table', { name: 'Snapshot history' }).waitFor();
    assert.equal(await page.getByRole('table', { name: 'Snapshot history' }).locator('tbody tr').count(), 2);
    await screenshot('snapshots');
    await navigate('Overview');
    await page.getByLabel('Snapshot', { exact: true }).selectOption(config.snapshot_a);
    await metricRows('1,000');
    await screenshot('overview-snapshot-a');
    await page.getByLabel('Snapshot', { exact: true }).selectOption('initial');
    await metricRows('0');
    await navigate('Storage');
    assert.equal(await page.getByRole('table', { name: 'Data files' }).locator('tbody tr').count(), 0);
    await screenshot('storage-initial');
    await page.getByLabel('Snapshot', { exact: true }).selectOption('latest');
    await page.waitForFunction(() => document.querySelectorAll('[aria-label="Data files"] tbody tr').length === 2);
    await page.getByRole('button', { name: /Inspect/ }).first().click();
    await page.getByRole('dialog').waitFor();
    await screenshot('file-details');
    await page.getByRole('dialog').locator('[data-action="close-modal"]').first().click();
    await navigate('Data studio');
    for (const [selector, expected, label] of [
        ['latest', 1010, 'latest'], [config.snapshot_a, 1000, 'snapshot-a'], ['initial', 0, 'initial'],
    ]) {
        await page.getByLabel('Snapshot', { exact: true }).selectOption(selector);
        await page.waitForLoadState('networkidle');
        await page.getByLabel('Result limit', { exact: true }).fill('1010');
        await page.locator('#run-query').click();
        const text = expected.toLocaleString('en-US') + ' rows';
        await page.getByText(new RegExp('^' + text)).first().waitFor();
        if (expected) await page.getByText('9007199254740993', { exact: true }).first().waitFor();
        await page.locator('[data-action="export-arrow"]').waitFor();
        const [download] = await Promise.all([
            page.waitForEvent('download'), page.locator('[data-action="export-arrow"]').click(),
        ]);
        const arrowPath = path.join(output, 'rows-' + label + '.arrow');
        await download.saveAs(arrowPath);
        execFileSync(process.env.GLAMDB_E2E_PYTHON || 'python3', ['-c',
            `import pyarrow as pa,pyarrow.ipc as ipc,sys
from collections import Counter
with open(sys.argv[1],'rb') as source: t=ipc.open_stream(source).read_all()
assert t.num_rows==int(sys.argv[2])
assert t.schema.names==['key','n','label'] and t.schema.field('n').type==pa.int64()
a=[('shared-'+str(i%37),9007199254740993+i,None if i%7==0 else 'lovely ✨ '+str(i)) for i in range(1000)]
expected=[] if sys.argv[3]=='initial' else a if sys.argv[3]=='snapshot-a' else a+a[:10]
assert Counter(tuple(row[name] for name in ('key','n','label')) for row in t.to_pylist())==Counter(expected)
`, arrowPath, String(expected), label], { stdio: 'pipe' });
        await screenshot('data-' + label);
    }
    await navigate('Explain');
    await page.locator('[data-action="explain-current"]').click();
    const plan = page.locator('.panel').filter({
        has: page.getByRole('heading', { name: 'Query plan', exact: true }),
    });
    await plan.getByText('Native scan', { exact: true }).waitFor();
    await screenshot('explain');
    await navigate('Operations');
    await page.getByLabel('Operation ID', { exact: true }).fill(config.operation_id);
    const lookedUp = page.waitForResponse((response) => response.url().endsWith('/operations/' + config.operation_id));
    await page.locator('#operation-lookup-form button[type="submit"]').click();
    const outcomeResponse = await lookedUp;
    assert.equal(outcomeResponse.status(), 200);
    assert.equal((await outcomeResponse.json()).outcome, 'committed');
    await page.getByText(config.operation_id, { exact: true }).first().waitFor();
    await screenshot('operation-outcome');
    await page.getByRole('dialog').locator('[data-action="close-modal"]').first().click();
    await navigate('Style studio');
    await screenshot('style-studio');
    // Reconnect after a true document reload. Metadata must come from the server.
    await page.reload();
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace', exact: true }).click();
    await page.locator('.collection-list').getByRole('button', { name: 'acceptance_ab', exact: true }).click();
    await metricRows('1,010');
    await screenshot('overview-after-reload');
    await page.keyboard.press('Tab');
    assert.equal(await page.evaluate(() => document.activeElement !== document.body), true);
    await page.setViewportSize({ width: 390, height: 844 });
    await screenshot('mobile-overview');
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1);
    assert.equal(overflow, false, 'workbench has horizontal overflow on a 390px viewport');
    await page.getByRole('button', { name: 'Toggle navigation', exact: true }).click();
    await navigate('Data studio');
    await screenshot('mobile-data-editor');
    assert.deepEqual(errors, []);
} catch (error) {
    failure = error;
    await screenshot('failure').catch(() => {});
} finally {
    const raw = path.join(output, 'trace.raw.zip');
    await context.tracing.stop({ path: raw });
    await browser.close();
    execFileSync(process.env.GLAMDB_E2E_PYTHON || 'python3', [
        fileURLToPath(new URL('sanitize_trace.py', import.meta.url)), raw, path.join(output, 'trace.zip'), config.token_file,
    ], { stdio: 'pipe' });
}
if (failure) throw failure;
console.log(JSON.stringify({ status: 'PASS', backend: 'local', mock: false, chromium: browserVersion, ui_rows: { latest: 1010, snapshot_a: 1000, initial: 0 },
    screenshots, browser_reload: 'PASS', keyboard: 'PASS', mobile: 'PASS', trace: path.join(output, 'trace.zip') }));
