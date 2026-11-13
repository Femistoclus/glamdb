import { test, expect } from '@playwright/test';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';

// The mock does not implement Parquet write settings. These scenarios use their
// own disposable mock child; the advertised-feature scenario adds the native
// capability and settings fields with request interception.
const token = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';
const root = fileURLToPath(new URL('../../../', import.meta.url));
const PARQUET = {
    default_compression: { codec: 'zstd', level: null },
    codecs: [
        { codec: 'uncompressed', level: null },
        { codec: 'snappy', level: null },
        { codec: 'gzip', level: { min: 1, max: 9, default: 9 } },
        { codec: 'lz4_raw', level: { min: 1, max: 12, default: 1 } },
        { codec: 'zstd', level: { min: 1, max: 22, default: 1 } },
    ],
};
// A server with dictionary and encoding options (#98) as well as codecs.
const PARQUET_ENCODINGS = {
    ...PARQUET,
    encodings: [
        { encoding: 'plain', types: ['string', 'bool', 'int64', 'float64', 'timestamp_us_utc'] },
        { encoding: 'delta_binary_packed', types: ['int64', 'timestamp_us_utc'] },
        { encoding: 'delta_length_byte_array', types: ['string'] },
        { encoding: 'delta_byte_array', types: ['string'] },
        { encoding: 'byte_stream_split', types: ['int64', 'float64', 'timestamp_us_utc'] },
        { encoding: 'rle', types: ['bool'] },
    ],
    dictionary: {
        policies: ['auto', 'enabled', 'disabled'],
        default_policy: 'auto',
        types: ['string', 'int64', 'float64', 'timestamp_us_utc'],
        fallback_encoding: 'plain',
        page_size_limit: { min: 4096, max: 8388608, default: 1048576 },
    },
};
let server, origin, pageErrors, consoleErrors;
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
            throw new Error(`Disposable write-settings mock exited: ${server.exitCode}`);
        try {
            if ((await fetch(`${origin}/healthz`)).ok) return;
        } catch {
            /* Wait for the isolated child to bind. */
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error('Disposable write-settings mock did not become ready');
});
test.afterAll(async () => {
    if (server && server.exitCode === null) {
        server.kill('SIGTERM');
        await once(server, 'exit');
    }
});
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
    const allowed = /conflict/.test(info.title) ? /Failed to load resource:.*409/ : null;
    expect(
        consoleErrors.filter((message) => !allowed?.test(message)),
        'unexpected browser console errors',
    ).toEqual([]);
});
async function connect(page) {
    await page.goto(origin);
    await page.getByLabel('Access token').fill(token);
    await page.getByRole('button', { name: 'Open my workspace' }).click();
    await expect(page.locator('.breadcrumb')).toBeVisible();
}
async function nav(page, name) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
}

// Intercepts capabilities and collection responses to emulate a native server
// with write settings on top of the disposable mock.
async function advertise(page, parquet) {
    const stored = { version: '0', settings: null, mode: 'append' };
    const posts = [];
    const withSettings = (collection) => ({
        ...collection,
        write_settings: stored.settings,
        write_settings_version: stored.version,
        mode: stored.mode,
    });
    await page.route('**/capabilities', async (route) => {
        const response = await route.fetch();
        const capabilities = await response.json();
        await route.fulfill({
            status: response.status(),
            json: {
                ...capabilities,
                features: { ...capabilities.features, write_settings: true, key_value: true },
                parquet,
            },
        });
    });
    await page.route('**/collections', async (route) => {
        if (route.request().method() !== 'POST') return route.continue();
        const body = route.request().postDataJSON();
        posts.push(body);
        // Like the native catalog: non-default creation settings are version 1.
        stored.settings = body.write_settings;
        stored.version = '1';
        stored.mode = body.mode ?? 'append';
        const { write_settings: omitted, mode, ...forwarded } = body;
        expect(omitted).toBeDefined();
        const response = await route.fetch({ postData: JSON.stringify(forwarded) });
        await route.fulfill({
            status: response.status(),
            json: withSettings(await response.json()),
        });
    });
    await page.route(/\/collections\/[0-9a-f-]{36}$/, async (route) => {
        const response = await route.fetch();
        await route.fulfill({
            status: response.status(),
            json: withSettings(await response.json()),
        });
    });
    await page.route(/\/collections\/[0-9a-f-]{36}\/write-settings$/, async (route) => {
        const body = route.request().postDataJSON();
        posts.push(body);
        if (body.expected_version !== stored.version)
            return route.fulfill({
                status: 409,
                json: {
                    error: {
                        code: 'WRITE_SETTINGS_CONFLICT',
                        message: `write settings version is ${stored.version}, not ${body.expected_version}`,
                        retry_hint: 'FixInput',
                    },
                },
            });
        stored.version = String(Number(stored.version) + 1);
        stored.settings = body.write_settings;
        const response = await page.request.get(
            route
                .request()
                .url()
                .replace(/\/write-settings$/, ''),
            { headers: { Authorization: `Bearer ${token}` } },
        );
        await route.fulfill({ status: 200, json: withSettings(await response.json()) });
    });

    return { stored, posts };
}

test('mock capabilities hide Parquet controls and create sends no write settings', async ({
    page,
}) => {
    let body;
    await page.route('**/collections', async (route) => {
        if (route.request().method() === 'POST') body = route.request().postDataJSON();
        await route.continue();
    });
    await connect(page);
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    const dialog = page.getByRole('dialog');
    await expect(dialog.locator('select[name="parquet-codec"]')).toHaveCount(0);
    await expect(dialog.locator('[data-parquet-codec]')).toHaveCount(0);
    await dialog.getByLabel('Collection name').fill(`plain_${Date.now()}`);
    await dialog.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(dialog).not.toBeVisible();
    expect(Object.keys(body)).toEqual(['collection_id', 'name', 'schema']);
    await nav(page, 'Schema');
    await expect(page.getByRole('table', { name: 'Collection schema' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Parquet write settings' })).toHaveCount(0);
});

for (const mode of ['append', 'kv']) {
    test(`${mode} creation retains compression and column encoding together`, async ({ page }) => {
        const { posts } = await advertise(page, PARQUET_ENCODINGS);
        await connect(page);
        await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
        const dialog = page.getByRole('dialog');
        await dialog.getByLabel('Collection name').fill(`${mode}_settings_${Date.now()}`);
        await dialog.getByLabel('Collection mode', { exact: true }).selectOption(mode);
        await dialog.locator('select[name="parquet-codec"]').selectOption('gzip');
        await dialog.getByLabel('Level', { exact: true }).fill('6');
        await dialog.getByLabel('Parquet compression for column 3').selectOption('zstd');
        await dialog.getByLabel('Parquet level for column 3').fill('3');
        await dialog.getByLabel('Encoding for column 3').selectOption('delta_binary_packed');
        await dialog.getByRole('button', { name: 'Add column', exact: false }).click();
        await dialog.getByLabel('Column name 4', { exact: true }).fill('extra');
        // Rebuilding the column editor must preserve all existing choices.
        await expect(dialog.getByLabel('Collection mode', { exact: true })).toHaveValue(mode);
        await expect(dialog.getByLabel('Parquet compression for column 3')).toHaveValue('zstd');
        await expect(dialog.getByLabel('Encoding for column 3')).toHaveValue('delta_binary_packed');
        await dialog.getByRole('button', { name: 'Create collection', exact: true }).click();
        await expect(dialog).not.toBeVisible();
        expect(posts).toHaveLength(1);
        expect(posts[0].mode ?? 'append').toBe(mode);
        expect(posts[0].write_settings).toEqual({
            parquet: {
                compression: { codec: 'gzip', level: 6 },
                columns: [
                    {
                        field: 'amount',
                        compression: { codec: 'zstd', level: 3 },
                        encoding: 'delta_binary_packed',
                    },
                ],
            },
        });
        await expect(page.locator('.page-footer')).toContainText(
            mode === 'kv' ? 'KV mode' : 'Append mode',
        );
    });
}

test('advertised settings create canonically, save by compare-and-set and reload on conflict', async ({
    page,
}, info) => {
    const { stored, posts } = await advertise(page, PARQUET);

    await connect(page);
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    const dialog = page.getByRole('dialog');
    await dialog.getByLabel('Collection name').fill(`compressed_${Date.now()}`);
    const level = dialog.getByLabel('Level', { exact: true });
    await expect(level).toBeDisabled();
    // The label's accessible name also contains the selected option text.
    await dialog.locator('select[name="parquet-codec"]').selectOption('zstd');
    await expect(level).toBeEnabled();
    await expect(level).toHaveAttribute('max', '22');
    await level.fill('3');
    await dialog.getByLabel('Parquet compression for column 3').selectOption('uncompressed');
    await expect(dialog.getByLabel('Parquet level for column 3')).toBeDisabled();
    await page.screenshot({ path: info.outputPath('parquet-create-desktop.png') });
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(dialog.getByLabel('Parquet compression for column 3')).toBeInViewport();
    await page.screenshot({ path: info.outputPath('parquet-create-mobile.png') });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await dialog.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(dialog).not.toBeVisible();
    expect(posts[0].write_settings).toEqual({
        parquet: {
            compression: { codec: 'zstd', level: 3 },
            columns: [{ field: 'amount', compression: { codec: 'uncompressed', level: null } }],
        },
    });

    await nav(page, 'Schema');
    const panel = page.locator('.write-settings-panel');
    await expect(panel.getByRole('heading', { name: 'Parquet write settings' })).toBeVisible();
    await expect(panel).toContainText('Version 1');
    await expect(panel.getByLabel('Collection compression')).toHaveValue('zstd');
    await expect(panel.getByLabel('Level', { exact: true })).toHaveValue('3');
    await expect(panel.getByLabel('Parquet compression for amount')).toHaveValue('uncompressed');
    await expect(panel.getByLabel('Parquet compression for key')).toHaveValue('');

    await panel.getByLabel('Collection compression').selectOption('gzip');
    await panel.getByLabel('Level', { exact: true }).fill('10');
    await panel.getByRole('button', { name: 'Save write settings' }).click();
    // Capability ranges reach the input, so the browser blocks the submission.
    expect(
        await panel
            .getByLabel('Level', { exact: true })
            .evaluate((input) => input.validity.rangeOverflow),
    ).toBe(true);
    expect(posts).toHaveLength(1);
    await panel.getByLabel('Level', { exact: true }).fill('6');
    await panel.getByRole('button', { name: 'Save write settings' }).click();
    await expect(panel).toContainText('Version 2');
    expect(posts[1]).toEqual({
        expected_version: '1',
        write_settings: {
            parquet: {
                compression: { codec: 'gzip', level: 6 },
                columns: [{ field: 'amount', compression: { codec: 'uncompressed', level: null } }],
            },
        },
    });
    await expect(page.locator('#toast')).toContainText('existing files keep their codecs');

    // Another client changes the settings; this form still shows version 2.
    stored.version = '3';
    stored.settings = { parquet: { compression: null, columns: [] } };
    await panel.getByLabel('Collection compression').selectOption('snappy');
    await panel.getByRole('button', { name: 'Save write settings' }).click();
    await expect(panel.getByRole('alert')).toContainText('Reloaded version 3');
    await expect(panel).toContainText('Version 3');
    await expect(panel.getByLabel('Collection compression')).toHaveValue('');
    expect(posts[2].expected_version).toBe('2');
    expect(stored.version).toBe('3');
});

test('dictionary and encoding choices follow column types and lock incompatible policies', async ({
    page,
}, info) => {
    const { posts } = await advertise(page, PARQUET_ENCODINGS);
    await connect(page);
    await page.getByRole('button', { name: 'New collection', exact: true }).first().click();
    const dialog = page.getByRole('dialog');
    await dialog.getByLabel('Collection name').fill(`encoded_${Date.now()}`);
    await expect(dialog.locator('input[name="parquet-page-limit"]')).toHaveAttribute(
        'max',
        '8388608',
    );
    // amount (int64) offers only encodings that accept int64.
    const amountEncoding = dialog.getByLabel('Encoding for column 3');
    const amountDictionary = dialog.getByLabel('Dictionary policy for column 3');
    expect(await amountEncoding.locator('option').allTextContents()).toEqual([
        'Default encoding',
        'plain',
        'delta_binary_packed',
        'byte_stream_split',
    ]);
    // Choosing an encoding locks the column dictionary and clears "enabled".
    await amountDictionary.selectOption('enabled');
    await amountEncoding.selectOption('delta_binary_packed');
    await expect(amountDictionary).toBeDisabled();
    await expect(amountDictionary).toHaveValue('');
    await amountEncoding.selectOption('');
    await expect(amountDictionary).toBeEnabled();
    await amountEncoding.selectOption('delta_binary_packed');
    // A bool column offers rle and no "enabled" dictionary.
    await dialog.getByLabel('Column type 2').selectOption('bool');
    expect(
        await dialog.getByLabel('Encoding for column 2').locator('option').allTextContents(),
    ).toEqual(['Default encoding', 'plain', 'rle']);
    expect(
        await dialog
            .getByLabel('Dictionary policy for column 2')
            .locator('option')
            .allTextContents(),
    ).toEqual(['Default dictionary', 'auto', 'disabled']);
    await dialog.locator('select[name="parquet-dictionary"]').selectOption('enabled');
    await dialog.locator('input[name="parquet-page-limit"]').fill('65536');
    await dialog.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(dialog.getByRole('alert')).toContainText(
        'needs a dictionary policy or encoding for bool column label',
    );
    expect(posts).toHaveLength(0);
    await dialog.getByLabel('Encoding for column 2').selectOption('rle');
    await page.screenshot({ path: info.outputPath('parquet-encodings-create-desktop.png') });
    await page.setViewportSize({ width: 390, height: 844 });
    await dialog.getByLabel('Encoding for column 3').scrollIntoViewIfNeeded();
    await expect(dialog.getByLabel('Encoding for column 3')).toBeInViewport();
    await page.screenshot({ path: info.outputPath('parquet-encodings-create-mobile.png') });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await dialog.getByRole('button', { name: 'Create collection', exact: true }).click();
    await expect(dialog).not.toBeVisible();
    expect(posts[0].write_settings).toEqual({
        parquet: {
            dictionary: 'enabled',
            dictionary_page_size_limit: '65536',
            columns: [
                { field: 'label', encoding: 'rle' },
                { field: 'amount', encoding: 'delta_binary_packed' },
            ],
        },
    });

    await nav(page, 'Schema');
    const panel = page.locator('.write-settings-panel');
    await expect(panel).toContainText('Version 1');
    await expect(panel.locator('select[name="parquet-dictionary"]')).toHaveValue('enabled');
    await expect(panel.locator('input[name="parquet-page-limit"]')).toHaveValue('65536');
    await expect(panel.getByLabel('Encoding for amount')).toHaveValue('delta_binary_packed');
    await expect(panel.getByLabel('Dictionary policy for amount')).toBeDisabled();
    await expect(panel.locator('tr', { hasText: 'key' }).first()).toContainText(
        'dictionary, plain fallback',
    );
    await expect(panel.locator('tr', { hasText: 'amount' })).toContainText('delta_binary_packed');
    await page.screenshot({ path: info.outputPath('parquet-encodings-panel.png'), fullPage: true });
    await panel.getByLabel('Encoding for amount').selectOption('');
    await panel.getByLabel('Dictionary policy for amount').selectOption('disabled');
    await panel.getByRole('button', { name: 'Save write settings' }).click();
    await expect(panel).toContainText('Version 2');
    expect(posts[1]).toEqual({
        expected_version: '1',
        write_settings: {
            parquet: {
                dictionary: 'enabled',
                dictionary_page_size_limit: '65536',
                columns: [
                    { field: 'label', encoding: 'rle' },
                    { field: 'amount', dictionary: 'disabled' },
                ],
            },
        },
    });
});
