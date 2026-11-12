import { defineConfig } from '@playwright/test';
import { accessSync, constants, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

const pythonSetting = process.env.GLAMDB_E2E_PYTHON ?? '../.venv/bin/python';
const python = /[/\\]/.test(pythonSetting)
    ? resolve(fileURLToPath(new URL('.', import.meta.url)), pythonSetting)
    : pythonSetting;
process.env.GLAMDB_E2E_PYTHON = python;
const port = process.env.GLAMDB_E2E_PORT ?? '8766';
const backend =
    process.env.GLAMDB_TEST_BACKEND ?? (process.env.GLAMDB_NATIVE_SERVER ? 'native' : 'mock');
if (!['mock', 'native', 'static'].includes(backend))
    throw new Error('GLAMDB_TEST_BACKEND must be mock, native or static');
if (backend === 'native') {
    try {
        accessSync(process.env.GLAMDB_NATIVE_SERVER ?? '', constants.X_OK);
        if (!statSync(process.env.GLAMDB_NATIVE_SERVER).isFile()) throw new Error('not a file');
    } catch {
        throw new Error('native tests require an executable GLAMDB_NATIVE_SERVER');
    }
}
process.env.GLAMDB_TEST_BACKEND = backend;
process.env.GLAMDB_E2E_STATE_FILE ??= fileURLToPath(
    new URL(`../artifacts/e2e/native-store-${process.pid}.json`, import.meta.url),
);
const quote = (value) => `'${value.replaceAll("'", "'\\''")}'`;
const apiServer = {
    command: `${quote(python)} tests/e2e/start-${backend === 'native' ? 'native' : 'server'}.py`,
    url: `http://127.0.0.1:${port}/healthz`,
    timeout: 30000,
    reuseExistingServer: false,
};
const staticServer = {
    command: `${quote(python)} tests/e2e/start-static.py`,
    url: `http://127.0.0.1:${process.env.GLAMDB_STATIC_PORT ?? '8768'}/glamdb/`,
    timeout: 30000,
    reuseExistingServer: false,
};
export default defineConfig({
    testDir: './tests/e2e',
    testMatch:
        backend === 'native'
            ? ['native.spec.mjs', 'shared-contract.spec.mjs']
            : backend === 'static'
              ? ['public-demo.spec.mjs', 'compact-workbench.spec.mjs']
              : '**/*.spec.mjs',
    testIgnore: backend === 'mock' ? 'native.spec.mjs' : [],
    timeout: 30000,
    expect: { timeout: 7000 },
    fullyParallel: false,
    workers: 1,
    retries: 0,
    reporter: [
        ['list'],
        ['html', { outputFolder: `../artifacts/playwright-report/${backend}`, open: 'never' }],
    ],
    outputDir: `../artifacts/e2e/${backend}/results`,
    use: {
        baseURL: `http://127.0.0.1:${port}`,
        viewport: { width: 1440, height: 1000 },
        trace: 'retain-on-failure',
        screenshot: 'only-on-failure',
        launchOptions: process.env.GLAMDB_BROWSER_PATH
            ? { executablePath: process.env.GLAMDB_BROWSER_PATH }
            : {},
    },
    webServer:
        backend === 'static'
            ? [staticServer]
            : backend === 'native'
              ? [apiServer]
              : [apiServer, staticServer],
});
