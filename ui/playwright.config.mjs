import { defineConfig } from '@playwright/test';

const port = process.env.GLAMDB_E2E_PORT ?? '8766';
export default defineConfig({
    testDir: './tests/e2e',
    timeout: 30000,
    expect: { timeout: 7000 },
    fullyParallel: false,
    workers: 1,
    retries: 0,
    reporter: [
        ['list'],
        ['html', { outputFolder: '../artifacts/playwright-report', open: 'never' }],
    ],
    outputDir: '../artifacts/e2e/results',
    use: {
        baseURL: `http://127.0.0.1:${port}`,
        viewport: { width: 1440, height: 1000 },
        trace: 'retain-on-failure',
        screenshot: 'only-on-failure',
        launchOptions: process.env.GLAMDB_BROWSER_PATH
            ? { executablePath: process.env.GLAMDB_BROWSER_PATH }
            : {},
    },
    webServer: [
        {
            command: '../.venv/bin/python tests/e2e/start-server.py',
            url: `http://127.0.0.1:${port}/healthz`,
            timeout: 30000,
            reuseExistingServer: false,
        },
        {
            command: '../.venv/bin/python tests/e2e/start-static.py',
            url: `http://127.0.0.1:${process.env.GLAMDB_STATIC_PORT ?? '8768'}/glamdb/`,
            timeout: 30000,
            reuseExistingServer: false,
        },
    ],
});
