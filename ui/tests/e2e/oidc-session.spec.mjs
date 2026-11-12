import { test, expect } from '@playwright/test';

// UI protocol fixtures only; native identity-provider integration is separate.
const ALPHA = '00112233-4455-6677-8899-aabbccddeeff';
const SHARED = '11112233-4455-6677-8899-aabbccddeeff';
const site = 'https://workbench.test';
const backendToken = process.env.GLAMDB_E2E_TOKEN ?? 'e2e-only-local-token';
const session = {
    version: 3,
    revision: 1,
    organization: {
        id: '22222222-3333-4444-8555-666666666666',
        name: 'Pink organization',
        login: 'Organization account',
    },
    subject: '33333333-4444-4555-8666-777777777777',
    databases: [
        { id: ALPHA, name: 'Alpha' },
        { id: SHARED, name: 'Shared' },
    ],
    account_url: 'https://identity.example/realms/glamdb/account/',
    login_url: '/oauth2/start',
    logout_url: '/workbench/logout',
};

test.afterEach(async ({ page }) => {
    // Finish fixture proxy callbacks before Playwright closes their request context.
    await page.unrouteAll({ behavior: 'wait' });
});

async function gateway(page) {
    const fixture = { status: 200, session: structuredClone(session), calls: [], logout: [] };
    const backendURL = (path) => new URL(path, test.info().project.use.baseURL).href;
    await page.route(`${site}/**`, async (route) => {
        const url = new URL(route.request().url());
        const response = await route.fetch({ url: backendURL(url.pathname + url.search) });
        if (url.pathname === '/') {
            const html = (await response.text()).replace(
                /<html\b/,
                '<html data-access-gateway="true"',
            );
            await route.fulfill({ status: 200, contentType: 'text/html', body: html });
        } else await route.fulfill({ response });
    });
    await page.route('**/workbench/session', (route) =>
        route.fulfill({
            status: fixture.status,
            json:
                fixture.status === 200
                    ? fixture.session
                    : { error: { code: 'UNAVAILABLE', message: 'Session unavailable' } },
        }),
    );
    await page.route('**/workbench/config', async (route) => {
        const response = await route.fetch({ url: backendURL('/workbench/config') });
        await route.fulfill({
            response,
            headers: {
                ...response.headers(),
                'X-GlamDB-Access-Revision': String(fixture.session.revision),
            },
        });
    });
    await page.route('**/workbench/logout', async (route) => {
        fixture.logout.push(route.request());
        fixture.status = 401;
        await route.fulfill({ json: { signed_out: true, redirect: '/oauth2/sign_out?rd=%2F' } });
    });
    await page.route('**/oauth2/sign_out?rd=%2F', (route) => route.fulfill({ status: 204 }));
    await page.route('**/databases/*/**', async (route) => {
        const request = route.request();
        fixture.calls.push(request);
        const headers = request.headers();
        expect(headers.authorization).toBeUndefined();
        if (request.method() === 'POST') {
            expect(headers['x-csrftoken']).toBe('browser-csrf-fixture');
            expect(headers.origin).toBe(site);
        }
        const url = new URL(request.url());
        url.pathname = url.pathname.replace(/^\/databases\/[^/]+/, '');
        const response = await route.fetch({
            url: backendURL(url.pathname + url.search),
            headers: { ...headers, authorization: `Bearer ${backendToken}` },
        });
        await route.fulfill({ response });
    });
    await page.context().addCookies([
        {
            name: '__Host-glamdb-csrf',
            value: 'browser-csrf-fixture',
            url: site + '/',
            secure: true,
            sameSite: 'Lax',
        },
    ]);
    return fixture;
}

async function opened(page) {
    await page.goto(site + '/');
    await expect(page.getByRole('heading', { name: 'A little overview' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Sign out', exact: true })).toBeVisible();
    await expect(page.getByRole('link', { name: 'Account', exact: true })).toHaveAttribute(
        'href',
        session.account_url,
    );
}

test('an expired OIDC session offers actual Sign in without a password form', async ({ page }) => {
    const fixture = await gateway(page);
    fixture.status = 401;
    await page.route('**/oauth2/start', (route) =>
        route.fulfill({ contentType: 'text/html', body: '<h1>OIDC sign-in boundary</h1>' }),
    );
    await page.goto(site + '/');
    await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible();
    await expect(page.getByLabel('Access token')).toHaveCount(0);
    await page.getByRole('button', { name: 'Sign in', exact: true }).click();
    await expect(page).toHaveURL(/\/oauth2\/start$/);
    await expect(page.getByRole('heading', { name: 'OIDC sign-in boundary' })).toBeVisible();
});

test('OIDC logout posts CSRF once and clears dialog, selector and late parsed results', async ({
    page,
}) => {
    const fixture = await gateway(page);
    await opened(page);
    await page.locator('nav').getByRole('button', { name: 'Data studio', exact: true }).click();
    await page.evaluate(async () => {
        const { WorkbenchAPI } = await import('/assets/api.js');
        const original = WorkbenchAPI.prototype.json;
        const held = new Promise((resolve) => (window.releaseOldScan = resolve));
        window.oldScanReady = false;
        window.oldScanFinished = false;
        WorkbenchAPI.prototype.json = async function (path, options) {
            const result = await original.call(this, path, options);
            if (path.endsWith('/scan')) {
                window.oldScanReady = true;
                window.oldAPI = this;
                await held;
                window.oldScanFinished = true;
            }
            return result;
        };
    });
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await expect.poll(() => page.evaluate(() => window.oldScanReady)).toBe(true);
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    // Exercise the logout handler while a modal and a parsed old response exist.
    await page.locator('[data-action="sign-out"]').evaluate((button) => button.click());
    await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('#database-selector')).toHaveCount(0);
    expect(fixture.logout).toHaveLength(1);
    expect(fixture.logout[0].method()).toBe('POST');
    expect(fixture.logout[0].headers()['x-csrftoken']).toBe('browser-csrf-fixture');
    expect(fixture.logout[0].headers().authorization).toBeUndefined();
    expect(fixture.logout[0].headers().origin).toBe(site);
    expect(await page.evaluate(() => window.oldAPI.controller.signal.aborted)).toBe(true);
    await page.evaluate(() => window.releaseOldScan());
    await expect.poll(() => page.evaluate(() => window.oldScanFinished)).toBe(true);
    await expect(page.getByRole('table', { name: 'Query results' })).toHaveCount(0);
    await expect(page.locator('.collection-list button')).toHaveCount(0);
    expect(await page.evaluate(() => ({ ...localStorage, ...sessionStorage }))).toEqual({});
});

test('session outage and identity change invalidate existing workspace and dialogs', async ({
    page,
}) => {
    const fixture = await gateway(page);
    await opened(page);
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    fixture.status = 503;
    await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible();
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('#database-selector')).toHaveCount(0);
    fixture.status = 200;
    fixture.session = {
        ...fixture.session,
        subject: '44444444-5555-4666-8777-888888888888',
        organization: { ...fixture.session.organization, name: 'New organization' },
        databases: [{ id: SHARED, name: 'Shared' }],
    };
    await page.getByRole('button', { name: 'Reload access', exact: true }).click();
    await expect(page.locator('#database-selector')).toHaveValue(SHARED);
    await expect(page.locator('.workspace-tag')).toContainText('New organization');
    await page.getByRole('button', { name: 'New collection', exact: true }).click();
    fixture.session = {
        ...fixture.session,
        subject: session.subject,
        organization: session.organization,
        databases: [{ id: ALPHA, name: 'Alpha' }],
    };
    await expect(page.locator('#database-selector')).toHaveValue(ALPHA);
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.locator('.workspace-tag')).toContainText('Pink organization');
});
