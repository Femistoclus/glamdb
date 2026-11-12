import { APIError, WorkbenchAPI, csrfToken } from './api.js';
import { resetConnection } from './model.js';

const ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const text = (value) => typeof value === 'string' && value.trim().length > 0 && value.length <= 256;

function accountURL(value) {
    if (typeof value !== 'string' || value.length > 4096) return false;
    try {
        const url = new URL(value);
        if (
            url.username ||
            url.password ||
            url.search ||
            url.hash ||
            !url.pathname.endsWith('/account/')
        )
            return false;
        return (
            url.protocol === 'https:' ||
            (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))
        );
    } catch {
        return false;
    }
}

export function validateAccessSession(value) {
    const organization = value?.organization;
    if (
        value?.version !== 3 ||
        !Number.isSafeInteger(value.revision) ||
        value.revision < 0 ||
        typeof value.subject !== 'string' ||
        !ID.test(value.subject) ||
        (organization !== null &&
            (!organization ||
                typeof organization.id !== 'string' ||
                !ID.test(organization.id) ||
                !text(organization.name) ||
                !text(organization.login))) ||
        !Array.isArray(value.databases) ||
        value.databases.length > 128 ||
        !accountURL(value.account_url) ||
        value.login_url !== '/oauth2/start' ||
        value.logout_url !== '/workbench/logout'
    )
        throw new Error('The access session is invalid');
    const seen = new Set();
    const databases = value.databases.map((database) => {
        if (
            typeof database?.id !== 'string' ||
            !ID.test(database.id) ||
            !text(database.name) ||
            seen.has(database.id)
        )
            throw new Error('The access session is invalid');
        seen.add(database.id);
        return { id: database.id, name: database.name };
    });
    return {
        version: 3,
        revision: value.revision,
        organization:
            organization === null
                ? null
                : { id: organization.id, name: organization.name, login: organization.login },
        subject: value.subject,
        databases,
        account_url: value.account_url,
        login_url: '/oauth2/start',
        logout_url: '/workbench/logout',
    };
}

export async function loadAccessSession(fetchImpl = globalThis.fetch.bind(globalThis), signal) {
    const response = await fetchImpl('/workbench/session', {
        cache: 'no-store',
        credentials: 'same-origin',
        redirect: 'error',
        headers: { Accept: 'application/json' },
        signal,
    });
    if (!response.ok)
        throw new APIError(response.status, {
            error: {
                code: 'SESSION_UNAVAILABLE',
                message: `Access session is unavailable (${response.status})`,
            },
        });
    return validateAccessSession(await response.json());
}

export async function signOutAccess(fetchImpl = globalThis.fetch.bind(globalThis), signal) {
    const response = await fetchImpl('/workbench/logout', {
        method: 'POST',
        cache: 'no-store',
        credentials: 'same-origin',
        redirect: 'error',
        signal,
        headers: { Accept: 'application/json', 'X-CSRFToken': csrfToken() },
    });
    if (!response.ok)
        throw new APIError(response.status, {
            error: {
                code: 'LOGOUT_UNAVAILABLE',
                message: `Sign out is unavailable (${response.status}); retry to finish signing out`,
            },
        });
    const value = await response.json();
    if (value?.signed_out !== true || value.redirect !== '/oauth2/sign_out?rd=%2F')
        throw new Error('The sign-out response is invalid');
    return '/oauth2/sign_out?rd=%2F';
}

export function selectDatabase(
    state,
    access,
    databaseId,
    { origin = globalThis.location.origin, fetchImpl, controllers = [] } = {},
) {
    access = validateAccessSession(access);
    if (databaseId !== null && !access.databases.some((database) => database.id === databaseId))
        throw new Error('This database is not in your access list');
    const base = new URL(origin).origin;
    const next = {
        ...resetConnection(state),
        base: databaseId === null ? base : `${base}/databases/${encodeURIComponent(databaseId)}`,
        access,
        databaseId,
        databaseLoading: false,
        databaseError: '',
    };
    if (databaseId !== null)
        next.api = new WorkbenchAPI(next.base, '', fetchImpl, undefined, { gateway: true });
    state.api?.close?.();
    for (const controller of controllers) controller?.abort();
    return next;
}
