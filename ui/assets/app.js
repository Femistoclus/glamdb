import { writeEditor, readEditorRows, previewRows, lockWriteForm } from './editor.js';
import { WorkbenchAPI, readMetadataPages, validateViewMetadata } from './api.js';
import { fileDetails } from './file-details.js';
import { loadAccessSession, selectDatabase, signOutAccess } from './access.js';
import { applyTheme, validateTheme, brandAsset } from './theme.js';
import { applyAppearance, readPreference, savePreference } from './appearance.js';
import { sortSnapshots, snapshotCreatedAt } from './snapshot-time.js';
import {
    escapeHTML as e,
    displayValue,
    selectView,
    resetConnection,
    switchDatabase,
    buildPredicate,
    wireRows,
    formRows,
    sessionSnapshots,
    sessionRowCount,
    prepareMutation,
    serializePendingFile,
    readPendingFile,
    schemaSignature,
    supportsFeature,
    schemaTypes,
    creationSchema,
    queryMaxRows,
    storageNotice,
    validateQueryCapabilities,
    parquetCapabilities,
    canonicalWriteSettings,
    sameWriteSettings,
    writeSettingsFromChoices,
    writeSettingsVersion,
    describeCompression,
    verifyCreatedCollection,
    creationFingerprint,
    columnParquetOptions,
    reconcileColumnDictionary,
    describeColumnEffect,
    wireWriteSettings,
    mutationKeys,
    compactionOptions,
} from './model.js';

const $ = (selector) => document.querySelector(selector);
const accessGateway = document.documentElement.dataset.accessGateway === 'true';
const publicDemo = !accessGateway && document.documentElement.dataset.publicDemo === 'true';
let colorMode = readPreference(
    'appearance',
    globalThis.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light',
);
let sidebarCollapsed = readPreference('sidebar-collapsed', 'false') === 'true';
const icon = (name, classes = '') =>
    `<svg class="icon ${classes}" aria-hidden="true"><use href="${new URL('icons.svg', import.meta.url).href}#${name}"></use></svg>`;
const nav = [
    ['overview', 'grid'],
    ['data', 'table'],
    ['schema', 'columns'],
    ['snapshots', 'clock'],
    ['storage', 'database'],
    ['explain', 'sparkle'],
    ['operations', 'activity'],
];
let state = resetConnection({
    base: location.origin,
    epoch: -1,
    theme: null,
});
let connectionController = null,
    viewController = null,
    queryController = null,
    countController = null,
    queryGeneration = 0,
    databaseSelectionGeneration = 0,
    toastTimer = null,
    themeStamp = '',
    configTimer = null,
    serverHealth = null,
    accessRevision = null,
    accessPollController = null,
    signingOut = false;
const labels = (name, fallback = name) => state.theme?.labels?.[name] ?? fallback;
const collectionPath = () => `/collections/${encodeURIComponent(state.collectionId)}`;
const viewQuery = () =>
    state.snapshotId === null ? '' : `?snapshot_id=${encodeURIComponent(state.snapshotId)}`;
const currentCollection = () =>
    state.collections.find((item) => item.collection_id === state.collectionId);
const isKV = () => (state.collection?.mode ?? currentCollection()?.mode) === 'kv';
const mutationAction = () => (isKV() ? 'upsert' : 'append');
const receiptTitle = (action) =>
    ({ append: 'Append', upsert: 'Upsert', delete: 'Delete', compact: 'Compaction' })[action] ??
    'Write';
const actionTitle = (action) =>
    ({
        append: 'Append rows',
        upsert: 'Upsert rows',
        delete: 'Delete keys',
        compact: 'Compact files',
    })[action] ?? 'Write';
const kvEnabled = (action) => isKV() && state.capabilities?.features?.[action] === true;
const number = (value) =>
    value === null || value === undefined
        ? '—'
        : typeof value === 'object'
          ? number(value.value)
          : String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
const fields = () => state.schema?.fields ?? [];
const json = (value) => e(JSON.stringify(value, null, 2));
const badge = (text, kind = '') => `<span class="pill ${kind}">${e(text)}</span>`;
// Parquet write settings controls appear only when the server advertises them.
const parquetSettings = () => parquetCapabilities(state.capabilities);
const defaultCodecLabel = (parquet) => describeCompression(null, parquet);
const dictionaryLocked = 'An explicit encoding turns this column’s dictionary off';
function codecOptions(parquet, selected, emptyLabel) {
    return choiceOptions(
        parquet.codecs.map(({ codec }) => codec),
        selected,
        emptyLabel,
    );
}
function choiceOptions(values, selected, emptyLabel) {
    return `<option value="" ${selected ? '' : 'selected'}>${e(emptyLabel)}</option>${values.map((value) => `<option value="${e(value)}" ${selected === value ? 'selected' : ''}>${e(value)}</option>`).join('')}`;
}
function levelAttributes(parquet, codec, level = null) {
    const range = parquet.codecs.find((item) => item.codec === codec)?.level;
    return range
        ? `min="${range.min}" max="${range.max}" placeholder="default ${range.default}" title="Level ${range.min}–${range.max}; empty uses the codec default" value="${e(level ?? '')}"`
        : 'disabled placeholder="—" value=""';
}
// Collection codec/level plus, where the server supports them, the dictionary
// policy and dictionary page size limit. `codecLabel` names the codec select.
function collectionParquetControls(parquet, settings, codecLabel) {
    const compression = settings?.compression ?? null;
    const dictionary = parquet.dictionary;
    return `<div class="parquet-choice-grid" data-parquet-choice data-parquet-default><div class="field-row"><label class="field">${e(codecLabel)}<select name="parquet-codec" data-parquet-codec>${codecOptions(parquet, compression?.codec, defaultCodecLabel(parquet))}</select></label><label class="field">Level<input name="parquet-level" type="number" step="1" inputmode="numeric" data-parquet-level ${levelAttributes(parquet, compression?.codec, compression?.level)}></label></div>${
        dictionary
            ? `<div class="field-row"><label class="field">Dictionary policy<select name="parquet-dictionary" data-parquet-dictionary>${choiceOptions(dictionary.policies, settings?.dictionary ?? '', `Server default (${dictionary.default_policy ?? 'auto'})`)}</select></label><label class="field">Dictionary page size limit (bytes)<input name="parquet-page-limit" type="number" step="1" inputmode="numeric" data-parquet-page-limit min="${dictionary.page_size_limit.min}" max="${dictionary.page_size_limit.max}" placeholder="default ${dictionary.page_size_limit.default}" title="${dictionary.page_size_limit.min}–${dictionary.page_size_limit.max} bytes; empty uses the server default" value="${e(settings?.dictionary_page_size_limit ?? '')}"></label></div>`
            : ''
    }</div>`;
}
// Per-column dictionary and encoding selects, filtered by the column type.
// Choosing an encoding locks the dictionary select: the server turns that
// column's dictionary off, so an incompatible policy is cleared.
function columnDictionarySelect(parquet, type, column, label) {
    if (!parquet.dictionary) return '';
    const options = columnParquetOptions(parquet, type);
    const encoding = options.encodings.includes(column.encoding) ? column.encoding : '';
    const dictionary = reconcileColumnDictionary(
        parquet,
        type,
        encoding,
        options.dictionaries.includes(column.dictionary) ? column.dictionary : '',
    );
    return `<select data-parquet-dictionary aria-label="Dictionary policy for ${e(label)}" ${encoding ? `disabled title="${e(dictionaryLocked)}"` : ''}>${choiceOptions(options.dictionaries, dictionary, 'Default dictionary')}</select>`;
}
function columnEncodingSelect(parquet, type, column, label) {
    if (!parquet.encodings.length) return '';
    const options = columnParquetOptions(parquet, type);
    const encoding = options.encodings.includes(column.encoding) ? column.encoding : '';
    return `<select data-parquet-encoding aria-label="Encoding for ${e(label)}">${choiceOptions(options.encodings, encoding, 'Default encoding')}</select>`;
}
const columnType = (container) =>
    container.querySelector('[name=field-type]')?.value ?? container.dataset.parquetType;
function syncParquetChoice(container) {
    const parquet = parquetSettings();
    if (!container || !parquet) return;
    const codec = container.querySelector('[data-parquet-codec]');
    const input = container.querySelector('[data-parquet-level]');
    if (codec && input) {
        const range = parquet.codecs.find((item) => item.codec === codec.value)?.level;
        input.disabled = !range;
        if (range) {
            input.min = String(range.min);
            input.max = String(range.max);
            input.placeholder = `default ${range.default}`;
            input.title = `Level ${range.min}–${range.max}; empty uses the codec default`;
        } else {
            input.value = '';
            input.placeholder = '—';
            input.removeAttribute('title');
        }
    }
    const encoding = container.querySelector('[data-parquet-encoding]');
    const dictionary = container.querySelector('[data-parquet-dictionary]');
    if (encoding && dictionary && !container.hasAttribute('data-parquet-default')) {
        dictionary.value = reconcileColumnDictionary(
            parquet,
            columnType(container),
            encoding.value,
            dictionary.value,
        );
        dictionary.disabled = Boolean(encoding.value);
        if (encoding.value) dictionary.title = dictionaryLocked;
        else dictionary.removeAttribute('title');
    }
}
// A changed column type changes the valid encodings and dictionary policies.
function refreshColumnOptions(row) {
    const parquet = parquetSettings();
    if (!parquet) return;
    const options = columnParquetOptions(parquet, columnType(row));
    for (const [selector, values, label] of [
        ['[data-parquet-encoding]', options.encodings, 'Default encoding'],
        ['[data-parquet-dictionary]', options.dictionaries, 'Default dictionary'],
    ]) {
        const select = row.querySelector(selector);
        if (select)
            select.innerHTML = choiceOptions(
                values,
                values.includes(select.value) ? select.value : '',
                label,
            );
    }
    syncParquetChoice(row);
}
function readParquetChoice(container) {
    const level = container?.querySelector('[data-parquet-level]');
    const value = (selector) => container?.querySelector(selector)?.value ?? '';
    return {
        codec: value('[data-parquet-codec]'),
        level: level && !level.disabled ? level.value : '',
        dictionary: value('[data-parquet-dictionary]'),
        encoding: value('[data-parquet-encoding]'),
        pageSizeLimit: value('[data-parquet-page-limit]'),
    };
}

function toast(message) {
    const node = $('#toast');
    node.textContent = message;
    node.classList.add('visible');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => node.classList.remove('visible'), 4500);
}
function report(error) {
    if (accessGateway && [401, 403].includes(error.status)) {
        resetSession();
        accessLogin('Sign-in is no longer authorized. Sign in to continue.');
        return;
    }
    if (error.name !== 'AbortError') toast(error.message ?? 'Something went wrong');
}
function resetSession({ access, databaseId = null, database } = {}) {
    databaseSelectionGeneration++;
    const controllers = [
        connectionController,
        viewController,
        queryController,
        countController,
        accessPollController,
    ];
    if (access)
        state = selectDatabase(state, access, databaseId, { origin: location.origin, controllers });
    else {
        state.api?.close?.();
        for (const controller of controllers) controller?.abort();
        state = database === undefined ? resetConnection(state) : switchDatabase(state, database);
    }
    connectionController =
        viewController =
        queryController =
        countController =
        accessPollController =
            null;
    queryGeneration++;
    creationFields = [];
    $('#modal').close();
    $('#modal').replaceChildren();
    clearTimeout(toastTimer);
    $('#toast').classList.remove('visible');
    $('#toast').replaceChildren();
}
function setBusy(button, busy) {
    if (button) {
        button.disabled = busy;
        button.setAttribute('aria-busy', String(busy));
    }
}
function download(name, bytes, type) {
    const url = URL.createObjectURL(new Blob([bytes], { type }));
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function countSnapshotRows(snapshot) {
    return snapshot.receipt?.receipt_version === 3 || snapshot.mutation_kind
        ? `${snapshot.rows_written ?? snapshot.receipt?.rows_written ?? '0'} written / ${snapshot.rows_removed ?? snapshot.receipt?.rows_removed ?? '0'} removed`
        : (snapshot.rows_added ?? snapshot.rows_appended ?? '—');
}

function syncQueryLimit() {
    const maximum = queryMaxRows(state.capabilities, state.theme);
    state.limit = Math.min(state.limit, maximum);
    const input = $('#query-form input[name="limit"]');
    if (input) {
        input.max = String(maximum);
        if (input.value !== '' && Number(input.value) > maximum) input.value = String(maximum);
    }
}
async function loadTheme(force = false) {
    const epoch = state.epoch;
    try {
        const response = await fetch(
            publicDemo ? new URL('../config/theme.json', import.meta.url) : '/workbench/config',
            {
                cache: 'no-store',
                ...(accessGateway
                    ? {
                          credentials: 'same-origin',
                          redirect: 'error',
                          signal: AbortSignal.timeout(5000),
                      }
                    : {}),
            },
        );
        if (signingOut || (accessGateway && epoch !== state.epoch)) return;
        if (accessGateway && [401, 403].includes(response.status)) {
            resetSession();
            accessLogin('Sign-in is no longer authorized. Sign in to continue.');
            throw new Error('Sign-in is no longer authorized.');
        }
        if (!response.ok) {
            if (accessGateway && state.theme) {
                resetSession();
                accessLogin('Access service is unavailable. Reload access when it recovers.');
            }
            throw new Error('Theme file is invalid; keeping the last valid theme.');
        }
        const revision = response.headers.get('X-GlamDB-Access-Revision');
        if (accessGateway && revision !== null && /^\d+$/.test(revision)) {
            const changed = accessRevision !== null && accessRevision !== revision;
            accessRevision = revision;
            if (changed && state.access && !signingOut) await reloadAccess();
        }
        if (accessGateway && !force && state.access) await pollAccess();
        const config = validateTheme(await response.json());
        const stamp = JSON.stringify(config);
        if (force || stamp !== themeStamp) {
            state.theme = config;
            themeStamp = stamp;
            applyTheme(config);
            applyAppearance(config, colorMode);
            syncQueryLimit();
            updateThemeText();
        }
        $('#theme-warning').hidden = true;
    } catch (error) {
        if (signingOut || (accessGateway && epoch !== state.epoch)) return;
        if (accessGateway && state.access && !signingOut) {
            resetSession();
            accessLogin('Access service is unavailable. Reload access when it recovers.');
        }
        const warning = $('#theme-warning');
        warning.textContent = error.message;
        warning.hidden = false;
        if (!state.theme) throw error;
    }
}
function updateThemeText() {
    document.title = `${state.theme.brand.name} · ${state.theme.brand.tagline}`;
    document.querySelectorAll('[data-label]').forEach((node) => {
        node.textContent = labels(node.dataset.label, node.textContent);
    });
    document.querySelectorAll('[data-brand]').forEach((node) => {
        node.textContent = state.theme.brand[node.dataset.brand] ?? '';
    });
    document.querySelectorAll('[data-brand-logo]').forEach((node) => {
        const key = node.dataset.brandLogo;
        if (state.theme.brand[key]) {
            node.src = brandAsset(state.theme, key);
            if (key === 'wordmark') node.alt = state.theme.brand.name;
        }
    });
    if (state.theme.brand.mark) $('#brand-favicon').href = brandAsset(state.theme, 'mark');
}
function brand() {
    if (state.theme.brand.wordmark)
        return `<div class="brand"><img class="brand-wordmark" data-brand-logo="wordmark" src="${e(brandAsset(state.theme, 'wordmark'))}" alt="${e(state.theme.brand.name)}"><span class="brand-name sr-only" data-brand="name">${e(state.theme.brand.name)}</span></div>`;
    return `<div class="brand">${icon('bow', 'decor-bow')}<span class="brand-name" data-brand="name">${e(state.theme.brand.name)}</span></div>`;
}
function login() {
    $('#app').innerHTML =
        `<main id="main-content" class="login-screen"><section class="login-art">${brand()}
    <h1>Big ideas.<br>Beautiful data.</h1>${icon('bow', 'hero-bow decor-bow')}${icon('sparkle', 'hero-star one decor-sparkle')}${icon('kiss', 'hero-kiss decor-kiss')}
    <p class="login-copy-bottom" data-label="footer">${e(labels('footer'))}</p></section>
    <section class="login-panel"><div class="login-box"><p class="eyebrow" data-brand="eyebrow">${e(state.theme.brand.eyebrow)}</p>
    <h2 data-label="connect_title">${e(labels('connect_title'))}</h2><p data-label="connect_copy">${e(labels('connect_copy'))}</p>
    <form id="connect-form"><label class="field">Server URL<input name="base" type="url" value="${e(state.base)}" required autocomplete="url"></label>
    <label class="field">Access token<input name="token" type="password" required autocomplete="off" placeholder="Your bearer token"></label>
    <button class="button primary" type="submit"><span data-label="connect_button">${e(labels('connect_button'))}</span>${icon('arrow')}</button></form>
    <p class="login-error" id="login-error" role="alert"></p><p class="login-footnote">Use the token from the server's GLAMDB_TOKEN environment.<br><span id="login-storage">${e(storageNotice(serverHealth))}</span></p></div></section></main>`;
}
async function loadHealth() {
    try {
        const response = await fetch('/healthz', { cache: 'no-store' });
        if (response.ok) serverHealth = await response.json();
    } catch {
        // Unavailable health keeps the neutral storage notice.
    }
    const notice = $('#login-storage');
    if (notice) notice.textContent = storageNotice(serverHealth);
}

async function databaseWorkspace(database = null) {
    const { base, token } = state;
    resetSession({ database });
    state.base = base;
    state.token = token;
    state.database = database;
    state.managed = true;
    const api = new WorkbenchAPI(
        database
            ? `${base.replace(/\/$/, '')}/databases/${encodeURIComponent(database.database_id)}`
            : base,
        token,
    );
    state.api = api;
    const controller = new AbortController();
    connectionController = controller;
    // Remove the previous database's controls before awaiting the new session.
    $('#app').innerHTML = '<main class="main"><p role="status">Loading workspace…</p></main>';
    try {
        const capabilities = await api.json('/capabilities', { signal: controller.signal });
        if (state.api !== api || controller.signal.aborted) return;
        state.capabilities = capabilities;
        syncQueryLimit();
        if (database) await refreshWorkspace();
        else await databaseList();
    } catch (error) {
        if (state.api !== api) return;
        if (database) {
            await databaseWorkspace();
            report(error);
        } else {
            $('#app').innerHTML =
                `<header class="topbar">${brand()}<div class="top-actions"><button class="text-button" data-action="databases">Refresh databases</button><button class="text-button" data-action="disconnect">Disconnect</button></div></header>
                <main id="main-content" class="main"><h1>Database list unavailable</h1><p role="alert">${e(error.message)}</p><p>Refresh to try again. Your saved operations remain available in this session.</p></main>`;
        }
    }
}
async function databaseList() {
    const api = state.api,
        signal = connectionController.signal;
    const result = await api.json('/databases', { signal });
    if (state.api !== api || signal.aborted) return;
    $('#app').innerHTML =
        `<header class="topbar">${brand()}<div class="top-actions"><button class="text-button" data-action="databases">Refresh databases</button><button class="text-button" data-action="disconnect">Disconnect</button></div></header>
    <main id="main-content" class="main">${heading('Your databases', 'Create a database or open an existing workspace.', false)}
    <section class="panel"><div class="panel-body"><form id="database-form" class="button-row"><label class="field">Database name<input name="name" required maxlength="128" autocomplete="off"></label><button class="button primary" type="submit">Create database</button></form></div></section>
    <section class="panel"><div class="table-wrap"><table aria-label="Databases"><thead><tr><th>Database</th><th>State</th><th>Actions</th></tr></thead><tbody>${result.items.map((db) => `<tr><td><strong>${e(db.name)}</strong><p class="small mono">${e(db.database_id)}</p>${db.error ? `<p role="alert">${e(db.error.message)}</p>` : ''}</td><td>${e(db.state)}</td><td><div class="button-row"><button class="button small" data-action="database-use" data-id="${e(db.database_id)}" ${['open', 'closed'].includes(db.state) ? '' : 'disabled'}>${db.state === 'open' ? 'Use database' : 'Open database'}</button><button class="button small" data-action="database-close" data-id="${e(db.database_id)}" ${['open', 'error'].includes(db.state) || (db.state === 'closing' && db.error) ? '' : 'disabled'}>Close database</button><button class="button small" data-action="database-reconcile" data-id="${e(db.database_id)}" ${['open', 'error'].includes(db.state) ? '' : 'disabled'}>Reconcile</button></div></td></tr>`).join('')}</tbody></table></div>${result.items.length ? '' : '<p class="result-empty">No databases yet. Create your first database above.</p>'}</section></main>`;
}
async function databaseAction(target) {
    const api = state.api,
        signal = connectionController.signal;
    const action = target.dataset.action.replace('database-', '');
    const selection = action === 'use' ? ++databaseSelectionGeneration : null;
    const current = () =>
        state.api === api &&
        !signal.aborted &&
        (selection === null || selection === databaseSelectionGeneration);
    setBusy(target, true);
    try {
        const db =
            action === 'use'
                ? await api.selectDatabase(target.dataset.id, signal)
                : await api.json(`/databases/${encodeURIComponent(target.dataset.id)}/${action}`, {
                      method: 'POST',
                      body: {},
                      signal,
                  });
        if (!current()) return;
        if (action === 'use') await databaseWorkspace(db);
        else await databaseList();
    } catch (error) {
        if (!current()) return;
        await databaseList();
        report(error);
    } finally {
        setBusy(target, false);
    }
}
async function createDatabase(form) {
    const api = state.api,
        signal = connectionController.signal;
    const button = form.querySelector('button');
    const selection = ++databaseSelectionGeneration;
    setBusy(button, true);
    try {
        const db = await api.json('/databases', {
            method: 'POST',
            body: { name: form.elements.name.value },
            signal,
        });
        if (state.api === api && !signal.aborted && selection === databaseSelectionGeneration)
            await databaseWorkspace(db);
    } finally {
        setBusy(button, false);
    }
}

function databaseOptions() {
    return (state.access?.databases ?? [])
        .map(
            (database) =>
                `<option value="${e(database.id)}" ${state.databaseId === database.id ? 'selected' : ''}>${e(database.name)}</option>`,
        )
        .join('');
}
function accessLogin(message = '') {
    const access = state.access;
    const organization = access?.organization;
    const available = access?.databases.length > 0;
    $('#app').innerHTML =
        `<main id="main-content" class="login-screen"><section class="login-art">${brand()}
    <h1>Big ideas.<br>Beautiful data.</h1>${icon('bow', 'hero-bow decor-bow')}${icon('sparkle', 'hero-star one decor-sparkle')}${icon('kiss', 'hero-kiss decor-kiss')}
    <p class="login-copy-bottom" data-label="footer">${e(labels('footer'))}</p></section>
    <section class="login-panel"><div class="login-box"><p class="eyebrow" data-brand="eyebrow">${e(state.theme.brand.eyebrow)}</p>
    <h2>Your databases</h2>${organization ? `<p>Organization: <strong>${e(organization.name)}</strong><br>Signed in as ${e(organization.login)}</p>` : access ? '<p>Your account has no organization workspace.</p>' : '<p>Sign in through your organization to open a database.</p>'}
    ${available ? `<form id="database-form"><label class="field">Database<select name="database" required><option value="">Choose a database</option>${databaseOptions()}</select></label><button class="button primary" type="submit">Open database${icon('arrow')}</button></form>` : access ? '<p class="notice">No databases are available for this account.</p>' : ''}
    <p class="login-error" id="login-error" role="alert">${e(message)}</p>
    <div class="button-row">${!access ? `<button class="button primary" data-action="sign-in" ${signingOut ? 'disabled' : ''}>Sign in</button>` : `<a class="button" href="${e(access.account_url)}">Account</a><button class="button" data-action="sign-out" ${signingOut ? 'disabled' : ''}>Sign out</button>`}<button class="button" data-action="reload-access" ${signingOut ? 'disabled' : ''}>Reload access</button></div>
    <p class="login-footnote">Organization sign-in, account settings and sessions are managed by the identity provider.</p></div></section></main>`;
}
async function signOut() {
    if (signingOut) return;
    signingOut = true;
    clearInterval(configTimer);
    resetSession();
    accessLogin('Signing out…');
    try {
        await signOutAccess();
        signingOut = false;
        accessLogin('Signed out.');
        location.assign('/oauth2/sign_out?rd=%2F');
    } catch (error) {
        signingOut = false;
        accessLogin(error.message ?? 'Sign out could not finish. Retry when the service recovers.');
        configTimer = setInterval(() => loadTheme(), 2000);
    }
}
async function pollAccess() {
    if (!state.access || signingOut || accessPollController) return;
    const controller = new AbortController(),
        epoch = state.epoch;
    accessPollController = controller;
    try {
        const access = await loadAccessSession(
            globalThis.fetch.bind(globalThis),
            AbortSignal.any([controller.signal, AbortSignal.timeout(5000)]),
        );
        if (
            controller !== accessPollController ||
            controller.signal.aborted ||
            state.epoch !== epoch ||
            signingOut
        )
            return;
        if (JSON.stringify(access) !== JSON.stringify(state.access)) {
            const databaseId = access.databases.some((database) => database.id === state.databaseId)
                ? state.databaseId
                : (access.databases[0]?.id ?? null);
            resetSession({ access });
            if (databaseId) await connectDatabase(databaseId);
            else accessLogin();
        }
    } catch (error) {
        if (
            controller !== accessPollController ||
            controller.signal.aborted ||
            state.epoch !== epoch ||
            signingOut
        )
            return;
        resetSession();
        accessLogin(
            error.status === 401
                ? 'Sign-in expired. Sign in to continue.'
                : (error.message ?? 'Access session is unavailable.'),
        );
    } finally {
        if (controller === accessPollController) accessPollController = null;
    }
}
async function connectDatabase(databaseId) {
    const access = state.access;
    resetSession({ access, databaseId });
    const api = state.api;
    const controller = new AbortController();
    connectionController = controller;
    state.databaseLoading = true;
    chrome();
    try {
        const capabilities = await api.json('/capabilities', { signal: controller.signal });
        if (api !== state.api || controller.signal.aborted) return;
        state.capabilities = capabilities;
        await refreshWorkspace();
        if (api !== state.api || controller.signal.aborted) return;
        state.databaseLoading = false;
        chrome();
    } catch (error) {
        if (api !== state.api || controller.signal.aborted) return;
        if ([401, 403].includes(error.status)) {
            report(error);
            return;
        }
        resetSession({ access, databaseId });
        state.databaseError = error.message ?? 'The database could not be reached';
        chrome();
    }
}
async function reloadAccess() {
    if (signingOut) return;
    resetSession();
    const controller = new AbortController();
    connectionController = controller;
    accessLogin('Loading your access list…');
    try {
        const access = await loadAccessSession(
            globalThis.fetch.bind(globalThis),
            controller.signal,
        );
        if (controller !== connectionController || controller.signal.aborted) return;
        accessRevision = String(access.revision);
        resetSession({ access });
        if (access.databases.length) await connectDatabase(access.databases[0].id);
        else accessLogin();
    } catch (error) {
        if (controller !== connectionController || controller.signal.aborted) return;
        resetSession();
        accessLogin(error.message);
    }
}

function chrome() {
    $('#app').innerHTML =
        `<div class="shell" data-collapsed="${sidebarCollapsed}"><aside class="sidebar" id="sidebar" aria-label="Workspace navigation"><div class="sidebar-head">${brand()}${state.theme.brand.mark ? `<img class="collapsed-brand" src="${e(brandAsset(state.theme, 'mark'))}" alt="${e(state.theme.brand.name)}">` : ''}<button class="icon-button sidebar-collapse" data-action="collapse-sidebar" aria-controls="sidebar" aria-expanded="${!sidebarCollapsed}" aria-label="${sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}" title="${sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}">${icon('chevron')}</button></div>
    <p class="brand-tagline" data-brand="tagline">${e(state.theme.brand.tagline)}</p><div class="workspace-tag"><span class="status-dot"></span>
    <div><strong>${accessGateway ? e(state.access?.organization?.name ?? 'Account') : e(state.database?.name ?? 'My workspace')}</strong><small>${accessGateway ? e(state.access?.organization?.login ?? 'Signed in') : e(new URL(state.base).host)} · ${state.databaseError ? 'UNAVAILABLE' : state.databaseLoading ? 'CONNECTING' : state.capabilities?.mock ? 'MOCK' : 'CONNECTED'}</small></div></div>
    ${accessGateway ? `<label class="field" style="margin:0 7px 20px">Database<select id="database-selector" aria-label="Database">${databaseOptions()}</select></label>` : ''}
    <nav class="nav">${nav.map(([key, symbol]) => `<button aria-label="${e(labels(`nav_${key}`))}" title="${e(labels(`nav_${key}`))}" data-action="nav" data-tab="${key}" class="${state.tab === key ? 'active' : ''}" ${state.tab === key ? 'aria-current="page"' : ''}>${icon(symbol)}<span data-label="nav_${key}">${e(labels(`nav_${key}`))}</span></button>`).join('')}</nav>
    <div class="collection-heading"><span data-label="collections">${e(labels('collections'))}</span><button class="icon-button" data-action="create" aria-label="New collection" ${accessGateway && (state.databaseLoading || state.databaseError) ? 'disabled' : ''}>${icon('plus')}</button></div>
    <div class="collection-list">${state.collections.map((item) => `<button aria-label="${e(item.name)}" title="${e(item.name)}" data-action="select-collection" data-id="${e(item.collection_id)}" class="${state.collectionId === item.collection_id ? 'selected' : ''}">${icon('table')}<span>${e(item.name)}</span></button>`).join('')}</div>
    <div class="sidebar-footer"><button class="text-button theme-toggle" data-action="toggle-appearance" aria-label="Switch to ${colorMode === 'dark' ? 'light' : 'dark'} theme" title="Switch to ${colorMode === 'dark' ? 'light' : 'dark'} theme">${icon('palette')}<span>${colorMode === 'dark' ? 'Dark theme' : 'Light theme'}</span></button></div></aside>
    <div class="workspace"><header class="topbar"><div class="breadcrumb"><button class="icon-button menu-toggle" data-action="menu" aria-label="Toggle navigation">${icon('menu')}</button><span>Workspace</span>${icon('chevron')}<strong>${e(currentCollection()?.name ?? 'Your collections')}</strong></div>
    <div class="top-actions">${state.managed ? '<button class="text-button" data-action="databases">Databases</button>' : ''}${publicDemo ? '' : '<button class="text-button" data-action="pending-requests">Pending requests</button>'}<button class="text-button docs-button" data-action="about">Quick guide</button><button class="icon-button" data-action="refresh" aria-label="Refresh workspace">${icon('refresh')}</button>${accessGateway ? `<a class="text-button" href="${e(state.access.account_url)}">Account</a>` : ''}<button class="text-button" data-action="${accessGateway ? 'sign-out' : 'disconnect'}">${accessGateway ? 'Sign out' : publicDemo ? 'Reset demo' : 'Disconnect'}</button><span class="avatar" aria-label="Workspace avatar">${state.theme.brand.mark ? `<img data-brand-logo="mark" class="brand-mark" src="${e(brandAsset(state.theme, 'mark'))}" alt="">` : 'G'}</span></div></header>
    <main id="main-content" class="main">${state.capabilities?.mock ? `<div class="mock-banner">${icon('sparkle')}<span data-label="${publicDemo ? 'public_demo_notice' : 'mock_notice'}">${e(labels(publicDemo ? 'public_demo_notice' : 'mock_notice'))}</span></div>` : ''}<div id="view"></div>
    <footer class="page-footer"><span>GlamDB · <span data-brand="tagline">${e(state.theme.brand.tagline)}</span></span><span>${isKV() ? 'KV mode · one full row per key' : 'Append mode · duplicate keys preserved'} ${icon('sparkle', 'decor-sparkle')}</span></footer></main></div></div>`;
    renderView();
}
function availableSnapshots() {
    return sortSnapshots(
        supportsFeature(state.capabilities, 'snapshots')
            ? state.snapshots
            : sessionSnapshots(state.operations, state.collectionId),
    );
}
function snapshotControl() {
    return `<label class="snapshot-control">${icon('clock')}<span class="sr-only">Snapshot</span><select id="snapshot-selector" aria-label="Snapshot"><option value="latest" ${state.snapshotId === null ? 'selected' : ''}>${e(labels('latest'))}</option><option value="initial" ${state.snapshotId === 'initial' ? 'selected' : ''}>Initial empty view</option>${availableSnapshots()
        .map(
            (s) =>
                `<option value="${e(s.snapshot_id)}" ${state.snapshotId === s.snapshot_id ? 'selected' : ''}>Snapshot ${e(s.snapshot_id)}</option>`,
        )
        .join('')}</select></label>`;
}
function heading(title, copy = '', snapshot = true) {
    return `<div class="view-heading"><div><p class="eyebrow" data-brand="eyebrow">${e(state.theme.brand.eyebrow)}</p><h1 id="view-title" tabindex="-1">${e(title)}</h1>${copy ? `<p>${e(copy)}</p>` : ''}</div>${snapshot && state.collectionId ? snapshotControl() : ''}</div>`;
}
function emptyView() {
    return `<section class="panel empty">${icon('table')}<h2 data-label="empty_title">${e(labels('empty_title'))}</h2><p data-label="empty_copy">${e(labels('empty_copy'))}</p><button class="button primary" data-action="create">${icon('plus')}New collection</button></section>`;
}
function schemaSummary(label = 'Overview schema') {
    return `<div class="table-wrap schema-preview"><table aria-label="${e(label)}"><thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead><tbody>${fields()
        .map(
            (field) =>
                `<tr><td>${e(field.name)}</td><td><span class="type">${e(field.type)}</span></td><td>${field.nullable ? 'Yes' : 'Required'}</td></tr>`,
        )
        .join('')}</tbody></table></div>`;
}
function overview() {
    const stats = state.stats ?? {};
    const rows =
        stats.rows ??
        (state.rowMeasurement?.snapshot_id === state.resolvedSnapshot
            ? state.rowMeasurement.rows
            : null) ??
        (state.resolvedSnapshot === 'initial'
            ? '0'
            : sessionRowCount(state.operations, state.collectionId, state.resolvedSnapshot));
    const databaseName =
        state.database?.name ??
        state.access?.databases?.find((db) => db.id === state.databaseId)?.name ??
        'Connected store';
    return `${heading(labels('overview_title'), labels('overview_copy'))}${
        state.collectionId
            ? `<div class="overview-summary"><section class="panel"><div class="panel-header"><div><h2>Collection schema</h2><p>${e(currentCollection()?.name)} · ${fields().length} columns</p></div><button class="button small" data-action="nav" data-tab="schema">Full schema</button></div>${schemaSummary()}</section><div class="overview-facts"><div class="database-context"><strong>${e(databaseName)}</strong><span>${e(currentCollection()?.name)} · snapshot <code>${e(state.resolvedSnapshot ?? 'initial')}</code></span></div><section class="metrics overview-metrics" aria-label="Collection statistics">${[
                  ['Rows', rows, 'table'],
                  ['Columns', fields().length, 'columns'],
                  [
                      'Snapshots',
                      stats.retained_snapshots ??
                          (supportsFeature(state.capabilities, 'snapshots')
                              ? state.snapshots.length
                              : null),
                      'clock',
                  ],
                  [
                      'Data files',
                      stats.data_files ??
                          (supportsFeature(state.capabilities, 'files')
                              ? state.files.length
                              : null),
                      'database',
                  ],
              ]
                  .map(
                      ([label, value, symbol]) =>
                          `<div class="metric"><div class="metric-icon">${icon(symbol)}</div><div><strong>${number(value)}</strong><span>${label.toUpperCase()}</span></div></div>`,
                  )
                  .join(
                      '',
                  )}</section>${!supportsFeature(state.capabilities, 'stats') ? `<div class="notice row-count-notice"><span>Count rows in the selected snapshot.</span><button class="button small" data-action="${countController ? 'cancel-count' : 'count-rows'}">${countController ? 'Cancel count' : 'Count rows'}</button></div>` : ''}</div></div><section class="panel overview-history"><div class="panel-header"><h2>Recent snapshots</h2><button class="text-button" data-action="nav" data-tab="snapshots">See all snapshots →</button></div><div class="panel-body">${snapshotList(3)}</div></section>`
            : emptyView()
    }`;
}
function snapshotList(max = 100) {
    const observed = !supportsFeature(state.capabilities, 'snapshots');
    const snapshots = availableSnapshots();
    return `${observed ? '<p class="muted small">Observed in this session. Complete history is not available.</p>' : ''}${
        snapshots.length
            ? snapshots
                  .slice()
                  .slice(0, max)
                  .map(
                      (s) =>
                          `<div class="snapshot-item"><span class="snapshot-marker"></span><div><strong>${e(s.mutation_kind ?? 'append')} snapshot</strong><p><code>${e(s.snapshot_id)}</code></p></div>${badge(`+${countSnapshotRows(s)} rows`)}</div>`,
                  )
                  .join('')
            : `<p class="muted small">${observed ? 'No committed appends observed in this session.' : 'No snapshot yet. Your initial view is empty.'}</p>`
    }`;
}
function readQuery() {
    draftWrite();
    const form = $('#query-form');
    if (form) {
        state.projection = [...form.querySelectorAll('input[name="column"]:checked')].map(
            (node) => node.value,
        );
        state.clauses = [...form.querySelectorAll('[data-condition]')].map((row) => ({
            field: row.querySelector('[name="field"]').value,
            op: row.querySelector('[name="op"]').value,
            value: row.querySelector('[name="value"]').value,
        }));
        state.join = form.elements.join.value;
        state.negate = form.elements.negate.checked;
        state.limit = Number(form.elements.limit.value);
    }
    if (!state.projection.length) throw new Error('Choose at least one output column');
    const maximum = queryMaxRows(state.capabilities, state.theme);
    if (!Number.isInteger(state.limit) || state.limit < 0 || state.limit > maximum)
        throw new Error(`Use a limit between 0 and ${maximum} in the browser`);
    const body = {
        projection: state.projection,
        predicate: buildPredicate(state.clauses, state.join, state.negate, fields()),
        limit: state.limit,
    };
    if (state.snapshotId !== null) body.snapshot_id = state.snapshotId;
    validateQueryCapabilities(state.capabilities, body);
    return body;
}
function dataView() {
    const predicates = state.capabilities?.predicate_ops;
    const composite = !predicates || predicates.includes('and') || predicates.includes('or');
    const columns = state.projection.length
        ? state.projection
        : fields().map((field) => field.name);
    return `${heading(labels('data_title'), labels('data_copy'))}<details class="panel schema-strip"><summary>Schema · ${fields().length} columns · ${e(currentCollection()?.name)}</summary>${schemaSummary('Data studio schema')}</details><div class="data-editors"><section class="panel read-panel" aria-labelledby="read-title"><div class="panel-header"><div><h2 id="read-title" data-label="read_title">${e(labels('read_title', 'Read rows'))}</h2><p id="read-snapshot-note">Choose columns and optional filters. Snapshot ${e(state.snapshotId ?? 'latest')}.</p></div></div><div class="panel-body"><form id="query-form" class="query-controls"><div><label>Columns to read</label><div class="column-choices" style="margin-top:9px">${fields()
        .map(
            (field) =>
                `<label class="column-check"><input type="checkbox" name="column" value="${e(field.name)}" ${columns.includes(field.name) ? 'checked' : ''}>${e(field.name)}</label>`,
        )
        .join('')}</div></div>
    <div><div class="filter-toolbar"><label>Filter your rows</label><div class="button-row" ${composite ? '' : 'hidden'}><select name="join" aria-label="Combine conditions"><option value="and" ${state.join === 'and' ? 'selected' : ''}>Match all · AND</option><option value="or" ${state.join === 'or' ? 'selected' : ''}>Match any · OR</option></select><label class="small"><input type="checkbox" name="negate" ${state.negate ? 'checked' : ''}> Negate</label></div></div>
    ${state.clauses
        .map(
            (c, i) =>
                `<div class="filter-row" data-condition="${i}"><select name="field" aria-label="Filter column ${i + 1}">${fields()
                    .map(
                        (f) =>
                            `<option value="${e(f.name)}" ${c.field === f.name ? 'selected' : ''}>${e(f.name)}</option>`,
                    )
                    .join('')}</select><select name="op" aria-label="Filter operator ${i + 1}">${[
                    ['eq', 'equals'],
                    ['ne', 'does not equal'],
                    ['lt', 'less than'],
                    ['le', 'at most'],
                    ['gt', 'greater than'],
                    ['ge', 'at least'],
                    ['in', 'is in array'],
                    ['is_null', 'is NULL'],
                    ['is_not_null', 'is not NULL'],
                ]
                    .filter(([op]) => !predicates || predicates.includes(op))
                    .map(
                        ([op, label]) =>
                            `<option value="${op}" ${c.op === op ? 'selected' : ''}>${label}</option>`,
                    )
                    .join(
                        '',
                    )}</select><input name="value" aria-label="Filter value ${i + 1}" value="${e(c.value)}" placeholder="A value, in its original type"><button type="button" class="icon-button" data-action="remove-condition" data-index="${i}" aria-label="Remove condition ${i + 1}">${icon('close')}</button></div>`,
        )
        .join('')}
    <button type="button" class="text-button" data-action="add-condition" ${!composite && state.clauses.length ? 'disabled title="This server supports one comparison per query"' : ''}>+ Add a condition</button></div><div class="query-footer"><label>Result limit<input name="limit" type="number" min="0" max="${queryMaxRows(state.capabilities, state.theme)}" value="${state.limit}"></label><div class="button-row"><button type="button" class="button" data-action="explain-query" ${supportsFeature(state.capabilities, 'explain') ? '' : 'disabled title="Unavailable on this server"'}>${icon('sparkle')}Explain</button><button type="submit" class="button primary" id="run-query">${icon('arrow')}<span data-label="run_query">${e(labels('run_query'))}</span></button></div></div><details class="editor-json"><summary>Query JSON</summary><pre id="query-json" class="json-block">${e(queryPreview())}</pre><button type="button" class="text-button" data-action="download-query-json">Download JSON</button></details></form></div><div id="read-results" aria-live="polite">${state.result ? resultTable() : '<div class="notice read-ready">Read rows to see the selected snapshot.</div>'}</div></section>${writePanel()}</div>`;
}
function queryPreview() {
    try {
        const body = {
            projection: state.projection.length ? state.projection : fields().map((f) => f.name),
            predicate: buildPredicate(state.clauses, state.join, state.negate, fields()),
            limit: state.limit,
        };
        if (state.snapshotId !== null) body.snapshot_id = state.snapshotId;
        return JSON.stringify(body, null, 2);
    } catch (error) {
        return error.message;
    }
}
function writePanel() {
    const operation =
        pendingForCollection(state.collectionId) ??
        state.operations.find(
            (op) =>
                op.operationId === state.writeOperationId && op.collectionId === state.collectionId,
        );
    const controls = isKV()
        ? `<div class="button-row" style="margin:12px 0">${kvEnabled('lookup') ? '<button class="button small" data-action="lookup-keys">Lookup keys</button>' : ''}${kvEnabled('delete') ? '<button class="button small" data-action="delete-keys">Delete keys</button>' : ''}${kvEnabled('compact') ? '<button class="button small" data-action="compact-files">Compact files</button>' : ''}</div>`
        : '';
    if (operation && ['delete', 'compact'].includes(operation.action))
        return `<section class="panel write-panel" data-write-panel><div class="panel-body"><h2>${e(actionTitle(operation.action))}</h2><p>A mutation is pending. Keep its original ID and payload.</p><code>${e(operation.operationId)}</code><div class="button-row"><button class="button" data-action="append">Review or retry same operation</button><button class="button" data-action="resolve-operation" data-id="${e(operation.operationId)}">Check outcome</button></div>${controls}</div></section>`;
    if (isKV() && !kvEnabled('upsert'))
        return '<section class="panel" data-write-panel><p class="notice">This connection does not advertise KV upsert.</p></section>';
    return `<div data-write-panel>${controls}${writeEditor(state.schema, state.writeDraft, operation, state.collectionId, state.theme.labels, isKV() ? 'kv' : 'append')}</div>`;
}
function draftWrite() {
    const form = $('#write-form');
    if (form) state.writeDraft = readEditorRows(form);
}
function resultTable() {
    const schema = state.result.schema?.fields ?? state.projection.map((name) => ({ name }));
    const page = state.rows.slice(state.page * 100, (state.page + 1) * 100);
    return `<section class="panel results-panel"><div class="panel-header"><div><h2>Your results</h2><p>${number(state.rows.length)} rows${state.result.next_cursor ? ' · partial preview; reduce the limit or use the SDK for all rows' : ''} · snapshot <code>${e(state.result.snapshot_id ?? 'initial empty')}</code></p></div><button class="button small" data-action="${publicDemo ? 'export-json' : 'export-arrow'}">${icon('download')}${publicDemo ? 'JSON' : 'Arrow'}</button></div><div class="table-wrap"><table aria-label="Query results"><thead><tr>${schema.map((f) => `<th>${e(f.name)}</th>`).join('')}</tr></thead><tbody>${page.map((row) => `<tr>${schema.map((f) => `<td title="${e(displayValue(row[f.name]))}">${row[f.name] === null ? '<span class="null">NULL</span>' : e(displayValue(row[f.name]))}</td>`).join('')}</tr>`).join('')}</tbody></table>${!page.length ? '<p class="result-empty">No rows match this query. The result schema is still available.</p>' : ''}</div><div class="table-footer"><span>${page.length ? state.page * 100 + 1 : 0}–${state.page * 100 + page.length} of ${number(state.rows.length)} · bounded preview</span><div class="button-row"><button class="button small" data-action="previous-page" ${state.page === 0 ? 'disabled' : ''}>Previous</button><button class="button small" data-action="next-page" ${(state.page + 1) * 100 >= state.rows.length ? 'disabled' : ''}>Next</button></div></div></section>`;
}

function renderReadResults() {
    const target = $('#read-results');
    if (!target) return;
    target.innerHTML = state.result
        ? resultTable()
        : '<div class="notice read-ready">Read rows to see the selected snapshot.</div>';
    if (state.result) $('#view').dataset.resultSnapshot = state.result.snapshot_id ?? 'initial';
    else delete $('#view').dataset.resultSnapshot;
}
function renderWritePanel({ removedIndex = null, reset = false } = {}) {
    const expanded = reset
        ? []
        : [...document.querySelectorAll('[data-write-row]')].flatMap((row, index) =>
              row.open && index !== removedIndex
                  ? [removedIndex !== null && index > removedIndex ? index - 1 : index]
                  : [],
          );
    const target = $('[data-write-panel]');
    if (target) {
        target.outerHTML = writePanel();
        for (const index of expanded) {
            const row = document.querySelectorAll('[data-write-row]')[index];
            if (row) row.open = true;
        }
    }
}
function updateDataSelection() {
    const select = $('#snapshot-selector');
    if (select) select.value = state.snapshotId ?? 'latest';
    const note = $('#read-snapshot-note');
    if (note)
        note.textContent = `Choose columns and optional filters. Snapshot ${state.resolvedSnapshot ?? state.snapshotId ?? 'latest'}.`;
    if ($('#query-json')) $('#query-json').textContent = queryPreview();
    renderReadResults();
}
function renderView() {
    if (!$('#view')) return;
    if (accessGateway && (state.databaseLoading || state.databaseError)) {
        $('#view').innerHTML = state.databaseError
            ? `${heading('Database unavailable', 'Choose another database or refresh to try again.', false)}<div class="notice" role="alert">${e(state.databaseError)}</div>`
            : `${heading('Opening database…', '', false)}<p class="notice" role="status">Loading the selected database.</p>`;
        return;
    }
    if (state.collectionId && !state.schema && !['overview', 'operations'].includes(state.tab)) {
        $('#view').innerHTML = '<p class="notice" role="status">Opening this snapshot…</p>';
        return;
    }
    if (!state.collectionId && !['overview'].includes(state.tab)) {
        $('#view').innerHTML = heading(labels(`nav_${state.tab}`), '', false) + emptyView();
        return;
    }
    const renders = {
        overview,
        data: dataView,
        schema: schemaView,
        snapshots: snapshotsView,
        storage: storageView,
        explain: explainView,
        operations: operationsView,
    };
    $('#view').innerHTML = (renders[state.tab] ?? overview)();
    if (state.tab === 'data' && state.result)
        $('#view').dataset.resultSnapshot = state.result.snapshot_id ?? 'initial';
    updateThemeText();
}

function schemaView() {
    return `${heading('The shape of your data', 'Fixed, typed, and a little more organised.')}<section class="panel"><div class="panel-header"><h2>Collection schema</h2>${badge('Unpartitioned', 'pink')}</div><div class="table-wrap"><table class="schema-table" aria-label="Collection schema"><thead><tr><th>Column</th><th>Type</th><th>Field ID</th><th>Nullable</th></tr></thead><tbody>${fields()
        .map(
            (f) =>
                `<tr><td><strong>${e(f.name)}</strong></td><td><span class="type">${e(f.type === 'decimal' ? `decimal(${f.precision},${f.scale})` : f.type)}</span></td><td>${e(f.field_id)}</td><td>${f.nullable ? 'Yes' : 'Required'}</td></tr>`,
        )
        .join(
            '',
        )}</tbody></table></div></section>${writeSettingsPanel()}<div class="notice" style="margin-top:20px">${isKV() ? 'KV mode keeps one full row per key. Upsert replaces the row; omitted nullable values become NULL. Mode is immutable.' : 'Append mode preserves duplicate keys. A field called key does not imply uniqueness.'} Schema evolution is not enabled.</div>`;
}
function writeSettingsPanel() {
    const parquet = parquetSettings();
    if (!parquet || !state.collection || state.collection.collection_id !== state.collectionId)
        return '';
    let version, current;
    try {
        version = writeSettingsVersion(state.collection.write_settings_version);
        current = canonicalWriteSettings(state.collection.write_settings, state.schema, {
            reported: true,
        });
    } catch (error) {
        // Saving would drop options this workbench cannot represent; show them read-only.
        return `<section class="panel write-settings-panel"><div class="panel-header"><h2>Parquet write settings</h2></div><div class="panel-body"><p class="notice">${e(error.message)}. Editing is disabled here; use a client that supports these settings.</p><pre class="json-block">${json(state.collection.write_settings ?? null)}</pre></div></section>`;
    }
    const overrides = new Map(current.parquet.columns.map((item) => [item.field, item]));
    const dictionary = Boolean(parquet.dictionary),
        encodings = parquet.encodings.length > 0;
    return `<section class="panel write-settings-panel"><div class="panel-header"><div><h2>Parquet write settings</h2><p>Compression, dictionary and value encodings of data files written by later appends. Existing files keep theirs. This is not HTTP compression.</p></div>${badge(`Version ${version}`, 'pink')}</div><div class="panel-body"><form id="write-settings-form" data-collection="${e(state.collectionId)}" data-version="${e(version)}">${collectionParquetControls(parquet, current.parquet, 'Collection compression')}<div class="table-wrap"><table class="write-settings-table" aria-label="Parquet column overrides"><thead><tr><th>Column</th><th>Codec</th><th>Level</th>${dictionary ? '<th>Dictionary</th>' : ''}${encodings ? '<th>Encoding</th>' : ''}<th>In effect</th></tr></thead><tbody>${fields()
        .map((field) => {
            const override = overrides.get(field.name) ?? {};
            return `<tr data-parquet-choice data-parquet-field="${e(field.name)}" data-parquet-type="${e(field.type)}"><td><strong>${e(field.name)}</strong><p class="small muted">${e(field.type)}</p></td><td><select data-parquet-codec aria-label="Parquet compression for ${e(field.name)}">${codecOptions(parquet, override.compression?.codec, 'Collection default')}</select></td><td><input type="number" step="1" inputmode="numeric" data-parquet-level aria-label="Parquet level for ${e(field.name)}" ${levelAttributes(parquet, override.compression?.codec, override.compression?.level)}></td>${dictionary ? `<td>${columnDictionarySelect(parquet, field.type, override, field.name)}</td>` : ''}${encodings ? `<td>${columnEncodingSelect(parquet, field.type, override, field.name)}</td>` : ''}<td>${e(describeColumnEffect(field.type, overrides.get(field.name), current, parquet))}</td></tr>`;
        })
        .join(
            '',
        )}</tbody></table></div><p class="small muted">An explicit encoding turns that column’s dictionary off. A dictionary larger than its page size limit falls back to ${e(parquet.dictionary?.fallback_encoding ?? 'plain')} pages.</p><p class="modal-error" id="write-settings-error" role="alert"></p><div class="button-row"><button class="button primary" type="submit">Save write settings</button></div><details class="raw-metadata"><summary class="small">Current canonical settings</summary><pre class="json-block">${json(current)}</pre></details></form></div></section>`;
}
function snapshotsView() {
    const observed = !supportsFeature(state.capabilities, 'snapshots');
    const snapshots = availableSnapshots();
    return `${heading('Every version has a story.', observed ? 'Observed in this session. Complete history requires server metadata support.' : 'Read an immutable view. Keep your latest data untouched.')}<section class="panel"><div class="panel-header"><h2>${observed ? 'Observed in this session' : 'Snapshot history'}</h2>${badge(`${snapshots.length} ${observed ? 'observed' : 'retained'}`)}</div><div class="table-wrap"><table aria-label="Snapshot history"><thead><tr><th>Snapshot ID</th><th>Created (UTC)</th><th>Parent</th><th>Rows written / removed</th><th></th></tr></thead><tbody>${snapshots
        .slice()
        .map(
            (s) =>
                `<tr><td class="mono">${e(s.snapshot_id)}</td><td>${e(snapshotCreatedAt(s) ?? 'Unavailable')}</td><td class="mono">${e(s.parent_id ?? s.parent_snapshot_id ?? 'Initial')}</td><td>${number(countSnapshotRows(s))}</td><td><button class="button small" data-action="open-snapshot" data-id="${e(s.snapshot_id)}">Explore${icon('arrow')}</button></td></tr>`,
        )
        .join(
            '',
        )}</tbody></table></div>${!snapshots.length ? `<p class="result-empty">${observed ? 'No committed appends observed in this session.' : 'No snapshot has been published yet.'}</p>` : ''}</section>`;
}
function storageView() {
    if (!supportsFeature(state.capabilities, 'files'))
        return `${heading('Storage receipts', 'Confirmed additions from this session. This is not a complete file inventory.')}<section class="panel"><div class="panel-header"><h2>Committed appends</h2></div><div class="table-wrap"><table aria-label="Storage receipts"><thead><tr><th>Snapshot</th><th>Rows added</th><th>Files added</th><th>Compressed bytes added</th></tr></thead><tbody>${state.operations
            .filter(
                (op) =>
                    op.collectionId === state.collectionId &&
                    op.outcome === 'committed' &&
                    op.result,
            )
            .map(
                (op) =>
                    `<tr><td class="mono">${e(op.result.snapshot_id)}</td><td>${number(op.result.rows_appended)}</td><td>${number(op.result.data_files_added)}</td><td>${number(op.result.compressed_data_bytes_added)}</td></tr>`,
            )
            .join(
                '',
            )}</tbody></table></div></section><div class="notice">File paths, total storage size and row-group details require server metadata support. Unknown values remain unknown.</div>`;
    return `${heading('A place for every file.', 'File membership belongs to the selected snapshot.')}<div class="notice" style="margin-bottom:20px">${state.capabilities?.mock ? 'These are synthetic descriptors for the mock adapter. No physical Parquet files are written.' : 'Immutable files; metadata is available where supported.'}</div><section class="panel"><div class="panel-header"><h2>Data files</h2>${badge(`${state.files.length} files`)}</div><div class="table-wrap"><table aria-label="Data files"><thead><tr><th>File</th><th>Rows</th><th>Bytes</th><th></th></tr></thead><tbody>${state.files.map((f, i) => `<tr><td><strong>Data file ${i + 1}</strong><p class="muted small mono">${e(f.file_id)}</p></td><td>${number(f.record_count)}</td><td>${f.compressed_bytes === null ? 'Unknown' : number(f.compressed_bytes)}</td><td><button class="button small" data-action="file" data-id="${e(f.file_id)}">Inspect${icon('arrow')}</button></td></tr>`).join('')}</tbody></table></div>${!state.files.length ? '<p class="result-empty">No files in this view.</p>' : ''}</section><div class="notice" style="margin-top:20px">${isKV() ? 'KV compaction rewrites a bounded group while retaining historical files.' : 'Compaction is unavailable in append mode.'} Expiration and garbage collection are unavailable. Optional index presence does not imply execution support.</div>`;
}
function explainView() {
    if (!supportsFeature(state.capabilities, 'explain'))
        return `${heading('Query plans', 'This server does not yet expose this capability.')}<div class="notice">Your data remains available in Data studio.</div>`;
    return `${heading('A peek behind the query.', 'Plan estimates and executed measurements stay separate.')}<section class="panel"><div class="panel-header"><h2>Query plan</h2><button class="button small" data-action="explain-current">${icon('refresh')}Refresh plan</button></div><div class="panel-body">${
        state.plan
            ? `<div class="button-row" style="margin-bottom:18px">${badge('Fixed snapshot', 'pink')}${badge(state.capabilities?.mock ? 'Reference mock scan' : 'Native scan')}${badge('Page/Bloom execution unsupported')}</div><dl class="metadata-list"><dt>Selected snapshot</dt><dd class="mono">${e(state.plan.snapshot_id ?? state.plan.identity?.snapshot_id ?? 'Initial empty view')}</dd><dt>Read columns</dt><dd>${e(state.plan.read_columns?.join(', ') ?? 'Unknown')}</dd><dt>Output columns</dt><dd>${e(state.plan.output_columns?.join(', ') ?? 'Unknown')}</dd><dt>Rows before filtering</dt><dd>${number(state.plan.estimates?.rows_before_filter)}</dd></dl>${[
                  ['Resolve snapshot', 'A single immutable identity is selected before reading.'],
                  ['Read projection', 'Read output columns and any additional predicate fields.'],
                  ['Filter rows', 'Exact SQL three-valued NULL logic; only TRUE survives.'],
                  ['Apply limit', 'Limit is applied after the residual predicate.'],
              ]
                  .map(
                      ([title, copy], i) =>
                          `<div class="stage"><span class="stage-number">${i + 1}</span><div><strong>${title}</strong><p>${copy}</p></div></div>`,
                  )
                  .join(
                      '',
                  )}<details style="margin-top:18px"><summary class="small">Inspect complete plan</summary><pre class="json-block">${json(state.plan)}</pre></details>`
            : '<p class="muted small">Build a query in Data studio, then choose Explain. You can also inspect the current all-column query.</p>'
    }</div></section>${state.result?.profile ? `<section class="panel"><div class="panel-header"><h2>Execution profile</h2>${badge(state.capabilities?.mock ? 'Mock measurements' : 'Native scan')}</div><div class="panel-body"><pre class="json-block">${json(state.result.profile)}</pre><p class="muted small">Returned row counts describe this scan. They do not establish pruning performance or power-loss durability.</p></div></section>` : ''}`;
}
function operationsView() {
    const ops = state.operations.filter((op) => op.collectionId === state.collectionId);
    const published =
        state.capabilities?.mock === false && supportsFeature(state.capabilities, 'snapshots')
            ? state.snapshots.filter((snapshot) => typeof snapshot.operation_id === 'string')
            : null;
    return `${heading('Keep your operations close.', 'An unknown outcome is unresolved. A timeout is not a rollback.')}${state.managed ? '<p><button class="button" data-action="reconcile-workspace">Reconcile database</button></p>' : ''}<section class="panel"><div class="panel-header"><h2>This session's mutations</h2>${badge('Synchronous API')}</div>${
        ops.length
            ? ops
                  .slice()
                  .map(
                      (op) =>
                          `<div class="operations-card">${icon('activity')}<div><strong>${e(op.outcome ?? 'Unresolved')}</strong>${isKV() ? `<p>${e(receiptTitle(op.action ?? op.result?.mutation_kind))}</p>` : ''}<p class="mono">${e(op.operationId)}</p><p>${op.result?.snapshot_created === false ? 'No new snapshot · effective view' : 'Snapshot'} ${e(op.result?.snapshot_id === '0' && op.result?.snapshot_created === false ? 'initial empty' : (op.result?.snapshot_id ?? 'not yet confirmed'))}</p></div><button class="button small" data-action="resolve-operation" data-id="${e(op.operationId)}">Check outcome</button></div>`,
                  )
                  .join('')
            : `<p class="result-empty">${isKV() ? 'No mutations' : 'No appends'} submitted in this browser session. Server outcomes are queried by operation ID.</p>`
    }</section>${
        published
            ? `<section class="panel"><div class="panel-header"><h2>Published snapshot operations</h2>${badge('Server history')}</div><p class="panel-body small muted">Original operation IDs from retained server snapshots. Look up the authoritative outcome without submitting a write.</p><div class="table-wrap"><table aria-label="Published snapshot operations"><thead><tr><th>Operation ID</th><th>Snapshot</th><th></th></tr></thead><tbody>${published
                  .slice()
                  .map(
                      (snapshot) =>
                          `<tr><td class="mono">${e(snapshot.operation_id)}</td><td class="mono">${e(snapshot.snapshot_id)}</td><td><button class="button small" data-action="lookup-operation" data-id="${e(snapshot.operation_id)}">Look up outcome</button></td></tr>`,
                  )
                  .join(
                      '',
                  )}</tbody></table></div>${published.length ? '' : '<p class="result-empty">No snapshot has been published yet.</p>'}</section>`
            : ''
    }<section class="panel"><div class="panel-body"><form id="operation-lookup-form" class="button-row"><label class="field">Operation ID<input name="operation" required autocomplete="off" placeholder="Original operation UUID"></label><button class="button" type="submit" ${state.collectionId ? '' : 'disabled'}>Look up operation</button></form><p class="small muted">Look up an original ID in this collection. This does not submit or retry a write.</p></div></section>`;
}
async function allPages(path, signal, api = state.api, view) {
    return readMetadataPages(api, path, { signal, view });
}
async function refreshSelection({ preserveEditors = false } = {}) {
    if (!state.collectionId) {
        state.schema = null;
        state.stats = null;
        state.snapshots = [];
        state.files = [];
        chrome();
        return;
    }
    countController?.abort();
    countController = null;
    viewController?.abort();
    viewController = new AbortController();
    const signal = viewController.signal,
        api = state.api,
        epoch = state.epoch,
        id = state.collectionId;
    const current = () =>
        !signal.aborted && api === state.api && epoch === state.epoch && id === state.collectionId;
    try {
        const info = await api.json(`/collections/${encodeURIComponent(id)}`, { signal });
        if (!current()) return;
        const selected =
            state.snapshotId === null ? (info.current_snapshot_id ?? 'initial') : state.snapshotId;
        const suffix = `?snapshot_id=${encodeURIComponent(selected)}`;
        // Native admission allows two data calls per database. Read metadata one
        // request at a time, leaving capacity for a query or outcome resolution.
        const schema = await api.json(`/collections/${encodeURIComponent(id)}/schema`, { signal });
        if (!current()) return;
        const stats = supportsFeature(state.capabilities, 'stats')
            ? await api.json(`/collections/${encodeURIComponent(id)}/stats${suffix}`, { signal })
            : null;
        if (!current()) return;
        const snapshots = supportsFeature(state.capabilities, 'snapshots')
            ? await allPages(
                  `/collections/${encodeURIComponent(id)}/snapshots`,
                  signal,
                  api,
                  publicDemo
                      ? undefined
                      : { collectionId: id, mock: state.capabilities?.mock === true },
              )
            : [];
        if (!current()) return;
        const filesResult = supportsFeature(state.capabilities, 'files')
            ? await allPages(
                  `/collections/${encodeURIComponent(id)}/files${suffix}`,
                  signal,
                  api,
                  publicDemo
                      ? undefined
                      : {
                            collectionId: id,
                            snapshotId: selected,
                            mock: state.capabilities?.mock === true,
                        },
              )
            : [];
        if (!current()) return;
        if (stats)
            validateViewMetadata(stats, {
                collectionId: id,
                snapshotId: selected,
                mock: state.capabilities?.mock === true,
            });
        state.collection = info;
        state.resolvedSnapshot = selected;
        state.schema = schema.schema ?? schema;
        state.stats = stats;
        state.snapshots = snapshots;
        state.files = filesResult;
        if (
            !state.projection.length ||
            state.projection.some((name) => !fields().some((f) => f.name === name))
        )
            state.projection = fields().map((f) => f.name);
        state.collections = state.collections.map((item) =>
            item.collection_id === id ? info : item,
        );
        if (preserveEditors && state.tab === 'data' && $('#query-form')) updateDataSelection();
        else chrome();
    } catch (error) {
        // A newer selection cancels these metadata requests. Its cancellation
        // must not reset the connection or overwrite the new workspace.
        if (current()) throw error;
    }
}
async function selectCollection(id, snapshot = null) {
    const changedCollection = id !== state.collectionId;
    const preserveEditors = !changedCollection && state.tab === 'data' && Boolean($('#query-form'));
    draftWrite();
    const writeDraft = state.writeDraft,
        writeOperationId = state.writeOperationId;
    if (!changedCollection && $('#query-form')) {
        try {
            readQuery();
        } catch {
            /* Retain edited drafts even if their predicate is not valid yet. */
        }
    }
    countController?.abort();
    countController = null;
    viewController?.abort();
    queryController?.abort();
    queryGeneration++;
    Object.assign(state, selectView(state, id, snapshot));
    if (changedCollection) {
        state.projection = [];
        state.clauses = [];
    }
    if (preserveEditors) {
        state.writeDraft = writeDraft;
        state.writeOperationId = writeOperationId;
        renderReadResults();
        $('#read-snapshot-note').textContent = 'Opening selected snapshot…';
    } else {
        state.schema = null;
        state.stats = null;
        state.snapshots = [];
        state.files = [];
        chrome();
        $('#view').innerHTML = '<p class="notice">Opening this snapshot…</p>';
    }
    await refreshSelection({ preserveEditors });
}
async function refreshWorkspace() {
    const api = state.api,
        epoch = state.epoch,
        signal = connectionController?.signal;
    const collections = await allPages('/collections', signal, api);
    if (api !== state.api || epoch !== state.epoch || signal?.aborted) return;
    state.collections = collections;
    const id = collections.some((c) => c.collection_id === state.collectionId)
        ? state.collectionId
        : (collections[0]?.collection_id ?? null);
    if (id !== state.collectionId) await selectCollection(id);
    else await refreshSelection();
}
async function countRows() {
    const api = state.api,
        epoch = state.epoch,
        id = state.collectionId,
        snapshot = state.resolvedSnapshot;
    const controller = new AbortController();
    countController?.abort();
    countController = controller;
    const current = () =>
        state.api === api &&
        state.epoch === epoch &&
        state.collectionId === id &&
        state.resolvedSnapshot === snapshot;
    const column =
        fields().find((f) => ['bool', 'int64', 'float64'].includes(f.type)) ?? fields()[0];
    renderView();
    try {
        const result = await api.countRows(
            `/collections/${encodeURIComponent(id)}/scan`,
            {
                projection: [column.name],
                snapshot_id: snapshot,
            },
            { signal: controller.signal },
        );
        if (!controller.signal.aborted && current()) {
            const identity = result.snapshot_id ?? 'initial';
            if (identity !== snapshot) throw new Error('Count returned a different snapshot');
            state.rowMeasurement = { ...result, snapshot_id: identity };
            toast(`${number(result.rows)} rows counted in the selected snapshot.`);
        }
    } finally {
        if (countController === controller) countController = null;
        if (current()) renderView();
    }
}
async function runQuery() {
    const body = readQuery(),
        epoch = state.epoch,
        generation = ++queryGeneration;
    queryController?.abort();
    const controller = new AbortController();
    queryController = controller;
    const button = $('#run-query');
    setBusy(button, true);
    try {
        const result = await (state.api.scanPreview ?? state.api.json).call(
            state.api,
            `${collectionPath()}/scan`,
            {
                method: 'POST',
                body,
                signal: controller.signal,
            },
        );
        if (epoch !== state.epoch || generation !== queryGeneration) return;
        state.result = result;
        state.rows = result.rows ?? [];
        state.page = 0;
        state.lastQuery = { ...body, snapshot_id: result.snapshot_id ?? 'initial' };
        renderReadResults();
        toast(
            `${state.rows.length} rows${result.next_cursor ? ' (partial preview)' : ''}. Fixed snapshot ${result.snapshot_id ?? 'initial empty'}.`,
        );
    } finally {
        if (queryController === controller) queryController = null;
        setBusy(button, false);
    }
}
async function explainQuery(fromForm) {
    if (!supportsFeature(state.capabilities, 'explain'))
        throw new Error('Explain is unavailable on this server');
    const epoch = state.epoch,
        api = state.api;
    const body = fromForm
        ? readQuery()
        : {
              projection: state.projection.length ? state.projection : fields().map((f) => f.name),
              predicate: buildPredicate(state.clauses, state.join, state.negate, fields()),
              limit: state.limit,
          };
    if (state.snapshotId !== null) body.snapshot_id = state.snapshotId;
    const result = await state.api.json(`${collectionPath()}/explain`, {
        method: 'POST',
        body,
        signal: viewController?.signal,
    });
    if (epoch !== state.epoch || api !== state.api) return;
    validateViewMetadata(result, {
        collectionId: state.collectionId,
        mock: state.capabilities?.mock === true,
        ...(state.snapshotId === null ? {} : { snapshotId: state.snapshotId }),
    });
    state.plan = result;
    state.tab = 'explain';
    chrome();
}
function showModal(title, body) {
    const modal = $('#modal');
    modal.innerHTML = `<header class="modal-header"><h2 id="modal-title">${e(title)}</h2><button class="icon-button" data-action="close-modal" aria-label="Close dialog">${icon('close')}</button></header><div class="modal-body">${body}</div>`;
    if (!modal.open) modal.showModal();
}
let creationFields = [];
function renderCreationFields() {
    const parquet = parquetSettings();
    $('#schema-editor').innerHTML = creationFields
        .map((field, i) => {
            const label = `column ${i + 1}`;
            const type = field.type ?? 'string';
            return `<div class="schema-editor-row${parquet ? ' with-parquet' : ''}" ${parquet ? 'data-parquet-choice' : ''}><input name="field-name" value="${e(field.name)}" aria-label="Column name ${i + 1}" required><div class="schema-type-controls"><select name="field-type" aria-label="Column type ${i + 1}">${schemaTypes(
                state.capabilities,
            )
                .map((option) => `<option ${type === option ? 'selected' : ''}>${option}</option>`)
                .join(
                    '',
                )}</select>${type === 'decimal' ? `<div class="decimal-parameters"><label>Precision<input name="field-precision" type="number" min="1" max="38" step="1" value="${e(field.precision ?? 18)}" aria-label="Decimal precision ${i + 1}" required></label><label>Scale<input name="field-scale" type="number" min="0" max="38" step="1" value="${e(field.scale ?? 2)}" aria-label="Decimal scale ${i + 1}" required></label></div>` : ''}</div><label><input name="field-nullable" type="checkbox" ${field.nullable ? 'checked' : ''}>NULL</label>${parquet ? `<div class="column-parquet"><select name="field-codec" data-parquet-codec aria-label="Parquet compression for ${label}">${codecOptions(parquet, field.codec, 'Default codec')}</select><input name="field-level" type="number" step="1" inputmode="numeric" data-parquet-level aria-label="Parquet level for ${label}" ${levelAttributes(parquet, field.codec, field.level)}>${columnDictionarySelect(parquet, type, field, label)}${columnEncodingSelect(parquet, type, field, label)}</div>` : ''}<button class="icon-button" type="button" data-action="remove-field" data-index="${i}" aria-label="Remove field ${i + 1}">${icon('close')}</button></div>`;
        })
        .join('');
}
function readCreationFields() {
    return [...$('#schema-editor').querySelectorAll('.schema-editor-row')].map((row) => ({
        name: row.querySelector('[name=field-name]').value,
        type: row.querySelector('[name=field-type]').value,
        nullable: row.querySelector('[name=field-nullable]').checked,
        ...(row.querySelector('[name=field-type]').value === 'decimal'
            ? {
                  precision: Number(row.querySelector('[name=field-precision]')?.value ?? 18),
                  scale: Number(row.querySelector('[name=field-scale]')?.value ?? 2),
              }
            : {}),
        ...readParquetChoice(row),
    }));
}
// Returns canonical settings, or null when no Parquet option was chosen.
function creationWriteSettings(form, rows, schema) {
    const parquet = parquetSettings(),
        choice = form.querySelector('[data-parquet-default]');
    if (!parquet || !choice) return null;
    const settings = writeSettingsFromChoices(
        parquet,
        readParquetChoice(choice),
        rows.map((row) => ({
            field: row.name,
            type: row.type,
            codec: row.codec,
            level: row.level,
            dictionary: row.dictionary,
            encoding: row.encoding,
        })),
    );
    return settings && canonicalWriteSettings(settings, schema);
}
function creationParquetControls(parquet, settings) {
    if (!parquet)
        return settings
            ? `<p class="notice">Parquet write settings of this request: <code>${e(JSON.stringify(settings))}</code></p>`
            : '';
    const canonical = settings ? canonicalWriteSettings(settings).parquet : null;
    const options = [
        'codec',
        parquet.dictionary && 'dictionary policy',
        parquet.encodings.length && 'value encoding',
    ].filter(Boolean);
    const list =
        options.length > 1
            ? `${options.slice(0, -1).join(', ')} and ${options.at(-1)}`
            : options[0];
    return `<div class="parquet-settings">${collectionParquetControls(parquet, canonical, 'Parquet compression')}<small>Parquet data files written by appends (not HTTP compression). Each column below can override the ${e(list)}${parquet.encodings.length ? '; an explicit encoding turns that column’s dictionary off' : ''}.</small></div>`;
}
function openCreate() {
    const frozenSettings = state.creation?.body.write_settings;
    const overrides = new Map(
        (frozenSettings?.parquet?.columns ?? []).map((item) => [item.field, item]),
    );
    creationFields = state.creation
        ? structuredClone(state.creation.body.schema.fields).map((field) => {
              const own = overrides.get(field.name);
              return {
                  ...field,
                  codec: own?.compression?.codec ?? '',
                  level: own?.compression?.level ?? '',
                  dictionary: own?.dictionary ?? '',
                  encoding: own?.encoding ?? '',
              };
          })
        : [
              { name: 'key', type: 'string', nullable: false },
              { name: 'label', type: 'string', nullable: false },
              { name: 'amount', type: 'int64', nullable: true },
          ];
    showModal(
        'A new collection, just for you.',
        `<form id="create-form"><label class="field">Collection name<input name="name" placeholder="my_collection" pattern="[a-z][a-z0-9_]{0,62}" required autofocus><small>Lowercase letters, numbers and underscores. Schema stays fixed.</small></label>${state.capabilities?.features?.key_value === true ? '<label class="field">Collection mode<select name="mode" aria-label="Collection mode"><option value="append">Append · preserve duplicate keys</option><option value="kv">KV · replace full rows by key</option></select><small>Mode is immutable. KV requires a nonnullable string key field.</small></label>' : ''}${creationParquetControls(parquetSettings(), frozenSettings)}<div style="margin-top:20px"><label>Columns</label><div id="schema-editor"></div><button type="button" class="text-button" data-action="add-field">+ Add column</button></div><div id="create-outcome" role="status"></div><p class="modal-error" id="modal-error" role="alert"></p><div class="modal-actions"><button type="button" class="button" data-action="close-modal">Cancel</button>${state.creation ? '<button type="button" class="button" data-action="export-pending" data-kind="create">Export pending request</button>' : ''}<button class="button primary" type="submit">Create collection${icon('arrow')}</button></div></form>`,
    );
    renderCreationFields();
    if (state.creation) {
        $('#create-form').elements.name.value = state.creation.body.name;
        if ($('#create-form').elements.mode)
            $('#create-form').elements.mode.value = state.creation.body.mode ?? 'append';
        freezeCreationForm($('#create-form'));
        $('#modal-error').textContent =
            'Creation is unresolved. Retry keeps the original UUID and exact schema.';
    }
}
function freezeCreationForm(form) {
    for (const input of form.querySelectorAll('input')) input.readOnly = true;
    for (const input of form.querySelectorAll(
        'select,input[type=checkbox],button[data-action="add-field"],button[data-action="remove-field"]',
    ))
        input.disabled = true;
}
function releaseCreationForm(form) {
    for (const input of form.querySelectorAll('input')) input.readOnly = false;
    for (const input of form.querySelectorAll(
        'select,input[type=checkbox],button[data-action="add-field"],button[data-action="remove-field"]',
    ))
        input.disabled = false;
    form.querySelectorAll('[data-parquet-choice]').forEach(syncParquetChoice);
    form.querySelector('[data-action="export-pending"]')?.remove();
}
function verifyCreated(created, body, options) {
    return verifyCreatedCollection(created, body, options);
}
async function createForm(form) {
    const button = form.querySelector('[type=submit]');
    setBusy(button, true);
    const api = state.api,
        epoch = state.epoch;
    let draft, created;
    try {
        draft = state.creation;
        if (!draft) {
            const rows = readCreationFields();
            const schema = creationSchema(
                rows.map(({ name, type, nullable, precision, scale }) => ({
                    name,
                    type,
                    nullable,
                    ...(type === 'decimal' ? { precision, scale } : {}),
                })),
                state.capabilities,
            );
            const mode = form.elements.mode?.value ?? 'append';
            if (
                mode === 'kv' &&
                !schema.fields.some(
                    (field) => field.name === 'key' && field.type === 'string' && !field.nullable,
                )
            )
                throw new Error('KV mode requires a nonnullable string key field');
            const writeSettings = creationWriteSettings(form, rows, schema);
            const body = {
                collection_id: crypto.randomUUID(),
                name: form.elements.name.value,
                schema,
                ...(mode === 'kv' ? { mode } : {}),
                ...(writeSettings ? { write_settings: wireWriteSettings(writeSettings) } : {}),
            };
            draft = { fingerprint: creationFingerprint(body), body, scope: recoveryScope() };
        }
        state.creation = draft;
        try {
            const received = await api.json('/collections', {
                method: 'POST',
                body: draft.body,
                allowOutcome: true,
            });
            if (received.outcome === 'definitely_not_committed') {
                const error = new Error(
                    received.error?.message ?? 'Collection definitely not created',
                );
                error.definite = true;
                throw error;
            }
            if (received.outcome === 'commit_unknown')
                throw new Error(received.error?.message ?? 'Creation requires recovery');
            created = verifyCreated(received.info ?? received.result ?? received, draft.body);
        } catch (error) {
            if (state.api !== api) return;
            if (
                error.definite ||
                [400, 401, 403, 409, 413, 415, 422, 429, 507].includes(error.status)
            ) {
                draft.terminal = true;
                draft.outcome = 'definitely_not_committed';
                if (state.creation === draft) state.creation = null;
                releaseCreationForm(form);
                throw error;
            }
            freezeCreationForm(form);
            // An unavailable response is not proof of absence. Resolve the exact
            // original UUID and schema, or keep the complete request for retry.
            try {
                created = verifyCreated(
                    await api.json(
                        `/collections:resolve?name=${encodeURIComponent(draft.body.name)}`,
                    ),
                    draft.body,
                    { authoritative: false },
                );
            } catch {
                ensurePendingExport(form, 'create');
                throw new Error(
                    `Creation is unresolved (${error.message}). Retry the same request; UUID ${draft.body.collection_id}.`,
                );
            }
        }
        if (state.api !== api) return;
        draft.terminal = true;
        draft.outcome = 'committed';
        if (state.creation === draft) state.creation = null;
        state.collections = [
            ...state.collections.filter((item) => item.collection_id !== created.collection_id),
            created,
        ];
        if (form.isConnected) {
            $('#create-outcome').innerHTML =
                `<div class="op-outcome"><strong>Collection created</strong><code>${e(created.collection_id)}</code></div>`;
            freezeCreationForm(form);
            button.textContent = 'Created';
            button.disabled = true;
            form.querySelector('[data-action="export-pending"]')?.remove();
        }
        try {
            const collections = await allPages('/collections', connectionController?.signal, api);
            if (state.api !== api) return;
            state.collections = collections;
            if (form.isConnected && epoch === state.epoch) {
                state.tab = 'data';
                await selectCollection(created.collection_id);
                $('#modal').close();
            } else chrome();
            toast('Collection created. Ready for your rows.');
        } catch (error) {
            if (state.api !== api) return;
            if (form.isConnected)
                form.querySelector('#modal-error').textContent =
                    `Collection created. Metadata refresh failed: ${error.message}. Use Refresh workspace to load it.`;
            toast('Collection created. Metadata refresh failed; refresh the workspace to load it.');
        }
    } catch (error) {
        if (state.api === api && form.isConnected)
            form.querySelector('#modal-error').textContent = error.message;
    } finally {
        setBusy(button, false);
        if (created) button.disabled = true;
    }
}
async function saveWriteSettings(form) {
    const api = state.api,
        epoch = state.epoch,
        id = form.dataset.collection,
        version = form.dataset.version;
    const button = form.querySelector('[type=submit]');
    const current = () => api === state.api && epoch === state.epoch && id === state.collectionId;
    const message = (text) => {
        const node = $('#write-settings-error');
        if (node) node.textContent = text;
    };
    message('');
    setBusy(button, true);
    try {
        const parquet = parquetSettings();
        if (!parquet || !current() || state.collection?.collection_id !== id)
            throw new Error('Reload this collection before changing its write settings');
        const schema = state.schema;
        const settings = canonicalWriteSettings(
            writeSettingsFromChoices(
                parquet,
                readParquetChoice(form.querySelector('[data-parquet-default]')),
                [...form.querySelectorAll('tr[data-parquet-field]')].map((row) => ({
                    field: row.dataset.parquetField,
                    type: row.dataset.parquetType,
                    ...readParquetChoice(row),
                })),
            ),
            schema,
        );
        // Each applied request creates a new version; do not create one for nothing.
        if (sameWriteSettings(settings, state.collection.write_settings, schema)) {
            message('No changes to save.');
            return;
        }
        let updated;
        try {
            updated = await api.json(`/collections/${encodeURIComponent(id)}/write-settings`, {
                method: 'POST',
                body: { expected_version: version, write_settings: wireWriteSettings(settings) },
            });
        } catch (error) {
            if (!current()) return;
            if (error.status === 409 && error.code === 'WRITE_SETTINGS_CONFLICT') {
                // Someone else changed them (or an earlier lost save applied):
                // show the current version instead of overwriting it.
                await refreshSelection();
                if (!current()) return;
                const text = `Write settings changed after version ${version}. Reloaded version ${state.collection?.write_settings_version ?? 'unknown'}; review it and save again.`;
                message(text);
                toast(text);
                return;
            }
            if (error.status && error.status < 500 && error.status !== 408) throw error;
            throw new Error(
                `Write settings outcome is unknown (${error.message}). ${error.code === 'RECOVERY_REQUIRED' ? 'Reconcile the database, then save' : 'Save'} the same settings again; an identical request that was applied is confirmed unchanged.`,
            );
        }
        if (!current()) return;
        let confirmed = false;
        try {
            confirmed =
                updated?.collection_id === id &&
                writeSettingsVersion(updated.write_settings_version) ===
                    (BigInt(version) + 1n).toString() &&
                sameWriteSettings(updated.write_settings, settings, schema);
        } catch {
            confirmed = false;
        }
        if (!confirmed)
            throw new Error(
                'The server returned different write settings. Refresh the workspace before editing them again.',
            );
        state.collection = updated;
        state.collections = state.collections.map((item) =>
            item.collection_id === id ? updated : item,
        );
        renderView();
        toast(
            `Parquet write settings saved (version ${updated.write_settings_version}). Later appends use them; existing files keep their codecs.`,
        );
    } catch (error) {
        if (!current()) return;
        message(error.message);
        throw error;
    } finally {
        setBusy(button, false);
    }
}
function recoveryScope() {
    return {
        server_url: new URL(state.base).href.replace(/\/$/, ''),
        database_id: state.database?.database_id ?? null,
        store_id:
            state.database?.store?.store_id ??
            state.capabilities?.store_id ??
            state.stats?.identity?.store_id ??
            null,
        backend: state.capabilities?.backend ?? 'unknown',
        mock: state.capabilities?.mock === true,
    };
}
function pendingEntries(allDatabases = false) {
    const entries = [],
        seen = new Set();
    const append = (draft, kind) => {
        if (!draft || draft.terminal) return;
        const key = kind === 'create' ? draft.body.collection_id : draft.operationId;
        if (seen.has(key)) return;
        seen.add(key);
        entries.push({ kind, draft });
    };
    const addState = (drafts) => {
        append(drafts.creation, 'create');
        drafts.operations?.forEach((draft) => append(draft, 'append'));
        append(drafts.mutation, 'append');
    };
    addState(state);
    if (allDatabases) for (const [, drafts] of state.databaseDrafts ?? []) addState(drafts);
    return entries;
}
function ensurePendingExport(form, kind, id = '') {
    if (!publicDemo && !form.querySelector('[data-action="export-pending"]'))
        form.insertAdjacentHTML(
            'beforeend',
            `<button type="button" class="button small" data-action="export-pending" data-kind="${kind}" data-id="${e(id)}">Export pending request</button>`,
        );
}
function exportRecovery(kind, id) {
    const entry = pendingEntries(true).find(
        (entry) =>
            entry.kind === kind &&
            (kind === 'create'
                ? !id || entry.draft.body.collection_id === id
                : entry.draft.operationId === id),
    );
    if (!entry) throw new Error('This request is already terminal or is no longer in this tab');
    const { draft } = entry;
    const exported = serializePendingFile(draft.scope ?? recoveryScope(), kind, draft);
    download(
        `glamdb-pending-${kind === 'create' ? draft.body.collection_id : draft.operationId}.json`,
        exported,
        'application/json',
    );
}
function openRecovery(disconnecting = false) {
    const entries = pendingEntries(disconnecting);
    showModal(
        disconnecting ? 'Keep your pending requests' : 'Pending requests',
        `
        <p>Export unresolved requests before reload or disconnect. After reconnecting to the same database, import the file, then check its outcome or retry the original request.</p>
        <p class="small muted">Recovery files contain the original schema and row data, without the access token. Export and import allow up to 32 MiB per file. Importing does not submit a write.</p>
        ${entries.length ? entries.map(({ kind, draft }) => `<div class="operations-card"><div><strong>${kind === 'create' ? 'Create collection' : actionTitle(draft.action ?? 'append')}</strong><p class="mono">${e(kind === 'create' ? draft.body.collection_id : draft.operationId)}</p></div><button class="button small" data-action="export-pending" data-kind="${kind}" data-id="${e(kind === 'create' ? draft.body.collection_id : draft.operationId)}">Export pending request</button></div>`).join('') : '<p class="result-empty">No unresolved requests in this database.</p>'}
        <form id="recovery-form"><label class="field">Recovery JSON file<input type="file" name="recovery" accept="application/json,.json" required></label><p id="modal-error" class="modal-error" role="alert"></p><div class="modal-actions"><button class="button" type="submit">Import pending request</button>${disconnecting ? '<button class="button" type="button" data-action="disconnect-clear">Disconnect and clear</button>' : ''}</div></form>`,
    );
}
async function importRecovery(form) {
    const api = state.api,
        epoch = state.epoch,
        scope = recoveryScope();
    const button = form.querySelector('[type=submit]');
    setBusy(button, true);
    try {
        const file = form.elements.recovery.files[0];
        const { kind, draft } = await readPendingFile(file, scope);
        draft.scope = scope;
        if (kind === 'append') {
            const schema = await api.json(
                `/collections/${encodeURIComponent(draft.collectionId)}`,
                { signal: connectionController?.signal },
            );
            const expectedMode = (draft.action ?? 'append') === 'append' ? 'append' : 'kv';
            if ((schema.mode ?? 'append') !== expectedMode)
                throw new Error('Collection mode differs from the original pending request');
            if (schemaSignature(schema.schema ?? schema) !== schemaSignature(draft.schema))
                throw new Error('Collection schema differs from the original pending request');
            if (api !== state.api || epoch !== state.epoch) return;
            const existing = state.operations.find(
                (item) => item.operationId === draft.operationId,
            );
            const pending = pendingForCollection(draft.collectionId);
            if (existing?.terminal)
                throw new Error('This operation already has a terminal outcome in this tab');
            if (
                (existing &&
                    (existing.fingerprint !== draft.fingerprint ||
                        existing.collectionId !== draft.collectionId ||
                        (existing.action ?? 'append') !== (draft.action ?? 'append'))) ||
                (pending && pending.operationId !== draft.operationId)
            )
                throw new Error(
                    'Resolve the existing pending request before importing different content',
                );
            const selectedEpoch = state.epoch + 1;
            await selectCollection(draft.collectionId);
            if (
                api !== state.api ||
                state.epoch !== selectedEpoch ||
                state.collectionId !== draft.collectionId
            )
                return;
            const operation = existing ?? draft;
            if (!existing) saveOutcome(operation, { outcome: 'unresolved' });
            state.mutation = operation;
            state.writeOperationId = operation.operationId;
            state.tab = 'data';
            $('#modal').close();
            chrome();
        } else {
            if (api !== state.api || epoch !== state.epoch) return;
            if (
                state.creation &&
                (state.creation.body.collection_id !== draft.body.collection_id ||
                    state.creation.fingerprint !== draft.fingerprint)
            )
                throw new Error(
                    'Resolve the existing pending creation before importing a different request',
                );
            state.creation = state.creation ?? draft;
            openCreate();
        }
        toast('Original request imported. Check its outcome or retry with the same ID.');
    } catch (error) {
        if (api === state.api && form.isConnected)
            form.querySelector('#modal-error').textContent = error.message;
    } finally {
        setBusy(button, false);
    }
}
function sampleRow() {
    return Object.fromEntries(
        fields().map((field) => [
            field.name,
            field.type === 'string'
                ? field.name === 'key'
                    ? 'new:001'
                    : `${field.name} value`
                : field.type === 'int64'
                  ? '42'
                  : field.type === 'float64'
                    ? 12.5
                    : field.type === 'bool'
                      ? true
                      : field.type === 'decimal'
                        ? '0' + (field.scale ? '.' + '0'.repeat(field.scale) : '')
                        : field.type === 'date'
                          ? '2026-11-10'
                          : '2026-11-10T12:00:00.000000Z',
        ]),
    );
}
function pendingForCollection(id) {
    return (
        state.operations.find((op) => op.collectionId === id && !op.terminal) ??
        (state.mutation?.collectionId === id && !state.mutation.terminal ? state.mutation : null)
    );
}
function openAppend(requestedAction = null) {
    if (!state.collectionId) return;
    const frozen = pendingForCollection(state.collectionId);
    const action = frozen?.action ?? requestedAction ?? mutationAction();
    if (action !== 'append' && !kvEnabled(action))
        throw new Error('This connection does not advertise this KV operation');
    const initial =
        action === 'delete'
            ? ['key_to_delete']
            : action === 'compact'
              ? { max_files: '64', target_file_bytes: '8388608' }
              : [sampleRow()];
    const content = frozen
        ? action === 'delete'
            ? frozen.body.keys
            : action === 'compact'
              ? frozen.body
              : frozen.body.rows
        : initial;
    const copy =
        action === 'upsert'
            ? 'Upsert replaces full rows. Last input occurrence wins; omitted nullable fields become NULL.'
            : action === 'delete'
              ? 'Delete ignores duplicate and missing keys. Historical snapshots keep their original rows.'
              : action === 'compact'
                ? 'Compact a bounded contiguous file group. A group without a useful reduction commits a durable no-op.'
                : 'Append preserves every row, including duplicate keys. Int64 values should be quoted decimal strings.';
    showModal(
        actionTitle(action),
        `<p class="muted small">${e(copy)}</p><form id="append-form"><label class="field">${action === 'delete' ? 'Keys · JSON array' : action === 'compact' ? 'Compaction options · JSON object' : 'Rows · JSON array'}<textarea name="rows" aria-label="${action === 'delete' ? 'Keys · JSON array' : action === 'compact' ? 'Compaction options · JSON object' : 'Rows · JSON array'}" rows="9" spellcheck="false" ${frozen ? 'readonly' : ''}>${e(JSON.stringify(content, null, 2))}</textarea></label><p class="small muted">${frozen ? 'This unresolved request is frozen. Retry keeps its original operation ID.' : 'The operation is synchronous and bounded.'}</p><div id="append-outcome" role="status"></div><p class="modal-error" id="modal-error" role="alert"></p><div class="modal-actions"><button type="button" class="button" data-action="close-modal">Close</button>${frozen ? '<button type="button" class="button" data-action="resolve-pending">Check outcome</button>' : ''}<button class="button primary" type="submit">${frozen ? 'Retry same operation' : actionTitle(action)}</button></div></form>`,
    );
    $('#append-form').dataset.collectionId = state.collectionId;
    $('#append-form').dataset.actionKind = action;
    if (frozen) {
        $('#append-form').dataset.operationId = frozen.operationId;
        showAppendOutcome(frozen, frozen);
    }
}

function openLookup() {
    if (!kvEnabled('lookup')) return;
    showModal(
        'Lookup keys',
        '<form id="lookup-form"><label class="field">Keys · JSON array<textarea name="keys" aria-label="Keys · JSON array" rows="5">["key_to_find", "missing_key"]</textarea></label><p>Results follow input order, including repeated keys and explicit misses.</p><p class="modal-error" id="modal-error" role="alert"></p><button class="button primary" type="submit">Lookup keys</button><pre id="lookup-result" class="json-block"></pre></form>',
    );
}

async function lookupKeys(form) {
    const api = state.api,
        epoch = state.epoch;
    try {
        const keys = mutationKeys(JSON.parse(form.elements.keys.value), 4096);
        const snapshot_id = state.snapshotId === 'initial' ? null : state.snapshotId;
        const body = { keys, ...(state.snapshotId === null ? {} : { snapshot_id }) };
        const result = await api.json(`${collectionPath()}/lookup`, { method: 'POST', body });
        if (state.api !== api || state.epoch !== epoch || !form.isConnected) return;
        if (
            result.collection_id !== state.collectionId ||
            !Array.isArray(result.rows) ||
            result.rows.length !== keys.length ||
            result.rows.some((row, index) => row !== null && row.key !== keys[index])
        )
            throw new Error('Invalid lookup response');
        form.querySelector('#lookup-result').textContent = JSON.stringify(result, null, 2);
    } catch (error) {
        if (state.api === api && form.isConnected)
            form.querySelector('#modal-error').textContent = error.message;
    }
}

function saveOutcome(operation, outcome) {
    if (operation.terminal) {
        return { outcome: operation.outcome, result: operation.result, error: operation.error };
    }
    Object.assign(operation, {
        outcome: outcome.outcome ?? outcome.state ?? 'unresolved',
        result: outcome.result,
        error: outcome.error,
        terminal: ['committed', 'definitely_not_committed'].includes(outcome.outcome),
    });
    const existing = state.operations.findIndex(
        (item) => item.operationId === operation.operationId,
    );
    if (existing < 0) state.operations.push(operation);
    else state.operations[existing] = operation;
    if (state.mutation?.operationId === operation.operationId) state.mutation = operation;
    return outcome;
}
function showAppendOutcome(operation, payload) {
    for (const form of document.querySelectorAll('#append-form, #write-form')) {
        if (
            form.dataset.collectionId !== operation.collectionId ||
            form.dataset.operationId !== operation.operationId
        )
            continue;
        const node = form.querySelector('#append-outcome, #write-outcome');
        const title =
            payload.outcome === 'committed'
                ? `${receiptTitle(operation.action ?? 'append')} committed`
                : payload.outcome === 'definitely_not_committed'
                  ? 'Definitely not committed'
                  : 'Outcome unresolved · keep this operation ID';
        node.innerHTML = `<div class="op-outcome"><strong>${e(title)}</strong><code>${e(operation.operationId)}</code>${payload.result ? `<p>${payload.result.snapshot_created === false ? 'No new snapshot · effective view' : 'Snapshot'} ${e(payload.result.snapshot_id === '0' && payload.result.snapshot_created === false ? 'initial empty' : payload.result.snapshot_id)}</p>` : ''}<p class="small muted">${e(payload.error?.message ?? (payload.recovery_required ? 'Recovery is required before a new mutation.' : ''))}</p></div>`;
        if (!operation.terminal) ensurePendingExport(form, 'append', operation.operationId);
        else form.querySelector('[data-action="export-pending"]')?.remove();
        if (form.id === 'write-form') {
            if (operation.terminal && !form.querySelector('[data-action="new-write"]'))
                form.insertAdjacentHTML(
                    'beforeend',
                    '<button type="button" class="button small" data-action="new-write">New rows</button>',
                );
            else if (!operation.terminal) {
                form.querySelector('[type=submit]').textContent = 'Retry same operation';
                if (!form.querySelector('[data-action="resolve-operation"]'))
                    form.insertAdjacentHTML(
                        'beforeend',
                        `<button type="button" class="button small" data-action="resolve-operation" data-id="${e(operation.operationId)}">Check outcome</button>`,
                    );
            }
        }
        if (operation.terminal) {
            const button = form.querySelector('[type=submit]');
            setBusy(button, false);
            button.disabled = true;
            button.textContent = operation.outcome === 'committed' ? 'Committed' : 'Not committed';
            if (form.elements.rows) form.elements.rows.readOnly = true;
            else lockWriteForm(form);
        }
    }
}
async function appendForm(form) {
    const button = form.querySelector('[type=submit]');
    setBusy(button, true);
    form.querySelector('#modal-error, #write-error').textContent = '';
    const api = state.api,
        collectionId = form.dataset.collectionId;
    let operation;
    try {
        const previous = pendingForCollection(collectionId);
        const action = previous?.action ?? form.dataset.actionKind ?? mutationAction();
        const rows =
            form.id === 'write-form' &&
            previous &&
            previous.operationId === form.dataset.operationId
                ? previous.body.rows
                : form.id === 'write-form'
                  ? formRows(state.schema, readEditorRows(form))
                  : ['delete', 'compact'].includes(action)
                    ? JSON.parse(form.elements.rows.value)
                    : wireRows(state.schema, JSON.parse(form.elements.rows.value));
        const body =
            action === 'delete'
                ? { keys: mutationKeys(rows) }
                : action === 'compact'
                  ? compactionOptions(rows)
                  : { rows };
        operation = previous ?? prepareMutation(null, body);
        operation.action ??= action;
        operation.collectionId = collectionId;
        operation.schema ??= structuredClone(state.schema);
        operation.scope ??= recoveryScope();
        state.mutation = operation;
        if (form.id === 'write-form') {
            state.writeOperationId = operation.operationId;
            lockWriteForm(form);
        }
        form.dataset.operationId = operation.operationId;
        saveOutcome(operation, { outcome: 'in_flight' });
        const received = await api.json(
            `/collections/${encodeURIComponent(collectionId)}/${operation.action}`,
            {
                method: 'POST',
                body: { ...operation.body, operation_id: operation.operationId },
                allowOutcome: true,
            },
        );
        if (state.api !== api) return;
        const payload = saveOutcome(operation, received);
        showAppendOutcome(operation, payload);
        if (payload.outcome === 'committed') {
            button.disabled = true;
            button.textContent = 'Committed';
            if (form.elements.rows) form.elements.rows.readOnly = true;
            else lockWriteForm(form);
            if (state.collectionId === collectionId) await refreshSelection();
            toast(
                payload.result?.snapshot_created === false
                    ? 'Mutation committed. No new snapshot.'
                    : `${actionTitle(operation.action)} committed.`,
            );
        } else if (!operation.terminal) {
            button.textContent = 'Retry same operation';
            if (form.elements.rows) form.elements.rows.readOnly = true;
            else lockWriteForm(form);
        }
    } catch (error) {
        if (state.api === api && operation && !operation.terminal) {
            saveOutcome(operation, { outcome: 'transport_unknown' });
            showAppendOutcome(operation, { outcome: 'commit_unknown', recovery_required: true });
            button.textContent = 'Retry same operation';
            if (form.elements.rows) form.elements.rows.readOnly = true;
            else lockWriteForm(form);
        }
        if (form.isConnected)
            form.querySelector('#modal-error, #write-error').textContent = error.message;
    } finally {
        if (!operation?.terminal) setBusy(button, false);
    }
}
async function lookupOperation(id) {
    const api = state.api,
        epoch = state.epoch,
        collectionId = state.collectionId;
    try {
        const outcome = await api.json(
            `/collections/${encodeURIComponent(collectionId)}/operations/${encodeURIComponent(id)}`,
            {
                allowOutcome: true,
                signal: connectionController?.signal,
            },
        );
        if (api === state.api && epoch === state.epoch)
            showModal('Operation outcome', `<pre class="json-block">${json(outcome)}</pre>`);
    } catch (error) {
        if (api === state.api && epoch === state.epoch) throw error;
    }
}
async function resolveOperation(id) {
    const operation = state.operations.find((item) => item.operationId === id) ?? state.mutation;
    if (!operation) throw new Error('No pending operation in this session');
    const api = state.api;
    try {
        const received = await api.json(
            `/collections/${encodeURIComponent(operation.collectionId)}/operations/${encodeURIComponent(id)}`,
            { allowOutcome: true, signal: connectionController?.signal },
        );
        if (state.api !== api) return;
        const payload = saveOutcome(operation, received);
        showAppendOutcome(operation, payload);
        if (state.tab === 'operations') renderView();
        toast(`Operation ${payload.outcome ?? payload.state ?? 'unresolved'}`);
    } catch (error) {
        if (state.api === api) throw error;
    }
}
async function exportArrow() {
    const api = state.api,
        epoch = state.epoch,
        name = `${currentCollection()?.name ?? 'glamdb'}.arrows`;
    try {
        const bytes = await api.arrow(`${collectionPath()}/scan`, state.lastQuery, {
            signal: viewController?.signal,
        });
        if (api !== state.api || epoch !== state.epoch) return;
        download(name, bytes, 'application/vnd.apache.arrow.stream');
        toast('Complete Arrow stream downloaded.');
    } catch (error) {
        if (api === state.api && epoch === state.epoch) throw error;
    }
}
async function inspectFile(id) {
    const api = state.api,
        epoch = state.epoch,
        collectionId = state.collectionId,
        selector = state.resolvedSnapshot ?? 'initial',
        controller = viewController;
    const current = () =>
        api === state.api &&
        epoch === state.epoch &&
        collectionId === state.collectionId &&
        selector === (state.resolvedSnapshot ?? 'initial') &&
        controller === viewController &&
        !controller?.signal.aborted;
    try {
        const data = await api.json(
            `/collections/${encodeURIComponent(collectionId)}/files/${encodeURIComponent(id)}?snapshot_id=${encodeURIComponent(selector)}`,
            { signal: controller?.signal },
        );
        if (!current()) return;
        if (!publicDemo)
            validateViewMetadata(data, {
                collectionId,
                snapshotId: selector,
                mock: state.capabilities?.mock === true,
            });
        showModal('A closer look.', fileDetails(data, selector));
    } catch (error) {
        if (current()) throw error;
    }
}
function navigate(tab) {
    if (!nav.some(([key]) => key === tab)) tab = 'overview';
    if (state.tab === 'data') {
        try {
            readQuery();
        } catch {
            /* Draft may be incomplete while navigating. */
        }
    }
    state.tab = tab;
    chrome();
    $('#sidebar')?.classList.remove('open');
}

document.addEventListener('submit', async (event) => {
    if (
        ![
            'connect-form',
            'database-form',
            'query-form',
            'create-form',
            'append-form',
            'write-form',
            'operation-lookup-form',
            'lookup-form',
            'recovery-form',
            'write-settings-form',
        ].includes(event.target.id)
    )
        return;
    event.preventDefault();
    const form = event.target,
        api = state.api;
    try {
        if (form.id === 'connect-form') {
            const button = form.querySelector('button');
            setBusy(button, true);
            resetSession();
            const controller = new AbortController();
            connectionController = controller;
            try {
                state.base = form.elements.base.value;
                state.token = form.elements.token.value;
                const api = new WorkbenchAPI(state.base, state.token);
                state.api = api;
                const capabilities = await api.json('/capabilities', {
                    signal: controller.signal,
                });
                if (state.api !== api || controller.signal.aborted) return;
                state.capabilities = capabilities;
                syncQueryLimit();
                if (capabilities.features?.database_management) await databaseWorkspace();
                else await refreshWorkspace();
            } catch (error) {
                if (connectionController !== controller) return;
                resetSession();
                login();
                $('#login-error').textContent = error.message;
            } finally {
                setBusy(button, false);
            }
        } else if (form.id === 'database-form') {
            if (accessGateway) await connectDatabase(form.elements.database.value);
            else await createDatabase(form);
        } else if (form.id === 'query-form') await runQuery();
        else if (form.id === 'append-form' || form.id === 'write-form') await appendForm(form);
        else if (form.id === 'lookup-form') await lookupKeys(form);
        else if (form.id === 'create-form') await createForm(form);
        else if (form.id === 'recovery-form') await importRecovery(form);
        else if (form.id === 'write-settings-form') await saveWriteSettings(form);
        else if (form.id === 'operation-lookup-form')
            await lookupOperation(form.elements.operation.value.trim());
    } catch (error) {
        if (state.api === api) report(error);
    }
});
document.addEventListener('click', async (event) => {
    const target = event.target.closest('[data-action]');
    if (!target || target.disabled) return;
    const action = target.dataset.action,
        api = state.api;
    try {
        if (signingOut) return;
        if (action === 'sign-in') {
            resetSession();
            location.assign('/oauth2/start');
        } else if (action === 'sign-out') await signOut();
        else if (action === 'databases') await databaseWorkspace();
        else if (action.startsWith('database-')) await databaseAction(target);
        else if (action === 'reconcile-workspace') {
            await api.json('/reconcile', { method: 'POST', body: {} });
            if (state.api === api)
                toast('Database reconciled. Check the original operation outcome.');
        } else if (action === 'nav') navigate(target.dataset.tab);
        else if (action === 'menu') $('#sidebar').classList.toggle('open');
        else if (action === 'collapse-sidebar') {
            sidebarCollapsed = !sidebarCollapsed;
            savePreference('sidebar-collapsed', sidebarCollapsed);
            $('.shell').dataset.collapsed = String(sidebarCollapsed);
            target.setAttribute(
                'aria-label',
                sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar',
            );
            target.setAttribute('title', sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar');
            target.setAttribute('aria-expanded', String(!sidebarCollapsed));
        } else if (action === 'toggle-appearance') {
            colorMode = colorMode === 'dark' ? 'light' : 'dark';
            savePreference('appearance', colorMode);
            applyAppearance(state.theme, colorMode);
            target.setAttribute(
                'aria-label',
                `Switch to ${colorMode === 'dark' ? 'light' : 'dark'} theme`,
            );
            target.setAttribute('title', target.getAttribute('aria-label'));
            target.querySelector('span').textContent =
                colorMode === 'dark' ? 'Dark theme' : 'Light theme';
        } else if (action === 'disconnect') {
            if (publicDemo) {
                location.reload();
                return;
            }
            if (pendingEntries(true).length) openRecovery(true);
            else {
                resetSession();
                if (accessGateway)
                    accessLogin('Workspace cleared. Reload access to open a database.');
                else login();
            }
        } else if (action === 'disconnect-clear') {
            resetSession();
            if (accessGateway)
                accessLogin('Workspace cleared. Sign in or reload access to open a database.');
            else login();
        } else if (action === 'pending-requests') openRecovery();
        else if (action === 'export-pending')
            exportRecovery(target.dataset.kind, target.dataset.id);
        else if (action === 'refresh') {
            if (accessGateway) {
                await connectDatabase(state.databaseId);
                return;
            }
            await refreshWorkspace();
            if (state.api === api) toast('Workspace refreshed.');
        } else if (action === 'reload-access') await reloadAccess();
        else if (action === 'select-collection') await selectCollection(target.dataset.id);
        else if (action === 'create') openCreate();
        else if (action === 'close-modal') $('#modal').close();
        else if (action === 'append') openAppend();
        else if (action === 'delete-keys') openAppend('delete');
        else if (action === 'compact-files') openAppend('compact');
        else if (action === 'lookup-keys') openLookup();
        else if (action === 'count-rows') await countRows();
        else if (action === 'cancel-count') countController?.abort();
        else if (action === 'add-write-row' || action === 'remove-write-row') {
            draftWrite();
            try {
                readQuery();
            } catch {
                /* Preserve drafts while editing. */
            }
            const removedIndex =
                action === 'remove-write-row' ? Number(target.dataset.index) : null;
            if (action === 'add-write-row' && state.writeDraft.length < 100)
                state.writeDraft.push({});
            else if (removedIndex !== null && state.writeDraft.length > 1)
                state.writeDraft.splice(removedIndex, 1);
            renderWritePanel({ removedIndex });
        } else if (action === 'new-write') {
            state.writeOperationId = null;
            state.writeDraft = null;
            renderWritePanel({ reset: true });
        } else if (action === 'download-query-json') {
            download('query.json', JSON.stringify(readQuery(), null, 2), 'application/json');
        } else if (action === 'add-field') {
            creationFields = readCreationFields();
            creationFields.push({ name: '', type: 'string', nullable: true });
            renderCreationFields();
        } else if (action === 'remove-field') {
            creationFields = readCreationFields();
            creationFields.splice(Number(target.dataset.index), 1);
            renderCreationFields();
        } else if (action === 'add-condition') {
            readQuery();
            state.clauses.push({ field: fields()[0].name, op: 'eq', value: '' });
            renderView();
        } else if (action === 'remove-condition') {
            try {
                readQuery();
            } catch {
                /* Keep valid form drafts while removing an invalid clause. */
            }
            state.clauses.splice(Number(target.dataset.index), 1);
            renderView();
        } else if (action === 'explain-query') await explainQuery(true);
        else if (action === 'explain-current') await explainQuery(false);
        else if (action === 'previous-page') {
            state.page = Math.max(0, state.page - 1);
            renderReadResults();
        } else if (action === 'next-page') {
            state.page++;
            renderReadResults();
        } else if (action === 'open-snapshot') {
            state.tab = 'data';
            await selectCollection(state.collectionId, target.dataset.id);
        } else if (action === 'export-json') {
            download(
                `${currentCollection()?.name ?? 'glamdb'}.json`,
                JSON.stringify(
                    {
                        schema: state.result.schema,
                        snapshot_id: state.result.snapshot_id,
                        rows: state.rows,
                    },
                    null,
                    2,
                ),
                'application/json',
            );
        } else if (action === 'export-arrow') await exportArrow();
        else if (action === 'file') await inspectFile(target.dataset.id);
        else if (action === 'lookup-operation') await lookupOperation(target.dataset.id);
        else if (action === 'resolve-operation') await resolveOperation(target.dataset.id);
        else if (action === 'resolve-pending')
            await resolveOperation($('#append-form').dataset.operationId);
        else if (action === 'about')
            showModal(
                'A pretty powerful workbench.',
                `<div class="metadata-list"><p>Browse collections, append typed rows, filter your data and explore immutable snapshots.</p><div class="notice">${publicDemo ? 'This public demo runs in your browser. Your rows stay in this tab; Reset demo or reload restores the samples. JSON export is available here. Connect to a server separately for the network SDK and Arrow IPC.' : 'Connected servers report their backend and supported capabilities. Native writes use the C++ engine; mock results are disposable. Physical power-loss durability remains unqualified.'}</div><p class="small muted">Theme: edit <code>ui/config/theme.json</code>. Changes reload automatically. No token is saved in browser storage.</p></div>`,
            );
    } catch (error) {
        if (state.api === api) report(error);
    }
});
document.addEventListener('input', (event) => {
    const form = event.target.closest('form');
    if (form?.id === 'write-form') {
        if (event.target.matches('[data-write-null]')) {
            const field = event.target.closest('[data-field]');
            field.querySelector('[data-write-value]').disabled = event.target.checked;
        }
        draftWrite();
        $('#write-json').textContent = previewRows(state.schema, state.writeDraft);
    } else if (form?.id === 'query-form') {
        try {
            readQuery();
        } catch {
            /* Validation appears on submit. */
        }
        $('#query-json').textContent = queryPreview();
    }
});
document.addEventListener('change', async (event) => {
    const target = event.target,
        api = state.api;
    try {
        if (target.id === 'database-selector') await connectDatabase(target.value);
        else if (target.id === 'snapshot-selector')
            await selectCollection(
                state.collectionId,
                target.value === 'latest' ? null : target.value,
            );
        else if (target.matches('[data-parquet-codec], [data-parquet-encoding]'))
            syncParquetChoice(target.closest('[data-parquet-choice]'));
        else if (target.matches('#schema-editor [name=field-type]')) {
            refreshColumnOptions(target.closest('.schema-editor-row'));
            creationFields = readCreationFields();
            renderCreationFields();
        }
    } catch (error) {
        if (state.api === api) report(error);
    }
});
$('#modal').addEventListener('click', (event) => {
    if (event.target === $('#modal')) $('#modal').close();
});
async function bootstrap() {
    try {
        await loadTheme(true);
        if (accessGateway) await reloadAccess();
        else if (publicDemo) {
            const [{ BrowserDemoAPI }, response] = await Promise.all([
                import('./demo.js'),
                fetch(new URL('../config/demo-data.json', import.meta.url)),
            ]);
            if (!response.ok) throw new Error('Demo data is unavailable');
            state.api = new BrowserDemoAPI(await response.json());
            state.capabilities = await state.api.json('/capabilities');
            syncQueryLimit();
            await refreshWorkspace();
        } else {
            login();
            loadHealth();
        }
        configTimer = setInterval(() => loadTheme(), 2000);
    } catch (error) {
        $('#app').innerHTML =
            `<main id="main-content" class="boot"><div><h1>Theme unavailable</h1><p>${e(error.message)}</p><p>Check the theme configuration and reload.</p></div></main>`;
    }
}
window.addEventListener('pagehide', () => {
    clearInterval(configTimer);
    connectionController?.abort();
    accessPollController?.abort();
    viewController?.abort();
    queryController?.abort();
    state.api?.close?.();
    state.token = '';
});
bootstrap();
