import { WorkbenchAPI } from './api.js';
import { applyTheme, validateTheme, brandAsset } from './theme.js';
import {
    escapeHTML as e,
    displayValue,
    selectView,
    resetConnection,
    buildPredicate,
    wireRows,
    prepareMutation,
} from './model.js';

const $ = (selector) => document.querySelector(selector);
const publicDemo = document.documentElement.dataset.publicDemo === 'true';
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
    ['style', 'palette'],
];
let state = resetConnection({
    base: location.origin,
    epoch: -1,
    theme: null,
    themePreview: false,
});
let connectionController = null,
    viewController = null,
    queryController = null,
    queryGeneration = 0,
    toastTimer = null,
    themeStamp = '',
    configTimer = null;
const labels = (name, fallback = name) => state.theme?.labels?.[name] ?? fallback;
const collectionPath = () => `/v0/collections/${encodeURIComponent(state.collectionId)}`;
const viewQuery = () =>
    state.snapshotId === null ? '' : `?snapshot_id=${encodeURIComponent(state.snapshotId)}`;
const currentCollection = () =>
    state.collections.find((item) => item.collection_id === state.collectionId);
const number = (value) =>
    value === null || value === undefined
        ? '—'
        : typeof value === 'object'
          ? number(value.value)
          : String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
const fields = () => state.schema?.fields ?? [];
const json = (value) => e(JSON.stringify(value, null, 2));
const badge = (text, kind = '') => `<span class="pill ${kind}">${e(text)}</span>`;

function toast(message) {
    const node = $('#toast');
    node.textContent = message;
    node.classList.add('visible');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => node.classList.remove('visible'), 4500);
}
function report(error) {
    if (error.name !== 'AbortError') toast(error.message ?? 'Something went wrong');
}
function resetSession() {
    connectionController?.abort();
    viewController?.abort();
    queryController?.abort();
    connectionController = viewController = queryController = null;
    queryGeneration++;
    state = resetConnection(state);
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
    return snapshot.rows_added ?? snapshot.rows_appended ?? '—';
}

async function loadTheme(force = false) {
    try {
        const response = await fetch(
            publicDemo ? new URL('../config/theme.json', import.meta.url) : '/workbench/config',
            { cache: 'no-store' },
        );
        if (!response.ok) throw new Error('Theme file is invalid; keeping the last valid theme.');
        const config = validateTheme(await response.json());
        const stamp = JSON.stringify(config);
        if (force || (!state.themePreview && stamp !== themeStamp)) {
            state.theme = config;
            themeStamp = stamp;
            applyTheme(config);
            updateThemeText();
        }
        $('#theme-warning').hidden = true;
    } catch (error) {
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
    <p class="login-error" id="login-error" role="alert"></p><p class="login-footnote">Local demo? Use the token from your GLAMDB_TOKEN environment.<br>Mock data is disposable. No storage durability is claimed.</p></div></section></main>`;
}

function chrome() {
    $('#app').innerHTML =
        `<div class="shell"><aside class="sidebar" id="sidebar" aria-label="Workspace navigation">${brand()}
    <p class="brand-tagline" data-brand="tagline">${e(state.theme.brand.tagline)}</p><div class="workspace-tag"><span class="status-dot"></span>
    <div><strong>My workspace</strong><small>${e(new URL(state.base).host)} · ${state.capabilities?.mock ? 'MOCK' : 'CONNECTED'}</small></div></div>
    <nav class="nav">${nav.map(([key, symbol]) => `<button data-action="nav" data-tab="${key}" class="${state.tab === key ? 'active' : ''}" ${state.tab === key ? 'aria-current="page"' : ''}>${icon(symbol)}<span data-label="nav_${key}">${e(labels(`nav_${key}`))}</span></button>`).join('')}</nav>
    <div class="collection-heading"><span data-label="collections">${e(labels('collections'))}</span><button class="icon-button" data-action="create" aria-label="New collection">${icon('plus')}</button></div>
    <div class="collection-list">${state.collections.map((item) => `<button data-action="select-collection" data-id="${e(item.collection_id)}" class="${state.collectionId === item.collection_id ? 'selected' : ''}">${icon('table')}<span>${e(item.name)}</span></button>`).join('')}</div>
    <div class="sidebar-footer">${icon('kiss', 'decor-kiss')}<p data-label="footer">${e(labels('footer'))}</p></div></aside>
    <div class="workspace"><header class="topbar"><div class="breadcrumb"><button class="icon-button menu-toggle" data-action="menu" aria-label="Toggle navigation">${icon('menu')}</button><span>Workspace</span>${icon('chevron')}<strong>${e(currentCollection()?.name ?? 'Your collections')}</strong></div>
    <div class="top-actions"><button class="text-button docs-button" data-action="about">Quick guide</button><button class="icon-button" data-action="refresh" aria-label="Refresh workspace">${icon('refresh')}</button><button class="text-button" data-action="disconnect">${publicDemo ? 'Reset demo' : 'Disconnect'}</button><span class="avatar" aria-label="Workspace avatar">${state.theme.brand.mark ? `<img data-brand-logo="mark" class="brand-mark" src="${e(brandAsset(state.theme, 'mark'))}" alt="">` : 'G'}</span></div></header>
    <main id="main-content" class="main">${state.capabilities?.mock ? `<div class="mock-banner">${icon('sparkle')}<span data-label="${publicDemo ? 'public_demo_notice' : 'mock_notice'}">${e(labels(publicDemo ? 'public_demo_notice' : 'mock_notice'))}</span></div>` : ''}<div id="view"></div>
    <footer class="page-footer"><span>GlamDB · <span data-brand="tagline">${e(state.theme.brand.tagline)}</span></span><span>Append mode · duplicate keys preserved ${icon('sparkle', 'decor-sparkle')}</span></footer></main></div></div>`;
    renderView();
}
function snapshotControl() {
    return `<label class="snapshot-control">${icon('clock')}<span class="sr-only">Snapshot</span><select id="snapshot-selector" aria-label="Snapshot"><option value="latest" ${state.snapshotId === null ? 'selected' : ''}>${e(labels('latest'))}</option><option value="initial" ${state.snapshotId === 'initial' ? 'selected' : ''}>Initial empty view</option>${state.snapshots.map((s) => `<option value="${e(s.snapshot_id)}" ${state.snapshotId === s.snapshot_id ? 'selected' : ''}>Snapshot ${e(s.snapshot_id)}</option>`).join('')}</select></label>`;
}
function heading(title, copy = '', snapshot = true) {
    return `<div class="view-heading"><div><p class="eyebrow" data-brand="eyebrow">${e(state.theme.brand.eyebrow)}</p><h1 id="view-title" tabindex="-1">${e(title)}</h1>${copy ? `<p>${e(copy)}</p>` : ''}</div>${snapshot && state.collectionId ? snapshotControl() : ''}</div>`;
}
function emptyView() {
    return `<section class="panel empty">${icon('table')}<h2 data-label="empty_title">${e(labels('empty_title'))}</h2><p data-label="empty_copy">${e(labels('empty_copy'))}</p><button class="button primary" data-action="create">${icon('plus')}New collection</button></section>`;
}
function overview() {
    const s = state.stats ?? {};
    return `${heading(labels('overview_title'), labels('overview_copy'))}<section class="hero"><div class="hero-copy"><p class="eyebrow" data-label="hero_eyebrow">${e(labels('hero_eyebrow'))}</p><h2 data-label="hero_title">${e(labels('hero_title'))}</h2><p data-label="hero_copy">${e(labels('hero_copy'))}</p><button class="button primary" data-action="nav" data-tab="data"><span data-label="hero_button">${e(labels('hero_button'))}</span>${icon('arrow')}</button></div><div class="hero-art" aria-hidden="true"><div class="mini-card back"></div><div class="mini-card"><div class="mini-card-head">${icon('table')}${e(currentCollection()?.name ?? 'your_collection')}</div><div class="mini-card-grid">${Array.from({ length: 12 }, () => '<span></span>').join('')}</div></div>${icon('bow', 'hero-bow decor-bow')}${icon('sparkle', 'hero-star one decor-sparkle')}${icon('sparkle', 'hero-star two decor-sparkle')}${icon('kiss', 'hero-kiss decor-kiss')}<p class="hero-note" data-label="hero_note">${e(labels('hero_note'))}</p></div></section>
    <section class="metrics" aria-label="Collection statistics">${[
        ['Rows', s.rows, 'table'],
        ['Columns', fields().length, 'columns'],
        ['Snapshots', s.retained_snapshots ?? state.snapshots.length, 'clock'],
        ['Data files', s.data_files ?? state.files.length, 'database'],
    ]
        .map(
            ([label, value, symbol]) =>
                `<div class="metric"><div class="metric-icon">${icon(symbol)}</div><div><strong>${number(value)}</strong><span>${label.toUpperCase()}</span></div></div>`,
        )
        .join('')}</section>
    ${
        state.collectionId
            ? `<div class="panels"><section class="panel"><div class="panel-header"><div><h2>Inside your collection</h2><p>${e(currentCollection()?.name)} · fixed schema</p></div><button class="button small" data-action="nav" data-tab="schema">View schema${icon('arrow')}</button></div><div class="panel-body">${fields()
                  .slice(0, 6)
                  .map(
                      (field) =>
                          `<div class="column-preview"><span class="column-name">${icon(field.name === 'key' ? 'key' : 'columns')}<strong>${e(field.name)}</strong></span><span class="type">${e(field.type)}${field.nullable ? ' · nullable' : ''}</span></div>`,
                  )
                  .join(
                      '',
                  )}</div></section><section class="panel"><div class="panel-header"><h2>A little history</h2>${badge('Immutable', 'pink')}</div><div class="panel-body">${snapshotList(3)}<button class="text-button" data-action="nav" data-tab="snapshots">See all snapshots →</button></div></section></div>`
            : emptyView()
    }`;
}
function snapshotList(max = 100) {
    return state.snapshots.length
        ? state.snapshots
              .slice()
              .reverse()
              .slice(0, max)
              .map(
                  (s) =>
                      `<div class="snapshot-item"><span class="snapshot-marker"></span><div><strong>Append snapshot</strong><p><code>${e(s.snapshot_id)}</code></p></div>${badge(`+${countSnapshotRows(s)} rows`)}</div>`,
              )
              .join('')
        : '<p class="muted small">No snapshot yet. Your initial view is empty.</p>';
}
function readQuery() {
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
    if (!Number.isInteger(state.limit) || state.limit < 0 || state.limit > 1000)
        throw new Error('Use a limit between 0 and 1000 in the browser');
    const body = {
        projection: state.projection,
        predicate: buildPredicate(state.clauses, state.join, state.negate, fields()),
        limit: state.limit,
    };
    if (state.snapshotId !== null) body.snapshot_id = state.snapshotId;
    return body;
}
function dataView() {
    const columns = state.projection.length
        ? state.projection
        : fields().map((field) => field.name);
    return `${heading(labels('data_title'), labels('data_copy'))}<section class="panel"><div class="panel-header"><div><h2>Make it your query</h2><p>Projection + typed filters. A fixed snapshot, every time.</p></div><button class="button small" data-action="append">${icon('plus')}<span data-label="append_rows">${e(labels('append_rows'))}</span></button></div><div class="panel-body"><form id="query-form" class="query-controls"><div><label>Columns to read</label><div class="column-choices" style="margin-top:9px">${fields()
        .map(
            (field) =>
                `<label class="column-check"><input type="checkbox" name="column" value="${e(field.name)}" ${columns.includes(field.name) ? 'checked' : ''}>${e(field.name)}</label>`,
        )
        .join('')}</div></div>
    <div><div class="filter-toolbar"><label>Filter your rows</label><div class="button-row"><select name="join" aria-label="Combine conditions"><option value="and" ${state.join === 'and' ? 'selected' : ''}>Match all · AND</option><option value="or" ${state.join === 'or' ? 'selected' : ''}>Match any · OR</option></select><label class="small"><input type="checkbox" name="negate" ${state.negate ? 'checked' : ''}> Negate</label></div></div>
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
                    .map(
                        ([op, label]) =>
                            `<option value="${op}" ${c.op === op ? 'selected' : ''}>${label}</option>`,
                    )
                    .join(
                        '',
                    )}</select><input name="value" aria-label="Filter value ${i + 1}" value="${e(c.value)}" placeholder="A value, in its original type"><button type="button" class="icon-button" data-action="remove-condition" data-index="${i}" aria-label="Remove condition ${i + 1}">${icon('close')}</button></div>`,
        )
        .join('')}
    <button type="button" class="text-button" data-action="add-condition">+ Add a condition</button></div><div class="query-footer"><label>Result limit<input name="limit" type="number" min="0" max="1000" value="${state.limit}"></label><div class="button-row"><button type="button" class="button" data-action="explain-query">${icon('sparkle')}Explain</button><button type="submit" class="button primary" id="run-query">${icon('arrow')}<span data-label="run_query">${e(labels('run_query'))}</span></button></div></div></form></div></section>
    ${state.result ? resultTable() : `<div class="notice" style="margin-top:20px">Ready when you are. Your result will appear here, with its exact snapshot identity.</div>`}`;
}
function resultTable() {
    const schema = state.result.schema?.fields ?? state.projection.map((name) => ({ name }));
    const page = state.rows.slice(state.page * 100, (state.page + 1) * 100);
    return `<section class="panel results-panel"><div class="panel-header"><div><h2>Your results</h2><p>${number(state.rows.length)} rows · snapshot <code>${e(state.result.snapshot_id ?? 'initial empty')}</code></p></div><button class="button small" data-action="${publicDemo ? 'export-json' : 'export-arrow'}">${icon('download')}${publicDemo ? 'JSON' : 'Arrow'}</button></div><div class="table-wrap"><table aria-label="Query results"><thead><tr>${schema.map((f) => `<th>${e(f.name)}</th>`).join('')}</tr></thead><tbody>${page.map((row) => `<tr>${schema.map((f) => `<td title="${e(displayValue(row[f.name]))}">${row[f.name] === null ? '<span class="null">NULL</span>' : e(displayValue(row[f.name]))}</td>`).join('')}</tr>`).join('')}</tbody></table>${!page.length ? '<p class="result-empty">No rows match this query. The result schema is still available.</p>' : ''}</div><div class="table-footer"><span>${page.length ? state.page * 100 + 1 : 0}–${state.page * 100 + page.length} of ${number(state.rows.length)} · bounded preview</span><div class="button-row"><button class="button small" data-action="previous-page" ${state.page === 0 ? 'disabled' : ''}>Previous</button><button class="button small" data-action="next-page" ${(state.page + 1) * 100 >= state.rows.length ? 'disabled' : ''}>Next</button></div></div></section>`;
}

function renderView() {
    if (!$('#view')) return;
    if (!state.collectionId && !['overview', 'style'].includes(state.tab)) {
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
        style: styleView,
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
                `<tr><td><strong>${e(f.name)}</strong></td><td><span class="type">${e(f.type)}</span></td><td>${e(f.field_id)}</td><td>${f.nullable ? 'Yes' : 'Required'}</td></tr>`,
        )
        .join(
            '',
        )}</tbody></table></div></section><div class="notice" style="margin-top:20px">Append mode preserves duplicate keys. A field called key does not imply uniqueness. Schema evolution is not enabled.</div>`;
}
function snapshotsView() {
    return `${heading('Every version has a story.', 'Read an immutable view. Keep your latest data untouched.')}<section class="panel"><div class="panel-header"><h2>Snapshot history</h2>${badge(`${state.snapshots.length} retained`)}</div><div class="table-wrap"><table aria-label="Snapshot history"><thead><tr><th>Snapshot ID</th><th>Parent</th><th>Rows added</th><th></th></tr></thead><tbody>${state.snapshots
        .slice()
        .reverse()
        .map(
            (s) =>
                `<tr><td class="mono">${e(s.snapshot_id)}</td><td class="mono">${e(s.parent_id ?? s.parent_snapshot_id ?? 'Initial')}</td><td>${number(countSnapshotRows(s))}</td><td><button class="button small" data-action="open-snapshot" data-id="${e(s.snapshot_id)}">Explore${icon('arrow')}</button></td></tr>`,
        )
        .join(
            '',
        )}</tbody></table></div>${!state.snapshots.length ? '<p class="result-empty">No append has been published yet.</p>' : ''}</section>`;
}
function storageView() {
    return `${heading('A place for every file.', 'File membership belongs to the selected snapshot.')}<div class="notice" style="margin-bottom:20px">${state.capabilities?.mock ? 'These are synthetic descriptors for the mock adapter. No physical Parquet files are written.' : 'Immutable files; metadata is available where supported.'}</div><section class="panel"><div class="panel-header"><h2>Data files</h2>${badge(`${state.files.length} files`)}</div><div class="table-wrap"><table aria-label="Data files"><thead><tr><th>File</th><th>Rows</th><th>Bytes</th><th></th></tr></thead><tbody>${state.files.map((f, i) => `<tr><td><strong>Data file ${i + 1}</strong><p class="muted small mono">${e(f.file_id)}</p></td><td>${number(f.record_count)}</td><td>${f.compressed_bytes === null ? 'Unknown' : number(f.compressed_bytes)}</td><td><button class="button small" data-action="file" data-id="${e(f.file_id)}">Inspect${icon('arrow')}</button></td></tr>`).join('')}</tbody></table></div>${!state.files.length ? '<p class="result-empty">No files in this view.</p>' : ''}</section><div class="notice" style="margin-top:20px">Compaction, expiration and garbage collection are unavailable in append v0. Optional index presence does not imply execution support.</div>`;
}
function explainView() {
    return `${heading('A peek behind the query.', 'Plan estimates and executed measurements stay separate.')}<section class="panel"><div class="panel-header"><h2>Query plan</h2><button class="button small" data-action="explain-current">${icon('refresh')}Refresh plan</button></div><div class="panel-body">${
        state.plan
            ? `<div class="button-row" style="margin-bottom:18px">${badge('Fixed snapshot', 'pink')}${badge(state.capabilities?.mock ? 'Reference mock scan' : 'Native scan')}${badge('Page/Bloom execution unsupported')}</div>${[
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
    }</div></section>${state.result?.profile ? `<section class="panel"><div class="panel-header"><h2>Execution profile</h2>${badge('Mock measurements')}</div><div class="panel-body"><pre class="json-block">${json(state.result.profile)}</pre><p class="muted small">A mock reference scan does not demonstrate native pruning, device IO or durability.</p></div></section>` : ''}`;
}
function operationsView() {
    const ops = state.operations.filter((op) => op.collectionId === state.collectionId);
    return `${heading('Keep your operations close.', 'An unknown outcome is unresolved. A timeout is not a rollback.')}<section class="panel"><div class="panel-header"><h2>This session's appends</h2>${badge('Synchronous API')}</div>${
        ops.length
            ? ops
                  .slice()
                  .reverse()
                  .map(
                      (op) =>
                          `<div class="operations-card">${icon('activity')}<div><strong>${e(op.outcome ?? 'Unresolved')}</strong><p class="mono">${e(op.operationId)}</p><p>Snapshot ${e(op.result?.snapshot_id ?? 'not yet confirmed')}</p></div><button class="button small" data-action="resolve-operation" data-id="${e(op.operationId)}">Check outcome</button></div>`,
                  )
                  .join('')
            : '<p class="result-empty">No appends submitted in this browser session. Server outcomes are queried by operation ID.</p>'
    }</section>`;
}
function styleView() {
    const c = state.theme;
    return `${heading('Make it your kind of pink.', 'A runtime theme, because a new mood should not need a new binary.', false)}<section class="panel"><div class="panel-header"><h2>Your palette</h2>${badge('Live preview', 'pink')}</div><div class="panel-body"><div class="style-grid">${Object.entries(
        c.colors,
    )
        .filter(([, v]) => /^#[0-9a-f]{6}$/i.test(v))
        .map(
            ([name, value]) =>
                `<label class="color-field"><input type="color" data-theme-color="${e(name)}" value="${e(value)}" aria-label="Color ${e(name)}"><span>${e(name.replaceAll('_', ' '))}<code>${e(value)}</code></span></label>`,
        )
        .join(
            '',
        )}</div><div class="section-heading"><h2>The little extras</h2></div><div class="toggle-row">${Object.entries(
        c.decoration,
    )
        .map(
            ([name, value]) =>
                `<label><input type="checkbox" data-theme-decoration="${e(name)}" ${value ? 'checked' : ''}>${e(name)}</label>`,
        )
        .join(
            '',
        )}</div><div class="button-row" style="margin-top:24px"><button class="button" data-action="reset-theme">Reset preview</button><button class="button primary" data-action="download-theme">${icon('download')}Download theme.json</button></div></div></section><div class="notice" style="margin-top:20px">Permanent changes: edit <code>ui/config/theme.json</code> (or your server's --theme-file). This page reloads valid changes automatically. Preview changes above are local to this tab until reset. No core rebuild, frontend build or server restart is required.</div>`;
}

async function allPages(path, signal, api = state.api) {
    const items = [];
    let cursor = null;
    for (let pages = 0; pages < 100; pages++) {
        const separator = path.includes('?') ? '&' : '?';
        const page = await api.json(
            `${path}${separator}page_size=100${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`,
            { signal },
        );
        items.push(...(page.items ?? []));
        cursor = page.next_cursor;
        if (!cursor) return items;
    }
    throw new Error('Metadata listing is too large for this workbench');
}
async function refreshSelection() {
    if (!state.collectionId) {
        state.schema = null;
        state.stats = null;
        state.snapshots = [];
        state.files = [];
        chrome();
        return;
    }
    viewController?.abort();
    viewController = new AbortController();
    const signal = viewController.signal,
        api = state.api,
        epoch = state.epoch,
        id = state.collectionId;
    const current = () =>
        !signal.aborted && api === state.api && epoch === state.epoch && id === state.collectionId;
    const info = await api.json(`/v0/collections/${encodeURIComponent(id)}`, { signal });
    if (!current()) return;
    const selected =
        state.snapshotId === null ? (info.current_snapshot_id ?? 'initial') : state.snapshotId;
    const suffix = `?snapshot_id=${encodeURIComponent(selected)}`;
    const [schema, stats, snapshots, filesResult] = await Promise.all([
        api.json(`/v0/collections/${encodeURIComponent(id)}/schema`, { signal }),
        api.json(`/v0/collections/${encodeURIComponent(id)}/stats${suffix}`, { signal }),
        allPages(`/v0/collections/${encodeURIComponent(id)}/snapshots`, signal, api),
        allPages(`/v0/collections/${encodeURIComponent(id)}/files${suffix}`, signal, api),
    ]);
    if (!current()) return;
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
    state.collections = state.collections.map((item) => (item.collection_id === id ? info : item));
    chrome();
}
async function selectCollection(id, snapshot = null) {
    const changedCollection = id !== state.collectionId;
    if (!changedCollection && $('#query-form')) {
        try {
            readQuery();
        } catch {
            /* Retain edited drafts even if their predicate is not valid yet. */
        }
    }
    viewController?.abort();
    queryController?.abort();
    queryGeneration++;
    Object.assign(state, selectView(state, id, snapshot));
    if (changedCollection) {
        state.projection = [];
        state.clauses = [];
    }
    state.schema = null;
    state.stats = null;
    state.snapshots = [];
    state.files = [];
    chrome();
    $('#view').innerHTML = '<p class="notice">Opening this snapshot…</p>';
    await refreshSelection();
}
async function refreshWorkspace() {
    const api = state.api,
        epoch = state.epoch,
        signal = connectionController?.signal;
    const collections = await allPages('/v0/collections', signal, api);
    if (api !== state.api || epoch !== state.epoch || signal?.aborted) return;
    state.collections = collections;
    const id = collections.some((c) => c.collection_id === state.collectionId)
        ? state.collectionId
        : (collections[0]?.collection_id ?? null);
    if (id !== state.collectionId) await selectCollection(id);
    else await refreshSelection();
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
        const result = await state.api.json(`${collectionPath()}/scan`, {
            method: 'POST',
            body,
            signal: controller.signal,
        });
        if (epoch !== state.epoch || generation !== queryGeneration) return;
        state.result = result;
        state.rows = result.rows ?? [];
        state.page = 0;
        state.lastQuery = { ...body, snapshot_id: result.snapshot_id ?? 'initial' };
        renderView();
        toast(
            `${state.rows.length} rows. Fixed snapshot ${result.snapshot_id ?? 'initial empty'}.`,
        );
    } finally {
        if (queryController === controller) queryController = null;
        setBusy(button, false);
    }
}
async function explainQuery(fromForm) {
    const epoch = state.epoch;
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
    if (epoch !== state.epoch) return;
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
    $('#schema-editor').innerHTML = creationFields
        .map(
            (field, i) =>
                `<div class="schema-editor-row"><input name="field-name" value="${e(field.name)}" aria-label="Column name ${i + 1}" required><select name="field-type" aria-label="Column type ${i + 1}">${['string', 'int64', 'float64', 'bool', 'timestamp_us_utc'].map((type) => `<option ${field.type === type ? 'selected' : ''}>${type}</option>`).join('')}</select><label><input name="field-nullable" type="checkbox" ${field.nullable ? 'checked' : ''}>NULL</label><button class="icon-button" type="button" data-action="remove-field" data-index="${i}" aria-label="Remove field ${i + 1}">${icon('close')}</button></div>`,
        )
        .join('');
}
function readCreationFields() {
    return [...$('#schema-editor').querySelectorAll('.schema-editor-row')].map((row) => ({
        name: row.querySelector('[name=field-name]').value,
        type: row.querySelector('[name=field-type]').value,
        nullable: row.querySelector('[name=field-nullable]').checked,
    }));
}
function openCreate() {
    creationFields = state.creation
        ? structuredClone(state.creation.body.schema.fields)
        : [
              { name: 'key', type: 'string', nullable: false },
              { name: 'label', type: 'string', nullable: false },
              { name: 'amount', type: 'int64', nullable: true },
          ];
    showModal(
        'A new collection, just for you.',
        `<form id="create-form"><label class="field">Collection name<input name="name" placeholder="my_collection" pattern="[a-z][a-z0-9_]{0,62}" required autofocus><small>Lowercase letters, numbers and underscores. Schema stays fixed.</small></label><div style="margin-top:20px"><label>Columns</label><div id="schema-editor"></div><button type="button" class="text-button" data-action="add-field">+ Add column</button></div><p class="modal-error" id="modal-error" role="alert"></p><div class="modal-actions"><button type="button" class="button" data-action="close-modal">Cancel</button><button class="button primary" type="submit">Create collection${icon('arrow')}</button></div></form>`,
    );
    renderCreationFields();
    if (state.creation) {
        $('#create-form').elements.name.value = state.creation.body.name;
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
function verifyCreated(created, body) {
    const signature = (list) =>
        JSON.stringify(
            list.map((field, index) => ({
                name: field.name,
                type: field.type,
                nullable: field.nullable,
                field_id: field.field_id ?? index + 1,
            })),
        );
    if (
        created.collection_id !== body.collection_id ||
        created.name !== body.name ||
        !Array.isArray(created.schema?.fields) ||
        signature(created.schema.fields) !== signature(body.schema.fields)
    )
        throw new Error(
            'Resolved collection identity or schema differs from the original create request. Keep its UUID for recovery.',
        );
    return created;
}
async function createForm(form) {
    const button = form.querySelector('[type=submit]');
    setBusy(button, true);
    const api = state.api,
        epoch = state.epoch;
    let draft;
    try {
        const content = {
            name: form.elements.name.value,
            schema: { fields: readCreationFields() },
        };
        const fingerprint = JSON.stringify(content);
        if (state.creation && state.creation.fingerprint !== fingerprint)
            throw new Error('Resolve the original create before changing its name or schema');
        draft = state.creation ?? {
            fingerprint,
            body: { collection_id: crypto.randomUUID(), ...content },
        };
        state.creation = draft;
        let created;
        try {
            created = verifyCreated(
                await api.json('/v0/collections', { method: 'POST', body: draft.body }),
                draft.body,
            );
        } catch (error) {
            if (state.api !== api) return;
            if ([400, 401, 403, 409, 413, 415, 422, 429].includes(error.status)) {
                state.creation = null;
                throw error;
            }
            freezeCreationForm(form);
            // An unavailable POST response is not evidence of absence. Resolve exact
            // name and verify the original UUID/schema before publishing success.
            try {
                created = verifyCreated(
                    await api.json(
                        `/v0/collections:resolve?name=${encodeURIComponent(draft.body.name)}`,
                    ),
                    draft.body,
                );
            } catch {
                throw new Error(
                    `Creation is unresolved (${error.message}). Retry the same request; UUID ${draft.body.collection_id}.`,
                );
            }
        }
        if (state.api !== api) return;
        if (state.creation === draft) state.creation = null;
        const collections = await allPages('/v0/collections', connectionController?.signal, api);
        if (state.api !== api) return;
        state.collections = collections;
        if (form.isConnected && epoch === state.epoch) {
            $('#modal').close();
            state.tab = 'data';
            await selectCollection(created.collection_id);
        } else chrome();
        toast('A new collection. A fresh possibility.');
    } catch (error) {
        if (form.isConnected) form.querySelector('#modal-error').textContent = error.message;
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
function openAppend() {
    if (!state.collectionId) {
        openCreate();
        return;
    }
    const frozen = pendingForCollection(state.collectionId);
    showModal(
        'A little more data.',
        `<p class="muted small">Append preserves every row, including duplicate keys. Int64 values should be quoted decimal strings.</p><form id="append-form"><label class="field">Rows · JSON array<textarea name="rows" rows="9" spellcheck="false" ${frozen ? 'readonly' : ''}>${e(JSON.stringify(frozen ? frozen.body.rows : [sampleRow()], null, 2))}</textarea></label><p class="small muted" style="margin-top:10px">${frozen ? 'This unresolved request is frozen. Retry keeps its original operation ID.' : 'Up to 10,000 rows per batch. Input is validated against this fixed schema.'}</p><div id="append-outcome" style="margin-top:12px"></div><p class="modal-error" id="modal-error" role="alert"></p><div class="modal-actions"><button type="button" class="button" data-action="close-modal">Close</button>${frozen ? '<button type="button" class="button" data-action="resolve-pending">Check outcome</button>' : ''}<button class="button primary" type="submit">${frozen ? 'Retry same operation' : 'Append rows'}${icon('arrow')}</button></div></form>`,
    );
    $('#append-form').dataset.collectionId = state.collectionId;
    if (frozen) {
        $('#append-form').dataset.operationId = frozen.operationId;
        $('#append-outcome').innerHTML =
            `<div class="op-outcome">Unresolved operation<code>${e(frozen.operationId)}</code></div>`;
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
    const form = $('#append-form');
    if (
        !form ||
        form.dataset.collectionId !== operation.collectionId ||
        form.dataset.operationId !== operation.operationId
    )
        return;
    const node = form.querySelector('#append-outcome');
    const title =
        payload.outcome === 'committed'
            ? 'Append committed'
            : payload.outcome === 'definitely_not_committed'
              ? 'Definitely not committed'
              : 'Outcome unresolved · keep this operation ID';
    node.innerHTML = `<div class="op-outcome"><strong>${e(title)}</strong><code>${e(operation.operationId)}</code>${payload.result ? `<p>Snapshot ${e(payload.result.snapshot_id)}</p>` : ''}<p class="small muted">${e(payload.error?.message ?? (payload.recovery_required ? 'Recovery is required before a new mutation.' : ''))}</p></div>`;
    if (operation.terminal) {
        const button = form.querySelector('[type=submit]');
        setBusy(button, false);
        button.disabled = true;
        button.textContent = operation.outcome === 'committed' ? 'Committed' : 'Not committed';
        form.elements.rows.readOnly = true;
    }
}
async function appendForm(form) {
    const button = form.querySelector('[type=submit]');
    setBusy(button, true);
    form.querySelector('#modal-error').textContent = '';
    const api = state.api,
        collectionId = form.dataset.collectionId;
    let operation;
    try {
        const rows = wireRows(state.schema, JSON.parse(form.elements.rows.value));
        const previous = pendingForCollection(collectionId);
        operation = prepareMutation(previous, { rows });
        operation.collectionId = collectionId;
        state.mutation = operation;
        form.dataset.operationId = operation.operationId;
        saveOutcome(operation, { outcome: 'in_flight' });
        const received = await api.json(
            `/v0/collections/${encodeURIComponent(collectionId)}/append`,
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
            form.elements.rows.readOnly = true;
            if (state.collectionId === collectionId) await refreshSelection();
            toast('Append committed. Every row added.');
        } else if (!operation.terminal) {
            button.textContent = 'Retry same operation';
            form.elements.rows.readOnly = true;
        }
    } catch (error) {
        if (state.api === api && operation && !operation.terminal) {
            saveOutcome(operation, { outcome: 'transport_unknown' });
            showAppendOutcome(operation, { outcome: 'commit_unknown', recovery_required: true });
        }
        if (form.isConnected) form.querySelector('#modal-error').textContent = error.message;
    } finally {
        if (!operation?.terminal) setBusy(button, false);
    }
}
async function resolveOperation(id) {
    const operation = state.operations.find((item) => item.operationId === id) ?? state.mutation;
    if (!operation) throw new Error('No pending operation in this session');
    const api = state.api;
    try {
        const received = await api.json(
            `/v0/collections/${encodeURIComponent(operation.collectionId)}/operations/${encodeURIComponent(id)}`,
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
            `/v0/collections/${encodeURIComponent(collectionId)}/files/${encodeURIComponent(id)}?snapshot_id=${encodeURIComponent(selector)}`,
            { signal: controller?.signal },
        );
        if (!current()) return;
        showModal(
            'A closer look.',
            `<p class="muted small">${data.synthetic ? 'Synthetic mock descriptor · no physical file exists.' : 'Metadata for the selected immutable file.'}</p><pre class="json-block">${json(data)}</pre>`,
        );
    } catch (error) {
        if (current()) throw error;
    }
}
function navigate(tab) {
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
    if (!['connect-form', 'query-form', 'create-form', 'append-form'].includes(event.target.id))
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
                const capabilities = await api.json('/v0/capabilities', {
                    signal: controller.signal,
                });
                if (state.api !== api || controller.signal.aborted) return;
                state.capabilities = capabilities;
                await refreshWorkspace();
            } catch (error) {
                if (connectionController !== controller) return;
                resetSession();
                login();
                $('#login-error').textContent = error.message;
            } finally {
                setBusy(button, false);
            }
        } else if (form.id === 'query-form') await runQuery();
        else if (form.id === 'append-form') await appendForm(form);
        else if (form.id === 'create-form') await createForm(form);
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
        if (action === 'nav') navigate(target.dataset.tab);
        else if (action === 'menu') $('#sidebar').classList.toggle('open');
        else if (action === 'disconnect') {
            if (publicDemo) {
                location.reload();
                return;
            }
            resetSession();
            login();
        } else if (action === 'refresh') {
            await refreshWorkspace();
            if (state.api === api) toast('Workspace refreshed.');
        } else if (action === 'select-collection') await selectCollection(target.dataset.id);
        else if (action === 'create') openCreate();
        else if (action === 'close-modal') $('#modal').close();
        else if (action === 'append') openAppend();
        else if (action === 'add-field') {
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
            renderView();
        } else if (action === 'next-page') {
            state.page++;
            renderView();
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
        else if (action === 'resolve-operation') await resolveOperation(target.dataset.id);
        else if (action === 'resolve-pending')
            await resolveOperation($('#append-form').dataset.operationId);
        else if (action === 'download-theme')
            download('theme.json', JSON.stringify(state.theme, null, 2), 'application/json');
        else if (action === 'reset-theme') {
            state.themePreview = false;
            await loadTheme(true);
            renderView();
            toast('Theme reloaded from your config file.');
        } else if (action === 'about')
            showModal(
                'A pretty powerful workbench.',
                `<div class="metadata-list"><p>Browse collections, append typed rows, filter your data and explore immutable snapshots.</p><div class="notice">${publicDemo ? 'This public demo runs in your browser. Your rows stay in this tab; Reset demo or reload restores the samples. JSON export is available here. Connect to a server separately for the network SDK and Arrow IPC.' : 'This workspace uses a disposable mock backend because the real C++ engine is not implemented yet. HTTP and Arrow transport are real; mock results do not qualify storage durability.'}</div><p class="small muted">Theme: edit <code>ui/config/theme.json</code>. Changes reload automatically. No token is saved in browser storage.</p></div>`,
            );
    } catch (error) {
        if (state.api === api) report(error);
    }
});
document.addEventListener('change', async (event) => {
    const target = event.target,
        api = state.api;
    try {
        if (target.id === 'snapshot-selector')
            await selectCollection(
                state.collectionId,
                target.value === 'latest' ? null : target.value,
            );
        else if (target.dataset.themeColor) {
            state.theme = structuredClone(state.theme);
            state.theme.colors[target.dataset.themeColor] = target.value;
            state.themePreview = true;
            applyTheme(state.theme);
            target.closest('label').querySelector('code').textContent = target.value;
        } else if (target.dataset.themeDecoration) {
            state.theme = structuredClone(state.theme);
            state.theme.decoration[target.dataset.themeDecoration] = target.checked;
            state.themePreview = true;
            applyTheme(state.theme);
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
        if (publicDemo) {
            const [{ BrowserDemoAPI }, response] = await Promise.all([
                import('./demo.js'),
                fetch(new URL('../config/demo-data.json', import.meta.url)),
            ]);
            if (!response.ok) throw new Error('Demo data is unavailable');
            state.api = new BrowserDemoAPI(await response.json());
            state.capabilities = await state.api.json('/v0/capabilities');
            await refreshWorkspace();
        } else login();
        configTimer = setInterval(() => loadTheme(), 2000);
    } catch (error) {
        $('#app').innerHTML =
            `<main id="main-content" class="boot"><div><h1>Theme unavailable</h1><p>${e(error.message)}</p><p>Check the mock server and its --theme-file setting, then reload.</p></div></main>`;
    }
}
window.addEventListener('pagehide', () => {
    clearInterval(configTimer);
    connectionController?.abort();
    viewController?.abort();
    queryController?.abort();
    state.token = '';
});
bootstrap();
