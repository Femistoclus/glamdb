import { escapeHTML as e } from './model.js';

const count = (value) =>
    value == null ? 'Unknown' : e(String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ','));
function bound(value, exact) {
    if (value == null) return '<span class="muted">Unknown</span>';
    const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
    return `<code class="file-bound" title="${e(text)}">${e(text)}</code><small class="muted">${exact === true ? 'Exact' : exact === false ? 'Inexact' : 'Precision unknown'}</small>`;
}

// A view adapter over returned footer values. It never infers missing statistics.
export function fileDetails(data, selector) {
    const groups = data.row_groups ?? [],
        perGroup = !data.footer_statistics?.length,
        fields = perGroup
            ? groups.flatMap((group) =>
                  (group.field_metrics ?? []).map((field) => ({ field, group: group.ordinal })),
              )
            : data.footer_statistics.map((field) => ({ field }));
    const snapshot = selector === 'initial' ? 'Initial empty view' : (selector ?? 'Unknown');
    return `<div class="file-details"><p class="muted small">${data.synthetic ? 'Synthetic mock descriptor · no physical file exists.' : data.physical_file_exists === true ? 'Verified Parquet footer · selected immutable file.' : 'Metadata for the selected immutable file.'}</p>
        <div class="metadata-row file-identity"><span>File ID</span><code title="${e(data.file_id)}">${e(data.file_id)}</code></div>
        <dl class="file-summary" aria-label="File summary">${[
            ['Rows', count(data.record_count)],
            ['File bytes', count(data.compressed_bytes)],
            ['Footer bytes', count(data.footer_bytes)],
            ['Selected snapshot', e(snapshot)],
        ]
            .map(([label, value]) => `<div><dt>${label}</dt><dd>${value}</dd></div>`)
            .join('')}</dl>
        <section class="file-details-section"><h3>Row groups</h3>${groups.length ? `<div class="table-wrap" tabindex="0"><table aria-label="${data.synthetic ? 'Mock row groups' : 'Parquet row groups'}"><thead><tr><th>Group</th><th>Rows</th><th>Compressed bytes</th></tr></thead><tbody>${groups.map((group) => `<tr><td>${count(group.ordinal)}</td><td>${count(group.record_count)}</td><td>${count(group.compressed_bytes)}</td></tr>`).join('')}</tbody></table></div>` : '<p class="small muted">Row-group metadata is unavailable for this descriptor.</p>'}</section>
        <section class="file-details-section"><h3>Field statistics</h3>${fields.length ? `<div class="table-wrap" tabindex="0"><table class="file-field-table" aria-label="Parquet field statistics">${perGroup ? '<caption>Observed per-row-group statistics</caption>' : ''}<thead><tr>${perGroup ? '<th>Group</th>' : ''}<th>Field ID</th><th>Column</th><th>Type</th><th>Nulls</th><th>Lower bound</th><th>Upper bound</th></tr></thead><tbody>${fields.map(({ field, group }) => `<tr>${perGroup ? `<td>${count(group)}</td>` : ''}<td>${count(field.field_id)}</td><td title="${e(field.name)}">${e(field.name)}</td><td><span class="type">${e(field.type)}</span></td><td>${count(field.null_count)}</td><td>${bound(field.lower_bound, field.lower_bound_exact)}</td><td>${bound(field.upper_bound, field.upper_bound_exact)}</td></tr>`).join('')}</tbody></table></div>` : '<p class="small muted">Field statistics are unavailable for this descriptor.</p>'}</section>
        <details class="raw-metadata"><summary>Raw metadata JSON</summary><pre class="json-block">${e(JSON.stringify(data, null, 2))}</pre></details></div>`;
}
