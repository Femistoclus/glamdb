import { escapeHTML as e } from './model.js';

const count = (value) =>
    value == null ? 'Unknown' : e(String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ','));
function bound(value, exact) {
    if (value == null) return '<span class="muted">Unknown</span>';
    const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
    return `<code class="file-bound" title="${e(text)}">${e(text)}</code><small class="muted">${exact === true ? 'Exact' : exact === false ? 'Inexact' : 'Precision unknown'}</small>`;
}

const DICTIONARY_DATA = new Set(['rle_dictionary', 'plain_dictionary']);
const DATA_PAGES = new Set(['data_page', 'data_page_v2']);
// Value encodings actually written for one column chunk. A dictionary that
// outgrew its page size limit leaves data pages in both a dictionary encoding
// and its fallback (e.g. rle_dictionary and plain); report that fallback.
export function chunkEncodings(field) {
    const encodings = Array.isArray(field?.encodings)
        ? field.encodings.filter((value) => typeof value === 'string')
        : null;
    const stats = (Array.isArray(field?.encoding_stats) ? field.encoding_stats : []).filter(
        (item) =>
            typeof item?.page_type === 'string' &&
            typeof item.encoding === 'string' &&
            Number.isSafeInteger(item.count) &&
            item.count > 0,
    );
    const data = stats.filter((item) => DATA_PAGES.has(item.page_type));
    const fallback = data.some((item) => DICTIONARY_DATA.has(item.encoding))
        ? [
              ...new Set(
                  data
                      .filter((item) => !DICTIONARY_DATA.has(item.encoding))
                      .map((item) => item.encoding),
              ),
          ]
        : [];
    return {
        encodings,
        fallback,
        pages: stats.map((item) => `${item.page_type} ${item.encoding} ×${item.count}`).join('; '),
    };
}
function encodingCell(field) {
    const { encodings, fallback, pages } = chunkEncodings(field);
    if (!encodings) return '<td><span class="muted">Unknown</span></td>';
    return `<td class="file-encodings" title="${e(pages)}"><span class="type">${e(encodings.join(', ') || 'none')}</span>${fallback.length ? `<span class="pill">Dictionary fallback: ${e(fallback.join(', '))}</span>` : ''}</td>`;
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
    // Footer column chunks report the Parquet codec actually used for their
    // values (not HTTP compression). Manifest-only metrics have none.
    const codecs = fields.some(({ field }) => typeof field.compression === 'string');
    const encodings = fields.some(({ field }) => Array.isArray(field.encodings));
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
        <section class="file-details-section"><h3>Field statistics</h3>${fields.length ? `<div class="table-wrap" tabindex="0"><table class="file-field-table" aria-label="Parquet field statistics">${perGroup ? '<caption>Observed per-row-group statistics</caption>' : ''}<thead><tr>${perGroup ? '<th>Group</th>' : ''}<th>Field ID</th><th>Column</th><th>Type</th><th>Nulls</th><th>Lower bound</th><th>Upper bound</th>${codecs ? '<th>Parquet codec</th>' : ''}${encodings ? '<th>Encodings</th>' : ''}</tr></thead><tbody>${fields.map(({ field, group }) => `<tr>${perGroup ? `<td>${count(group)}</td>` : ''}<td>${count(field.field_id)}</td><td title="${e(field.name)}">${e(field.name)}</td><td><span class="type">${e(field.type)}</span></td><td>${count(field.null_count)}</td><td>${bound(field.lower_bound, field.lower_bound_exact)}</td><td>${bound(field.upper_bound, field.upper_bound_exact)}</td>${codecs ? `<td>${typeof field.compression === 'string' ? `<span class="type">${e(field.compression)}</span>` : '<span class="muted">Unknown</span>'}</td>` : ''}${encodings ? encodingCell(field) : ''}</tr>`).join('')}</tbody></table></div>` : '<p class="small muted">Field statistics are unavailable for this descriptor.</p>'}</section>
        <details class="raw-metadata"><summary>Raw metadata JSON</summary><pre class="json-block">${e(JSON.stringify(data, null, 2))}</pre></details></div>`;
}
