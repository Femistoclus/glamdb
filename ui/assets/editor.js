import { escapeHTML as e, formRows } from './model.js';

export function writeEditor(schema, draft, operation, collectionId, labels = {}, mode = 'append') {
    const frozen = Boolean(operation);
    const action = mode === 'kv' ? 'Upsert' : 'Append';
    const rows = operation?.body.rows ?? draft ?? [{}];
    const fields = schema.fields;
    return `<section class="panel write-panel" aria-labelledby="write-title"><div class="panel-header"><div><h2 id="write-title" data-label="write_title">${e(labels.write_title ?? 'Write rows')}</h2><p data-label="write_copy">${e(mode === 'kv' ? 'Replace full rows by key. Last input occurrence wins; omitted nullable values become NULL.' : (labels.write_copy ?? 'Fill the fields. JSON is created for you.'))}</p></div><button class="text-button" data-action="append" data-label="advanced_batch">${e(labels.advanced_batch ?? 'Batch editor')}</button></div><div class="panel-body"><form id="write-form" data-collection-id="${e(collectionId)}" ${operation ? `data-operation-id="${e(operation.operationId)}"` : ''}>
    <div class="write-rows">${rows
        .map(
            (row, index) =>
                `<details class="write-row" data-write-row ${frozen ? 'open' : ''}><summary><span>Row ${index + 1}</span><small>${fields.length} columns</small>${rows.length > 1 && !frozen ? `<button type="button" class="text-button" data-action="remove-write-row" data-index="${index}" aria-label="Remove row ${index + 1}">Remove</button>` : ''}</summary><div class="write-row-fields">${fields
                    .map((field, fi) => {
                        const value = row[field.name];
                        const isNull = field.nullable && (value === null || value === undefined);
                        const id = `write-${index}-${fi}`;
                        const disabled = frozen || isNull ? 'disabled' : '';
                        const control =
                            field.type === 'bool'
                                ? `<select id="${id}" data-write-value ${disabled} aria-label="${e(field.name)} row ${index + 1}"><option value="true" ${value === true || value === 'true' ? 'selected' : ''}>true</option><option value="false" ${value !== true && value !== 'true' ? 'selected' : ''}>false</option></select>`
                                : `<input id="${id}" data-write-value type="text" ${['int64', 'float64', 'decimal'].includes(field.type) ? 'inputmode="decimal"' : ''} value="${e(value ?? '')}" ${disabled} aria-label="${e(field.name)} row ${index + 1}" placeholder="${field.type === 'timestamp_us_utc' ? '2026-11-11T12:00:00.000000Z' : field.type === 'date' ? 'YYYY-MM-DD' : e(field.name)}">`;
                        return `<div class="write-field" data-field="${e(field.name)}"><label for="${id}">${e(field.name)} <span class="field-type">${e(field.type)}</span></label><div class="write-value">${control}${field.nullable ? `<label class="null-toggle"><input type="checkbox" data-write-null ${isNull ? 'checked' : ''} ${frozen ? 'disabled' : ''} aria-label="${e(field.name)} row ${index + 1} NULL">NULL</label>` : ''}</div></div>`;
                    })
                    .join('')}</div></details>`,
        )
        .join('')}</div>
    <div class="write-actions"><button type="button" class="text-button" data-action="add-write-row" ${frozen || rows.length >= 100 ? 'disabled' : ''}>+ Add row</button><button class="button primary" type="submit" ${operation?.terminal ? 'disabled' : ''}>${operation?.terminal ? (operation.outcome === 'committed' ? 'Committed' : 'Not committed') : operation ? 'Retry same operation' : `${action} rows`}</button></div>
    <details class="editor-json"><summary>Generated JSON</summary><pre id="write-json" class="json-block">${e(previewRows(schema, rows))}</pre></details>
    <div id="write-outcome" role="status">${operation ? `<div class="op-outcome"><strong>${operation.outcome === 'committed' ? `${action} committed` : operation.outcome === 'definitely_not_committed' ? 'Definitely not committed' : 'Outcome unresolved · keep this operation ID'}</strong><code>${e(operation.operationId)}</code></div>` : ''}</div><p id="write-error" class="modal-error" role="alert"></p>
    ${operation && !operation.terminal ? '<button type="button" class="button small" data-action="resolve-operation" data-id="' + e(operation.operationId) + '">Check outcome</button>' : ''}${operation?.terminal ? '<button type="button" class="button small" data-action="new-write">New rows</button>' : ''}
    </form></div></section>`;
}
export function readEditorRows(form) {
    return [...form.querySelectorAll('[data-write-row]')].map((row) =>
        Object.fromEntries(
            [...row.querySelectorAll('[data-field]')].map((field) => [
                field.dataset.field,
                field.querySelector('[data-write-null]')?.checked
                    ? null
                    : field.querySelector('[data-write-value]').value,
            ]),
        ),
    );
}
export function previewRows(schema, rows) {
    try {
        return JSON.stringify(formRows(schema, rows), null, 2);
    } catch (error) {
        return error.message;
    }
}
export function lockWriteForm(form) {
    for (const input of form.querySelectorAll(
        '[data-write-value],[data-write-null],[data-action="add-write-row"],[data-action="remove-write-row"]',
    ))
        input.disabled = true;
}
