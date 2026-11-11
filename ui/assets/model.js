const OPERATORS = new Set(['eq', 'ne', 'lt', 'le', 'gt', 'ge', 'in', 'is_null', 'is_not_null']);
const BOOL_OPERATORS = new Set(['eq', 'ne', 'in', 'is_null', 'is_not_null']);
const INTEGER = /^-?(?:0|[1-9]\d*)$/;
const TIMESTAMP =
    /^(?!0000)\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/;

export const selectionKey = (collectionId, snapshotId) =>
    JSON.stringify([collectionId, snapshotId]);
export function resetConnection(state) {
    return {
        base: state.base,
        theme: state.theme,
        themePreview: state.themePreview,
        api: null,
        token: '',
        capabilities: null,
        collections: [],
        collectionId: null,
        snapshotId: null,
        resolvedSnapshot: null,
        collection: null,
        schema: null,
        stats: null,
        snapshots: [],
        files: [],
        rows: [],
        result: null,
        plan: null,
        selectedFile: null,
        lastQuery: null,
        epoch: (state.epoch ?? 0) + 1,
        tab: 'overview',
        clauses: [],
        join: 'and',
        negate: false,
        projection: [],
        limit: 1000,
        page: 0,
        operations: [],
        mutation: null,
        creation: null,
        writeDraft: null,
        writeOperationId: null,
        rowMeasurement: null,
    };
}
// Keep frozen write identities only within this authenticated connection.
// A disconnect still uses resetConnection and discards every database draft.
export function switchDatabase(state, database) {
    const drafts = new Map(state.databaseDrafts ?? []);
    if (state.database) {
        drafts.set(state.database.database_id, {
            operations: state.operations,
            mutation: state.mutation,
            creation: state.creation,
        });
    }
    return {
        ...resetConnection(state),
        databaseDrafts: drafts,
        database,
        ...(drafts.get(database?.database_id) ?? {}),
    };
}
export function selectView(state, collectionId, snapshotId = null) {
    return {
        ...state,
        collectionId,
        snapshotId,
        resolvedSnapshot: null,
        rows: [],
        result: null,
        plan: null,
        selectedFile: null,
        lastQuery: null,
        page: 0,
        writeDraft: null,
        writeOperationId: null,
        rowMeasurement: null,
        epoch: (state.epoch ?? 0) + 1,
    };
}
export const escapeHTML = (value) =>
    String(value ?? '').replace(
        /[&<>"']/g,
        (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c],
    );
export const displayValue = (value) =>
    value === null || value === undefined ? 'NULL' : String(value);

function scalar(field, value, fromForm = false) {
    if (value === null || value === undefined) {
        if (!field.nullable) throw new Error(`${field.name} is required`);
        return null;
    }
    switch (field.type) {
        case 'string':
            if (typeof value !== 'string') throw new Error(`${field.name} must be text`);
            if (field.name === 'key' && new TextEncoder().encode(value).length > 1024)
                throw new Error('Key exceeds 1024 UTF-8 bytes');
            return value;
        case 'int64': {
            if (
                typeof value === 'boolean' ||
                (typeof value === 'number' && !Number.isSafeInteger(value))
            )
                throw new Error(`${field.name}: quote large integers to preserve precision`);
            const text = String(value);
            if (!INTEGER.test(text)) throw new Error(`${field.name} must be an integer`);
            const integer = BigInt(text);
            if (integer < -(1n << 63n) || integer >= 1n << 63n)
                throw new Error(`${field.name} is outside int64`);
            return integer.toString();
        }
        case 'float64': {
            if (
                typeof value === 'boolean' ||
                (!fromForm && typeof value !== 'number') ||
                (typeof value === 'string' && value.trim() === '')
            )
                throw new Error(`${field.name} must be a finite number`);
            const number = Number(value);
            if (!Number.isFinite(number)) throw new Error(`${field.name} must be a finite number`);
            return Object.is(number, -0) ? 0 : number;
        }
        case 'bool':
            if (typeof value === 'boolean') return value;
            if (fromForm && (value === 'true' || value === 'false')) return value === 'true';
            throw new Error(`${field.name} must be true or false`);
        case 'timestamp_us_utc':
            if (typeof value !== 'string' || !TIMESTAMP.test(value))
                throw new Error(`${field.name}: use a timezone and at most 6 fractional digits`);
            return value;
        default:
            throw new Error(`Unsupported type ${field.type}`);
    }
}

export function wireRows(schema, rows, fromForm = false) {
    if (!Array.isArray(rows) || rows.length === 0) throw new Error('Append needs at least one row');
    if (rows.length > 10000) throw new Error('A batch can contain at most 10,000 rows');
    const fields = schema.fields;
    return rows.map((row) => {
        if (!row || Array.isArray(row) || typeof row !== 'object')
            throw new Error('Each row must be an object');
        for (const name of Object.keys(row)) {
            if (!fields.some((field) => field.name === name))
                throw new Error(`Unknown column ${name}`);
        }
        return Object.fromEntries(
            fields.map((field) => [field.name, scalar(field, row[field.name], fromForm)]),
        );
    });
}

export function formRows(schema, rows) {
    return wireRows(schema, rows, true);
}

export function sessionSnapshots(operations, collectionId) {
    const snapshots = new Map();
    for (const operation of operations) {
        if (
            operation.collectionId !== collectionId ||
            operation.outcome !== 'committed' ||
            !operation.result?.snapshot_id
        )
            continue;
        const result = operation.result;
        snapshots.set(result.snapshot_id, {
            snapshot_id: result.snapshot_id,
            parent_snapshot_id: result.parent_snapshot_id ?? null,
            rows_added: result.rows_appended,
        });
    }
    return [...snapshots.values()];
}

export function sessionRowCount(operations, collectionId, snapshotId) {
    const counts = new Map();
    const decimal = (value) => (/^(?:0|[1-9]\d{0,18})$/.test(String(value)) ? BigInt(value) : null);
    for (const op of operations) {
        const result = op.result;
        if (op.collectionId !== collectionId || op.outcome !== 'committed' || !result?.snapshot_id)
            continue;
        const added = decimal(result.rows_appended);
        if (added === null) continue;
        if (Object.hasOwn(result, 'parent_snapshot_id') && result.parent_snapshot_id === null)
            counts.set(result.snapshot_id, added);
        else if (counts.has(result.parent_snapshot_id))
            counts.set(result.snapshot_id, counts.get(result.parent_snapshot_id) + added);
    }
    return counts.has(snapshotId) ? counts.get(snapshotId).toString() : null;
}

export function buildPredicate(clauses, join = 'and', negate = false, fields = []) {
    if (!Array.isArray(clauses) || clauses.length > 16)
        throw new Error('Use at most 16 conditions');
    if (!['and', 'or'].includes(join)) throw new Error('Choose AND or OR');
    const nodes = clauses.map((clause) => {
        const field = fields.find((field) => field.name === clause.field);
        if (!field) throw new Error(`Unknown column ${clause.field}`);
        if (!OPERATORS.has(clause.op) || (field.type === 'bool' && !BOOL_OPERATORS.has(clause.op)))
            throw new Error(`Unsupported operator for ${field.name}`);
        const leaf = { op: clause.op, field: field.name };
        if (['is_null', 'is_not_null'].includes(clause.op)) return leaf;
        if (clause.op === 'in') {
            const values = Array.isArray(clause.value) ? clause.value : JSON.parse(clause.value);
            if (!Array.isArray(values) || values.length > 1000)
                throw new Error('IN expects an array of up to 1000 values');
            if (values.some((value) => value === null)) throw new Error('Use is null to test NULL');
            return { ...leaf, values: values.map((value) => scalar(field, value, true)) };
        }
        if (clause.value === null) throw new Error('Use is null to test NULL');
        return { ...leaf, value: scalar(field, clause.value, true) };
    });
    if (!nodes.length) return null;
    const result = nodes.length === 1 ? nodes[0] : { op: join, args: nodes };
    return negate ? { op: 'not', arg: result } : result;
}

export function prepareMutation(previous, body, idFactory = () => crypto.randomUUID()) {
    const fingerprint = JSON.stringify(body);
    if (previous && !previous.terminal) {
        if (previous.fingerprint !== fingerprint)
            throw new Error('Resolve the previous operation before changing its payload');
        return previous;
    }
    return { operationId: idFactory(), fingerprint, body: structuredClone(body), terminal: false };
}

export function supportsFeature(capabilities, feature) {
    return capabilities?.features?.[feature] ?? capabilities?.mock === true;
}
// The default preview remains 1,000 rows; this is its adjustable upper bound.
export function queryMaxRows(capabilities, theme) {
    const bounded = (value) => (Number.isSafeInteger(value) && value >= 1 ? value : 10000);
    return Math.min(
        10000,
        bounded(theme?.editor?.query_max_rows),
        bounded(capabilities?.limits?.scan_page_rows),
        capabilities?.browser_demo === true || capabilities?.backend === 'browser-demo'
            ? 1000
            : 10000,
    );
}
// Pre-login storage notice from public /healthz; unknown or missing health makes no claim.
export function storageNotice(health) {
    if (health?.mock === true) return 'Mock data is disposable. No storage durability is claimed.';
    if (
        health?.mock === false &&
        health.backend === 'local' &&
        health.durability_validation === 'designed'
    )
        return 'Persistent local storage. Power-loss durability is not yet qualified.';
    return 'Storage durability depends on the server you connect to.';
}
export function validateQueryCapabilities(capabilities, body) {
    if (body.projection?.length === 0 && !supportsFeature(capabilities, 'zero_column_projection'))
        throw new Error('Empty projection is not supported by this server');
    const supported = capabilities?.predicate_ops;
    const visit = (predicate) => {
        if (!predicate) return;
        if (supported && !supported.includes(predicate.op))
            throw new Error(`Predicate ${predicate.op} is not supported by this server`);
        predicate.args?.forEach(visit);
        if (predicate.arg) visit(predicate.arg);
    };
    visit(body.predicate);
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TYPES = new Set(['string', 'int64', 'float64', 'bool', 'timestamp_us_utc']);
function requirePending(condition, message = 'Invalid pending request file') {
    if (!condition) throw new Error(message);
}
function pendingObject(value, keys) {
    requirePending(value && typeof value === 'object' && !Array.isArray(value));
    requirePending(Object.keys(value).every((key) => keys.includes(key)));
}
function pendingSchema(schema) {
    pendingObject(schema, ['fields', 'schema_id']);
    requirePending(
        Array.isArray(schema.fields) && schema.fields.length > 0 && schema.fields.length <= 64,
    );
    const names = new Set(),
        ids = new Set();
    schema.fields.forEach((field, index) => {
        pendingObject(field, ['name', 'type', 'nullable', 'field_id']);
        requirePending(
            typeof field.name === 'string' &&
                field.name.length > 0 &&
                !field.name.includes('\0') &&
                new TextEncoder().encode(field.name).length <= 128,
        );
        requirePending(
            !names.has(field.name) && TYPES.has(field.type) && typeof field.nullable === 'boolean',
        );
        const id = field.field_id ?? index + 1;
        requirePending(Number.isInteger(id) && id > 0 && id < 2 ** 31 && !ids.has(id));
        names.add(field.name);
        ids.add(id);
    });
    if (schema.schema_id !== undefined)
        requirePending(
            Number.isInteger(schema.schema_id) &&
                schema.schema_id >= 0 &&
                schema.schema_id < 2 ** 31,
        );
    return schema;
}
export function schemaSignature(schema) {
    return JSON.stringify(
        pendingSchema(schema).fields.map((field, index) => ({
            name: field.name,
            type: field.type,
            nullable: field.nullable,
            field_id: field.field_id ?? index + 1,
        })),
    );
}
function pendingScope(scope) {
    pendingObject(scope, ['server_url', 'database_id', 'store_id', 'backend', 'mock']);
    const url = new URL(scope.server_url);
    requirePending(
        ['http:', 'https:'].includes(url.protocol) &&
            !url.username &&
            !url.password &&
            !url.search &&
            !url.hash,
    );
    requirePending(
        typeof scope.backend === 'string' &&
            scope.backend.length > 0 &&
            typeof scope.mock === 'boolean',
    );
    for (const id of [scope.database_id, scope.store_id])
        requirePending(id === null || (typeof id === 'string' && UUID.test(id)));
    return { ...scope, server_url: url.href.replace(/\/$/, '') };
}
function validatePending(value) {
    pendingObject(value, ['format', 'version', 'scope', 'request']);
    requirePending(value.format === 'glamdb-pending-request' && value.version === 1);
    const scope = pendingScope(value.scope),
        request = value.request;
    pendingObject(request, ['kind', 'collection_id', 'operation_id', 'schema', 'body']);
    requirePending(typeof request.collection_id === 'string' && UUID.test(request.collection_id));
    requirePending(['append', 'create'].includes(request.kind));
    const body = request.body;
    if (request.kind === 'create') {
        requirePending(
            !Object.hasOwn(request, 'operation_id') && !Object.hasOwn(request, 'schema'),
        );
        pendingObject(body, ['collection_id', 'name', 'schema']);
        requirePending(
            body.collection_id === request.collection_id &&
                typeof body.name === 'string' &&
                /^[a-z][a-z0-9_]{0,62}$/.test(body.name),
        );
        pendingSchema(body.schema);
    } else {
        requirePending(typeof request.operation_id === 'string' && UUID.test(request.operation_id));
        pendingSchema(request.schema);
        pendingObject(body, ['rows', 'expected_snapshot']);
        wireRows(request.schema, body.rows);
        if (Object.hasOwn(body, 'expected_snapshot') && body.expected_snapshot !== null) {
            const snapshot = body.expected_snapshot;
            requirePending(typeof snapshot === 'string' && INTEGER.test(snapshot));
            requirePending(BigInt(snapshot) >= -(1n << 63n) && BigInt(snapshot) < 1n << 63n);
        }
    }
    return { ...value, scope };
}
// Export only request data, never the API client, token, response or tab state.
export function exportPending(scope, kind, draft) {
    requirePending(!draft.terminal, 'Only a pending request can be exported');
    const request =
        kind === 'create'
            ? { kind, collection_id: draft.body.collection_id, body: structuredClone(draft.body) }
            : {
                  kind,
                  collection_id: draft.collectionId,
                  operation_id: draft.operationId,
                  schema: structuredClone(draft.schema),
                  body: structuredClone(draft.body),
              };
    return validatePending({
        format: 'glamdb-pending-request',
        version: 1,
        scope: structuredClone(scope),
        request,
    });
}
export function importPending(value, currentScope) {
    const validated = validatePending(value),
        expected = pendingScope(currentScope);
    for (const key of ['server_url', 'database_id', 'backend', 'mock'])
        requirePending(
            validated.scope[key] === expected[key],
            'Pending request belongs to a different server, database or backend',
        );
    if (validated.scope.store_id !== null)
        requirePending(
            validated.scope.store_id === expected.store_id,
            'Pending request store identity does not match',
        );
    const request = validated.request,
        body = structuredClone(request.body);
    if (request.kind === 'create')
        return {
            kind: 'create',
            draft: { body, fingerprint: JSON.stringify({ name: body.name, schema: body.schema }) },
        };
    return {
        kind: 'append',
        draft: {
            operationId: request.operation_id,
            collectionId: request.collection_id,
            schema: structuredClone(request.schema),
            body,
            fingerprint: JSON.stringify(body),
            terminal: false,
            outcome: 'unresolved',
        },
    };
}

// Recovery files include the request, schema, scope and pretty-print whitespace.
// Their independent cap must not depend on an API response or request-body limit.
const PENDING_FILE_MAX_BYTES = 32 * 1024 * 1024;
function checkPendingFileSize(bytes) {
    requirePending(
        Number.isSafeInteger(bytes) && bytes >= 0 && bytes <= PENDING_FILE_MAX_BYTES,
        'Choose a recovery JSON file within the 32 MiB recovery file limit',
    );
}
export function serializePendingFile(scope, kind, draft) {
    const text = JSON.stringify(exportPending(scope, kind, draft), null, 2);
    checkPendingFileSize(new TextEncoder().encode(text).byteLength);
    return text;
}
export async function readPendingFile(file, currentScope) {
    requirePending(file, 'Choose a recovery JSON file');
    // Reject large files before allocating their text or parsing their JSON.
    checkPendingFileSize(file.size);
    const text = await file.text();
    checkPendingFileSize(new TextEncoder().encode(text).byteLength);
    return importPending(JSON.parse(text), currentScope);
}
