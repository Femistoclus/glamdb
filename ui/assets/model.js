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
                value === ''
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

export function wireRows(schema, rows) {
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
            fields.map((field) => [field.name, scalar(field, row[field.name])]),
        );
    });
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
