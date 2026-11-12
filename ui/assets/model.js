export const MAX_SCHEMA_FIELDS = 128;

const OPERATORS = new Set(['eq', 'ne', 'lt', 'le', 'gt', 'ge', 'in', 'is_null', 'is_not_null']);
const BOOL_OPERATORS = new Set(['eq', 'ne', 'in', 'is_null', 'is_not_null']);
const INTEGER = /^-?(?:0|[1-9]\d*)$/;
const TIMESTAMP =
    /^(?!0000)\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/;
const LEGACY_TYPES = ['string', 'int64', 'float64', 'bool', 'timestamp_us_utc'];
const NATIVE_TYPES = [...LEGACY_TYPES, 'decimal', 'date'];

function decimalParameters(field) {
    if (
        !Number.isInteger(field.precision) ||
        field.precision < 1 ||
        field.precision > 38 ||
        !Number.isInteger(field.scale) ||
        field.scale < 0 ||
        field.scale > field.precision
    )
        throw new Error(`${field.name}: decimal requires precision 1..38 and scale 0..precision`);
}

function decimalScalar(field, value) {
    decimalParameters(field);
    if (typeof value !== 'string' || value.length > 128)
        throw new Error(`${field.name}: decimal values require strings of at most 128 characters`);
    const match = /^(-?)([0-9]+)(?:\.([0-9]+))?$/.exec(value);
    if (!match) throw new Error(`${field.name}: use decimal digits without exponent or whitespace`);
    const fraction = match[3] ?? '';
    if (/[^0]/.test(fraction.slice(field.scale)))
        throw new Error(`${field.name}: decimal exceeds its scale`);
    const fixed = fraction.slice(0, field.scale).padEnd(field.scale, '0');
    const integer = match[2].replace(/^0+/, '') || '0';
    const coefficient = (integer + fixed).replace(/^0+/, '') || '0';
    if (coefficient.length > field.precision)
        throw new Error(`${field.name}: decimal exceeds its precision`);
    return `${match[1] && coefficient !== '0' ? '-' : ''}${integer}${field.scale ? '.' + fixed : ''}`;
}

function dateScalar(field, value) {
    if (typeof value !== 'string' || !/^(?!0000)[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(value))
        throw new Error(`${field.name}: date must use YYYY-MM-DD in years 0001..9999`);
    const year = Number(value.slice(0, 4)),
        month = Number(value.slice(5, 7)),
        day = Number(value.slice(8, 10));
    const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if (month < 1 || month > 12 || day < 1 || day > days[month - 1])
        throw new Error(`${field.name}: invalid calendar date`);
    return value;
}

export function schemaTypes(capabilities) {
    return Array.isArray(capabilities?.schema_types)
        ? NATIVE_TYPES.filter((type) => capabilities.schema_types.includes(type))
        : [...LEGACY_TYPES];
}

export function creationSchema(fields, capabilities) {
    const schema = { fields };
    pendingSchema(schema);
    const supported = schemaTypes(capabilities);
    for (const field of fields)
        if (!supported.includes(field.type))
            throw new Error(`This server does not support ${field.type} fields`);
    return schema;
}

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
        case 'decimal':
            return decimalScalar(field, value);
        case 'date':
            return dateScalar(field, value);
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
            return { ...leaf, values: values.map((value) => predicateScalar(field, value)) };
        }
        if (clause.value === null) throw new Error('Use is null to test NULL');
        return { ...leaf, value: predicateScalar(field, clause.value) };
    });
    if (!nodes.length) return null;
    const result = nodes.length === 1 ? nodes[0] : { op: join, args: nodes };
    return negate ? { op: 'not', arg: result } : result;
}

function predicateScalar(field, value) {
    const parsed = scalar(field, value, true);
    if (field.type === 'decimal')
        return { type: 'decimal', value: parsed, precision: field.precision, scale: field.scale };
    if (field.type === 'date') return { type: 'date', value: parsed };
    return parsed;
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

// Parquet write settings choose how later appends write data files: value
// compression, dictionary policy and value encodings. They are unrelated to
// HTTP Content-Encoding.
export const PARQUET_CODECS = ['uncompressed', 'snappy', 'gzip', 'lz4_raw', 'zstd'];
export const DICTIONARY_POLICIES = ['auto', 'enabled', 'disabled'];
export const PARQUET_ENCODINGS = [
    'plain',
    'delta_binary_packed',
    'delta_length_byte_array',
    'delta_byte_array',
    'byte_stream_split',
    'rle',
];
const FIELD_TYPES = NATIVE_TYPES;
const PARQUET_KEYS = ['compression', 'dictionary', 'dictionary_page_size_limit', 'columns'];
const COLUMN_KEYS = ['field', 'compression', 'dictionary', 'encoding'];
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const isInt32 = (value) => Number.isInteger(value) && value >= -(2 ** 31) && value < 2 ** 31;
// Settings reported by a server may carry newer options this workbench does
// not know: unset (null) ones are ignored, set ones cannot be represented.
function settingsObject(value, keys, label, reported = false) {
    if (!isObject(value))
        throw new Error(`${label} must be an object with only ${keys.join(', ')}`);
    const unknown = Object.keys(value).filter((key) => !keys.includes(key));
    if (unknown.length && !reported)
        throw new Error(`${label} must be an object with only ${keys.join(', ')}`);
    if (unknown.some((key) => value[key] !== null))
        throw new Error(
            `${label} sets options this workbench does not support: ${unknown.join(', ')}`,
        );
    return value;
}
function canonicalCompression(value, label, reported = false) {
    if (value === null || value === undefined) return null;
    settingsObject(value, ['codec', 'level'], label, reported);
    if (!PARQUET_CODECS.includes(value.codec))
        throw new Error(`${label} codec must be one of ${PARQUET_CODECS.join(', ')}`);
    const level = value.level ?? null;
    if (level !== null && !isInt32(level))
        throw new Error(`${label} level must be an integer or null`);
    return { codec: value.codec, level };
}
function canonicalChoice(value, choices, label) {
    if (value === null || value === undefined) return null;
    if (!choices.includes(value)) throw new Error(`${label} must be one of ${choices.join(', ')}`);
    return value;
}
// The wire form is an int64 decimal string; the server checks the range.
function canonicalPageSizeLimit(value) {
    if (value === null || value === undefined) return null;
    const text = typeof value === 'number' && Number.isSafeInteger(value) ? String(value) : value;
    if (typeof text !== 'string' || !/^[1-9]\d{0,18}$/.test(text) || BigInt(text) >= 1n << 63n)
        throw new Error('Dictionary page size limit must be a positive integer');
    return text;
}
// Same canonical form as the server and Python SDK: every option key present
// (null when unset), columns in schema order, explicit null level (codec
// default) and a decimal-string page size limit. Types and conflicts are
// checked by writeSettingsFromChoices and, authoritatively, by the server.
// `reported` accepts server-reported settings with unknown unset options.
export function canonicalWriteSettings(value, schema = null, { reported = false } = {}) {
    const settings =
        value == null ? {} : settingsObject(value, ['parquet'], 'Write settings', reported);
    const parquet =
        settings.parquet == null
            ? {}
            : settingsObject(settings.parquet, PARQUET_KEYS, 'Parquet settings', reported);
    const compression = canonicalCompression(parquet.compression, 'Parquet compression', reported);
    const dictionary = canonicalChoice(
        parquet.dictionary,
        DICTIONARY_POLICIES,
        'Dictionary policy',
    );
    const limit = canonicalPageSizeLimit(parquet.dictionary_page_size_limit);
    const columns = parquet.columns ?? [];
    if (!Array.isArray(columns) || columns.length > MAX_SCHEMA_FIELDS)
        throw new Error(
            `Parquet column settings must be an array of at most ${MAX_SCHEMA_FIELDS} entries`,
        );
    const order = schema ? schema.fields.map((field) => field.name) : null;
    const seen = new Map();
    for (const item of columns) {
        settingsObject(item, COLUMN_KEYS, 'Parquet column setting', reported);
        const field = item.field;
        if (typeof field !== 'string' || !field)
            throw new Error('Parquet column setting requires a field name');
        if (order && !order.includes(field))
            throw new Error(`Parquet column setting names an unknown column ${field}`);
        if (seen.has(field)) throw new Error(`Parquet column setting repeats ${field}`);
        const column = {
            field,
            compression: canonicalCompression(
                item.compression,
                `Parquet compression of ${field}`,
                reported,
            ),
            dictionary: canonicalChoice(
                item.dictionary,
                DICTIONARY_POLICIES,
                `Dictionary policy of ${field}`,
            ),
            encoding: canonicalChoice(item.encoding, PARQUET_ENCODINGS, `Encoding of ${field}`),
        };
        if (!column.compression && !column.dictionary && !column.encoding)
            throw new Error(
                `Parquet column setting for ${field} must set compression, dictionary or encoding`,
            );
        seen.set(field, column);
    }
    const ordered = [...seen.values()];
    if (order) ordered.sort((a, b) => order.indexOf(a.field) - order.indexOf(b.field));
    return {
        parquet: {
            compression,
            dictionary,
            dictionary_page_size_limit: limit,
            columns: ordered,
        },
    };
}
const DEFAULT_SETTINGS = JSON.stringify(canonicalWriteSettings(null));
// Request form of canonical settings: unset options are omitted, so settings
// that use only compression stay acceptable to servers without encodings.
export function wireWriteSettings(canonical) {
    const set = (object) =>
        Object.fromEntries(Object.entries(object).filter(([, value]) => value !== null));
    const { columns, ...parquet } = canonical.parquet;
    return { parquet: { ...set(parquet), columns: columns.map(set) } };
}
// Compares settings in canonical form; either side may be server-reported.
export const sameWriteSettings = (left, right, schema = null) =>
    JSON.stringify(canonicalWriteSettings(left, schema, { reported: true })) ===
    JSON.stringify(canonicalWriteSettings(right, schema, { reported: true }));
export function writeSettingsVersion(value) {
    if (
        typeof value !== 'string' ||
        !/^(?:0|[1-9]\d{0,18})$/.test(value) ||
        BigInt(value) >= 1n << 63n
    )
        throw new Error('Write settings version must be a non-negative decimal string');
    return value;
}
function encodingCapabilities(list) {
    if (!Array.isArray(list)) return [];
    return list
        .filter((item) => isObject(item) && PARQUET_ENCODINGS.includes(item.encoding))
        .map((item) => ({
            encoding: item.encoding,
            types: Array.isArray(item.types)
                ? item.types.filter((type) => FIELD_TYPES.includes(type))
                : [],
        }))
        .filter((item) => item.types.length);
}
function dictionaryCapabilities(value) {
    const limit = value?.page_size_limit;
    if (
        !isObject(value) ||
        !Array.isArray(value.policies) ||
        !Array.isArray(value.types) ||
        !isObject(limit) ||
        ![limit.min, limit.max, limit.default].every(Number.isSafeInteger) ||
        !(limit.min > 0 && limit.min <= limit.default && limit.default <= limit.max)
    )
        return null;
    const policies = value.policies.filter((policy) => DICTIONARY_POLICIES.includes(policy));
    if (!policies.length) return null;
    return {
        policies,
        default_policy: DICTIONARY_POLICIES.includes(value.default_policy)
            ? value.default_policy
            : null,
        types: value.types.filter((type) => FIELD_TYPES.includes(type)),
        fallback_encoding:
            typeof value.fallback_encoding === 'string' ? value.fallback_encoding : null,
        page_size_limit: { min: limit.min, max: limit.max, default: limit.default },
    };
}
// Only a server advertising the feature and a valid codec list enables the
// controls; the mock and the public demo never send write settings. Servers
// without encodings or a dictionary capability get compression controls only.
export function parquetCapabilities(capabilities) {
    const parquet = capabilities?.parquet;
    if (capabilities?.features?.write_settings !== true || !isObject(parquet)) return null;
    if (!Array.isArray(parquet.codecs)) return null;
    const codecs = [];
    for (const item of parquet.codecs) {
        if (!isObject(item) || !PARQUET_CODECS.includes(item.codec)) continue;
        const level = item.level;
        if (level === null) codecs.push({ codec: item.codec, level: null });
        else if (
            isObject(level) &&
            [level.min, level.max, level.default].every(isInt32) &&
            level.min <= level.default &&
            level.default <= level.max
        )
            codecs.push({
                codec: item.codec,
                level: { min: level.min, max: level.max, default: level.default },
            });
    }
    if (!codecs.length) return null;
    let defaultCompression = null;
    try {
        defaultCompression = canonicalCompression(parquet.default_compression, 'Default');
    } catch {
        /* An unknown default is shown as the server default without a codec name. */
    }
    return {
        default_compression: defaultCompression,
        codecs,
        encodings: encodingCapabilities(parquet.encodings),
        dictionary: dictionaryCapabilities(parquet.dictionary),
    };
}
const codecLevels = (parquet, codec) =>
    parquet.codecs.find((item) => item.codec === codec)?.level ?? null;
// Choices a column of this type can use: encodings that accept the type and
// dictionary policies (no "enabled" for types without dictionaries, e.g. bool).
export function columnParquetOptions(parquet, type) {
    const dictionary = parquet?.dictionary;
    return {
        encodings: (parquet?.encodings ?? [])
            .filter((item) => item.types.includes(type))
            .map((item) => item.encoding),
        dictionaries: dictionary
            ? dictionary.policies.filter(
                  (policy) => policy !== 'enabled' || dictionary.types.includes(type),
              )
            : [],
    };
}
// An explicit encoding turns the column's dictionary off: "enabled", or "auto"
// on a type that has dictionaries, cannot accompany it, so choosing an
// encoding clears such a policy (the column then has no dictionary).
export function reconcileColumnDictionary(parquet, type, encoding, dictionary) {
    if (!encoding) return dictionary;
    const dictionaryType = parquet?.dictionary?.types.includes(type) ?? type !== 'bool';
    return dictionary === 'enabled' || (dictionary === 'auto' && dictionaryType) ? '' : dictionary;
}
function chosenCompression(parquet, choice, label) {
    const codec = choice?.codec ?? '';
    const text = String(choice?.level ?? '').trim();
    if (!codec) {
        if (text) throw new Error(`${label}: choose a codec before its level`);
        return null;
    }
    if (!parquet.codecs.some((item) => item.codec === codec))
        throw new Error(`${label}: ${codec} is not available on this server`);
    if (!text) return { codec, level: null };
    const range = codecLevels(parquet, codec);
    if (!range) throw new Error(`${label}: ${codec} has no compression level`);
    if (!/^-?\d+$/.test(text) || Number(text) < range.min || Number(text) > range.max)
        throw new Error(`${label}: ${codec} level must be ${range.min}–${range.max}`);
    return { codec, level: Number(text) };
}
function chosenPageSizeLimit(parquet, value) {
    const text = String(value ?? '').trim();
    if (!text) return null;
    const range = parquet.dictionary?.page_size_limit;
    if (!range) throw new Error('This server does not accept a dictionary page size limit');
    if (!/^\d+$/.test(text) || Number(text) < range.min || Number(text) > range.max)
        throw new Error(`Dictionary page size limit must be ${range.min}–${range.max} bytes`);
    return String(Number(text));
}
// Build wire settings from form choices (strings, '' = unset):
// collection {codec, level, dictionary, pageSizeLimit} and per column
// {field, type, codec, level, dictionary, encoding}. Checks field types and
// dictionary/encoding conflicts against capabilities so the form can explain
// them; the server remains authoritative. Returns null when nothing is chosen,
// so create can omit write_settings.
export function writeSettingsFromChoices(parquet, collectionChoice, columnChoices = []) {
    const compression = chosenCompression(parquet, collectionChoice, 'Parquet compression');
    const policies = parquet.dictionary?.policies ?? [];
    const dictionary = collectionChoice?.dictionary || null;
    if (dictionary && !policies.includes(dictionary))
        throw new Error(`Dictionary policy ${dictionary} is not available on this server`);
    const limit = chosenPageSizeLimit(parquet, collectionChoice?.pageSizeLimit);
    const columns = [];
    for (const choice of columnChoices) {
        const name = choice.field;
        const column = {
            field: name,
            compression: chosenCompression(parquet, choice, `Parquet compression of ${name}`),
            dictionary: choice.dictionary || null,
            encoding: choice.encoding || null,
        };
        const options = columnParquetOptions(parquet, choice.type);
        if (column.encoding && !options.encodings.includes(column.encoding))
            throw new Error(
                `Encoding ${column.encoding} does not accept ${choice.type} column ${name}`,
            );
        if (column.dictionary && !options.dictionaries.includes(column.dictionary))
            throw new Error(
                `Dictionary policy ${column.dictionary} is not available for ${choice.type} column ${name}`,
            );
        if (
            column.encoding &&
            reconcileColumnDictionary(parquet, choice.type, column.encoding, column.dictionary) !==
                column.dictionary
        )
            throw new Error(
                `${name}: an explicit encoding turns the dictionary off; choose disabled or the default dictionary`,
            );
        if (
            dictionary === 'enabled' &&
            !column.dictionary &&
            !column.encoding &&
            !(parquet.dictionary?.types ?? []).includes(choice.type)
        )
            throw new Error(
                `An enabled collection dictionary needs a dictionary policy or encoding for ${choice.type} column ${name}`,
            );
        if (column.compression || column.dictionary || column.encoding) columns.push(column);
    }
    return compression || dictionary || limit || columns.length
        ? {
              parquet: {
                  compression,
                  dictionary,
                  dictionary_page_size_limit: limit,
                  columns,
              },
          }
        : null;
}
export function describeCompression(compression, parquet = null) {
    if (!compression) {
        const fallback = parquet?.default_compression;
        return fallback
            ? `Server default (${describeCompression(fallback, parquet)})`
            : 'Server default';
    }
    if (compression.level !== null) return `${compression.codec} · level ${compression.level}`;
    const range = parquet ? codecLevels(parquet, compression.codec) : null;
    return range ? `${compression.codec} · default level ${range.default}` : compression.codec;
}
// What later appends write for one column, following the server's precedence:
// column option, then collection default, then server default. An explicit
// encoding turns the dictionary off; bool columns never use dictionaries.
export function describeColumnEffect(type, column, settings, parquet = null) {
    const own = column ?? {};
    const compression = describeCompression(
        own.compression ?? settings.parquet.compression,
        parquet,
    );
    const policy =
        own.dictionary ??
        (own.encoding
            ? 'disabled'
            : (settings.parquet.dictionary ?? parquet?.dictionary?.default_policy ?? 'auto'));
    const dictionaryType = parquet?.dictionary?.types.includes(type) ?? type !== 'bool';
    const encoding = own.encoding ?? 'plain';
    const values =
        policy !== 'disabled' && dictionaryType
            ? `dictionary, ${parquet?.dictionary?.fallback_encoding ?? 'plain'} fallback`
            : encoding;
    return `${compression} · ${values}`;
}
// Write settings are part of a collection definition. Creation stores
// non-default settings as version "1"; defaults store nothing (version "0");
// every update adds one. A create response is authoritative: the server
// compared the settings the collection was created with. A resolved (GET)
// collection only shows current settings, which prove the creation settings
// only while none were ever stored; otherwise the create stays unresolved and
// only repeating the original request can confirm it.
export function verifyCreatedCollection(created, body, { authoritative = true } = {}) {
    if (
        created?.collection_id !== body.collection_id ||
        created.name !== body.name ||
        !Array.isArray(created.schema?.fields) ||
        schemaSignature(created.schema) !== schemaSignature(body.schema)
    )
        throw new Error(
            'Resolved collection identity or schema differs from the original create request. Keep its UUID for recovery.',
        );
    const reported =
        Object.hasOwn(created, 'write_settings') ||
        Object.hasOwn(created, 'write_settings_version');
    if (!reported) {
        if (body.write_settings !== undefined)
            throw new Error(
                'The server did not report the requested Parquet write settings. Keep its UUID for recovery.',
            );
        return created;
    }
    let matches;
    let version = 0n;
    try {
        version = BigInt(writeSettingsVersion(created.write_settings_version));
        const actual = JSON.stringify(
            canonicalWriteSettings(created.write_settings, body.schema, { reported: true }),
        );
        const expected = JSON.stringify(
            canonicalWriteSettings(body.write_settings ?? null, body.schema),
        );
        const createdVersion = expected === DEFAULT_SETTINGS ? 0n : 1n;
        matches =
            version >= createdVersion &&
            (version === 0n || !authoritative || version > createdVersion || actual === expected) &&
            (version !== 0n || actual === expected);
    } catch {
        matches = false;
    }
    if (!matches)
        throw new Error(
            'Resolved collection Parquet write settings differ from the original create request. Keep its UUID for recovery.',
        );
    if (!authoritative && version !== 0n)
        throw new Error(
            'Current Parquet write settings cannot prove the settings this collection was created with. Retry the same create request to confirm it.',
        );
    return created;
}
// The frozen create identity: name, schema and optional write settings.
export const creationFingerprint = (body) =>
    JSON.stringify({
        name: body.name,
        schema: body.schema,
        ...(body.write_settings === undefined ? {} : { write_settings: body.write_settings }),
    });

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TYPES = new Set(NATIVE_TYPES);
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
        Array.isArray(schema.fields) &&
            schema.fields.length > 0 &&
            schema.fields.length <= MAX_SCHEMA_FIELDS,
    );
    const names = new Set(),
        ids = new Set();
    schema.fields.forEach((field, index) => {
        pendingObject(field, ['name', 'type', 'nullable', 'field_id', 'precision', 'scale']);
        requirePending(
            typeof field.name === 'string' &&
                field.name.length > 0 &&
                !field.name.includes('\0') &&
                new TextEncoder().encode(field.name).length <= 128,
        );
        requirePending(
            !names.has(field.name) && TYPES.has(field.type) && typeof field.nullable === 'boolean',
        );
        if (field.type === 'decimal') decimalParameters(field);
        else requirePending(!Object.hasOwn(field, 'precision') && !Object.hasOwn(field, 'scale'));
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
            ...(field.type === 'decimal' ? { precision: field.precision, scale: field.scale } : {}),
        })),
    );
}
// The workbench sends only the compact canonical form; a retry must send exactly it.
function pendingWriteSettings(body) {
    let wire;
    try {
        wire = wireWriteSettings(canonicalWriteSettings(body.write_settings, body.schema));
    } catch {
        requirePending(false);
    }
    requirePending(JSON.stringify(wire) === JSON.stringify(body.write_settings));
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
        pendingObject(body, ['collection_id', 'name', 'schema', 'write_settings']);
        requirePending(
            body.collection_id === request.collection_id &&
                typeof body.name === 'string' &&
                /^[a-z][a-z0-9_]{0,62}$/.test(body.name),
        );
        pendingSchema(body.schema);
        if (Object.hasOwn(body, 'write_settings')) pendingWriteSettings(body);
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
        return { kind: 'create', draft: { body, fingerprint: creationFingerprint(body) } };
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
