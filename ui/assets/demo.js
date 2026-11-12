// UI-only, per-page simulation for static hosting. No network/storage writer.
import { APIError } from './api.js';
import { MAX_SCHEMA_FIELDS, wireRows } from './model.js';

const TYPES = new Set(['string', 'bool', 'int64', 'float64', 'timestamp_us_utc']);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const encoder = new TextEncoder();
const clone = (value) => structuredClone(value);
const mock = {
    mock: true,
    backend: 'browser-demo',
    durable: false,
    durability_validation: 'not_validated',
    synthetic: true,
};
const fail = (status, message) => {
    throw new APIError(status, {
        error: { message, code: status === 404 ? 'NOT_FOUND' : 'DEMO_ERROR' },
    });
};
const requireUUID = (value) => {
    if (typeof value !== 'string' || !UUID.test(value)) fail(422, 'A UUID is required');
    return value;
};

export function timestampMicros(text) {
    const match =
        /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:\d{2})$/.exec(
            text,
        );
    if (!match)
        throw new Error('Use a valid timestamp with timezone and at most 6 fractional digits');
    const [, y, mo, d, h, mi, s, f = '', zone] = match;
    const [year, month, day, hour, minute, second] = [y, mo, d, h, mi, s].map(Number);
    const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    if (
        year < 1 ||
        month < 1 ||
        month > 12 ||
        day < 1 ||
        day > [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1] ||
        hour > 23 ||
        minute > 59 ||
        second > 59
    )
        throw new Error('Invalid timestamp calendar value');
    let offset = 0;
    if (zone !== 'Z') {
        const zh = Number(zone.slice(1, 3)),
            zm = Number(zone.slice(4));
        if (zh > 23 || zm > 59) throw new Error('Invalid timestamp offset');
        offset = (zh * 60 + zm) * (zone[0] === '+' ? 1 : -1);
    }
    // Date is used only for exact integer whole-second calendar conversion.
    // Fractional microseconds never pass through Date or floating arithmetic.
    const seconds = Date.parse(`${y}-${mo}-${d}T${h}:${mi}:${s}Z`) / 1000 - offset * 60;
    const value = BigInt(seconds) * 1000000n + BigInt(f.padEnd(6, '0'));
    if (value < -62135596800000000n || value > 253402300799999999n)
        throw new Error('Timestamp UTC year must be 0001..9999');
    return value;
}
function normalizedTimestamp(text) {
    const us = timestampMicros(text),
        fraction = ((us % 1000000n) + 1000000n) % 1000000n;
    const seconds = (us - fraction) / 1000000n;
    return `${new Date(Number(seconds) * 1000).toISOString().slice(0, 19)}.${String(fraction).padStart(6, '0')}Z`;
}
function normalizeSchema(value) {
    if (
        !Array.isArray(value?.fields) ||
        value.fields.length < 1 ||
        value.fields.length > MAX_SCHEMA_FIELDS
    )
        fail(422, `Use 1..${MAX_SCHEMA_FIELDS} schema fields`);
    const names = new Set();
    const fields = value.fields.map((f, i) => {
        if (
            typeof f.name !== 'string' ||
            !f.name ||
            encoder.encode(f.name).length > 128 ||
            names.has(f.name) ||
            !TYPES.has(f.type) ||
            (f.nullable !== undefined && typeof f.nullable !== 'boolean') ||
            (f.field_id !== undefined && f.field_id !== i + 1)
        )
            fail(422, 'Invalid fixed schema');
        names.add(f.name);
        if (f.name === 'key' && (f.type !== 'string' || f.nullable === true))
            fail(422, 'key must be a required string');
        return { field_id: i + 1, name: f.name, type: f.type, nullable: f.nullable ?? false };
    });
    return { schema_id: 0, fields };
}
function normalizedRows(schema, rows) {
    const result = wireRows(schema, rows);
    for (const row of result) {
        for (const field of schema.fields) {
            if (row[field.name] !== null && field.type === 'timestamp_us_utc')
                row[field.name] = normalizedTimestamp(row[field.name]);
            if (
                field.type === 'string' &&
                row[field.name] !== null &&
                encoder.encode(row[field.name]).length > 1024 * 1024
            )
                fail(422, 'String exceeds demo bound');
        }
        if (encoder.encode(JSON.stringify(row)).length > 2 * 1024 * 1024)
            fail(422, 'Row exceeds demo bound');
    }
    if (encoder.encode(JSON.stringify(result)).length > 16 * 1024 * 1024)
        fail(422, 'Append exceeds demo bound');
    return result;
}
function compare(field, a, b) {
    if (field.type === 'int64') {
        a = BigInt(a);
        b = BigInt(b);
    } else if (field.type === 'timestamp_us_utc') {
        a = timestampMicros(a);
        b = timestampMicros(b);
    } else if (field.type === 'string') {
        const x = encoder.encode(a),
            y = encoder.encode(b);
        for (let i = 0; i < Math.min(x.length, y.length); i++)
            if (x[i] !== y[i]) return x[i] < y[i] ? -1 : 1;
        return Math.sign(x.length - y.length);
    }
    return a === b ? 0 : a < b ? -1 : 1;
}
function predicateEvaluator(schema, node, depth = 0) {
    if (node === null || node === undefined) return () => true;
    if (!node || typeof node !== 'object' || depth > 32) fail(422, 'Invalid predicate');
    const truthNot = (x) => (x === null ? null : !x);
    if (node.op === 'not') {
        const evaluate = predicateEvaluator(schema, node.arg, depth + 1);
        return (row) => truthNot(evaluate(row));
    }
    if (node.op === 'and' || node.op === 'or') {
        if (!Array.isArray(node.args) || !node.args.length || node.args.length > 64)
            fail(422, 'Invalid predicate arguments');
        const args = node.args.map((n) => predicateEvaluator(schema, n, depth + 1));
        return (row) => {
            const values = args.map((f) => f(row));
            return node.op === 'and'
                ? values.includes(false)
                    ? false
                    : values.includes(null)
                      ? null
                      : true
                : values.includes(true)
                  ? true
                  : values.includes(null)
                    ? null
                    : false;
        };
    }
    const field = schema.fields.find((f) => f.name === node.field);
    if (!field) fail(422, 'Unknown predicate field');
    if (node.op === 'is_null') return (row) => row[field.name] === null;
    if (node.op === 'is_not_null') return (row) => row[field.name] !== null;
    if (
        !['eq', 'ne', 'lt', 'le', 'gt', 'ge', 'in'].includes(node.op) ||
        (field.type === 'bool' && !['eq', 'ne', 'in'].includes(node.op))
    )
        fail(422, 'Unsupported predicate operator');
    const validate = (value) => {
        if (value === null) return null;
        const one = { fields: [{ ...field, nullable: true }] };
        const converted = wireRows(one, [{ [field.name]: value }])[0][field.name];
        if (field.type === 'timestamp_us_utc') timestampMicros(converted);
        return converted;
    };
    if (node.op === 'in') {
        if (!Array.isArray(node.values) || node.values.length > 1000)
            fail(422, 'Invalid IN literals');
        const values = node.values.map(validate);
        return (row) => {
            const actual = row[field.name];
            if (actual === null) return null;
            return values.some((v) => v !== null && compare(field, actual, v) === 0)
                ? true
                : values.includes(null)
                  ? null
                  : false;
        };
    }
    const expected = validate(node.value);
    return (row) => {
        const actual = row[field.name];
        if (actual === null || expected === null) return null;
        const order = compare(field, actual, expected);
        return {
            eq: order === 0,
            ne: order !== 0,
            lt: order < 0,
            le: order <= 0,
            gt: order > 0,
            ge: order >= 0,
        }[node.op];
    };
}

export class BrowserDemoAPI {
    constructor(seed = [], { maxRows = 10000, maxBytes = 16 * 1024 * 1024 } = {}) {
        this.collections = new Map();
        this.cursors = new Map();
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.stateBytes = 0;
        this.nextSnapshot = 1n;
        this.storeId = crypto.randomUUID();
        this.ready = this.initialize(seed);
    }
    async initialize(seed) {
        for (const item of seed) {
            const c = this.create(item.info);
            let previous = 0;
            for (const snapshot of item.snapshots) {
                await this.append(c, {
                    operation_id: snapshot.operation_id,
                    rows: snapshot.rows.slice(previous),
                });
                previous = snapshot.rows.length;
            }
        }
    }
    info(c) {
        return {
            collection_id: c.id,
            name: c.name,
            table_uuid: c.tableUuid,
            schema: c.schema,
            mode: 'append',
            creation_sequence: c.sequence,
            current_snapshot_id: c.snapshots.at(-1)?.snapshot_id ?? null,
            ...mock,
        };
    }
    create(body) {
        const id = requireUUID(body.collection_id),
            schema = normalizeSchema(body.schema),
            name = body.name;
        if (
            typeof name !== 'string' ||
            !name ||
            encoder.encode(name).length > 128 ||
            name.includes('\0')
        )
            fail(422, 'Invalid collection name');
        if (body.mode !== undefined && body.mode !== 'append')
            fail(422, 'Only append mode is available');
        const existing = this.collections.get(id);
        if (existing) {
            if (
                existing.name === name &&
                JSON.stringify(existing.schema) === JSON.stringify(schema)
            )
                return existing;
            fail(409, 'Collection UUID already exists');
        }
        if ([...this.collections.values()].some((c) => c.name === name))
            fail(409, 'Exact collection name is already in use');
        if (this.collections.size >= 16)
            fail(429, 'Demo collection limit reached. Reset demo to start again.');
        const reserve = encoder.encode(JSON.stringify(schema)).length * 4 + 4096;
        if (this.stateBytes + reserve > this.maxBytes)
            fail(429, 'Demo metadata limit reached. Reset demo to start again.');
        const c = {
            id,
            name,
            schema,
            tableUuid: crypto.randomUUID(),
            sequence: this.collections.size + 1,
            snapshots: [],
            operations: new Map(),
        };
        this.collections.set(id, c);
        this.stateBytes += reserve;
        return c;
    }
    view(c, selector = 'latest') {
        if (selector === 'latest' || selector === undefined)
            selector = c.snapshots.at(-1)?.snapshot_id ?? null;
        if (selector === null || selector === 'initial') return null;
        const snapshot = c.snapshots.find((s) => s.snapshot_id === String(selector));
        if (!snapshot) fail(404, 'Snapshot does not belong to this collection');
        return snapshot;
    }
    identity(c, s) {
        return {
            store_id: this.storeId,
            collection_id: c.id,
            table_uuid: c.tableUuid,
            snapshot_id: s?.snapshot_id ?? null,
            current_snapshot_id: s?.snapshot_id ?? null,
            schema_id: 0,
            spec_id: 0,
            ...mock,
        };
    }
    rejection(c, id, fingerprint, message, code = 'RESOURCE_EXHAUSTED') {
        const payload = {
            outcome: 'definitely_not_committed',
            operation_id: id,
            error: { code, message },
            ...mock,
        };
        // Bounded UI-only control records; this is not the native durable ledger.
        if (c.operations.size < 128) c.operations.set(id, { fingerprint, payload });
        return payload;
    }
    async append(c, body) {
        const id = requireUUID(body.operation_id);
        let rows, encoded, validationError;
        try {
            rows = normalizedRows(c.schema, body.rows);
            encoded = JSON.stringify(rows);
        } catch (error) {
            validationError = error;
            encoded = JSON.stringify(body.rows) ?? 'null';
        }
        const fingerprint = Array.from(
            new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(encoded))),
            (byte) => byte.toString(16).padStart(2, '0'),
        ).join('');
        const original = c.operations.get(id);
        if (original) {
            if (original.fingerprint !== fingerprint)
                fail(409, 'Operation ID is bound to different content');
            return original.payload;
        }
        if (c.operations.size >= 128)
            return this.rejection(
                c,
                id,
                fingerprint,
                'Demo history limit reached. Reset demo to start again.',
            );
        if (validationError)
            return this.rejection(c, id, fingerprint, validationError.message, 'INVALID_ARGUMENT');
        const previous = c.snapshots.at(-1),
            head = previous?.snapshot_id ?? null;
        let payload;
        if (body.expected_snapshot !== undefined && body.expected_snapshot !== head) {
            payload = {
                outcome: 'definitely_not_committed',
                operation_id: id,
                error: { code: 'PRECONDITION_FAILED', message: 'Snapshot precondition failed' },
                ...mock,
            };
        } else {
            const total = [...this.collections.values()].reduce(
                (sum, item) => sum + (item.snapshots.at(-1)?.rows.length ?? 0),
                0,
            );
            if (total + rows.length > this.maxRows)
                return this.rejection(
                    c,
                    id,
                    fingerprint,
                    'Demo row limit reached. Reduce this batch or reset demo.',
                );
            const reserve =
                encoder.encode(encoded).length * 4 +
                ((previous?.rows.length ?? 0) + rows.length) * 16 +
                4096;
            if (this.stateBytes + reserve > this.maxBytes)
                return this.rejection(
                    c,
                    id,
                    fingerprint,
                    'Demo byte limit reached. Reduce this batch or reset demo.',
                );
            const snapshot = {
                snapshot_id: String(this.nextSnapshot++),
                parent_id: head,
                created_at_us: String(BigInt(Date.now()) * 1000n),
                operation_id: id,
                rows_added: rows.length,
                rows: [...(previous?.rows ?? []), ...rows],
            };
            c.snapshots.push(snapshot);
            this.stateBytes += reserve;
            payload = {
                outcome: 'committed',
                result: {
                    collection_id: c.id,
                    operation_id: id,
                    snapshot_id: snapshot.snapshot_id,
                    rows_appended: rows.length,
                    rows_total: snapshot.rows.length,
                    ...mock,
                },
                ...mock,
            };
        }
        c.operations.set(id, { fingerprint, payload });
        return payload;
    }
    files(c, s) {
        if (!s) return [];
        return c.snapshots.slice(0, c.snapshots.indexOf(s) + 1).map((item) => ({
            file_id: item.operation_id,
            snapshot_id: item.snapshot_id,
            path: `demo://${c.id}/${item.snapshot_id}.parquet`,
            content: 'data',
            record_count: item.rows_added,
            compressed_bytes: null,
            field_metrics: null,
            physical_file_exists: false,
            ...mock,
        }));
    }
    page(items, url) {
        const size = Number(url.searchParams.get('page_size') ?? 100);
        if (!Number.isInteger(size) || size < 1 || size > 1000) fail(400, 'Invalid page size');
        const params = [...url.searchParams].filter(([key]) => key !== 'cursor').sort();
        const signature = url.pathname + JSON.stringify(params),
            token = url.searchParams.get('cursor');
        let record = { items: clone(items), offset: 0, signature, expires: Date.now() + 900000 };
        if (token) {
            record = this.cursors.get(token);
            if (!record || record.signature !== signature || record.expires < Date.now())
                fail(400, 'Invalid or expired cursor');
        }
        const result = record.items.slice(record.offset, record.offset + size);
        let next = null;
        if (record.offset + size < record.items.length) {
            next = crypto.randomUUID();
            if (this.cursors.size >= 128) this.cursors.delete(this.cursors.keys().next().value);
            this.cursors.set(next, { ...record, offset: record.offset + size });
        }
        return { items: result, next_cursor: next, ...mock };
    }
    async json(path, { method = 'GET', body, signal } = {}) {
        await this.ready;
        if (signal?.aborted) throw new DOMException('Read cancelled', 'AbortError');
        try {
            return clone(
                await this.dispatch(new URL(path, 'https://browser-demo.invalid'), method, body),
            );
        } catch (error) {
            if (error instanceof APIError) throw error;
            fail(422, error.message ?? 'Invalid demo input');
        }
    }
    dispatch(url, method, body) {
        if (url.origin !== 'https://browser-demo.invalid')
            fail(400, 'Demo requests stay in this page');
        const path = url.pathname;
        if (path === '/capabilities')
            return {
                ...mock,
                browser_demo: true,
                arrow_ipc: false,
                mode: 'append',
                key_unique: false,
                page_pruning: false,
                bloom_pruning: false,
                cow: false,
                compaction: false,
                async_admission: false,
            };
        if (path === '/collections') {
            if (method === 'POST') return this.info(this.create(body));
            return this.page(
                [...this.collections.values()].map((c) => this.info(c)),
                url,
            );
        }
        if (path === '/collections:resolve') {
            const c = [...this.collections.values()].find(
                (c) => c.name === url.searchParams.get('name'),
            );
            if (!c) fail(404, 'Exact collection name does not exist');
            return this.info(c);
        }
        const route = /^\/collections\/([^/]+)(?:\/(.*))?$/.exec(path);
        const c = route && this.collections.get(decodeURIComponent(route[1]));
        if (!c) fail(404, 'Collection does not exist');
        const action = route[2];
        if (!action) return this.info(c);
        if (action === 'schema') return c.schema;
        if (action === 'append' && method === 'POST') return this.append(c, body);
        if (action.startsWith('operations/')) {
            const value = c.operations.get(action.slice(11));
            if (!value) fail(404, 'Operation does not exist');
            return value.payload;
        }
        if (action === 'snapshots')
            return this.page(
                c.snapshots.map(({ rows, ...meta }) => ({ ...meta, ...mock })),
                url,
            );
        const selected = this.view(
            c,
            method === 'POST'
                ? body.snapshot_id === undefined
                    ? 'latest'
                    : body.snapshot_id
                : (url.searchParams.get('snapshot_id') ?? 'latest'),
        );
        const rows = selected?.rows ?? [],
            identity = this.identity(c, selected),
            files = this.files(c, selected);
        if (action === 'stats')
            return {
                identity,
                rows: rows.length,
                data_files: files.length,
                compressed_data_bytes: null,
                root_metadata_bytes: null,
                retained_snapshots: c.snapshots.length,
                ...mock,
            };
        if (action === 'files') return this.page(files, url);
        if (action.startsWith('files/')) {
            const file = files.find((f) => f.file_id === decodeURIComponent(action.slice(6)));
            if (!file) fail(404, 'File does not belong to selected view');
            return {
                ...file,
                row_groups: [
                    {
                        ordinal: 0,
                        record_count: file.record_count,
                        compressed_bytes: null,
                        synthetic: true,
                    },
                ],
                page_index_present: false,
                bloom_filter_present: false,
                execution_capabilities: { page_pruning: false, bloom_pruning: false },
            };
        }
        if ((action === 'scan' || action === 'explain') && method === 'POST') {
            const projection = body.projection ?? c.schema.fields.map((f) => f.name);
            if (
                !Array.isArray(projection) ||
                new Set(projection).size !== projection.length ||
                projection.some((name) => !c.schema.fields.some((f) => f.name === name))
            )
                fail(422, 'Invalid projection');
            const evaluate = predicateEvaluator(c.schema, body.predicate),
                limit = body.limit ?? 1000;
            if (!Number.isInteger(limit) || limit < 0 || limit > 1000)
                fail(422, 'Demo query limit must be 0..1000');
            const schema = {
                schema_id: 0,
                fields: projection.map((name) => c.schema.fields.find((f) => f.name === name)),
            };
            if (action === 'explain')
                return {
                    identity,
                    schema,
                    output_columns: projection,
                    predicate: body.predicate ?? null,
                    plan: [
                        'Fix browser demo snapshot',
                        'Read tab-local rows',
                        'Exact three-valued filter',
                        'Projection and limit',
                    ],
                    estimates: { rows_before_filter: rows.length, compressed_bytes: null },
                    ...mock,
                };
            return {
                schema,
                snapshot_id: selected?.snapshot_id ?? null,
                identity,
                rows: rows
                    .filter((row) => evaluate(row) === true)
                    .slice(0, limit)
                    .map((row) => Object.fromEntries(projection.map((name) => [name, row[name]]))),
                profile: {
                    rows_considered: rows.length,
                    compressed_bytes_read: null,
                    browser_demo: true,
                },
                ...mock,
            };
        }
        fail(404, 'Unsupported demo route');
    }
}
