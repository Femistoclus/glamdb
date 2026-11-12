import test from 'node:test';
import assert from 'node:assert/strict';
import * as model from '../../assets/model.js';
import { fileDetails } from '../../assets/file-details.js';
import { BrowserDemoAPI } from '../../assets/demo.js';

const scope = {
    server_url: 'http://localhost:8766',
    database_id: '00000000-0000-4000-8000-000000000001',
    store_id: '00000000-0000-4000-8000-000000000002',
    backend: 'local',
    mock: false,
};
const collectionId = '00000000-0000-4000-8000-000000000003';
const schema = {
    fields: [
        { name: 'key', type: 'string', nullable: false },
        { name: 'n', type: 'int64', nullable: true },
        { name: 'payload', type: 'string', nullable: true },
    ],
};
const DEFAULT = { parquet: { compression: null, columns: [] } };
// Columns deliberately out of schema order and without explicit levels.
const REQUEST = {
    parquet: {
        compression: { codec: 'zstd', level: 3 },
        columns: [
            { field: 'payload', compression: { codec: 'uncompressed' } },
            { field: 'key', compression: { codec: 'gzip', level: 9 } },
        ],
    },
};
const CANONICAL = {
    parquet: {
        compression: { codec: 'zstd', level: 3 },
        columns: [
            { field: 'key', compression: { codec: 'gzip', level: 9 } },
            { field: 'payload', compression: { codec: 'uncompressed', level: null } },
        ],
    },
};
const NATIVE = {
    backend: 'local',
    mock: false,
    features: { write_settings: true },
    parquet: {
        default_compression: { codec: 'zstd', level: null },
        codecs: [
            { codec: 'uncompressed', level: null },
            { codec: 'snappy', level: null },
            { codec: 'gzip', level: { min: 1, max: 9, default: 9 } },
            { codec: 'lz4_raw', level: { min: 1, max: 12, default: 1 } },
            { codec: 'zstd', level: { min: 1, max: 22, default: 1 } },
        ],
    },
};
// Non-default creation settings are stored as version "1"; defaults stay "0".
const created = (settings = CANONICAL, version = '1', changes = {}) => ({
    collection_id: collectionId,
    name: 'events',
    schema: {
        fields: schema.fields.map((field, index) => ({ field_id: index + 1, ...field })),
    },
    mode: 'append',
    write_settings: settings,
    write_settings_version: version,
    ...changes,
});

test('write settings have one canonical form in schema order with explicit default levels', () => {
    for (const value of [
        null,
        undefined,
        {},
        { parquet: null },
        { parquet: {} },
        { parquet: { compression: null, columns: null } },
    ])
        assert.deepEqual(model.canonicalWriteSettings(value, schema), DEFAULT);
    assert.deepEqual(model.canonicalWriteSettings(REQUEST, schema), CANONICAL);
    assert.deepEqual(
        model.canonicalWriteSettings(REQUEST).parquet.columns.map((item) => item.field),
        ['payload', 'key'],
    );
    assert.ok(model.sameWriteSettings(REQUEST, CANONICAL, schema));
    assert.ok(
        !model.sameWriteSettings(
            { parquet: { compression: { codec: 'zstd', level: 1 } } },
            { parquet: { compression: { codec: 'zstd' } } },
        ),
        'an explicit level differs from the codec default',
    );
});

test('invalid write settings shapes are rejected', () => {
    for (const value of [
        [],
        'zstd',
        { compression: { codec: 'zstd' } },
        { parquet: { codec: 'zstd' } },
        { parquet: { compression: 'zstd' } },
        { parquet: { compression: { codec: 'brotli' } } },
        { parquet: { compression: { level: 3 } } },
        { parquet: { compression: { codec: 'zstd', level: '3' } } },
        { parquet: { compression: { codec: 'zstd', level: 1.5 } } },
        { parquet: { compression: { codec: 'zstd', level: 2 ** 31 } } },
        { parquet: { compression: { codec: 'zstd', dictionary: true } } },
        { parquet: { columns: 'key' } },
        { parquet: { columns: [{ field: 'key' }] } },
        { parquet: { columns: [{ field: 'key', compression: null }] } },
        { parquet: { columns: [{ field: '', compression: { codec: 'snappy' } }] } },
        { parquet: { columns: [{ field: 'missing', compression: { codec: 'snappy' } }] } },
        {
            parquet: {
                columns: [
                    { field: 'key', compression: { codec: 'snappy' } },
                    { field: 'key', compression: { codec: 'zstd' } },
                ],
            },
        },
        {
            parquet: {
                columns: Array.from({ length: 65 }, (_, index) => ({
                    field: `c${index}`,
                    compression: { codec: 'snappy' },
                })),
            },
        },
    ])
        assert.throws(() => model.canonicalWriteSettings(value, schema), undefined, value);
});

test('controls are enabled only by an advertised feature and valid codec list', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    assert.deepEqual(parquet.default_compression, { codec: 'zstd', level: null });
    assert.deepEqual(
        parquet.codecs.map((item) => item.codec),
        ['uncompressed', 'snappy', 'gzip', 'lz4_raw', 'zstd'],
    );
    for (const capabilities of [
        null,
        {},
        { mock: true },
        { mock: true, parquet: NATIVE.parquet },
        { ...NATIVE, features: {} },
        { ...NATIVE, features: { write_settings: false } },
        { ...NATIVE, parquet: null },
        { ...NATIVE, parquet: { default_compression: null, codecs: [] } },
        {
            ...NATIVE,
            parquet: {
                codecs: [
                    { codec: 'brotli', level: null },
                    { codec: 'zstd', level: {} },
                ],
            },
        },
    ])
        assert.equal(model.parquetCapabilities(capabilities), null, JSON.stringify(capabilities));
    const filtered = model.parquetCapabilities({
        ...NATIVE,
        parquet: {
            default_compression: { codec: 'future' },
            codecs: [{ codec: 'future', level: null }, ...NATIVE.parquet.codecs.slice(0, 2)],
        },
    });
    assert.deepEqual(
        filtered.codecs.map((item) => item.codec),
        ['uncompressed', 'snappy'],
    );
    assert.equal(filtered.default_compression, null);
});

test('mock and static demo capabilities never enable Parquet write settings', async () => {
    const demo = await new BrowserDemoAPI().json('/capabilities');
    assert.equal(model.parquetCapabilities(demo), null);
    // The mock advertises no features object (supportsFeature treats that as
    // legacy mock support); write settings need an explicit native feature.
    assert.equal(model.parquetCapabilities({ backend: 'mock', mock: true }), null);
});

test('form choices produce settings only when something is chosen and levels are in range', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    assert.equal(
        model.writeSettingsFromChoices(parquet, { codec: '', level: '' }, [
            { field: 'key', codec: '', level: '' },
        ]),
        null,
    );
    assert.deepEqual(
        model.writeSettingsFromChoices(parquet, { codec: 'zstd', level: ' 3 ' }, [
            { field: 'key', codec: 'gzip', level: '9' },
            { field: 'n', codec: '', level: '' },
            { field: 'payload', codec: 'uncompressed', level: '' },
        ]),
        CANONICAL,
    );
    assert.deepEqual(
        model.writeSettingsFromChoices(parquet, { codec: '', level: '' }, [
            { field: 'payload', codec: 'snappy', level: '' },
        ]),
        {
            parquet: {
                compression: null,
                columns: [{ field: 'payload', compression: { codec: 'snappy', level: null } }],
            },
        },
    );
    for (const [choice, message] of [
        [{ codec: 'zstd', level: '23' }, /1–22/],
        [{ codec: 'zstd', level: '0' }, /1–22/],
        [{ codec: 'zstd', level: '2.5' }, /1–22/],
        [{ codec: 'snappy', level: '1' }, /no compression level/],
        [{ codec: '', level: '1' }, /choose a codec/],
        [{ codec: 'brotli', level: '' }, /not available/],
    ])
        assert.throws(() => model.writeSettingsFromChoices(parquet, choice, []), message);
    assert.throws(
        () =>
            model.writeSettingsFromChoices(parquet, { codec: '', level: '' }, [
                { field: 'key', codec: 'gzip', level: '10' },
            ]),
        /key: gzip level must be 1–9/,
    );
});

test('compression descriptions name the server default and default levels', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    assert.equal(
        model.describeCompression(null, parquet),
        'Server default (zstd · default level 1)',
    );
    assert.equal(model.describeCompression(null), 'Server default');
    assert.equal(model.describeCompression({ codec: 'gzip', level: 4 }, parquet), 'gzip · level 4');
    assert.equal(model.describeCompression({ codec: 'snappy', level: null }, parquet), 'snappy');
});

test('created collection verification compares settings until a later update replaces them', () => {
    const body = {
        collection_id: collectionId,
        name: 'events',
        schema,
        write_settings: model.canonicalWriteSettings(REQUEST, schema),
    };
    assert.equal(model.verifyCreatedCollection(created(), body).name, 'events');
    // A later compare-and-set update replaced the creation settings.
    assert.ok(model.verifyCreatedCollection(created(DEFAULT, '3'), body));
    for (const value of [
        created(DEFAULT),
        created(DEFAULT, '0'),
        created(CANONICAL, '0'),
        created({ parquet: { compression: { codec: 'zstd', level: null }, columns: [] } }),
        created(CANONICAL, 0),
        created(CANONICAL, '01'),
        created(CANONICAL, '-1'),
        created({ parquet: { compression: { codec: 'brotli', level: null }, columns: [] } }, '3'),
        Object.fromEntries(Object.entries(created()).filter(([key]) => key !== 'write_settings')),
        Object.fromEntries(
            Object.entries(created()).filter(([key]) => !key.startsWith('write_settings')),
        ),
    ])
        assert.throws(
            () => model.verifyCreatedCollection(value, body),
            /write settings/,
            JSON.stringify(value),
        );
    assert.throws(
        () => model.verifyCreatedCollection(created(CANONICAL, '1', { name: 'other' }), body),
        /identity or schema/,
    );
    const plain = { collection_id: collectionId, name: 'events', schema };
    assert.ok(model.verifyCreatedCollection(created(DEFAULT, '0'), plain));
    assert.throws(
        () => model.verifyCreatedCollection(created(CANONICAL, '0'), plain),
        /write settings/,
    );
    // Version 1 is also a default creation after its first update.
    assert.ok(model.verifyCreatedCollection(created(), plain));
    // Servers without write settings (mock, demo, older native) report neither field.
    assert.ok(
        model.verifyCreatedCollection(
            Object.fromEntries(
                Object.entries(created()).filter(([key]) => !key.startsWith('write_settings')),
            ),
            plain,
        ),
    );
});

test('resolved (GET) collections confirm a lost create only while no settings were stored', () => {
    const body = {
        collection_id: collectionId,
        name: 'events',
        schema,
        write_settings: model.canonicalWriteSettings(REQUEST, schema),
    };
    const plain = { collection_id: collectionId, name: 'events', schema };
    const resolved = (value, request) =>
        model.verifyCreatedCollection(value, request, { authoritative: false });
    assert.ok(resolved(created(DEFAULT, '0'), plain));
    assert.throws(() => resolved(created(DEFAULT, '0'), body), /differ from the original/);
    // A stored version is never proof: create zstd, update to snappy (v2),
    // then resolve a pending create that asked for gzip.
    const gzip = {
        ...body,
        write_settings: model.canonicalWriteSettings(
            { parquet: { compression: { codec: 'gzip' } } },
            schema,
        ),
    };
    for (const [value, request] of [
        [created(DEFAULT, '2'), gzip],
        [created(), body],
        [created(), plain],
        [created(DEFAULT, '3'), body],
    ])
        assert.throws(() => resolved(value, request), /Retry the same create request/);
    // Identity/schema mismatches are still definite.
    assert.throws(
        () => resolved(created(DEFAULT, '2', { name: 'other' }), gzip),
        /identity or schema/,
    );
});

test('create fingerprint is unchanged without settings and binds settings when present', () => {
    const plain = { collection_id: collectionId, name: 'events', schema };
    assert.equal(model.creationFingerprint(plain), JSON.stringify({ name: 'events', schema }));
    const withSettings = { ...plain, write_settings: CANONICAL };
    assert.notEqual(model.creationFingerprint(withSettings), model.creationFingerprint(plain));
    assert.equal(
        model.creationFingerprint(withSettings),
        JSON.stringify({ name: 'events', schema, write_settings: CANONICAL }),
    );
});

test('pending create export retains exact canonical write settings and rejects altered ones', () => {
    const body = {
        collection_id: collectionId,
        name: 'events',
        schema,
        write_settings: model.canonicalWriteSettings(REQUEST, schema),
    };
    const exported = model.exportPending(scope, 'create', { body });
    const imported = model.importPending(JSON.parse(JSON.stringify(exported)), scope);
    assert.deepEqual(imported.draft.body, body);
    assert.equal(imported.draft.fingerprint, model.creationFingerprint(body));
    for (const change of [
        (value) => (value.request.body.write_settings = REQUEST),
        (value) => (value.request.body.write_settings = null),
        (value) => (value.request.body.write_settings = {}),
        (value) => (value.request.body.write_settings.parquet.extra = true),
        (value) => (value.request.body.write_settings.parquet.compression.codec = 'brotli'),
        (value) => (value.request.body.write_settings.parquet.compression.level = '3'),
        (value) => (value.request.body.write_settings.parquet.columns[0].field = 'missing'),
        (value) => delete value.request.body.write_settings.parquet.columns[1].compression.level,
        (value) => value.request.body.write_settings.parquet.columns.reverse(),
    ]) {
        const invalid = structuredClone(exported);
        change(invalid);
        assert.throws(() => model.importPending(invalid, scope), /pending/i);
    }
});

test('file details show the Parquet codec of each column chunk after its bounds', () => {
    const metrics = (compression) => ({
        field_id: 1,
        name: 'n',
        type: 'int64',
        null_count: '0',
        lower_bound: '1',
        upper_bound: '2',
        ...(compression === undefined ? {} : { compression }),
    });
    const html = fileDetails(
        {
            file_id: 'f',
            record_count: '1',
            footer_statistics: [metrics('zstd'), { ...metrics(null), name: 'm<x>' }],
            row_groups: [],
        },
        '5',
    );
    const table = html.match(/aria-label="Parquet field statistics">([\s\S]*?)<\/table>/)[1];
    assert.match(table, /<th>Upper bound<\/th><th>Parquet codec<\/th><\/tr>/);
    assert.match(table, /<span class="type">zstd<\/span><\/td><\/tr>/);
    assert.match(table, /m&lt;x&gt;[\s\S]*Unknown<\/span><\/td><\/tr>/);
    const manifestOnly = fileDetails(
        { file_id: 'f', footer_statistics: [metrics(undefined)], row_groups: [] },
        '5',
    );
    assert.doesNotMatch(manifestOnly, /Parquet codec/);
});

// Settings as reported by a newer server whose extra options are all unset.
const withUnsetOptions = (settings) => ({
    parquet: {
        columns: settings.parquet.columns.map((column) => ({
            dictionary: null,
            encoding: null,
            ...column,
        })),
        compression: settings.parquet.compression,
        dictionary: null,
        dictionary_page_size_limit: null,
    },
});

test('reported settings ignore unknown unset options and refuse set ones', () => {
    const reported = withUnsetOptions(CANONICAL);
    assert.deepEqual(model.canonicalWriteSettings(reported, schema, { reported: true }), CANONICAL);
    assert.throws(() => model.canonicalWriteSettings(reported, schema));
    assert.ok(model.sameWriteSettings(reported, REQUEST, schema));
    const body = {
        collection_id: collectionId,
        name: 'events',
        schema,
        write_settings: CANONICAL,
    };
    assert.ok(model.verifyCreatedCollection(created(reported), body));
    for (const changed of [
        { parquet: { ...reported.parquet, dictionary: 'off' } },
        { parquet: { ...reported.parquet, dictionary_page_size_limit: '4096' } },
        {
            parquet: {
                ...reported.parquet,
                columns: [{ field: 'n', compression: null, encoding: 'plain', dictionary: null }],
            },
        },
    ]) {
        assert.throws(
            () => model.canonicalWriteSettings(changed, schema, { reported: true }),
            /does not support/,
        );
        assert.throws(
            () => model.verifyCreatedCollection(created(changed), body),
            /write settings/,
        );
    }
    const exported = model.exportPending(scope, 'create', { body });
    exported.request.body.write_settings = reported;
    assert.throws(() => model.importPending(exported, scope), /pending/i);
});
