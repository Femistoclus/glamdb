import test from 'node:test';
import assert from 'node:assert/strict';
import * as model from '../../assets/model.js';
import { chunkEncodings, fileDetails } from '../../assets/file-details.js';
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
        { name: 'flag', type: 'bool', nullable: true },
    ],
};
const DEFAULT = {
    parquet: { compression: null, dictionary: null, dictionary_page_size_limit: null, columns: [] },
};
// Columns deliberately out of schema order, without explicit levels or unset
// options, and with a numeric page size limit.
const REQUEST = {
    parquet: {
        compression: { codec: 'zstd', level: 3 },
        dictionary: 'auto',
        dictionary_page_size_limit: 65536,
        columns: [
            { field: 'payload', compression: { codec: 'uncompressed' } },
            { field: 'n', encoding: 'delta_binary_packed' },
            { field: 'key', compression: { codec: 'gzip', level: 9 }, dictionary: 'disabled' },
        ],
    },
};
const CANONICAL = {
    parquet: {
        compression: { codec: 'zstd', level: 3 },
        dictionary: 'auto',
        dictionary_page_size_limit: '65536',
        columns: [
            {
                field: 'key',
                compression: { codec: 'gzip', level: 9 },
                dictionary: 'disabled',
                encoding: null,
            },
            { field: 'n', compression: null, dictionary: null, encoding: 'delta_binary_packed' },
            {
                field: 'payload',
                compression: { codec: 'uncompressed', level: null },
                dictionary: null,
                encoding: null,
            },
        ],
    },
};
// The request form omits unset options.
const WIRE = {
    parquet: {
        compression: { codec: 'zstd', level: 3 },
        dictionary: 'auto',
        dictionary_page_size_limit: '65536',
        columns: [
            { field: 'key', compression: { codec: 'gzip', level: 9 }, dictionary: 'disabled' },
            { field: 'n', encoding: 'delta_binary_packed' },
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
        encodings: [
            {
                encoding: 'plain',
                types: [
                    'string',
                    'bool',
                    'int64',
                    'float64',
                    'timestamp_us_utc',
                    'decimal',
                    'date',
                ],
            },
            { encoding: 'delta_binary_packed', types: ['int64', 'timestamp_us_utc', 'date'] },
            { encoding: 'delta_length_byte_array', types: ['string'] },
            { encoding: 'delta_byte_array', types: ['string'] },
            {
                encoding: 'byte_stream_split',
                types: ['int64', 'float64', 'timestamp_us_utc', 'date'],
            },
            { encoding: 'rle', types: ['bool'] },
        ],
        dictionary: {
            policies: ['auto', 'enabled', 'disabled'],
            default_policy: 'auto',
            types: ['string', 'int64', 'float64', 'timestamp_us_utc', 'decimal', 'date'],
            fallback_encoding: 'plain',
            page_size_limit: { min: 4096, max: 8388608, default: 1048576 },
        },
    },
};
// A #97 server: compression only.
const COMPRESSION_ONLY = {
    ...NATIVE,
    parquet: {
        default_compression: NATIVE.parquet.default_compression,
        codecs: NATIVE.parquet.codecs,
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
const choice = (field, type, values = {}) => ({
    field,
    type,
    codec: '',
    level: '',
    dictionary: '',
    encoding: '',
    ...values,
});

test('write settings have one canonical form with every option key in schema order', () => {
    for (const value of [
        null,
        undefined,
        {},
        { parquet: null },
        { parquet: {} },
        { parquet: { compression: null, columns: null } },
        { parquet: { dictionary: null, dictionary_page_size_limit: null } },
    ])
        assert.deepEqual(model.canonicalWriteSettings(value, schema), DEFAULT);
    assert.deepEqual(model.canonicalWriteSettings(REQUEST, schema), CANONICAL);
    assert.deepEqual(
        model.canonicalWriteSettings(REQUEST).parquet.columns.map((item) => item.field),
        ['payload', 'n', 'key'],
    );
    assert.ok(model.sameWriteSettings(REQUEST, CANONICAL, schema));
    assert.deepEqual(model.wireWriteSettings(CANONICAL), WIRE);
    assert.deepEqual(model.wireWriteSettings(DEFAULT), { parquet: { columns: [] } });
    assert.deepEqual(model.canonicalWriteSettings(WIRE, schema), CANONICAL);
    assert.ok(
        !model.sameWriteSettings(
            { parquet: { compression: { codec: 'zstd', level: 1 } } },
            { parquet: { compression: { codec: 'zstd' } } },
        ),
        'an explicit level differs from the codec default',
    );
    assert.ok(
        !model.sameWriteSettings({ parquet: { dictionary: 'auto' } }, DEFAULT),
        'an explicit auto policy differs from an unset one',
    );
    assert.deepEqual(
        model.canonicalWriteSettings({ parquet: { columns: [{ field: 'flag', encoding: 'rle' }] } })
            .parquet.columns,
        [{ field: 'flag', compression: null, dictionary: null, encoding: 'rle' }],
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
        { parquet: { dictionary: 'on' } },
        { parquet: { dictionary: true } },
        { parquet: { dictionary_page_size_limit: '4k' } },
        { parquet: { dictionary_page_size_limit: '065536' } },
        { parquet: { dictionary_page_size_limit: 0 } },
        { parquet: { dictionary_page_size_limit: 4096.5 } },
        { parquet: { columns: 'key' } },
        { parquet: { columns: [{ field: 'key' }] } },
        {
            parquet: {
                columns: [{ field: 'key', compression: null, dictionary: null, encoding: null }],
            },
        },
        { parquet: { columns: [{ field: 'key', dictionary: 'off' }] } },
        { parquet: { columns: [{ field: 'key', encoding: 'rle_dictionary' }] } },
        { parquet: { columns: [{ field: 'key', encoding: 'plain', statistics: null }] } },
        { parquet: { columns: [{ field: '', compression: { codec: 'snappy' } }] } },
        { parquet: { columns: [{ field: 'missing', compression: { codec: 'snappy' } }] } },
        {
            parquet: {
                columns: [
                    { field: 'key', compression: { codec: 'snappy' } },
                    { field: 'key', encoding: 'plain' },
                ],
            },
        },
        {
            parquet: {
                columns: Array.from({ length: 129 }, (_, index) => ({
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
    assert.equal(parquet.encodings.length, 6);
    assert.deepEqual(parquet.dictionary.page_size_limit, {
        min: 4096,
        max: 8388608,
        default: 1048576,
    });
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
            ...NATIVE.parquet,
            default_compression: { codec: 'future' },
            codecs: [{ codec: 'future', level: null }, ...NATIVE.parquet.codecs.slice(0, 2)],
            encodings: [
                { encoding: 'future', types: ['string'] },
                { encoding: 'plain', types: ['string', 'binary'] },
            ],
        },
    });
    assert.deepEqual(
        filtered.codecs.map((item) => item.codec),
        ['uncompressed', 'snappy'],
    );
    assert.equal(filtered.default_compression, null);
    assert.deepEqual(filtered.encodings, [{ encoding: 'plain', types: ['string'] }]);
    // A compression-only server (or an invalid dictionary capability) hides
    // the dictionary and encoding controls but keeps compression.
    const older = model.parquetCapabilities(COMPRESSION_ONLY);
    assert.deepEqual(older.encodings, []);
    assert.equal(older.dictionary, null);
    const invalidDictionary = model.parquetCapabilities({
        ...NATIVE,
        parquet: {
            ...NATIVE.parquet,
            dictionary: { ...NATIVE.parquet.dictionary, page_size_limit: { min: 9, max: 1 } },
        },
    });
    assert.equal(invalidDictionary.dictionary, null);
});

test('mock and static demo capabilities never enable Parquet write settings', async () => {
    const demo = await new BrowserDemoAPI().json('/capabilities');
    assert.equal(model.parquetCapabilities(demo), null);
    // The mock advertises no features object (supportsFeature treats that as
    // legacy mock support); write settings need an explicit native feature.
    assert.equal(model.parquetCapabilities({ backend: 'mock', mock: true }), null);
});

test('column choices are filtered by type from capabilities', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    const options = (type) => model.columnParquetOptions(parquet, type);
    assert.deepEqual(options('string'), {
        encodings: ['plain', 'delta_length_byte_array', 'delta_byte_array'],
        dictionaries: ['auto', 'enabled', 'disabled'],
    });
    assert.deepEqual(options('int64').encodings, [
        'plain',
        'delta_binary_packed',
        'byte_stream_split',
    ]);
    assert.deepEqual(options('float64').encodings, ['plain', 'byte_stream_split']);
    assert.deepEqual(options('bool'), {
        encodings: ['plain', 'rle'],
        dictionaries: ['auto', 'disabled'],
    });
    assert.deepEqual(options('decimal'), {
        encodings: ['plain'],
        dictionaries: ['auto', 'enabled', 'disabled'],
    });
    assert.deepEqual(options('date'), {
        encodings: ['plain', 'delta_binary_packed', 'byte_stream_split'],
        dictionaries: ['auto', 'enabled', 'disabled'],
    });
    assert.deepEqual(
        model.columnParquetOptions(model.parquetCapabilities(COMPRESSION_ONLY), 'int64'),
        {
            encodings: [],
            dictionaries: [],
        },
    );
});

test('choosing an encoding clears an incompatible column dictionary policy', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    const reconcile = (type, encoding, dictionary) =>
        model.reconcileColumnDictionary(parquet, type, encoding, dictionary);
    assert.equal(reconcile('int64', '', 'enabled'), 'enabled');
    assert.equal(reconcile('int64', 'plain', 'enabled'), '');
    assert.equal(reconcile('int64', 'plain', 'auto'), '');
    assert.equal(reconcile('int64', 'plain', 'disabled'), 'disabled');
    assert.equal(reconcile('int64', 'plain', ''), '');
    // bool columns have no dictionary under auto, so auto stays valid.
    assert.equal(reconcile('bool', 'rle', 'auto'), 'auto');
    assert.equal(reconcile('bool', 'rle', 'disabled'), 'disabled');
});

test('form choices produce settings only when something is chosen and levels are in range', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    assert.equal(
        model.writeSettingsFromChoices(
            parquet,
            { codec: '', level: '', dictionary: '', pageSizeLimit: '' },
            [choice('key', 'string')],
        ),
        null,
    );
    assert.deepEqual(
        model.canonicalWriteSettings(
            model.writeSettingsFromChoices(
                parquet,
                { codec: 'zstd', level: ' 3 ', dictionary: 'auto', pageSizeLimit: '65536' },
                [
                    choice('key', 'string', { codec: 'gzip', level: '9', dictionary: 'disabled' }),
                    choice('n', 'int64', { encoding: 'delta_binary_packed' }),
                    choice('payload', 'string', { codec: 'uncompressed' }),
                    choice('flag', 'bool'),
                ],
            ),
            schema,
        ),
        CANONICAL,
    );
    assert.deepEqual(
        model.writeSettingsFromChoices(parquet, { codec: '', level: '' }, [
            choice('payload', 'string', { codec: 'snappy' }),
        ]),
        {
            parquet: {
                compression: null,
                dictionary: null,
                dictionary_page_size_limit: null,
                columns: [
                    {
                        field: 'payload',
                        compression: { codec: 'snappy', level: null },
                        dictionary: null,
                        encoding: null,
                    },
                ],
            },
        },
    );
    for (const [collection, message] of [
        [{ codec: 'zstd', level: '23' }, /1–22/],
        [{ codec: 'zstd', level: '0' }, /1–22/],
        [{ codec: 'zstd', level: '2.5' }, /1–22/],
        [{ codec: 'snappy', level: '1' }, /no compression level/],
        [{ codec: '', level: '1' }, /choose a codec/],
        [{ codec: 'brotli', level: '' }, /not available/],
        [{ dictionary: 'sometimes' }, /not available/],
        [{ pageSizeLimit: '4095' }, /4096–8388608 bytes/],
        [{ pageSizeLimit: '8388609' }, /4096–8388608 bytes/],
        [{ pageSizeLimit: '64k' }, /4096–8388608 bytes/],
    ])
        assert.throws(() => model.writeSettingsFromChoices(parquet, collection, []), message);
    assert.throws(
        () =>
            model.writeSettingsFromChoices(parquet, {}, [
                choice('key', 'string', { codec: 'gzip', level: '10' }),
            ]),
        /key: gzip level must be 1–9/,
    );
});

test('form choices check encoding types and dictionary conflicts before sending', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    const build = (collection, columns) =>
        model.writeSettingsFromChoices(parquet, collection, columns);
    for (const [collection, columns, message] of [
        [{}, [choice('n', 'int64', { encoding: 'rle' })], /rle does not accept int64 column n/],
        [{}, [choice('key', 'string', { encoding: 'byte_stream_split' })], /does not accept/],
        [{}, [choice('flag', 'bool', { dictionary: 'enabled' })], /not available for bool/],
        [
            {},
            [choice('n', 'int64', { encoding: 'plain', dictionary: 'enabled' })],
            /turns the dictionary off/,
        ],
        [
            {},
            [choice('n', 'int64', { encoding: 'plain', dictionary: 'auto' })],
            /turns the dictionary off/,
        ],
        [
            { dictionary: 'enabled' },
            [choice('key', 'string'), choice('flag', 'bool')],
            /needs a dictionary policy or encoding for bool column flag/,
        ],
    ])
        assert.throws(() => build(collection, columns), message);
    // Valid combinations the server accepts.
    for (const columns of [
        [choice('flag', 'bool', { encoding: 'rle', dictionary: 'auto' })],
        [choice('n', 'int64', { encoding: 'plain', dictionary: 'disabled' })],
        [choice('n', 'int64', { encoding: 'byte_stream_split' })],
    ])
        assert.ok(
            build({ dictionary: 'enabled' }, [
                ...columns,
                choice('flag2', 'bool', { dictionary: 'disabled' }),
            ]),
        );
    assert.ok(build({ dictionary: 'enabled' }, [choice('flag', 'bool', { encoding: 'rle' })]));
    assert.throws(
        () =>
            model.writeSettingsFromChoices(model.parquetCapabilities(COMPRESSION_ONLY), {}, [
                choice('n', 'int64', { encoding: 'plain' }),
            ]),
        /does not accept/,
    );
});

test('descriptions follow the server precedence for compression, dictionary and encoding', () => {
    const parquet = model.parquetCapabilities(NATIVE);
    assert.equal(
        model.describeCompression(null, parquet),
        'Server default (zstd · default level 1)',
    );
    assert.equal(model.describeCompression(null), 'Server default');
    assert.equal(model.describeCompression({ codec: 'gzip', level: 4 }, parquet), 'gzip · level 4');
    assert.equal(model.describeCompression({ codec: 'snappy', level: null }, parquet), 'snappy');
    const settings = model.canonicalWriteSettings(REQUEST, schema);
    const column = (name) => settings.parquet.columns.find((item) => item.field === name);
    assert.equal(
        model.describeColumnEffect('string', column('key'), settings, parquet),
        'gzip · level 9 · plain',
    );
    assert.equal(
        model.describeColumnEffect('int64', column('n'), settings, parquet),
        'zstd · level 3 · delta_binary_packed',
    );
    assert.equal(
        model.describeColumnEffect('string', column('payload'), settings, parquet),
        'uncompressed · dictionary, plain fallback',
    );
    assert.equal(
        model.describeColumnEffect('bool', undefined, settings, parquet),
        'zstd · level 3 · plain',
    );
    assert.equal(
        model.describeColumnEffect(
            'float64',
            undefined,
            model.canonicalWriteSettings(null),
            parquet,
        ),
        'Server default (zstd · default level 1) · dictionary, plain fallback',
    );
});

test('created collection verification compares settings until a later update replaces them', () => {
    const body = { collection_id: collectionId, name: 'events', schema, write_settings: WIRE };
    assert.equal(model.verifyCreatedCollection(created(), body).name, 'events');
    // A later compare-and-set update replaced the creation settings.
    assert.ok(model.verifyCreatedCollection(created(DEFAULT, '3'), body));
    const without = (key) =>
        Object.fromEntries(Object.entries(created()).filter(([name]) => !name.startsWith(key)));
    for (const value of [
        created(DEFAULT),
        created(DEFAULT, '0'),
        created(CANONICAL, '0'),
        created({ parquet: { ...CANONICAL.parquet, dictionary: null } }),
        created({ parquet: { ...CANONICAL.parquet, dictionary_page_size_limit: '4096' } }),
        created({ parquet: { ...CANONICAL.parquet, columns: CANONICAL.parquet.columns.slice(1) } }),
        created(CANONICAL, 0),
        created(CANONICAL, '01'),
        created(CANONICAL, '-1'),
        created({ parquet: { compression: { codec: 'brotli', level: null }, columns: [] } }, '3'),
        without('write_settings_version'),
        without('write_settings'),
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
    // A compression-only server reports no dictionary or encoding keys.
    const compressionOnly = { parquet: { compression: { codec: 'zstd', level: 3 }, columns: [] } };
    assert.ok(
        model.verifyCreatedCollection(created(compressionOnly), {
            ...plain,
            write_settings: { parquet: { compression: { codec: 'zstd', level: 3 }, columns: [] } },
        }),
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
    const withSettings = { ...plain, write_settings: WIRE };
    assert.notEqual(model.creationFingerprint(withSettings), model.creationFingerprint(plain));
    assert.equal(
        model.creationFingerprint(withSettings),
        JSON.stringify({ name: 'events', schema, write_settings: WIRE }),
    );
});

test('pending create export retains exact request settings and rejects altered ones', () => {
    const body = { collection_id: collectionId, name: 'events', schema, write_settings: WIRE };
    const exported = model.exportPending(scope, 'create', { body });
    const imported = model.importPending(JSON.parse(JSON.stringify(exported)), scope);
    assert.deepEqual(imported.draft.body, body);
    assert.equal(imported.draft.fingerprint, model.creationFingerprint(body));
    for (const change of [
        (value) => (value.request.body.write_settings = REQUEST),
        (value) => (value.request.body.write_settings = CANONICAL),
        (value) => (value.request.body.write_settings = null),
        (value) => (value.request.body.write_settings = {}),
        (value) => (value.request.body.write_settings.parquet.extra = true),
        (value) => (value.request.body.write_settings.parquet.compression.codec = 'brotli'),
        (value) => (value.request.body.write_settings.parquet.compression.level = '3'),
        (value) => (value.request.body.write_settings.parquet.dictionary = 'on'),
        (value) => (value.request.body.write_settings.parquet.dictionary_page_size_limit = 65536),
        (value) =>
            (value.request.body.write_settings.parquet.columns[1].encoding = 'rle_dictionary'),
        (value) => (value.request.body.write_settings.parquet.columns[0].field = 'missing'),
        (value) => delete value.request.body.write_settings.parquet.columns[2].compression.level,
        (value) => value.request.body.write_settings.parquet.columns.reverse(),
    ]) {
        const invalid = structuredClone(exported);
        change(invalid);
        assert.throws(() => model.importPending(invalid, scope), /pending/i);
    }
});

test('file details show each column chunk codec and encodings after its bounds', () => {
    const metrics = (extra = {}) => ({
        field_id: 1,
        name: 'n',
        type: 'int64',
        null_count: '0',
        lower_bound: '1',
        upper_bound: '2',
        ...extra,
    });
    const html = fileDetails(
        {
            file_id: 'f',
            record_count: '1',
            footer_statistics: [
                metrics({
                    compression: 'zstd',
                    encodings: ['plain', 'rle', 'rle_dictionary'],
                    encoding_stats: [
                        { page_type: 'dictionary_page', encoding: 'plain', count: 1 },
                        { page_type: 'data_page', encoding: 'rle_dictionary', count: 3 },
                        { page_type: 'data_page', encoding: 'plain', count: 2 },
                    ],
                }),
                metrics({
                    name: 'm<x>',
                    compression: null,
                    encodings: ['delta_binary_packed'],
                    encoding_stats: [
                        { page_type: 'data_page', encoding: 'delta_binary_packed', count: 1 },
                    ],
                }),
            ],
            row_groups: [],
        },
        '5',
    );
    const table = html.match(/aria-label="Parquet field statistics">([\s\S]*?)<\/table>/)[1];
    assert.match(table, /<th>Upper bound<\/th><th>Parquet codec<\/th><th>Encodings<\/th><\/tr>/);
    const rows = [...table.matchAll(/<tr>([\s\S]*?)<\/tr>/g)].slice(1).map((match) => match[1]);
    assert.match(rows[0], /<span class="type">zstd<\/span>/);
    assert.match(rows[0], /plain, rle, rle_dictionary/);
    assert.match(rows[0], /Dictionary fallback: plain/);
    assert.match(
        rows[0],
        /title="dictionary_page plain ×1; data_page rle_dictionary ×3; data_page plain ×2"/,
    );
    assert.match(rows[1], /m&lt;x&gt;[\s\S]*Unknown<\/span><\/td>/);
    assert.match(rows[1], /delta_binary_packed/);
    assert.doesNotMatch(rows[1], /fallback/);
    const manifestOnly = fileDetails(
        { file_id: 'f', footer_statistics: [metrics()], row_groups: [] },
        '5',
    );
    assert.doesNotMatch(manifestOnly, /Parquet codec|Encodings/);
});

test('dictionary fallback is derived only from data page encodings', () => {
    assert.deepEqual(chunkEncodings({}), { encodings: null, fallback: [], pages: '' });
    const page = (page_type, encoding, count = 1) => ({ page_type, encoding, count });
    const fallback = (stats) => chunkEncodings({ encodings: [], encoding_stats: stats }).fallback;
    assert.deepEqual(
        fallback([page('dictionary_page', 'plain'), page('data_page', 'rle_dictionary')]),
        [],
    );
    assert.deepEqual(fallback([page('data_page', 'plain')]), []);
    assert.deepEqual(
        fallback([page('data_page_v2', 'rle_dictionary'), page('data_page_v2', 'plain')]),
        ['plain'],
    );
    assert.deepEqual(
        fallback([page('data_page', 'rle_dictionary'), page('data_page', 'plain', 0)]),
        [],
        'empty page counts are ignored',
    );
    assert.deepEqual(
        fallback([
            page('data_page', 'rle_dictionary'),
            { page_type: 'data_page', encoding: 'plain' },
        ]),
        [],
        'malformed entries are ignored',
    );
});

// Settings as reported by a future server whose extra options are all unset.
const withUnknownOptions = (settings) => ({
    parquet: {
        bloom_filter: null,
        ...settings.parquet,
        columns: settings.parquet.columns.map((column) => ({ statistics: null, ...column })),
    },
});

test('reported settings ignore unknown unset options and refuse set ones', () => {
    const reported = withUnknownOptions(CANONICAL);
    assert.deepEqual(model.canonicalWriteSettings(reported, schema, { reported: true }), CANONICAL);
    assert.throws(() => model.canonicalWriteSettings(reported, schema));
    assert.ok(model.sameWriteSettings(reported, REQUEST, schema));
    const body = { collection_id: collectionId, name: 'events', schema, write_settings: WIRE };
    assert.ok(model.verifyCreatedCollection(created(reported), body));
    for (const changed of [
        { parquet: { ...reported.parquet, bloom_filter: 'enabled' } },
        {
            parquet: {
                ...reported.parquet,
                columns: [{ field: 'n', encoding: 'plain', statistics: 'page' }],
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
    // Known options keep their value checks in reported settings too.
    assert.throws(() =>
        model.canonicalWriteSettings(
            { parquet: { ...reported.parquet, dictionary: 'sometimes' } },
            schema,
            { reported: true },
        ),
    );
    const exported = model.exportPending(scope, 'create', { body });
    exported.request.body.write_settings = reported;
    assert.throws(() => model.importPending(exported, scope), /pending/i);
});
