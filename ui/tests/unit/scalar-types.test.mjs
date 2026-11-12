import test from 'node:test';
import assert from 'node:assert/strict';
import {
    wireRows,
    formRows,
    buildPredicate,
    schemaSignature,
    schemaTypes,
    creationSchema,
} from '../../assets/model.js';
import { writeEditor } from '../../assets/editor.js';

const fields = [
    { name: 'key', type: 'string', nullable: false },
    { name: 'amount', type: 'decimal', precision: 38, scale: 4, nullable: true },
    { name: 'day', type: 'date', nullable: true },
];
const schema = { fields };

test('decimal form and JSON values preserve all digits and canonical fixed scale', () => {
    const rows = [
        { key: 'a', amount: '9999999999999999999999999999999999.9999', day: '0001-01-01' },
        { key: 'b', amount: '-00123.450000', day: '2000-02-29' },
        { key: 'c', amount: '-000.00000', day: '9999-12-31' },
    ];
    const expected = rows.map((row) => ({ ...row }));
    expected[1].amount = '-123.4500';
    expected[2].amount = '0.0000';
    assert.deepEqual(wireRows(schema, rows), expected);
    assert.deepEqual(formRows(schema, rows), expected);
    for (const amount of [
        1.25,
        '+1',
        '1e2',
        ' 1',
        '1.',
        '.1',
        '1.00001',
        '1'.repeat(39),
        '0'.repeat(129),
    ])
        assert.throws(
            () => wireRows(schema, [{ key: 'a', amount }]),
            /decimal|precision|scale|digits/i,
        );
});

test('date fields validate Gregorian calendar dates without timezone conversion', () => {
    assert.equal(wireRows(schema, [{ key: 'a', day: '2000-02-29' }])[0].day, '2000-02-29');
    for (const day of [
        '0000-01-01',
        '1900-02-29',
        '2026-04-31',
        '2026-11-12T00:00:00Z',
        '2026-1-01',
        0,
    ])
        assert.throws(() => wireRows(schema, [{ key: 'a', day }]), /date|YYYY/);
});

test('decimal and date predicates use exact typed literals', () => {
    assert.deepEqual(
        buildPredicate([{ field: 'amount', op: 'eq', value: '0001.23000' }], 'and', false, fields),
        {
            op: 'eq',
            field: 'amount',
            value: { type: 'decimal', precision: 38, scale: 4, value: '1.2300' },
        },
    );
    assert.deepEqual(
        buildPredicate(
            [{ field: 'day', op: 'in', value: ['0001-01-01', '2000-02-29'] }],
            'and',
            false,
            fields,
        ),
        {
            op: 'in',
            field: 'day',
            values: [
                { type: 'date', value: '0001-01-01' },
                { type: 'date', value: '2000-02-29' },
            ],
        },
    );
});

test('schema retry identity includes decimal parameters and rejects parameters on other fields', () => {
    assert.notEqual(
        schemaSignature(schema),
        schemaSignature({
            fields: fields.map((field) =>
                field.type === 'decimal' ? { ...field, scale: 3 } : field,
            ),
        }),
    );
    assert.throws(() =>
        schemaSignature({ fields: [{ name: 'n', type: 'decimal', nullable: false }] }),
    );
    assert.throws(() =>
        schemaSignature({ fields: [{ name: 'n', type: 'date', nullable: false, scale: 0 }] }),
    );
});

test('row editor keeps decimal text inputs and exposes date format', () => {
    const html = writeEditor(schema, [{}], null, 'c');
    assert.match(html, /inputmode="decimal"/);
    assert.match(html, /YYYY-MM-DD/);
});

test('decimal and date creation is gated on native schema capabilities', () => {
    for (const capabilities of [null, { mock: true }, { backend: 'browser-demo' }]) {
        assert.ok(!schemaTypes(capabilities).includes('decimal'));
        assert.ok(!schemaTypes(capabilities).includes('date'));
        assert.throws(() => creationSchema(fields, capabilities), /does not support/);
    }
    assert.deepEqual(
        creationSchema(fields, { schema_types: ['string', 'decimal', 'date'] }),
        schema,
    );
});
