import test from 'node:test';
import assert from 'node:assert/strict';
import { schemaSignature, canonicalWriteSettings, wireRows } from '../../assets/model.js';

const fields = (width) =>
    Array.from({ length: width }, (_, index) => ({
        name: index === width - 1 ? 'CLID' : `c${index}`,
        type: 'int64',
        nullable: false,
        field_id: index + 1,
    }));

for (const width of [65, 105, 128]) {
    test(`wide ${width}-column recovery schema preserves exact rows and settings`, () => {
        const schema = { fields: fields(width) };
        assert.equal(JSON.parse(schemaSignature(schema)).length, width);
        const row = Object.fromEntries(
            schema.fields.map((field) => [field.name, '9007199254740993']),
        );
        assert.deepEqual(wireRows(schema, [row, row]), [row, row]);
        const columns = schema.fields.toReversed().map((field) => ({
            field: field.name,
            compression: { codec: 'snappy' },
        }));
        const settings = canonicalWriteSettings({ parquet: { columns } }, schema);
        assert.deepEqual(
            settings.parquet.columns.map((column) => column.field),
            schema.fields.map((field) => field.name),
        );
    });
}

test('recovery schemas and column settings retain the 128-field upper bound', () => {
    assert.throws(() => schemaSignature({ fields: fields(129) }));
    const columns = fields(129).map((field) => ({
        field: field.name,
        compression: { codec: 'snappy' },
    }));
    assert.throws(() => canonicalWriteSettings({ parquet: { columns } }));
});
