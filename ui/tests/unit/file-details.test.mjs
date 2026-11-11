import test from 'node:test';
import assert from 'node:assert/strict';
import * as view from '../../assets/file-details.js';

test('file details expose exact typed footer bounds and escaped values before collapsed JSON', () => {
    assert.equal(typeof view.fileDetails, 'function');
    const result = view.fileDetails(
        {
            file_id: 'sha256-<untrusted>',
            record_count: '3',
            compressed_bytes: '9007199254740993',
            footer_bytes: '450',
            synthetic: false,
            row_groups: [{ ordinal: 0, record_count: '3', compressed_bytes: '120' }],
            footer_statistics: [
                {
                    field_id: 2,
                    name: 'n<img src=x>',
                    type: 'int64',
                    null_count: null,
                    lower_bound: '9007199254740993',
                    upper_bound: '9223372036854775807',
                    lower_bound_exact: true,
                    upper_bound_exact: null,
                },
            ],
        },
        '9007199254740995',
    );
    assert.match(result, /aria-label="File summary"/);
    assert.match(result, /9,007,199,254,740,993/);
    assert.match(result, />9007199254740993</);
    assert.match(result, />9223372036854775807</);
    assert.match(result, /n&lt;img src=x&gt;/);
    assert.ok(!result.includes('<img src=x>'));
    assert.match(result, /sha256-&lt;untrusted&gt;/);
    assert.match(result, /aria-label="Parquet row groups"/);
    assert.match(result, /aria-label="Parquet field statistics"/);
    assert.match(result, /<details class="raw-metadata">/);
    assert.ok(!result.includes('<details class="raw-metadata" open'));
    assert.ok(result.indexOf('Parquet field statistics') < result.indexOf('raw-metadata'));
});

test('synthetic and missing footer metrics remain explicitly unavailable', () => {
    assert.equal(typeof view.fileDetails, 'function');
    const result = view.fileDetails(
        {
            file_id: 'mock-file',
            synthetic: true,
            record_count: '0',
            compressed_bytes: null,
            footer_bytes: null,
        },
        'initial',
    );
    assert.match(result, /Synthetic mock descriptor/);
    assert.match(result, /no physical file/i);
    assert.match(result, /Unknown/);
    assert.match(result, /Initial empty view/);
    assert.match(result, /Field statistics are unavailable/);
    assert.ok(!result.includes('Verified Parquet footer'));
});

test('missing aggregate statistics expose each observed row group without combining bounds', () => {
    const result = view.fileDetails(
        {
            file_id: 'f'.repeat(64),
            path: null,
            record_count: '4',
            physical_file_exists: true,
            footer_statistics: null,
            row_groups: [
                {
                    ordinal: 0,
                    record_count: '2',
                    field_metrics: [
                        {
                            field_id: 1,
                            name: 'n',
                            type: 'int64',
                            null_count: null,
                            lower_bound: '9007199254740993',
                            upper_bound: '9007199254740995',
                            lower_bound_exact: null,
                            upper_bound_exact: null,
                        },
                    ],
                },
                {
                    ordinal: 1,
                    record_count: '2',
                    field_metrics: [
                        {
                            field_id: 1,
                            name: 'n',
                            type: 'int64',
                            null_count: '0',
                            lower_bound: '-9',
                            upper_bound: '-1',
                            lower_bound_exact: null,
                            upper_bound_exact: null,
                        },
                    ],
                },
            ],
        },
        '9223372036854775807',
    );
    const table = result.match(
        /<table[^>]*aria-label="Parquet field statistics">([\s\S]*?)<\/table>/,
    )?.[1];
    assert.ok(table, 'available per-group metrics must be visible');
    assert.match(table, /<caption>Observed per-row-group statistics<\/caption>/);
    assert.match(table, /<th>Group<\/th>/);
    const rows = [...table.matchAll(/<tr>([\s\S]*?)<\/tr>/g)].slice(1).map((match) => match[1]);
    assert.equal(rows.length, 2);
    assert.match(rows[0], /^<td>0<\/td><td>1<\/td>/);
    assert.match(rows[0], />9007199254740993</);
    assert.match(rows[0], />9007199254740995</);
    assert.match(rows[1], /^<td>1<\/td><td>1<\/td>/);
    assert.match(rows[1], />-9</);
    assert.match(rows[1], />-1</);
    assert.equal((table.match(/Precision unknown/g) ?? []).length, 4);
    assert.doesNotMatch(table, /Inexact|>Exact</);
    assert.match(result, /<details class="raw-metadata">/);
});
