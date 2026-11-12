import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { WorkbenchAPI } from '../../assets/api.js';

// PyArrow 25 IPC: non-null int64 n=[4294967295]. Its batch body ends in
// ffffffff00000000, deliberately identical to the modern IPC EOS marker.
const stream = Buffer.from(
    '/////3gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAEAAAAUAAAAEAAUAAgAAAAHAAwAAAAQABAAAAAAAAACEAAAABwAAAAEAAAAAAAAAAEAAABuAAAACAAMAAgABwAIAAAAAAAAAUAAAAD/////iAAAABQAAAAAAAAADAAWAAYABQAIAAwADAAAAAADBAAYAAAACAAAAAAAAAAAAAoAGAAMAAQACAAKAAAAPAAAABAAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAD/////AAAAAP////8AAAAA',
    'base64',
);
const path = '/collections/id/scan';
const invalidSchema = Buffer.from(stream);
// Point Message.header into padding and replace the Schema table's vtable
// distance with an out-of-bounds value. Length/hash alone remain consistent.
invalidSchema.writeUInt32LE(92, 32);
invalidSchema.writeUInt32LE(0x7fffffff, 124);
const unsupportedVersion = Buffer.from(stream);
const message = 8 + stream.readUInt32LE(8);
const vtable = message - stream.readInt32LE(message);
unsupportedVersion.writeInt16LE(32767, message + stream.readUInt16LE(vtable + 4));

function connection({ bytes = stream, headers = {}, body = { snapshot_id: '9' } } = {}) {
    const events = [];
    let response;
    const api = new WorkbenchAPI('http://localhost', 'secret', async (url) => {
        if (url.endsWith('/open')) return new Response(JSON.stringify({ cursor: 'first' }));
        if (url.endsWith('/cancel')) {
            events.push('cancel');
            return new Response('{}');
        }
        const metadata = new Headers({
            'Content-Type': 'application/vnd.apache.arrow.stream',
            'Content-Length': String(bytes.length),
            'X-GlamDB-Snapshot-Id': '9',
            'X-Content-SHA256': createHash('sha256').update(bytes).digest('hex'),
        });
        for (const [name, value] of Object.entries(headers)) {
            if (value === null) metadata.delete(name);
            else metadata.set(name, value);
        }
        response = new Response(
            new ReadableStream({
                start(controller) {
                    controller.enqueue(bytes);
                    controller.close();
                },
                cancel() {
                    events.push('body-cancel');
                },
            }),
            { headers: metadata },
        );
        return response;
    });
    return { api, events, body, response: () => response };
}

test('complete Arrow framing, fixed snapshot, byte count and digest permit download', async () => {
    const { api, events, body } = connection();
    assert.deepEqual(Buffer.from(await api.arrow(path, body)), stream);
    assert.deepEqual(events, ['cancel']);
});

for (const [name, value, message] of [
    ['Content-Type', 'application/json', /Content-Type/],
    ['Content-Encoding', 'gzip', /encoding/],
    ['Content-Length', null, /Content-Length/],
    ['Content-Length', '312x', /Content-Length/],
    ['Content-Length', '1', /interrupted/],
    ['X-Content-SHA256', null, /SHA256/],
    ['X-Content-SHA256', 'invalid', /SHA256/],
    ['X-Content-SHA256', '0'.repeat(64), /checksum/],
    ['X-GlamDB-Snapshot-Id', null, /snapshot/],
    ['X-GlamDB-Snapshot-Id', '09', /snapshot/],
    ['X-GlamDB-Snapshot-Id', '9223372036854775808', /snapshot/],
    ['X-GlamDB-Snapshot-Id', '10', /snapshot/],
    ['X-GlamDB-Snapshot-Id', 'initial', /snapshot/],
]) {
    test(`rejects Arrow ${name}=${value} and releases ownership`, async () => {
        const { api, events, body, response } = connection({ headers: { [name]: value } });
        await assert.rejects(api.arrow(path, body), message);
        assert.ok(events.includes('cancel'));
        assert.ok(response().bodyUsed, 'response was consumed or cancelled');
        assert.equal(response().body.locked, false);
    });
}

for (const [name, bytes] of [
    ['missing EOS despite body resembling EOS', stream.subarray(0, -8)],
    ['truncated metadata', stream.subarray(0, 60)],
    ['trailing bytes', Buffer.concat([stream, Buffer.from([0])])],
    ['EOS without schema', stream.subarray(-8)],
    ['batch without schema', stream.subarray(128)],
    ['arbitrary bytes', Buffer.from('not Arrow')],
    ['invalid schema table', invalidSchema],
    ['unsupported metadata version', unsupportedVersion],
]) {
    test(`rejects ${name} even with matching length and checksum`, async () => {
        const { api, events, body } = connection({ bytes });
        await assert.rejects(api.arrow(path, body), /Arrow IPC/);
        assert.ok(events.includes('cancel'));
    });
}

for (const snapshot of ['initial', null]) {
    test(`initial view download accepts selector ${snapshot}`, async () => {
        const { api, body } = connection({
            headers: { 'X-GlamDB-Snapshot-Id': 'initial' },
            body: { snapshot_id: snapshot },
        });
        assert.deepEqual(Buffer.from(await api.arrow(path, body)), stream);
    });
}
