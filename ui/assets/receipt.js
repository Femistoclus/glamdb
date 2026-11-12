// Read committed ordinary and bulk results without changing their wire values.
const DECIMAL = /^(0|-[1-9][0-9]*|[1-9][0-9]*)$/;
const DIGEST = /^[0-9a-fA-F]{64}$/;

function require(value) {
    if (!value) throw new Error('Invalid or incomplete append receipt');
}

function integer(value, mock = false) {
    require(
        (typeof value === 'string' && DECIMAL.test(value)) || (mock && Number.isSafeInteger(value)),
    );
    const result = BigInt(value);
    require(result >= -(1n << 63n) && result < 1n << 63n);
    return result;
}

export function validateCommittedReceipt(payload, status, path, body) {
    const match = path.match(/\/collections\/([^/]+)\/(?:append|operations\/([^/?]+))$/);
    if (!match) return;
    const result = payload.result;
    require(status === 200 && result && typeof result === 'object' && !Array.isArray(result));
    require(payload.error == null);
    const mock = payload.mock === true || result.mock === true;
    require(result.collection_id === decodeURIComponent(match[1]));
    const operation = body?.operation_id ?? decodeURIComponent(match[2] ?? '');
    require(result.operation_id === operation);
    require(payload.operation_id === undefined || payload.operation_id === operation);
    require(typeof result.payload_hash === 'string' && DIGEST.test(result.payload_hash));
    require(result.hash_version === 1);
    require(result.receipt_version === 1 || result.receipt_version === 2);
    const bulk = result.receipt_version === 2;
    const rows = integer(result.rows_appended, mock);
    require(rows >= 1n && rows <= (bulk ? 25600000n : 10000n));
    const snapshot = integer(result.snapshot_id);
    const timestamp = integer(result.snapshot_created_at_us);
    require(timestamp >= -62135596800000000n && timestamp <= 253402300799999999n);
    if (bulk) require(timestamp % 1000n === 0n);
    if (!mock || bulk)
        for (const field of [
            'data_files_added',
            'compressed_data_bytes_added',
            'parent_snapshot_id',
            'durability_profile',
        ])
            require(Object.hasOwn(result, field));
    let files = 1n;
    if (Object.hasOwn(result, 'data_files_added')) {
        files = integer(result.data_files_added, mock);
        require(bulk ? files >= 1n && files <= 256n && rows <= files * 100000n : files === 1n);
    }
    if (Object.hasOwn(result, 'compressed_data_bytes_added')) {
        const bytes = integer(result.compressed_data_bytes_added, mock);
        require(bytes >= 1n && bytes <= files * 33554432n);
    }
    if (Object.hasOwn(result, 'rows_total')) require(integer(result.rows_total, mock) >= rows);
    if (Object.hasOwn(result, 'parent_snapshot_id') && result.parent_snapshot_id !== null)
        require(integer(result.parent_snapshot_id) !== snapshot);
    if (Object.hasOwn(result, 'durability_profile'))
        require(
            typeof result.durability_profile === 'string' &&
                result.durability_profile.length > 0 &&
                new TextEncoder().encode(result.durability_profile).length <= 128,
        );
}
