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
    const match = path.match(
        /\/collections\/([^/]+)\/(?:append|upsert|delete|compact|operations\/([^/?]+))$/,
    );
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
    require([1, 2, 3].includes(result.receipt_version));
    if (result.receipt_version === 3) {
        require(
            payload.phase === 'complete' &&
                (payload.recovery_required === undefined || payload.recovery_required === false) &&
                !Object.hasOwn(payload, 'recovery'),
        );
        require(!path.endsWith('/append'));
        const action = path.split('/').at(-1);
        if (['upsert', 'delete', 'compact'].includes(action))
            require(result.mutation_kind === action);
        validateMutationReceipt(result);
        return;
    }
    require(!['upsert', 'delete', 'compact'].some((action) => path.endsWith('/' + action)));
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

function validateMutationReceipt(result) {
    require(['upsert', 'delete', 'compact'].includes(result.mutation_kind));
    require(typeof result.snapshot_created === 'boolean');
    const values = Object.fromEntries(
        [
            'snapshot_id',
            'rows_appended',
            'snapshot_created_at_us',
            'input_rows',
            'rows_affected',
            'rows_written',
            'rows_removed',
            'data_files_added',
            'data_files_removed',
            'compressed_data_bytes_added',
            'compressed_data_bytes_removed',
        ].map((name) => [name, integer(result[name])]),
    );
    require(Object.hasOwn(result, 'parent_snapshot_id'));
    const parent = result.parent_snapshot_id === null ? null : integer(result.parent_snapshot_id);
    require(
        typeof result.durability_profile === 'string' &&
            result.durability_profile.length > 0 &&
            new TextEncoder().encode(result.durability_profile).length <= 128,
    );
    require(values.rows_appended === 0n);
    for (const name of ['input_rows', 'rows_written', 'rows_removed'])
        require(values[name] >= 0n && values[name] <= 25600000n);
    require(values.rows_affected >= 0n && values.rows_affected <= values.input_rows);
    for (const suffix of ['added', 'removed']) {
        const files = values['data_files_' + suffix];
        require(files >= 0n && files <= 256n);
        require(
            values['compressed_data_bytes_' + suffix] >= 0n &&
                values['compressed_data_bytes_' + suffix] <= files * 33554432n,
        );
    }
    if (result.mutation_kind === 'compact') require(values.input_rows === 0n);
    if (!result.snapshot_created) {
        require(values.snapshot_id === (parent ?? 0n) && values.snapshot_created_at_us === 0n);
        for (const name of [
            'rows_affected',
            'rows_written',
            'rows_removed',
            'data_files_added',
            'data_files_removed',
            'compressed_data_bytes_added',
            'compressed_data_bytes_removed',
        ])
            require(values[name] === 0n);
        return;
    }
    require(values.snapshot_id !== 0n && values.snapshot_id !== parent);
    require(
        values.snapshot_created_at_us >= -62135596800000000n &&
            values.snapshot_created_at_us <= 253402300799999999n &&
            values.snapshot_created_at_us % 1000n === 0n,
    );
    const added = values.data_files_added,
        removed = values.data_files_removed;
    require(added > 0n || removed > 0n);
    for (const [suffix, name] of [
        ['added', 'rows_written'],
        ['removed', 'rows_removed'],
    ]) {
        const files = values['data_files_' + suffix];
        require((files === 0n) === (values[name] === 0n));
        require((files === 0n) === (values['compressed_data_bytes_' + suffix] === 0n));
        require(values[name] <= files * 100000n);
    }
    if (result.mutation_kind === 'compact') {
        require(
            values.input_rows === 0n &&
                values.rows_affected === 0n &&
                values.rows_written === values.rows_removed,
        );
        require(added >= 1n && removed >= added * 4n);
    } else {
        require(values.input_rows > 0n && values.rows_affected > 0n);
        if (result.mutation_kind === 'delete')
            require(values.rows_removed - values.rows_written === values.rows_affected);
        else
            require(
                values.rows_written >= values.rows_removed &&
                    values.rows_written - values.rows_removed <= values.rows_affected,
            );
    }
}
