// Validate complete recovery objects while preserving the original wire values.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DIGEST = /^[0-9a-fA-F]{64}$/;
const DECIMAL = /^(0|-[1-9][0-9]*|[1-9][0-9]*)$/;
const PHASES = [
    'binding',
    'catalog_transaction',
    'catalog_commit',
    'recording_failure',
    'reconciling',
];
const HINTS = ['SameIdAfterReconcile', 'NewOperationId', 'FixInput', 'OperatorAction'];

function require(value) {
    if (!value) throw new Error('Invalid or incomplete recovery context');
}
function object(value, fields) {
    require(value && typeof value === 'object' && !Array.isArray(value));
    for (const field of fields.split(' ')) require(Object.hasOwn(value, field));
    return value;
}
function uuid(value) {
    require(typeof value === 'string' && UUID.test(value));
}
function text(value, limit) {
    require(typeof value === 'string' && value.length > 0 && !value.includes('\0'));
    require(new TextEncoder().encode(value).length <= limit);
}
function digest(value) {
    require(typeof value === 'string' && DIGEST.test(value));
}
function snapshot(value) {
    if (value === null) return;
    require(typeof value === 'string' && DECIMAL.test(value));
    const number = BigInt(value);
    require(number >= -(1n << 63n) && number < 1n << 63n);
}
function error(value) {
    object(value, 'code message retry_hint');
    text(value.code, 128);
    require(
        typeof value.message === 'string' && new TextEncoder().encode(value.message).length <= 2048,
    );
    require(HINTS.includes(value.retry_hint));
}

export function validateRecovery(payload, status, path, body) {
    require(status === 503 && payload.recovery_required === true && PHASES.includes(payload.phase));
    require(payload.result == null && payload.info == null);
    error(payload.error);
    const create = /\/collections$/.test(path);
    const recovery = object(
        payload.recovery,
        create
            ? 'store_id collection_id exact_name schema_hash attempt_id candidate_table_uuid candidate_metadata_location publication_started'
            : 'store_id collection_id operation_id payload_hash hash_version base attempt_id candidate_snapshot_id candidate_metadata_location first_error publication_started lookup_blocked',
    );
    uuid(recovery.store_id);
    uuid(recovery.collection_id);
    uuid(recovery.attempt_id);
    require(typeof recovery.publication_started === 'boolean');
    if (recovery.candidate_metadata_location !== null)
        text(recovery.candidate_metadata_location, 2048);
    if (create) {
        require(
            recovery.collection_id === body?.collection_id && recovery.exact_name === body?.name,
        );
        text(recovery.exact_name, 128);
        digest(recovery.schema_hash);
        if (recovery.candidate_table_uuid !== null) uuid(recovery.candidate_table_uuid);
        return;
    }
    const match = path.match(/\/collections\/([^/]+)\/(?:append|operations\/([^/?]+))$/);
    require(match && recovery.collection_id === decodeURIComponent(match[1]));
    const operation = body?.operation_id ?? decodeURIComponent(match[2] ?? '');
    require(recovery.operation_id === operation);
    require(payload.operation_id === operation);
    uuid(recovery.operation_id);
    digest(recovery.payload_hash);
    require(recovery.hash_version === 1);
    const base = object(
        recovery.base,
        'store_id collection_id table_uuid metadata_location current_snapshot_id schema_id spec_id',
    );
    require(base.store_id === recovery.store_id && base.collection_id === recovery.collection_id);
    uuid(base.table_uuid);
    text(base.metadata_location, 2048);
    snapshot(base.current_snapshot_id);
    for (const field of ['schema_id', 'spec_id'])
        require(Number.isInteger(base[field]) && base[field] >= 0 && base[field] < 2 ** 31);
    snapshot(recovery.candidate_snapshot_id);
    error(recovery.first_error);
    require(typeof recovery.lookup_blocked === 'boolean');
    if (recovery.lookup_blocked)
        require(
            !recovery.publication_started &&
                recovery.candidate_snapshot_id === null &&
                recovery.candidate_metadata_location === null,
        );
}
