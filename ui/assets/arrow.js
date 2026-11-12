// Check the modern IPC envelope produced by the native encoder and PyArrow 25.
// This walks Message metadata/body lengths; payload bytes resembling EOS do not
// terminate the stream. Logical types and values remain the encoder's contract.
export function validateArrowFraming(bytes) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const invalid = () => {
        throw new Error('Invalid or incomplete Arrow IPC stream');
    };
    let offset = 0,
        schemaSeen = false;
    while (offset + 8 <= bytes.length) {
        if (view.getUint32(offset, true) !== 0xffffffff) invalid();
        const length = view.getInt32(offset + 4, true);
        offset += 8;
        if (length === 0) {
            if (!schemaSeen || offset !== bytes.length) invalid();
            return;
        }
        if (length < 8 || length % 8 !== 0 || length > bytes.length - offset) invalid();
        const metadata = new DataView(bytes.buffer, bytes.byteOffset + offset, length);
        const contains = (start, size) => start >= 0 && start <= length - size;
        const tableFields = (table) => {
            if (table < 4 || !contains(table, 4)) invalid();
            const distance = metadata.getInt32(table, true);
            const vtable = table - distance;
            if (!distance || !contains(vtable, 4)) invalid();
            const vtableSize = metadata.getUint16(vtable, true);
            const objectSize = metadata.getUint16(vtable + 2, true);
            if (
                vtableSize < 4 ||
                vtableSize % 2 !== 0 ||
                objectSize < 4 ||
                !contains(vtable, vtableSize) ||
                !contains(table, objectSize)
            )
                invalid();
            return (index, size) => {
                const entry = 4 + index * 2;
                if (entry + 2 > vtableSize) return null;
                const relative = metadata.getUint16(vtable + entry, true);
                if (!relative) return null;
                if (
                    relative < 4 ||
                    relative > objectSize - size ||
                    !contains(table + relative, size)
                )
                    invalid();
                return table + relative;
            };
        };
        // Arrow format/Message.fbs: header_type=1, header=2, bodyLength=3.
        const field = tableFields(metadata.getUint32(0, true));
        const versionAt = field(0, 2),
            typeAt = field(1, 1),
            headerAt = field(2, 4),
            bodyAt = field(3, 8);
        if (versionAt === null || typeAt === null || headerAt === null) invalid();
        const version = metadata.getInt16(versionAt, true);
        if (version !== 3 && version !== 4) invalid(); // Arrow IPC V4/V5.
        const type = metadata.getUint8(typeAt),
            header = headerAt + metadata.getUint32(headerAt, true),
            bodyLength = bodyAt === null ? 0n : metadata.getBigInt64(bodyAt, true);
        if (!contains(header, 4) || header <= headerAt) invalid();
        tableFields(header);
        if (!schemaSeen ? type !== 1 : type !== 2 && type !== 3) invalid();
        if (type === 1 && bodyLength !== 0n) invalid();
        offset += length;
        if (bodyLength < 0n || bodyLength % 8n !== 0n || bodyLength > BigInt(bytes.length - offset))
            invalid();
        offset += Number(bodyLength);
        schemaSeen = true;
    }
    invalid();
}

export function validateArrowHeaders(headers, requestedSnapshot) {
    const selected = requestedSnapshot === null ? 'initial' : requestedSnapshot;
    if (
        headers.get('Content-Type')?.split(';', 1)[0].trim().toLowerCase() !==
        'application/vnd.apache.arrow.stream'
    )
        throw new Error('Expected Arrow Content-Type');
    if ((headers.get('Content-Encoding') ?? 'identity').trim().toLowerCase() !== 'identity')
        throw new Error('Arrow download requires identity content encoding');
    const snapshot = headers.get('X-GlamDB-Snapshot-Id');
    if (snapshot !== 'initial') {
        if (!/^(0|-?[1-9][0-9]*)$/.test(snapshot ?? '') || snapshot.length > 20)
            throw new Error('Arrow download requires a fixed snapshot');
        const id = BigInt(snapshot);
        if (id < -(2n ** 63n) || id >= 2n ** 63n) throw new Error('Invalid Arrow snapshot');
    }
    if (selected !== undefined && selected !== 'latest' && snapshot !== selected)
        throw new Error('Arrow download returned a different snapshot');
    const length = headers.get('Content-Length');
    if (!/^(0|[1-9][0-9]*)$/.test(length ?? '') || !Number.isSafeInteger(Number(length)))
        throw new Error('Arrow download requires a valid Content-Length');
    const checksum = headers.get('X-Content-SHA256');
    if (!/^[0-9a-f]{64}$/i.test(checksum ?? ''))
        throw new Error('Arrow download requires a valid X-Content-SHA256');
    return checksum.toLowerCase();
}
