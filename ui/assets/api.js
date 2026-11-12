import { validateRecovery } from './recovery.js';
import { validateArrowFraming, validateArrowHeaders } from './arrow.js';

export function csrfToken(cookie = globalThis.document?.cookie ?? '') {
    const values = cookie
        .split(';')
        .map((part) => part.trim())
        .filter((part) => part.startsWith('__Host-glamdb-csrf='));
    if (values.length !== 1)
        throw new Error('Reload your access session before submitting this request');
    let token;
    try {
        token = decodeURIComponent(values[0].slice('__Host-glamdb-csrf='.length));
    } catch {
        throw new Error('The access session CSRF cookie is invalid');
    }
    if (!token || token.length > 4096 || /[\x00-\x20\x7f]/.test(token))
        throw new Error('The access session CSRF cookie is invalid');
    return token;
}

export class APIError extends Error {
    constructor(status, payload) {
        const detail = payload?.error ?? payload;
        super(typeof detail?.message === 'string' ? detail.message : `Request failed (${status})`);
        this.name = 'APIError';
        this.status = status;
        this.code = detail?.code ?? 'HTTP_ERROR';
        this.retryHint = detail?.retry_hint;
        this.outcome = payload?.outcome;
    }
}
export class WorkbenchAPI {
    constructor(
        baseUrl,
        token,
        fetchImpl = globalThis.fetch.bind(globalThis),
        maxBytes = 16 * 1024 * 1024,
        { gateway = false } = {},
    ) {
        const url = new URL(baseUrl);
        const local = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
        if (
            url.username ||
            url.password ||
            url.search ||
            url.hash ||
            !['http:', 'https:'].includes(url.protocol) ||
            (url.protocol === 'http:' && !local)
        )
            throw new Error('Use HTTPS, or HTTP on localhost, without credentials in the URL');
        if (gateway && globalThis.location?.origin && url.origin !== globalThis.location.origin)
            throw new Error('Gateway requests must remain same-origin');
        this.base = url.href.replace(/\/$/, '');
        this.gateway = gateway;
        this.token = gateway ? '' : token;
        this.fetch = fetchImpl;
        this.maxBytes = maxBytes;
        this.controller = new AbortController();
    }
    close() {
        this.controller.abort();
        this.token = '';
    }
    async selectDatabase(id, signal) {
        const path = `/databases/${encodeURIComponent(id)}`;
        const database = await this.json(path, { signal });
        if (!['open', 'closed'].includes(database.state))
            throw new Error(
                `Database is ${database.state}; refresh or reconcile before selecting it`,
            );
        return database.state === 'closed'
            ? this.json(`${path}/open`, { method: 'POST', body: {}, signal })
            : database;
    }
    async request(path, { method = 'GET', body, signal, headers = {} } = {}) {
        signal = signal
            ? AbortSignal.any([signal, this.controller.signal])
            : this.controller.signal;
        signal.throwIfAborted();
        const options = {
            method,
            signal,
            headers: {
                Accept: 'application/json',
                ...(this.gateway ? {} : { Authorization: `Bearer ${this.token}` }),
                ...headers,
            },
            cache: 'no-store',
        };
        if (this.gateway) {
            options.credentials = 'same-origin';
            options.redirect = 'error';
            for (const name of Object.keys(options.headers)) {
                if (['authorization', 'x-csrftoken'].includes(name.toLowerCase()))
                    delete options.headers[name];
            }
            if (!['GET', 'HEAD', 'OPTIONS'].includes(method.toUpperCase()))
                options.headers['X-CSRFToken'] = csrfToken();
        }
        if (body !== undefined) {
            options.body = JSON.stringify(body);
            options.headers['Content-Type'] = 'application/json';
        }
        const response = await this.fetch(`${this.base}${path}`, options);
        if (signal.aborted) {
            await response.body?.cancel();
            signal.throwIfAborted();
        }
        return response;
    }
    async readBytes(response) {
        // Fetch decodes the body but keeps the encoded Content-Length header.
        const encoding = response.headers.get('Content-Encoding')?.trim().toLowerCase();
        const declared =
            !encoding || encoding === 'identity' ? response.headers.get('Content-Length') : null;
        if (declared && Number(declared) > this.maxBytes) {
            await response.body?.cancel();
            throw new Error('Response is too large');
        }
        if (!response.body) return new Uint8Array();
        const reader = response.body.getReader();
        const cancel = () => reader.cancel().catch(() => {});
        this.controller.signal.addEventListener('abort', cancel, { once: true });
        const chunks = [];
        let length = 0;
        try {
            for (;;) {
                this.controller.signal.throwIfAborted();
                const { done, value } = await reader.read();
                this.controller.signal.throwIfAborted();
                if (done) break;
                length += value.byteLength;
                if (length > this.maxBytes) {
                    await reader.cancel();
                    throw new Error('Response is too large');
                }
                chunks.push(value);
            }
        } finally {
            this.controller.signal.removeEventListener('abort', cancel);
            reader.releaseLock();
        }
        if (declared && Number(declared) !== length) throw new Error('Response was interrupted');
        const bytes = new Uint8Array(length);
        let offset = 0;
        for (const chunk of chunks) {
            bytes.set(chunk, offset);
            offset += chunk.byteLength;
        }
        return bytes;
    }
    async json(path, options = {}) {
        const response = await this.request(path, options);
        const bytes = await this.readBytes(response);
        this.controller.signal.throwIfAborted();
        let payload;
        try {
            payload = JSON.parse(new TextDecoder().decode(bytes));
        } catch {
            throw new Error(`Server returned invalid JSON (${response.status})`);
        }
        if (payload?.outcome === 'commit_unknown')
            validateRecovery(payload, response.status, path, options.body);
        if (
            !response.ok &&
            !(
                options.allowOutcome &&
                ['commit_unknown', 'definitely_not_committed'].includes(payload?.outcome)
            )
        )
            throw new APIError(response.status, payload);
        if (path === '/capabilities')
            this.scanPagination = payload.features?.scan_pagination ?? payload.scan_pagination;
        return payload;
    }
    async cancelScan(path, cursor) {
        try {
            await this.json(`${path}/cancel`, {
                method: 'POST',
                body: { cursor },
                signal: AbortSignal.timeout(5000),
            });
        } catch {
            /* Server expiry releases cursors if cleanup cannot reach it. */
        }
    }
    async openScan(path, body, signal, accept = 'application/json') {
        if (this.scanPagination === false) return null;
        try {
            const opened = await this.json(`${path}/open`, {
                method: 'POST',
                body,
                signal,
                headers: { Accept: accept },
            });
            if (typeof opened.cursor !== 'string' || !opened.cursor)
                throw new Error('Server did not return a scan cursor');
            return opened.cursor;
        } catch (error) {
            // Only the legacy mock's missing route permits the single-response
            // protocol; database/collection errors and lost responses do not.
            if (error.status === 404 && error.code === 'ROUTE_NOT_FOUND') return null;
            throw error;
        }
    }
    async scanPreview(path, options) {
        const cursor = await this.openScan(path, options.body, options.signal);
        let cleanup = cursor;
        try {
            const result = await this.json(path, {
                ...options,
                body: cursor ? { cursor } : options.body,
            });
            cleanup = cursor ?? result.next_cursor;
            return result;
        } finally {
            if (cleanup) await this.cancelScan(path, cleanup);
        }
    }
    async countRows(path, body, { signal } = {}) {
        let cursor = await this.openScan(path, body, signal);
        let cleanup = cursor,
            snapshot,
            count = 0n;
        const seen = new Set();
        try {
            do {
                signal?.throwIfAborted();
                if (cursor && seen.has(cursor)) throw new Error('Scan cursor repeated');
                if (seen.size >= 100000)
                    throw new Error('Count is too large for this browser session');
                if (cursor) seen.add(cursor);
                const page = await this.json(path, {
                    method: 'POST',
                    body: cursor ? { cursor } : body,
                    signal,
                });
                if (!Array.isArray(page.rows)) throw new Error('Invalid scan rows');
                if (snapshot !== undefined && page.snapshot_id !== snapshot)
                    throw new Error('Scan view changed while counting');
                snapshot = page.snapshot_id;
                if (snapshot === undefined) throw new Error('Missing scan identity');
                count += BigInt(page.rows.length);
                cleanup = cursor ?? page.next_cursor ?? cleanup;
                cursor = page.next_cursor;
                if (cursor != null && (typeof cursor !== 'string' || !cursor))
                    throw new Error('Invalid scan cursor');
            } while (cursor != null);
            return { rows: count.toString(), snapshot_id: snapshot };
        } finally {
            if (cleanup) await this.cancelScan(path, cleanup);
        }
    }
    async arrow(path, body, { signal } = {}) {
        const accept = 'application/vnd.apache.arrow.stream';
        const cursor = await this.openScan(path, body, signal, accept);
        let cleanup = cursor;
        let response;
        try {
            response = await this.request(path, {
                method: 'POST',
                body: cursor ? { cursor } : body,
                signal,
                headers: { Accept: accept },
            });
            if (!response.ok) throw new APIError(response.status, await response.json());
            const next = response.headers.get('X-GlamDB-Next-Cursor');
            cleanup = cursor ?? next;
            if (next) {
                await response.body?.cancel();
                throw new Error(
                    'This export requires multiple pages. Reduce the preview limit or use the Python SDK for a full Arrow scan.',
                );
            }
            const expected = validateArrowHeaders(response.headers, body?.snapshot_id);
            const bytes = await this.readBytes(response);
            const digest = await crypto.subtle.digest('SHA-256', bytes);
            const actual = Array.from(new Uint8Array(digest), (byte) =>
                byte.toString(16).padStart(2, '0'),
            ).join('');
            if (actual !== expected) throw new Error('Arrow download checksum mismatch');
            validateArrowFraming(bytes);
            this.controller.signal.throwIfAborted();
            return bytes;
        } finally {
            try {
                await response?.body?.cancel();
            } catch {
                /* Preserve the original failure while still cancelling the scan. */
            }
            if (cleanup) await this.cancelScan(path, cleanup);
        }
    }
}

// A root's current head can differ from the snapshot selected by this response.
export function validateViewMetadata(payload, { collectionId, snapshotId, mock = false }) {
    if (!mock && payload?.mock === true)
        throw new Error('Mock metadata returned by a native backend');
    if (!payload?.identity || payload.identity.collection_id !== collectionId)
        throw new Error('Metadata returned a different collection');
    if (snapshotId !== undefined) {
        const selected =
            mock && Object.hasOwn(payload.identity, 'snapshot_id')
                ? payload.identity.snapshot_id
                : Object.hasOwn(payload, 'snapshot_id')
                  ? payload.snapshot_id
                  : payload.identity.snapshot_id;
        if (selected === undefined || (selected ?? 'initial') !== snapshotId)
            throw new Error('Metadata returned a different snapshot');
    }
    return payload;
}
export async function readMetadataPages(api, path, { signal, view } = {}) {
    const items = [],
        seen = new Set();
    let cursor = null,
        identity;
    for (let pages = 0; pages < 100; pages++) {
        signal?.throwIfAborted();
        const separator = path.includes('?') ? '&' : '?';
        const page = await api.json(
            `${path}${separator}page_size=100${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`,
            { signal },
        );
        if (view) validateViewMetadata(page, view);
        if (!Array.isArray(page.items)) throw new Error('Invalid metadata listing');
        const currentIdentity = JSON.stringify(page.identity ?? null);
        if (identity !== undefined && identity !== currentIdentity)
            throw new Error('Metadata identity changed between pages');
        identity = currentIdentity;
        items.push(...page.items);
        cursor = page.next_cursor;
        if (cursor == null) return items;
        if (typeof cursor !== 'string' || !cursor) throw new Error('Invalid metadata cursor');
        if (seen.has(cursor)) throw new Error('Metadata cursor repeated');
        seen.add(cursor);
    }
    throw new Error('Metadata listing is too large for this workbench');
}
