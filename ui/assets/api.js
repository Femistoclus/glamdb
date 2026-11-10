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
        this.base = url.href.replace(/\/$/, '');
        this.token = token;
        this.fetch = fetchImpl;
        this.maxBytes = maxBytes;
    }
    async request(path, { method = 'GET', body, signal, headers = {} } = {}) {
        const options = {
            method,
            signal,
            headers: {
                Accept: 'application/json',
                Authorization: `Bearer ${this.token}`,
                ...headers,
            },
            cache: 'no-store',
        };
        if (body !== undefined) {
            options.body = JSON.stringify(body);
            options.headers['Content-Type'] = 'application/json';
        }
        return this.fetch(`${this.base}${path}`, options);
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
        const chunks = [];
        let length = 0;
        try {
            for (;;) {
                const { done, value } = await reader.read();
                if (done) break;
                length += value.byteLength;
                if (length > this.maxBytes) {
                    await reader.cancel();
                    throw new Error('Response is too large');
                }
                chunks.push(value);
            }
        } finally {
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
        let payload;
        try {
            payload = JSON.parse(new TextDecoder().decode(bytes));
        } catch {
            throw new Error(`Server returned invalid JSON (${response.status})`);
        }
        if (
            !response.ok &&
            !(
                options.allowOutcome &&
                ['commit_unknown', 'definitely_not_committed'].includes(payload?.outcome)
            )
        )
            throw new APIError(response.status, payload);
        return payload;
    }
    async arrow(path, body, { signal } = {}) {
        const response = await this.request(path, {
            method: 'POST',
            body,
            signal,
            headers: { Accept: 'application/vnd.apache.arrow.stream' },
        });
        if (!response.ok) throw new APIError(response.status, await response.json());
        const bytes = await this.readBytes(response);
        const expected = response.headers.get('X-Content-SHA256');
        if (expected) {
            const digest = await crypto.subtle.digest('SHA-256', bytes);
            const actual = Array.from(new Uint8Array(digest), (byte) =>
                byte.toString(16).padStart(2, '0'),
            ).join('');
            if (actual !== expected.toLowerCase())
                throw new Error('Arrow download checksum mismatch');
        }
        return bytes;
    }
}
