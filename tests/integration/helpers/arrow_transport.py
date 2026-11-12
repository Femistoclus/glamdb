"""Track real HTTP Arrow owners; corrupt selected real responses for fault tests."""

import hashlib
import json

import httpx


class TrackedArrowBody(httpx.SyncByteStream):
    def __init__(self, source, *, fail_close=False):
        self.source = source
        self.fail_close = fail_close
        self.closed = False
        self.bytes_yielded = 0

    def __iter__(self):
        for chunk in self.source:
            self.bytes_yielded += len(chunk)
            yield chunk

    def close(self):
        try:
            self.source.close()
        finally:
            self.closed = True
        if self.fail_close:
            raise RuntimeError('transport close failed')


class ArrowHTTPTransport(httpx.BaseTransport):
    def __init__(self):
        self.http = httpx.HTTPTransport()
        self.fault = None
        self.bodies = []
        self.cancellations = []

    def handle_request(self, request):
        response = self.http.handle_request(request)
        if request.url.path.endswith('/scan/cancel'):
            self.cancellations.append((json.loads(request.content)['cursor'], response.status_code))
        if response.headers.get('content-type') != 'application/vnd.apache.arrow.stream':
            return response
        headers = dict(response.headers)
        source = response.stream
        if self.fault in ('missing_eos', 'truncated_body', 'trailing_data', 'schema'):
            # The bytes originate from the selected real HTTP backend. Buffering
            # here is deliberate fault injection, never the SDK's read path.
            raw = response.read()
            raw = (raw[:-8] if self.fault == 'missing_eos' else
                   raw[:-13] if self.fault == 'truncated_body' else
                   raw + b'trailing' if self.fault == 'trailing_data' else raw[:32])
            headers['content-length'] = str(len(raw))
            headers['x-content-sha256'] = hashlib.sha256(raw).hexdigest()
            source = httpx.ByteStream(raw)
        elif self.fault == 'checksum':
            headers['x-content-sha256'] = '0' * 64
        elif self.fault == 'length':
            headers['content-length'] = str(int(headers['content-length']) + 1)
        elif self.fault in ('snapshot', 'snapshot_close'):
            headers['x-glamdb-snapshot-id'] = 'initial'
        elif self.fault == 'missing_snapshot':
            del headers['x-glamdb-snapshot-id']
        body = TrackedArrowBody(source, fail_close=self.fault in ('close', 'snapshot_close'))
        self.bodies.append(body)
        return httpx.Response(response.status_code, headers=headers, stream=body)

    def close(self):
        self.http.close()
