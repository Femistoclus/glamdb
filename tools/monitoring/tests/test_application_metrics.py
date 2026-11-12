import asyncio
from concurrent.futures import ThreadPoolExecutor
import unittest

from tools.monitoring.application_metrics import ApplicationMetrics
from urllib.request import urlopen

from prometheus_client import generate_latest, start_http_server
from prometheus_client.parser import text_string_to_metric_families


def samples(metrics):
    return { (sample.name, tuple(sorted(sample.labels.items()))): sample.value
             for family in text_string_to_metric_families(generate_latest(metrics.registry).decode())
             for sample in family.samples if not sample.name.endswith("_created") }


class ApplicationMetricsTests(unittest.TestCase):
    def test_fixed_domains_and_concurrent_counts(self):
        metrics = ApplicationMetrics()
        baseline = samples(metrics)
        with ThreadPoolExecutor(max_workers=8) as workers:
            list(workers.map(lambda _: metrics.observe_http('scan', 'POST', 200, 0.01), range(8000)))
        metrics.observe_http('secret/user-id', 'SENSITIVE', 500, 0.25)
        after = samples(metrics)
        labels = tuple(sorted({'route': 'scan', 'method': 'POST', 'status_class': '2xx'}.items()))
        self.assertEqual(after['glamdb_gateway_http_requests_total', labels], 8000)
        self.assertEqual(after['glamdb_gateway_http_responses_total', (('status_code', '200'),)], 8000)
        duration_labels = (('route', 'scan'),)
        self.assertEqual(after['glamdb_gateway_http_request_duration_seconds_count', duration_labels], 8000)
        self.assertAlmostEqual(after['glamdb_gateway_http_request_duration_seconds_sum', duration_labels], 80)
        self.assertEqual(after['glamdb_gateway_http_request_duration_seconds_bucket', tuple(sorted(dict(duration_labels, le='0.01').items()))], 8000)
        self.assertEqual(len(after), len(baseline))
        self.assertNotIn('secret', generate_latest(metrics.registry).decode())
        self.assertEqual(samples(ApplicationMetrics()), baseline)

    def test_readiness_and_inflight(self):
        metrics = ApplicationMetrics()
        self.assertEqual(samples(metrics)['glamdb_gateway_ready', ()], 0)
        metrics.set_ready(True)
        metrics.in_flight('scan', 1)
        metrics.in_flight('scan', -1)
        metrics.observe_stage('pdp', 0.5)
        data = samples(metrics)
        self.assertEqual(data['glamdb_gateway_ready', ()], 1)
        self.assertEqual(data['glamdb_gateway_http_in_flight', (('route', 'scan'),)], 0)
        self.assertEqual(data['glamdb_gateway_stage_duration_seconds_sum', (('stage', 'pdp'),)], 0.5)

    def test_library_http_endpoint_and_teardown(self):
        metrics = ApplicationMetrics()
        server, thread = start_http_server(0, addr="127.0.0.1", registry=metrics.registry)
        url = "http://127.0.0.1:" + str(server.server_port) + "/metrics"
        try:
            with urlopen(url, timeout=2) as response:
                self.assertEqual(response.status, 200)
                self.assertIn(b"glamdb_gateway_ready 0.0", response.read())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        with self.assertRaises(OSError):
            urlopen(url, timeout=1)

    def test_real_library_asgi_endpoint(self):
        metrics = ApplicationMetrics()
        messages = []
        async def send(message):
            messages.append(message)
        async def receive():
            return {'type': 'http.request', 'body': b''}
        asyncio.run(metrics.metrics_app()({'type': 'http', 'method': 'GET', 'path': '/metrics', 'headers': [], 'query_string': b''}, receive, send))
        self.assertEqual(messages[0]['status'], 200)
        self.assertIn(b'glamdb_gateway_ready 0.0', messages[-1]['body'])


if __name__ == '__main__':
    unittest.main()
