"""Finite application instrumentation; prometheus_client owns collection/exposition.

Mount metrics_app() on the application's private listener. Readiness is set only
by the owner of the actual admission/backend state; scrape success is separate.
"""

import math

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, make_asgi_app

ROUTES = ("session", "csrf", "logout", "capabilities", "health", "collections", "collection",
          "snapshots", "snapshot", "files", "file", "statistics", "scan", "append", "operation",
          "admin", "other")
METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "OTHER")
STATUS_CLASSES = ("1xx", "2xx", "3xx", "4xx", "5xx")
STATUS_CODES = (200, 201, 202, 204, 301, 302, 304, 307, 308, 400, 401, 403, 404, 405, 409,
                413, 415, 422, 423, 429, 499, 500, 501, 502, 503, 504)
STAGES = ("jwt_validate", "introspect", "pdp", "proxy", "catalog")
BUCKETS = (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 60)


def _route(route):
    return route if route in ROUTES else "other"


def _seconds(seconds):
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("duration must be finite nonnegative seconds")
    return seconds


class ApplicationMetrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self._requests = Counter("glamdb_gateway_http_requests_total", "Completed gateway requests.",
                                 ("route", "method", "status_class"), registry=self.registry)
        self._responses = Counter("glamdb_gateway_http_responses_total", "Gateway responses by finite status code.",
                                  ("status_code",), registry=self.registry)
        self._duration = Histogram("glamdb_gateway_http_request_duration_seconds",
                                   "Gateway duration through response body completion; includes proxy streaming.",
                                   ("route",), buckets=BUCKETS, registry=self.registry)
        self._inflight = Gauge("glamdb_gateway_http_in_flight", "Admitted gateway calls and streams.",
                               ("route",), registry=self.registry)
        self._stages = Histogram("glamdb_gateway_stage_duration_seconds", "Measured gateway regions.",
                                 ("stage",), buckets=BUCKETS, registry=self.registry)
        self._ready = Gauge("glamdb_gateway_ready", "Authoritative gateway readiness; default closed.",
                            registry=self.registry)
        for route in ROUTES:
            self._duration.labels(route)
            self._inflight.labels(route)
            for method in METHODS:
                for status_class in STATUS_CLASSES:
                    self._requests.labels(route, method, status_class)
        for status in (*STATUS_CODES, "other"):
            self._responses.labels(str(status))
        for stage in STAGES:
            self._stages.labels(stage)
        self._ready.set(0)
        self._app = make_asgi_app(registry=self.registry)

    def observe_http(self, route, method, status, seconds):
        if not isinstance(status, int) or not 100 <= status <= 599:
            raise ValueError("HTTP status must be 100..599")
        seconds = _seconds(seconds)
        route = _route(route)
        method = method if method in METHODS else "OTHER"
        self._requests.labels(route, method, str(status // 100) + "xx").inc()
        self._responses.labels(str(status) if status in STATUS_CODES else "other").inc()
        self._duration.labels(route).observe(seconds)

    def in_flight(self, route, delta):
        self._inflight.labels(_route(route)).inc(delta)

    def observe_stage(self, stage, seconds):
        if stage not in STAGES:
            raise ValueError("unknown instrumentation stage")
        self._stages.labels(stage).observe(_seconds(seconds))

    def set_ready(self, ready):
        self._ready.set(bool(ready))

    def metrics_app(self):
        return self._app
