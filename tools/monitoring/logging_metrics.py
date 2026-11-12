"""Thin adapter for actual Python logger accounting, using client metric families."""

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

COUNTERS = ("submitted", "accepted", "sampled", "rate_limited", "dropped_full",
            "dropped_contended", "dropped_stopped", "sink_errors", "adapter_errors")


class LoggingMetrics:
    """Register once with an application's CollectorRegistry.

    snapshot is the logger's snapshot callable. Counts not yet observed are
    zero; unsupported delivery/backlog accounting is never emitted.
    """
    def __init__(self, snapshot):
        self._snapshot = snapshot

    def describe(self):
        for name in COUNTERS:
            yield CounterMetricFamily("glamdb_python_log_" + name, "Python logging adapter accounting.")
        yield GaugeMetricFamily("glamdb_python_log_queue_capacity", "Bounded stdlib log queue records.")

    def collect(self):
        stats = self._snapshot()
        for name in COUNTERS:
            yield CounterMetricFamily("glamdb_python_log_" + name, "Python logging adapter accounting.",
                                      value=stats.get(name, 0))
        if "queue_capacity" in stats:
            yield GaugeMetricFamily("glamdb_python_log_queue_capacity", "Bounded stdlib log queue records.",
                                    value=stats["queue_capacity"])
