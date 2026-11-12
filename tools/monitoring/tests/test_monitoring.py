import json
from pathlib import Path
import unittest

from prometheus_client import CollectorRegistry, generate_latest
from tools.monitoring.logging_metrics import LoggingMetrics

ROOT = Path(__file__).resolve().parents[3]


class CollectorContracts(unittest.TestCase):
    def test_actual_logging_snapshot_uses_library_metrics(self):
        snapshot = {"submitted": 4, "accepted": 3, "dropped_full": 1, "queue_capacity": 256}
        registry = CollectorRegistry()
        registry.register(LoggingMetrics(lambda: snapshot.copy()))
        first = generate_latest(registry).decode()
        self.assertIn("glamdb_python_log_accepted_total 3.0", first)
        self.assertIn("glamdb_python_log_dropped_full_total 1.0", first)
        self.assertIn("glamdb_python_log_queue_capacity 256.0", first)
        for unsupported in ("written", "backlog", "dropped_sink"):
            self.assertNotIn("glamdb_python_log_" + unsupported, first)
        snapshot["accepted"] = 5
        self.assertIn("glamdb_python_log_accepted_total 5.0", generate_latest(registry).decode())

    def test_pinned_infrastructure_sources(self):
        lock = json.loads((ROOT / "deploy/monitoring/versions.lock.json").read_text())
        for entry in lock["images"].values():
            self.assertRegex(entry["digest"], r"^sha256:[a-f0-9]{64}$")
            self.assertTrue(entry["image"].endswith("@" + entry["digest"]))
        self.assertEqual(lock["charts"]["kube-prometheus-stack"]["version"], "92.7.0")
        values = (ROOT / "deploy/monitoring/kubernetes/values.yml").read_text()
        self.assertIn("insecureSkipVerify: false", values)
        self.assertIn("cAdvisor: true", values)
        self.assertIn("cAdvisorMetricRelabelings: []", values)
        exporters = (ROOT / "deploy/monitoring/compose.exporters.yml").read_text()
        self.assertNotIn("resource_exporter", exporters)
        self.assertNotIn("textfile", exporters)

    def test_no_data_is_preserved_and_queries_are_domain_only(self):
        rules = json.loads((ROOT / "deploy/monitoring/prometheus/rules/recording.yml").read_text())
        for group in rules["groups"]:
            for rule in group["rules"]:
                self.assertNotIn("glamdb_cgroup_", rule["expr"])
                self.assertNotIn("vector(0)", rule["expr"])
        for path in (ROOT / "deploy/monitoring/grafana/dashboards").glob("*.json"):
            dashboard = json.loads(path.read_text())
            self.assertEqual(len(dashboard["panels"]), len({panel["id"] for panel in dashboard["panels"]}))
            for panel in dashboard["panels"]:
                for target in panel.get("targets", []):
                    self.assertNotIn("vector(0)", target["expr"])
                    self.assertNotIn("snapshot_timestamp", target["expr"])


if __name__ == "__main__":
    unittest.main()
