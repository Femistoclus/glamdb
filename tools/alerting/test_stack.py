"""Real Prometheus rules and Alertmanager with synthetic loopback metric sources."""

import importlib.util
from html import unescape
import json
import os
from pathlib import Path
import time
import unittest
from urllib.parse import quote
from urllib.request import urlopen

from tools.alerting.test_native import NativeProcess, ROOT, TelegramPeer, api, free_port, running, wait_until


def generator():
    spec = importlib.util.spec_from_file_location("generate", Path(__file__).with_name("generate.py"))
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


@unittest.skipUnless(os.environ.get("PROMETHEUS_BIN") and os.environ.get("ALERTMANAGER_BIN"),
                     "set PROMETHEUS_BIN and ALERTMANAGER_BIN, or run tools/alerting/check.py")
class StackTest(NativeProcess, unittest.TestCase):
    def start_prom(self, config, name="prometheus", port=None):
        port = port or free_port()
        path = self.root / (name + ".json")
        path.write_text(config if isinstance(config, str) else json.dumps(config))
        process = self.start_process([os.environ["PROMETHEUS_BIN"], "--config.file=" + str(path),
                                      "--storage.tsdb.path=" + str(self.root / (name + "-data")),
                                      "--web.listen-address=127.0.0.1:" + str(port), "--web.enable-lifecycle",
                                      "--rules.alert.resend-delay=1s", "--log.level=warn"], name)
        url = f"http://127.0.0.1:{port}"
        wait_until(lambda: urlopen(url + "/-/ready", timeout=1).close() is None)
        return process, url, path

    def query(self, url, expression):
        return api(url + "/api/v1/query?query=" + quote(expression))["data"]["result"]

    def state_config(self, checks):
        config = json.loads((ROOT / "deploy/alerting/checks.json").read_text())
        config["checks"] = checks
        config["hold"] = {k: "0s" for k in config["hold"]}
        config["evaluation_interval"] = "1s"
        path = self.root / "states.yml"
        path.write_text(json.dumps(generator().rules(config)))
        return {"global": {"scrape_interval": "1s", "evaluation_interval": "1s"}, "rule_files": [str(path)],
                "alerting": {"alertmanagers": [{"static_configs": [{"targets": [self.am_url.split("://")[1]]}]}]},
                "scrape_configs": []}

    def test_all_four_states_are_exclusive_and_missing_source_never_becomes_ok(self):
        exporter = TelegramPeer()
        exporter.metrics = "glamdb_test_value 0\n"
        target = self.enter(running(exporter.httpd)).split("://")[1]
        check = {"id": "metric", "query": "glamdb_test_value", "fresh": ["glamdb_test_value"],
                 "warn": 1, "alarm": 2, "notify_nodata": True, "max_age_seconds": 15,
                 "summary": "Loopback four-state metric", "description": "Synthetic source, real rules and delivery"}
        config = self.state_config([check])
        config["scrape_configs"] = [{"job_name": "metric", "static_configs": [{"targets": [target]}]}]
        _, url, path = self.start_prom(config)

        def state(expected):
            result = self.query(url, 'glamdb_alert_state{check="metric"}')
            return len(result) == 1 and float(result[0]["value"][1]) == expected

        for value, public in ((0, "OK"), (1, "WARN"), (2, "ALARM")):
            before = len(self.peer.calls)
            exporter.metrics = f"glamdb_test_value {value}\n"
            wait_until(lambda: state(value))
            wait_until(lambda: any(public + " · metric" in c[1]["text"] for c in self.peer.calls[before:]))
            active = self.query(url, 'ALERTS{check="metric",alertstate="firing"}')
            self.assertEqual([a["metric"]["state"] for a in active], [public])
        alarm_count = len(self.peer.calls)
        config["scrape_configs"] = []
        path.write_text(json.dumps(config))
        api(url + "/-/reload", {}, method="POST")
        wait_until(lambda: state(3), seconds=25)
        wait_until(lambda: any("NODATA · metric" in c[1]["text"] for c in self.peer.calls[alarm_count:]))
        self.assertFalse(any("OK ·" in c[1]["text"] for c in self.peer.calls[alarm_count:]))
        # Recovery is a new explicit healthy measurement, never a resolved alert.
        before = len(self.peer.calls)
        exporter.metrics = "glamdb_test_value 0\n"
        config["scrape_configs"] = [{"job_name": "metric", "static_configs": [{"targets": [target]}]}]
        path.write_text(json.dumps(config))
        api(url + "/-/reload", {}, method="POST")
        wait_until(lambda: any("OK · metric" in c[1]["text"] for c in self.peer.calls[before:]))
        text = self.peer.calls[-1][1]["text"]
        observed = float(unescape(text.split("Observed at (Unix seconds): ")[1].splitlines()[0]))
        self.assertLess(abs(time.time() - observed), 15)

    def test_old_healthy_sample_is_nodata_despite_recent_instant_query_timestamp(self):
        exporter = TelegramPeer()
        exporter.metrics = f"glamdb_test_value 0 {int((time.time() - 120) * 1000)}\n"
        target = self.enter(running(exporter.httpd)).split("://")[1]
        check = {"id": "stale", "query": "glamdb_test_value", "fresh": ["glamdb_test_value"],
                 "warn": 1, "alarm": 2, "notify_nodata": True, "max_age_seconds": 15,
                 "summary": "Stale healthy measurement", "description": "Must remain NODATA"}
        config = self.state_config([check])
        config["scrape_configs"] = [{"job_name": "old-metric", "static_configs": [{"targets": [target]}]}]
        _, url, _ = self.start_prom(config)
        wait_until(lambda: bool(self.query(url, "glamdb_test_value")))
        raw = self.query(url, "glamdb_test_value")[0]
        self.assertGreater(float(raw["value"][0]), time.time() - 15)
        self.assertLess(float(self.query(url, "timestamp(glamdb_test_value)")[0]["value"][1]), time.time() - 90)
        wait_until(lambda: any("NODATA · stale" in c[1]["text"] for c in self.peer.calls))
        self.assertFalse(any("OK ·" in c[1]["text"] for c in self.peer.calls))

    def test_real_caddy_fixture_keeps_gateway_auth_check_fresh(self):
        exporter = TelegramPeer()
        exporter.metrics = (ROOT / "tools/alerting/fixtures/caddy-2.11.9.prom").read_text()
        target = self.enter(running(exporter.httpd)).split("://")[1]
        checks = json.loads((ROOT / "deploy/alerting/checks.json").read_text())["checks"]
        config = self.state_config([c for c in checks if c["id"] == "http_auth_failures"])
        config["scrape_configs"] = [{"job_name": "glamdb-edge", "static_configs": [
            {"targets": [target], "labels": {"cluster": "local", "service": "glamdb"}}]}]
        _, url, _ = self.start_prom(config)
        wait_until(lambda: bool(self.query(url, "caddy_http_request_duration_seconds_count")))
        wait_until(lambda: bool(self.query(url, 'glamdb_alert_state{check="http_auth_failures"}')) and
                   float(self.query(url, 'glamdb_alert_state{check="http_auth_failures"}')[0]["value"][1]) == 0)
        wait_until(lambda: any("OK · HTTP auth failures" in c[1]["text"] for c in self.peer.calls))


    def test_independent_observer_reports_loss_of_main_prometheus_and_alertmanager(self):
        source_am_port = free_port()
        source_am = self.start_process([os.environ["ALERTMANAGER_BIN"],
                                        "--config.file=" + str(self.am_config),
                                        "--storage.path=" + str(self.root / "source-am-data"),
                                        "--web.listen-address=127.0.0.1:" + str(source_am_port),
                                        "--cluster.listen-address=", "--log.level=warn"], "source-alertmanager")
        wait_until(lambda: urlopen(f"http://127.0.0.1:{source_am_port}/-/ready", timeout=1).close() is None)
        source_rules = self.root / "source-rules.yml"
        source_rules.write_text(json.dumps({"groups": [{"name": "alive", "interval": "1s", "rules": [
            {"record": "loopback_evaluator_alive", "expr": "vector(1)"}]}]}))
        source_port = free_port()
        main_config = {"global": {"scrape_interval": "1s", "evaluation_interval": "1s"},
                       "rule_files": [str(source_rules)], "scrape_configs": [
                           {"job_name": "self", "static_configs": [{"targets": [f"127.0.0.1:{source_port}"]}]}]}
        source_prom, source_url, _ = self.start_prom(main_config, "source-prometheus", source_port)
        wait_until(lambda: bool(self.query(source_url, "loopback_evaluator_alive")))
        rules = self.root / "observer-rules.yml"
        rules.write_text((ROOT / "deploy/alerting/external-observer/rules.yml").read_text()
                         .replace("interval: 15s", "interval: 1s").replace("for: 1m", "for: 1s"))
        config = (ROOT / "deploy/alerting/external-observer/prometheus.yml").read_text()
        config = config.replace("15s", "1s").replace("rule_files: [rules.yml]", "rule_files: [" + str(rules) + "]")
        config = config.replace("glamdb-host.example.invalid:9090", source_url.split("://")[1])
        config = config.replace("glamdb-host.example.invalid:9093", f"127.0.0.1:{source_am_port}")
        config = config.replace("independent-alertmanager.example.invalid:9093", self.am_url.split("://")[1])
        self.assertNotIn("example.invalid", config)
        _, observer, _ = self.start_prom(config, "independent-observer")
        wait_until(lambda: len(self.query(observer, 'up{job="glamdb-pipeline"}')) == 2)
        wait_until(lambda: all(float(v["value"][1]) == 1 for v in self.query(observer, 'up{job="glamdb-pipeline"}')))
        source_prom.terminate()
        source_prom.wait(5)
        source_am.terminate()
        source_am.wait(5)
        wait_until(lambda: any("NODATA · Prometheus external" in c[1]["text"] for c in self.peer.calls))
        wait_until(lambda: any("NODATA · Alerts external" in c[1]["text"] for c in self.peer.calls))
        with urlopen(self.am_url + "/-/ready", timeout=1) as response:
            self.assertEqual(response.status, 200)
        # These are independent local processes. Physical host/fault-domain
        # separation remains an explicit deployment step.
        self.assertTrue(self.query(observer, 'ALERTS{alertname="GlamDBPipelineSourceUnavailable",alertstate="firing"}'))

    def test_real_rule_evaluation_error_is_reported_as_a_separate_pipeline_fault(self):
        checks = json.loads((ROOT / "deploy/alerting/checks.json").read_text())["checks"]
        config = self.state_config([c for c in checks if c["id"] == "prometheus_rule_failures"])
        broken = self.root / "broken-rules.yml"
        broken.write_text(json.dumps({"groups": [{"name": "broken", "interval": "1s", "rules": [
            {"record": "loopback_many", "expr": 'label_replace(vector(1), "key", "one", "", "") or label_replace(vector(1), "key", "two", "", "")'},
            {"record": "loopback_invalid_match", "expr": "loopback_many * on() group_left loopback_many"}]}]}))
        config["rule_files"].append(str(broken))
        port = free_port()
        config["scrape_configs"] = [{"job_name": "prometheus", "static_configs": [
            {"targets": [f"127.0.0.1:{port}"], "labels": {"cluster": "local", "service": "monitoring"}}]}]
        _, url, _ = self.start_prom(config, port=port)
        wait_until(lambda: any("ALARM · Prometheus rule failures" in c[1]["text"] for c in self.peer.calls))
        groups = api(url + "/api/v1/rules")["data"]["groups"]
        errors = [r for g in groups for r in g["rules"] if r["name"] == "loopback_invalid_match"]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["health"], "err")
        self.assertIn("duplicate series", errors[0]["lastError"])


if __name__ == "__main__":
    unittest.main()
