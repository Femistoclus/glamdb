"""Render the maintained Telegram message with the pinned native amtool."""

from html.parser import HTMLParser
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]


class TelegramHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.text = []

    def handle_starttag(self, tag, attrs):
        if tag != "b" or attrs:
            raise AssertionError("unexpected Telegram HTML element")
        self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack.pop() != tag:
            raise AssertionError("unbalanced Telegram HTML")

    def handle_data(self, data):
        self.text.append(data)


@unittest.skipUnless(os.environ.get("AMTOOL_BIN"), "set AMTOOL_BIN or run tools/alerting/check.py")
class TelegramTemplateTest(unittest.TestCase):
    def render(self, checks, states=("OK", "WARN", "ALARM", "NODATA"), observed_at="1794300000", **labels):
        config = (ROOT / "deploy/alerting/alertmanager.yml").read_text()
        body = textwrap.dedent(config.split("        message: |-\n", 1)[1]
                               .split("        http_config:", 1)[0])
        alerts = [{"status": "firing", "labels": {"check": check, "state": states[index % len(states)],
                   "cluster": "sample", "service": "glamdb", "node": f"node-{index:03}", **labels},
                   "annotations": {"observed_at": observed_at}}
                  for index, check in enumerate(checks)]
        data = {"status": "firing", "receiver": "telegram", "alerts": alerts,
                "commonLabels": {"cluster": labels.get("cluster", "sample"), "service": "glamdb"}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "message.tmpl").write_text('{{ define "message" }}' + body + '{{ end }}')
            (root / "data.json").write_text(json.dumps(data))
            result = subprocess.run([os.environ["AMTOOL_BIN"], "template", "render",
                                     "--template.glob=" + str(root / "message.tmpl"),
                                     '--template.text={{ template "message" . }}',
                                     "--template.type=html",
                                     "--template.data=" + str(root / "data.json")],
                                    text=True, capture_output=True, check=True)
        parser = TelegramHTML()
        parser.feed(result.stdout)
        self.assertEqual(parser.stack, [], "native truncation must never cut an HTML element")
        self.assertLessEqual(len(result.stdout.encode()), 3800)
        return result.stdout, "".join(parser.text)

    def test_groups_have_bold_headings_blank_lines_and_short_labels(self):
        raw, text = self.render(["core_readiness", "host_cpu", "host_memory", "disk_space",
                                 "network_errors", "log_source_unavailable", "prometheus_availability"])
        for title in ("Database / API", "CPU", "Memory", "Disk", "Network", "Logs", "Monitoring / delivery"):
            self.assertIn("\n\n<b>" + title + "</b>\n", raw)
        self.assertNotIn("<b>Other</b>", raw)
        self.assertIn("OK · Native readiness", text)
        self.assertIn("WARN · Host CPU", text)
        self.assertIn("1794300000", text)

    def test_every_current_linux_check_and_all_four_states_appear_once(self):
        base = json.loads((ROOT / "deploy/alerting/checks.json").read_text())["checks"]
        extension = json.loads((ROOT / "deploy/monitoring/collectors/checks.extension.json").read_text())["checks"]
        checks = {item["id"] for item in base + extension if not item["id"].startswith(("pod_", "macos_"))}
        raw, text = self.render(sorted(checks))
        self.assertNotIn("Inventory too large", text)
        lines = [line for line in text.splitlines() if line.startswith(("OK ·", "WARN ·", "ALARM ·", "NODATA ·"))]
        self.assertEqual(len(lines), len(checks))
        for index in range(len(checks)):
            self.assertEqual(text.count(f"node-{index:03}"), 1)

    def test_unknown_checks_and_dynamic_labels_are_html_escaped(self):
        raw, text = self.render(['future_<b>&"check'], states=("<i>WARN</i>",),
                                 cluster='<a href="bad">&', namespace="<script>&")
        self.assertIn("<b>Other</b>", raw)
        self.assertIn("&lt;", raw)
        self.assertNotIn("<script>", raw)
        self.assertNotIn("<i>", raw)
        self.assertIn('future <b>&"check', text)

    def test_empty_inventory_has_no_empty_group_or_index_error(self):
        raw, text = self.render([])
        self.assertIn("No current checks", text)
        self.assertNotIn("<b>CPU</b>", raw)
        self.assertNotIn("<b>Other</b>", raw)

    def test_scientific_observation_time_remains_readable_after_html_decoding(self):
        raw, text = self.render(["host_cpu"], observed_at="1.794492948383e+09")
        self.assertIn("1.794492948383e+09", text)

    def test_brand_name_only_suppresses_the_service_label(self):
        raw, text = self.render(["host_cpu"], cluster="glamdb", namespace="glamdb", pod="glamdb", node="glamdb")
        self.assertEqual(text.count(" · glamdb"), 4)

    def test_oversized_inventory_has_explicit_bounded_state_totals(self):
        raw, text = self.render([f"future_check_{index:04}" for index in range(1000)])
        self.assertIn("Inventory too large", text)
        self.assertIn("1000 checks", text)
        for state in ("OK", "WARN", "ALARM", "NODATA"):
            self.assertIn(state + ": 250", text)
        self.assertIn("Alertmanager / Grafana", text)

    def test_hostile_long_labels_cannot_overflow_or_break_html(self):
        raw, text = self.render(["<script>&" * 1000] * 70, cluster="<&" * 1000,
                                 namespace="<&" * 1000, pod="<&" * 1000)
        self.assertIn("Inventory too large", text)
        self.assertNotIn("<script>", raw)


if __name__ == "__main__":
    unittest.main()
