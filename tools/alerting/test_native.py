"""Native Alertmanager delivery to a loopback Telegram protocol peer only."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def running(httpd):
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(5)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until(predicate, seconds=15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            if predicate():
                return
        except HTTPError as error:
            error.close()
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(0.05)
    raise AssertionError("loopback process did not reach the expected condition")


def api(url, payload=None, method=None):
    request = Request(url, data=json.dumps(payload).encode() if payload is not None else None,
                      method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=2) as response:
            data = response.read()
            return json.loads(data) if data else None
    except HTTPError as error:
        error.close()
        raise


class TelegramPeer:
    """A protocol test peer; never used by application/runtime code."""

    def __init__(self):
        self.calls = []
        self.replies = []
        self.metrics = ""
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.end_headers()
                self.wfile.write(owner.metrics.encode())

            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                owner.calls.append((self.path, payload, time.monotonic()))
                code, body, delay = owner.replies.pop(0) if owner.replies else (200, {
                    "ok": True, "result": {"message_id": 1, "chat": {"id": -10012345, "type": "supergroup"}}}, 0)
                time.sleep(delay)
                self.send_response(code)
                if code == 302:
                    self.send_header("Location", "/redirected")
                self.end_headers()
                try:
                    self.wfile.write(json.dumps(body).encode())
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)


class NativeConfigTest(unittest.TestCase):
    def test_telegram_reads_both_secrets_without_a_webhook(self):
        config = (ROOT / "deploy/alerting/alertmanager.yml").read_text()
        self.assertIn("telegram_configs:", config)
        self.assertIn("bot_token_file: /run/secrets/telegram_bot_token", config)
        self.assertIn("chat_id_file: /run/secrets/telegram_chat_id", config)
        self.assertIn("send_resolved: false", config)
        self.assertIn(".Annotations.observed_at", config)
        self.assertNotIn("webhook_configs:", config)


class NativeProcess:
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.addCleanup(self.assert_logs_hide_token)
        self.token = self.root / "token"
        self.token.write_text("12345:FAKE_TEST_TOKEN")
        self.chat = self.root / "chat"
        self.chat.write_text("-10012345")
        self.peer = TelegramPeer()
        self.peer_url = self.enter(running(self.peer.httpd))
        self.am_port = free_port()
        self.am_url = f"http://127.0.0.1:{self.am_port}"
        config = (ROOT / "deploy/alerting/alertmanager.yml").read_text()
        self.assertEqual(config.count("api_url: https://api.telegram.org"), 1)
        config = config.replace("https://api.telegram.org", self.peer_url)
        config = config.replace("/run/secrets/telegram_bot_token", str(self.token))
        config = config.replace("/run/secrets/telegram_chat_id", str(self.chat))
        config = config.replace("group_wait: 30s", "group_wait: 200ms")
        if not getattr(self, "daily_summary", False):
            # Delivery/retry tests keep their fast, always-active protocol peer.
            # The daily-summary tests below retain the production cadence/window.
            config = config.replace("group_interval: 1m", "group_interval: 1s")
            config = config.replace("      active_time_intervals: [daily-summary]\n", "")
        config = config.replace("timeout: 15s", "timeout: 250ms")
        self.am_config = self.root / "alertmanager.yml"
        self.am_config.write_text(config)
        self.am = self.start_am()

    def enter(self, context):
        result = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        return result

    def start_process(self, args, name):
        log = (self.root / (name + ".log")).open("ab")
        self.addCleanup(log.close)
        process = subprocess.Popen(args, stdout=log, stderr=log)

        def stop():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(5)

        self.addCleanup(stop)
        return process

    def start_am(self):
        process = self.start_process([os.environ["ALERTMANAGER_BIN"],
                                      "--config.file=" + str(self.am_config),
                                      "--storage.path=" + str(self.root / "am-data"),
                                      "--web.listen-address=127.0.0.1:" + str(self.am_port),
                                      "--cluster.listen-address=", "--log.level=warn"], "alertmanager")
        wait_until(lambda: urlopen(self.am_url + "/-/ready", timeout=1).close() is None)
        return process

    def assert_logs_hide_token(self):
        for path in self.root.glob("*.log"):
            self.assertNotIn("FAKE_TEST_TOKEN", path.read_text())

    def alerts(self, state, check="cpu", resolved=False, **labels):
        now = datetime.now(timezone.utc)
        return [{"labels": {"alertname": "GlamDBCheckState", "check": check, "cluster": "local",
                            "service": "glamdb", "state": state, "notify_nodata": "true", **labels},
                 "annotations": {"summary": "Loopback integration check", "observed_at": "1794300000"},
                 "startsAt": getattr(self, "alert_start", (now - timedelta(minutes=2)).isoformat()),
                 "endsAt": (now if resolved else now + timedelta(minutes=5)).isoformat()}]

    def post(self, state, check="cpu", resolved=False, **labels):
        api(self.am_url + "/api/v2/alerts", self.alerts(state, check, resolved, **labels))

    def metric(self, name):
        with urlopen(self.am_url + "/metrics", timeout=2) as response:
            return sum(float(line.rsplit(" ", 1)[1]) for line in response.read().decode().splitlines()
                       if line.startswith(name + "{") and 'integration="telegram"' in line)

@unittest.skipUnless(os.environ.get("ALERTMANAGER_BIN"), "set ALERTMANAGER_BIN or run tools/alerting/check.py")
class NativeDeliveryTest(NativeProcess, unittest.TestCase):
    def test_native_delivery_observation_time_dedup_and_no_resolved_ok(self):
        self.post("ALARM", chat_id="999")
        wait_until(lambda: len(self.peer.calls) == 1)
        path, body, _ = self.peer.calls[0]
        self.assertEqual(path, "/bot12345:FAKE_TEST_TOKEN/sendMessage")
        self.assertEqual(int(body["chat_id"]), -10012345)
        self.assertIn("ALARM GlamDB cpu", body["text"])
        self.assertIn("1794300000", body["text"])
        self.assertFalse(body.get("parse_mode"))
        self.post("ALARM", chat_id="999")
        time.sleep(1.1)
        self.assertEqual(len(self.peer.calls), 1)
        self.post("ALARM", resolved=True, chat_id="999")
        time.sleep(1.1)
        self.assertEqual(len(self.peer.calls), 1)

    def test_429_retries_and_exposes_failure_without_durable_chat_throttle(self):
        # This release uses the standard notification backoff. It does not
        # persist or honor Telegram's retry_after as a chat-wide deadline.
        self.peer.replies = [(429, {"ok": False, "error_code": 429,
                                   "description": "Too Many Requests", "parameters": {"retry_after": 60}}, 0)]
        self.post("ALARM")
        wait_until(lambda: len(self.peer.calls) >= 2)
        self.assertLess(self.peer.calls[1][2] - self.peer.calls[0][2], 60)
        wait_until(lambda: self.metric("alertmanager_notification_requests_failed_total") >= 1)
        wait_until(lambda: self.metric("alertmanager_notification_requests_total") >= 2)

    def test_5xx_retries_and_exposes_failure(self):
        self.peer.replies = [(503, {"ok": False, "error_code": 503, "description": "Unavailable"}, 0)]
        self.post("WARN")
        wait_until(lambda: len(self.peer.calls) >= 2)
        wait_until(lambda: self.metric("alertmanager_notification_requests_failed_total") >= 1)
        self.assertIn("WARN GlamDB cpu", self.peer.calls[-1][1]["text"])

    def test_request_timeout_is_bounded_and_retried(self):
        self.peer.replies = [(200, {"ok": True, "result": {}}, 0.6)]
        self.post("ALARM")
        wait_until(lambda: len(self.peer.calls) >= 2)
        wait_until(lambda: self.metric("alertmanager_notification_requests_failed_total") >= 1)
        self.assertLess(self.peer.calls[1][2] - self.peer.calls[0][2], 5)

    def test_silence_and_notification_log_survive_restart_but_active_alerts_need_resend(self):
        now = datetime.now(timezone.utc)
        silence = {"matchers": [{"name": "check", "value": "maintenance", "isRegex": False, "isEqual": True}],
                   "startsAt": (now - timedelta(seconds=1)).isoformat(),
                   "endsAt": (now + timedelta(minutes=5)).isoformat(),
                   "createdBy": "local-test", "comment": "loopback persistence test"}
        result = api(self.am_url + "/api/v2/silences", silence)
        alarm = self.alerts("ALARM")
        api(self.am_url + "/api/v2/alerts", alarm)
        wait_until(lambda: len(self.peer.calls) == 1)
        self.am.terminate()
        self.am.wait(5)
        self.am = self.start_am()
        restored = api(self.am_url + "/api/v2/silence/" + result["silenceID"])
        self.assertEqual(restored["status"]["state"], "active")
        self.assertEqual(api(self.am_url + "/api/v2/alerts"), [])
        api(self.am_url + "/api/v2/alerts", alarm)
        self.post("ALARM", check="maintenance")
        time.sleep(1.5)
        self.assertEqual(len(self.peer.calls), 1)
        api(self.am_url + "/api/v2/silence/" + result["silenceID"], method="DELETE")
        wait_until(lambda: len(self.peer.calls) == 2)
        self.assertIn("maintenance", self.peer.calls[-1][1]["text"])

    def test_restart_during_retry_requires_current_alert_resend(self):
        self.peer.replies = [(503, {"ok": False, "error_code": 503, "description": "Unavailable"}, 0)] * 20
        self.post("ALARM")
        wait_until(lambda: len(self.peer.calls) >= 1)
        wait_until(lambda: self.metric("alertmanager_notification_requests_failed_total") >= 1)
        self.am.kill()
        self.am.wait(5)
        count = len(self.peer.calls)
        self.peer.replies = []
        self.am = self.start_am()
        time.sleep(1.2)
        self.assertEqual(len(self.peer.calls), count)
        self.post("NODATA")
        wait_until(lambda: len(self.peer.calls) == count + 1)
        self.assertIn("NODATA GlamDB cpu", self.peer.calls[-1][1]["text"])

    def test_inhibition_suppresses_lower_states_and_nodata_inhibits_alarm(self):
        self.alert_start = datetime.now(timezone.utc).isoformat()
        api(self.am_url + "/api/v2/alerts", self.alerts("OK") + self.alerts("WARN") + self.alerts("ALARM"))
        wait_until(lambda: len(self.peer.calls) == 1)
        self.assertIn("ALARM GlamDB cpu", self.peer.calls[0][1]["text"])
        self.assertNotIn("WARN GlamDB", self.peer.calls[0][1]["text"])
        self.assertNotIn("OK GlamDB", self.peer.calls[0][1]["text"])
        self.post("NODATA")
        wait_until(lambda: len(self.peer.calls) == 2)
        self.assertIn("NODATA GlamDB cpu", self.peer.calls[1][1]["text"])
        self.assertNotIn("ALARM GlamDB", self.peer.calls[1][1]["text"])

    def test_grouping_batches_same_identity_and_repeats_at_configured_interval(self):
        self.am.terminate()
        self.am.wait(5)
        config = self.am_config.read_text().replace("repeat_interval: 12h", "repeat_interval: 1s")
        self.am_config.write_text(config)
        self.am = self.start_am()
        self.alert_start = datetime.now(timezone.utc).isoformat()
        alerts = self.alerts("ALARM", replica="one") + self.alerts("ALARM", replica="two")
        api(self.am_url + "/api/v2/alerts", alerts)
        wait_until(lambda: len(self.peer.calls) == 1)
        self.assertEqual(self.peer.calls[0][1]["text"].count("ALARM GlamDB cpu"), 2)
        wait_until(lambda: len(self.peer.calls) >= 2)

    def test_startup_batches_fifty_checks_in_one_service_without_truncation(self):
        self.am.terminate()
        self.am.wait(5)
        self.am_config.write_text(self.am_config.read_text().replace("group_wait: 200ms", "group_wait: 2s"))
        self.am = self.start_am()
        self.alert_start = datetime.now(timezone.utc).isoformat()
        alerts = []
        for index in range(50):
            alerts += self.alerts("ALARM", check=f"check_{index:02}")
        api(self.am_url + "/api/v2/alerts", alerts)
        wait_until(lambda: len(api(self.am_url + "/api/v2/alerts")) == 50)
        wait_until(lambda: len(self.peer.calls) >= 1)
        text = self.peer.calls[0][1]["text"]
        for index in range(50):
            self.assertIn(f"ALARM GlamDB check_{index:02}", text)
        self.assertLessEqual(len(text), 4096)
        time.sleep(1.1)
        self.assertEqual(len(self.peer.calls), 1)

    def test_quick_states_can_coalesce_before_group_wait(self):
        self.am.terminate()
        self.am.wait(5)
        self.am_config.write_text(self.am_config.read_text().replace("group_wait: 200ms", "group_wait: 1s"))
        self.am = self.start_am()
        self.alert_start = datetime.now(timezone.utc).isoformat()
        self.post("WARN")
        self.post("WARN", resolved=True)
        self.post("ALARM")
        self.post("ALARM", resolved=True)
        self.post("NODATA")
        wait_until(lambda: len(self.peer.calls) == 1)
        self.assertIn("NODATA GlamDB cpu", self.peer.calls[0][1]["text"])
        self.assertNotIn("WARN GlamDB", self.peer.calls[0][1]["text"])
        self.assertNotIn("ALARM GlamDB", self.peer.calls[0][1]["text"])


@unittest.skipUnless(os.environ.get("ALERTMANAGER_BIN"), "set ALERTMANAGER_BIN or run tools/alerting/check.py")
class DailySummaryTest(NativeProcess, unittest.TestCase):
    daily_summary = True

    def window(self, start, end):
        import re
        self.am.terminate()
        self.am.wait(5)
        config = self.am_config.read_text()
        config = re.sub(r'start_time: "[0-9:]+"', f'start_time: "{start}"', config, count=1)
        config = re.sub(r'end_time: "[0-9:]+"', f'end_time: "{end}"', config, count=1)
        self.am_config.write_text(config)
        self.am = self.start_am()

    def test_state_changes_outside_daily_window_send_no_messages(self):
        now = datetime.now(ZoneInfo("Europe/Moscow"))
        hour = (now.hour + 12) % 24
        self.window(f"{hour:02}:00", f"{hour:02}:01")
        self.post("WARN")
        wait_until(lambda: len(api(self.am_url + "/api/v2/alerts")) == 1)
        time.sleep(.5)
        self.post("WARN", resolved=True)
        self.post("ALARM")
        time.sleep(1.2)
        self.assertEqual(len(self.peer.calls), 0, "A state change escaped the daily delivery window")

    def test_daily_window_sends_one_combined_snapshot_and_then_stays_quiet(self):
        now = datetime.now(ZoneInfo("Europe/Moscow"))
        if now.second >= 54:
            time.sleep(61 - now.second)
            now = datetime.now(ZoneInfo("Europe/Moscow"))
        next_minute = now + timedelta(minutes=1)
        end = "24:00" if next_minute.date() != now.date() else next_minute.strftime("%H:%M")
        self.window(now.strftime("%H:%M"), end)
        # An expired repeat horizon is deliberate: only the active window and
        # production one-minute cadence may prevent a second successful send.
        self.am.terminate()
        self.am.wait(5)
        config = self.am_config.read_text().replace("repeat_interval: 12h", "repeat_interval: 1s")
        # Let all identities enter the group, as the production 30s wait does.
        # A 200ms protocol-test wait can race grouping under parallel test load.
        self.am_config.write_text(config.replace("group_wait: 200ms", "group_wait: 2s"))
        self.am = self.start_am()
        # Old StartsAt bypasses group_wait in Alertmanager and flushes before
        # the whole batch is dispatched. This fixture is a fresh startup batch.
        self.alert_start = datetime.now(timezone.utc).isoformat()
        alerts = []
        states = ("OK", "WARN", "ALARM", "NODATA")
        for index in range(50):
            alerts += self.alerts(states[index % 4], check=f"check_{index:02}",
                                  service="glamdb" if index % 2 else "monitoring")
        api(self.am_url + "/api/v2/alerts", alerts)
        wait_until(lambda: len(api(self.am_url + "/api/v2/alerts")) == 50)
        wait_until(lambda: len(self.peer.calls) >= 1)
        text = self.peer.calls[0][1]["text"]
        for index in range(50):
            self.assertIn(f"{states[index % 4]} GlamDB check_{index:02}", text)
        self.assertLessEqual(len(text), 4096)
        self.post("ALARM", check="new_problem_after_summary")
        # Exercise a complete real production polling interval and closure of
        # the one-minute window, without changing the host or VM clock.
        time.sleep(31)
        time.sleep(31)
        self.assertEqual(len(self.peer.calls), 1, "A second message escaped the one-minute daily window")


if __name__ == "__main__":
    unittest.main()
