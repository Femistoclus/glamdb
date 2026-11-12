"""Opt-in real Linux/Podman lifecycle test on an explicitly disposable lab config.

Run only with GLAMDB_OBSERVABILITY_LIVE_CONFIG pointing to an owned
glamdb-lab-* configuration. Does not create/format a filesystem or remove data.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest


ROOT = Path(__file__).resolve().parents[3]
LIVE_CONFIG = os.environ.get("GLAMDB_OBSERVABILITY_LIVE_CONFIG")


@unittest.skipUnless(LIVE_CONFIG, "requires an explicitly selected disposable Linux lab")
class LivePodmanLifecycle(unittest.TestCase):
    def setUp(self):
        if not sys.platform.startswith("linux") or os.geteuid() != 0:
            self.skipTest("real lifecycle requires root on the target Linux host")
        self.config = Path(LIVE_CONFIG).resolve()
        self.input = json.loads(self.config.read_text())
        if not self.input["name"].startswith("glamdb-lab-"):
            self.fail("live tests require a separate glamdb-lab-* installation")
        self.state = Path(self.input["state_dir"])
        self.results = {}
        self.addCleanup(self.cleanup)

    def cli(self, command, expected=0):
        result = subprocess.run([sys.executable, str(ROOT / "scripts/observability.py"), command,
                                 "--config", str(self.config), "--no-systemd"],
                                text=True, capture_output=True, timeout=240)
        self.assertEqual(result.returncode, expected, result.stderr)
        return json.loads(result.stdout) if result.stdout else result.stderr

    def podman(self, *args):
        return subprocess.check_output(["podman", "--remote=false", *args], text=True, timeout=60)

    def cleanup(self):
        result = subprocess.run([sys.executable, str(ROOT / "scripts/observability.py"), "down",
                                 "--config", str(self.config), "--no-systemd"], capture_output=True, text=True, timeout=240)
        if result.returncode:
            raise RuntimeError("owned lab cleanup failed: " + result.stderr)

    def checked(self):
        deadline = time.monotonic() + 90
        while True:
            result = subprocess.run([sys.executable, str(ROOT / "scripts/observability.py"), "check",
                                     "--config", str(self.config)], capture_output=True, text=True, timeout=90)
            if result.returncode == 0:
                value = json.loads(result.stdout)
                self.assertEqual(value["result"], "PASS")
                self.assertEqual(value["container_coverage"]["missing"], [])
                self.assertEqual(value["logging"]["result"], "PASS")
                return value
            if time.monotonic() >= deadline:
                self.fail(result.stdout + result.stderr)
            time.sleep(2)

    def volume_inventory(self):
        names = [self.input["name"] + "-" + service + "-data"
                 for service in ("prometheus", "grafana", "alertmanager", "alloy")]
        return {row["Name"]: {key: row[key] for key in ("Name", "CreatedAt", "Mountpoint", "Labels")}
                for row in json.loads(self.podman("volume", "inspect", *names))}

    def test_actual_up_repeat_down_restore_preserves_real_state(self):
        self.cli("up")
        first = self.checked()
        before = self.cli("status")
        volumes = self.volume_inventory()
        owner = json.loads((self.state / "manifest.json").read_text())["owner"]
        password = (self.state / "secrets/grafana-password").read_bytes()
        key = (self.state / "secrets/grafana-secret-key").read_bytes()
        repeated = self.cli("up")
        after = self.cli("status")
        self.assertEqual([r["id"] for r in before], [r["id"] for r in after])
        self.assertEqual(set(repeated["services"].values()), {"unchanged"})
        restart = json.loads(self.podman("container", "inspect", self.input["name"] + "-loki"))[0]["HostConfig"]["RestartPolicy"]["Name"]
        self.assertIn(restart, ("no", ""), "Loki must not bypass its storage guard by autonomous restart")
        self.cli("down")
        self.assertTrue(all(not row["exists"] for row in self.cli("status")))
        self.assertEqual(self.volume_inventory(), volumes)
        self.cli("up")
        restored = self.checked()
        self.assertEqual(self.volume_inventory(), volumes)
        self.assertEqual((self.state / "secrets/grafana-password").read_bytes(), password)
        self.assertEqual((self.state / "secrets/grafana-secret-key").read_bytes(), key)
        self.assertEqual(json.loads((self.state / "manifest.json").read_text())["owner"], owner)
        self.assertGreaterEqual(restored["logging"]["matched_records"], 1)
        self.assertIn(restored["logging"]["telemetry"]["loki_source_file_read_lines_total"], (None, 0.0),
                      "orderly restart should restore EOF positions, without borrowing old exporter counter samples")
        self.results = {"result": "PASS", "same_ids_on_repeated_up": True, "volumes_preserved_on_down_up": True,
                        "grafana_credentials_preserved": True, "loki_autonomous_restart_disabled": True,
                        "before_check": first, "restored_check": restored}
        (self.state / "live-lifecycle-result.json").write_text(json.dumps(self.results, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
