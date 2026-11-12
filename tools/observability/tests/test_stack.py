"""Behavior tests for deployment generation and resource/storage ownership."""

import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "tools/observability/stack.py"


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.logs = self.base / "application-logs"
        self.logs.mkdir()
        (self.logs / "glamdb.jsonl").write_text("")
        self.configuration = {
            "schema_version": 1, "name": "obs-test", "cluster": "test", "node": "server-a",
            "state_dir": str(self.base / "state"),
            "metrics": {"core": {"url": "http://127.0.0.1:19094/metrics"}},
            "logs": {"directory": str(self.logs), "uid": os.getuid(), "gid": os.getgid()},
            "loki": {"mount_path": str(self.base / "dedicated"), "max_bytes": 1073741824},
        }
        self.config = self.base / "input.json"
        self.write_config()

    def module(self):
        self.assertTrue(MODULE.is_file(), "portable deployment implementation is missing")
        name = "observability_stack_tested"
        if name not in sys.modules:
            spec = importlib.util.spec_from_file_location(name, MODULE)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
        return sys.modules[name]

    def write_config(self):
        self.config.write_text(json.dumps(self.configuration))

    def initialize(self):
        obs = self.module()
        deployment = obs.Deployment(self.config)
        deployment.initialize()
        return deployment

    def test_init_generates_real_endpoints_private_services_and_both_platform_plans(self):
        # Catches a host-loopback rewrite, public exporter bind, or missing image architecture.
        deployment = self.initialize()
        generated = self.base / "state/generated"
        prom = json.loads((generated / "prometheus.json").read_text())
        core = next(j for j in prom["scrape_configs"] if j["job_name"] == "glamdb-core")
        self.assertEqual(core["static_configs"][0]["targets"], ["127.0.0.1:19094"])
        self.assertEqual(core["metrics_path"], "/metrics")
        self.assertEqual(core["static_configs"][0]["labels"]["service_instance"], "core")
        overview = json.loads((generated / "grafana/dashboards/glamdb-overview.json").read_text())
        scope = next(p["options"]["content"] for p in overview["panels"] if p["type"] == "text")
        self.assertIn("Linux", scope)
        self.assertNotIn("Real macOS host", scope)
        self.assertTrue(all("glamdb-macos-node" not in link.get("url", "") for link in overview.get("links", [])))
        for arch in ("amd64", "arm64"):
            specs = deployment.service_specs(arch)
            self.assertEqual(set(specs), {"prometheus", "grafana", "loki", "alloy", "alertmanager",
                                         "node", "podman", "cgroup", "cgroup-systemd"})
            for name, spec in specs.items():
                self.assertRegex(spec["image"], r"@sha256:[0-9a-f]{64}$")
                self.assertEqual(spec["network"], "host")
                self.assertNotIn("--privileged", spec["options"])
            self.assertIn("127.0.0.1:13232", specs["cgroup"]["command"])
            self.assertIn("127.0.0.1:13233", specs["cgroup-systemd"]["command"])
            self.assertNotEqual(specs["cgroup"]["image"], deployment.service_specs(
                "arm64" if arch == "amd64" else "amd64")["cgroup"]["image"])

    def test_repeated_init_preserves_password_owner_data_and_generated_mtimes(self):
        # Catches password rotation, adopting another state directory, and repeated rewrites.
        self.initialize()
        state = self.base / "state"
        (state / "operator-note").write_text("keep")
        before = {str(p.relative_to(state)): (p.read_bytes(), p.stat().st_mtime_ns)
                  for p in state.rglob("*") if p.is_file()}
        self.initialize()
        after = {str(p.relative_to(state)): (p.read_bytes(), p.stat().st_mtime_ns)
                 for p in state.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        self.assertEqual(stat.S_IMODE((state / "secrets/grafana-password").stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(state.stat().st_mode), 0o700)

    def test_restrictive_umask_keeps_nonsensitive_configs_readable_and_secrets_private(self):
        # Catches the real Loki UID10001 failure caused by root-owned0600 config.
        old = os.umask(0o077)
        try:
            self.initialize()
        finally:
            os.umask(old)
        state = self.base / "state"
        for name in ("loki.yaml", "config.alloy", "prometheus.json", "alertmanager.yml"):
            self.assertEqual(stat.S_IMODE((state / "generated" / name).stat().st_mode), 0o644)
        self.assertEqual(stat.S_IMODE((state / "secrets/grafana-password").stat().st_mode), 0o600)

    def test_secrets_are_files_and_never_embedded_in_generated_config_or_run_arguments(self):
        # Catches interpolation of real secrets into inspectable argv/configs.
        token = self.base / "token"
        chat = self.base / "chat"
        token.write_text("123456:private-token-value")
        chat.write_text("-1001234567890")
        token.chmod(0o600)
        chat.chmod(0o600)
        self.configuration["telegram"] = {"bot_token_file": str(token), "chat_id_file": str(chat)}
        self.write_config()
        deployment = self.initialize()
        public = "\n".join(p.read_text() for p in (self.base / "state/generated").rglob("*") if p.is_file())
        args = json.dumps(deployment.service_specs("amd64"))
        for secret in (token.read_text(), chat.read_text()):
            self.assertNotIn(secret, public)
            self.assertNotIn(secret, args)
        self.assertEqual((self.base / "state/secrets/telegram-bot-token").read_text(), token.read_text())

    def test_refuses_unowned_nonempty_state_or_changed_storage_identity(self):
        # Catches accidental adoption or reuse of another installation's data path.
        state = self.base / "state"
        state.mkdir()
        (state / "unrelated").write_text("do not touch")
        with self.assertRaisesRegex(self.module().DeploymentError, "unowned"):
            self.initialize()
        self.assertEqual((state / "unrelated").read_text(), "do not touch")
        (state / "unrelated").unlink()
        self.initialize()
        self.configuration["loki"]["mount_path"] = str(self.base / "other-disk")
        self.write_config()
        with self.assertRaisesRegex(self.module().DeploymentError, "immutable"):
            self.initialize()

    def test_rejects_unsafe_urls_paths_ports_and_public_secrets(self):
        obs = self.module()
        for value in ("http://user:secret@localhost:9094/metrics", "http://localhost/metrics?token=secret"):
            with self.subTest(url=value):
                self.configuration["metrics"]["core"]["url"] = value
                self.write_config()
                with self.assertRaises(obs.DeploymentError):
                    obs.Deployment(self.config)
        self.configuration["metrics"]["core"]["url"] = "http://127.0.0.1:19094/metrics"
        self.configuration["ports"] = {"grafana": 9090}
        self.write_config()
        with self.assertRaisesRegex(obs.DeploymentError, "port"):
            obs.Deployment(self.config)
        self.configuration.pop("ports")
        public = self.base / "public-secret"
        public.write_text("private")
        public.chmod(0o644)
        self.configuration["telegram"] = {"bot_token_file": str(public), "chat_id_file": str(public)}
        self.write_config()
        with self.assertRaisesRegex(obs.DeploymentError, "private"):
            self.initialize()

    def test_mount_validation_requires_whole_dedicated_bounded_persistent_filesystem(self):
        # Catches a missing mount falling through onto /, a shared bind, tmpfs, or oversized storage.
        obs = self.module()
        good = {"target": "/var/lib/observability/loki", "source": "/dev/loop20", "fstype": "ext4",
                "fsroot": "/", "uuid": "1111-2222", "capacity_bytes": 1073741824}
        obs.validate_mount(good, "/var/lib/observability/loki", 1073741824)
        for change in ({"target": "/"}, {"fstype": "tmpfs"}, {"fsroot": "/shared"},
                       {"uuid": ""}, {"capacity_bytes": 2147483648}):
            with self.subTest(change=change), self.assertRaises(obs.DeploymentError):
                obs.validate_mount({**good, **change}, "/var/lib/observability/loki", 1073741824)

    def test_resource_identity_rejects_same_name_foreign_or_missing_owner(self):
        # Catches replacing an unrelated existing container/volume because its name matches.
        obs = self.module()
        for record in ({"Labels": {}}, {"Config": {"Labels": {"io.glamdb.observability.owner": "other"}}}):
            with self.assertRaisesRegex(obs.DeploymentError, "owned"):
                obs.require_owned(record, "ours", "obs-test-grafana")
        obs.require_owned({"Labels": {"io.glamdb.observability.owner": "ours"}}, "ours", "obs-test-data")

    def test_read_only_state_load_does_not_require_original_notification_secrets(self):
        # Catches being unable to inspect or stop the owned deployment after input credentials disappear.
        token, chat = self.base / "token", self.base / "chat"
        token.write_text("123456:private-token-value")
        chat.write_text("-1001234567890")
        token.chmod(0o600)
        chat.chmod(0o600)
        self.configuration["telegram"] = {"bot_token_file": str(token), "chat_id_file": str(chat)}
        self.write_config()
        owner = self.initialize().owner
        token.unlink()
        chat.unlink()
        deployment = self.module().Deployment(self.config)
        self.assertTrue(hasattr(deployment, "load_state"), "read-only lifecycle needs state loading without regenerating secrets")
        deployment.load_state()
        self.assertEqual(deployment.owner, owner)

    def test_concurrent_first_init_keeps_one_owner_and_password(self):
        # Catches first-run generation occurring outside the process lock.
        self.module()
        command = [sys.executable, str(ROOT / "scripts/observability.py"), "init", "--config", str(self.config)]
        children = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(8)]
        outputs = [child.communicate(timeout=30) for child in children]
        self.assertEqual([child.returncode for child in children], [0] * 8, outputs)
        reports = [json.loads(stdout) for stdout, _ in outputs]
        self.assertEqual(len({r.get("installation_id") for r in reports}), 1)
        self.assertIsNotNone(reports[0].get("installation_id"))
        self.assertEqual(len({r.get("grafana_credential_fingerprint") for r in reports}), 1)
        self.assertIsNotNone(reports[0].get("grafana_credential_fingerprint"))
        owner = json.loads((self.base / "state/manifest.json").read_text())["owner"]
        self.assertEqual(self.initialize().owner, owner)

    def test_logging_validation_rejects_no_delivery_and_schema_drop_only_sources(self):
        # Catches files_active being treated as proof of genuine delivered records.
        obs = self.module()
        self.assertTrue(hasattr(obs, "source_records"), "genuine source correlation is missing")
        source = self.logs / "glamdb.jsonl"
        entry = {"schema": 1, "request_id": "1" * 32, "timestamp": "2026-11-11T12:00:00.000001Z",
                 "event": "request_completed"}
        source.write_text(json.dumps(entry) + "\n")
        records = obs.source_records(source)
        self.assertFalse(obs.correlate_logs(records, {"data": {"result": []}}))
        delivered = {"data": {"result": [{"values": [["1794398400000001000", json.dumps(entry)]]}]}}
        self.assertTrue(obs.correlate_logs(records, delivered))
        source.write_text(json.dumps({**entry, "schema": 0}) + "\n")
        self.assertFalse(obs.correlate_logs(obs.source_records(source), delivered))
        source.write_text(json.dumps({**entry, "schema": "1"}) + "\n")
        self.assertTrue(obs.correlate_logs(obs.source_records(source), delivered))

    def test_existing_grafana_secret_loss_fails_without_changing_installation_identity(self):
        # Catches silently generating credentials/keys that disagree with the preserved Grafana DB.
        for filename in ("grafana-password", "grafana-secret-key"):
            with self.subTest(filename=filename):
                deployment = self.initialize()
                state = self.base / "state"
                before = (state / "manifest.json").read_bytes()
                lost = state / "secrets" / filename
                saved = lost.read_bytes()
                lost.unlink()
                with self.assertRaisesRegex(self.module().DeploymentError, "restore"):
                    self.initialize()
                self.assertFalse(lost.exists())
                self.assertEqual((state / "manifest.json").read_bytes(), before)
                self.assertEqual(json.loads(before)["owner"], deployment.owner)
                lost.write_bytes(saved)
                lost.chmod(0o600)

    def test_node_check_separates_missing_baseline_from_optional_no_data(self):
        # Catches absent NFS/ZFS hardware being reported as failure of healthy CPU/memory/disk monitoring.
        obs = self.module()
        self.assertTrue(hasattr(obs, "node_collector_status"), "baseline/optional collector classification is missing")
        rows = [{"metric": {"collector": c}, "value": [0, "1"]}
                for c in ("cpu", "meminfo", "diskstats", "filesystem", "netdev", "stat", "vmstat", "uname")]
        rows.append({"metric": {"collector": "nfs"}, "value": [0, "0"]})
        result = obs.node_collector_status(rows)
        self.assertEqual(result["required_missing_or_failed"], [])
        self.assertEqual(result["optional_no_data"], ["nfs"])
        rows[0]["value"][1] = "0"
        self.assertEqual(obs.node_collector_status(rows)["required_missing_or_failed"], ["cpu"])

    def test_remote_podman_server_cannot_reuse_a_local_mount_proof(self):
        # Catches proving a local bounded filesystem while starting Loki on another host.
        obs = self.module()
        self.assertTrue(hasattr(obs, "validate_local_host"), "remote-engine guard is missing")
        host = {"security": {"rootless": False}, "cgroupVersion": "v2", "cgroupManager": "systemd",
                "os": "linux", "arch": "arm64", "serviceIsRemote": False}
        self.assertEqual(obs.validate_local_host(host), "arm64")
        with self.assertRaisesRegex(obs.DeploymentError, "local"):
            obs.validate_local_host({**host, "serviceIsRemote": True})


if __name__ == "__main__":
    unittest.main()
