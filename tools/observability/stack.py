"""Own only explicitly labelled Podman resources and preserve their data."""

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import secrets
import socket
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
OWNER_LABEL = "io.glamdb.observability.owner"
SPEC_LABEL = "io.glamdb.observability.spec"
PORTS = {"prometheus": 9090, "grafana": 3000, "loki": 3100, "loki-grpc": 9096,
         "alloy": 12345, "alertmanager": 9093, "node": 9100, "podman": 9882,
         "cgroup": 13232, "cgroup-systemd": 13233}
RESOURCES = {"prometheus": (512, 0.5, 256), "grafana": (768, 0.75, 256),
             "loki": (1024, 1.5, 256), "alloy": (256, 0.5, 128),
             "alertmanager": (128, 0.25, 128), "node": (128, 0.25, 128),
             "podman": (128, 0.25, 128), "cgroup": (64, 0.25, 64),
             "cgroup-systemd": (64, 0.25, 64)}
ORDER = ("loki", "alertmanager", "node", "podman", "cgroup", "cgroup-systemd",
         "alloy", "grafana", "prometheus")
VOLUMES = {"prometheus": "/prometheus", "grafana": "/var/lib/grafana",
           "alertmanager": "/alertmanager", "alloy": "/var/lib/alloy"}


class DeploymentError(RuntimeError):
    pass


def encoded(value):
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def digest(value):
    return hashlib.sha256(value).hexdigest()


def safe_path(value, field):
    if not isinstance(value, str) or not value.startswith("/") or any(c in value for c in "\n\r\x00:"):
        raise DeploymentError(f"{field} must be an absolute path without colon/control characters")
    path = Path(value)
    if path == Path("/") or path.is_symlink():
        raise DeploymentError(f"unsafe {field}")
    return path.resolve()


def require_owned(record, owner, name):
    labels = record.get("Labels") or record.get("Config", {}).get("Labels") or {}
    if labels.get(OWNER_LABEL) != owner:
        raise DeploymentError(f"resource {name} is not owned by this installation; left untouched")


def validate_mount(info, target, max_bytes):
    if (info.get("target") != str(target) or str(target) == "/"
            or info.get("fstype") not in ("ext4", "xfs") or info.get("fsroot") != "/"
            or not info.get("uuid") or not 0 < info.get("capacity_bytes", 0) <= max_bytes):
        raise DeploymentError("Loki requires a whole dedicated persistent ext4/XFS mount with UUID and bounded capacity")


def write_if_changed(path, content, mode=0o644):
    data = content.encode() if isinstance(content, str) else content
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise DeploymentError(f"refusing unsafe generated path {path}")
    if not path.exists() or path.read_bytes() != data:
        temporary = path.with_name(path.name + ".tmp")
        if temporary.is_symlink():
            raise DeploymentError("unsafe temporary file")
        with temporary.open("wb") as stream:
            stream.write(data)
        temporary.chmod(mode)
        temporary.replace(path)
    path.chmod(mode)


def read_private(path):
    path = safe_path(path, "secret file")
    if not path.is_file() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise DeploymentError("secret input must be a private regular file, mode0600 or0400")
    data = path.read_bytes()
    if not data.strip() or len(data) > 65536:
        raise DeploymentError("secret input is empty or too large")
    return data


def source_records(path):
    """Read a bounded tail; never manufacture application traffic or print its payload."""
    if not path.is_file() or path.is_symlink():
        return []
    with path.open("rb") as stream:
        size = stream.seek(0, 2)
        start = max(0, size - 256 * 1024)
        stream.seek(start)
        lines = stream.read(256 * 1024).splitlines()
    if start:
        lines = lines[1:]
    records = []
    for line in reversed(lines):
        try:
            item = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            continue
        schema = item.get("schema") if isinstance(item, dict) else None
        valid_schema = (type(schema) is int and schema == 1) or (type(schema) is str and schema == "1")
        if (isinstance(item, dict) and valid_schema
                and re.fullmatch(r"[a-f0-9]{32}", str(item.get("request_id", "")))
                and item.get("event") and item.get("timestamp")):
            records.append({key: str(item[key]) for key in ("request_id", "event", "timestamp")})
            if len(records) == 3:
                break
    return records


def correlate_logs(records, result):
    if not records:
        return False
    delivered = set()
    for stream in result.get("data", {}).get("result", []):
        for _, line in stream.get("values", []):
            try:
                item = json.loads(line)
            except (ValueError, TypeError):
                continue
            if type(item.get("schema")) is int and item["schema"] == 1:
                delivered.add(tuple(str(item.get(k, "")) for k in ("request_id", "event", "timestamp")))
    return all(tuple(item[k] for k in ("request_id", "event", "timestamp")) in delivered for item in records)


def node_collector_status(rows):
    required = ("cpu", "meminfo", "diskstats", "filesystem", "netdev", "stat", "vmstat", "uname")
    values = {r["metric"]["collector"]: float(r["value"][1]) for r in rows}
    return {"required_missing_or_failed": sorted(c for c in required if values.get(c) != 1),
            "optional_no_data": sorted(c for c, value in values.items() if c not in required and value != 1)}


def validate_local_host(host):
    if host.get("serviceIsRemote") is not False:
        raise DeploymentError("Podman must be local: a local mount proof cannot authorize a remote server")
    if (host["security"]["rootless"] or host["cgroupVersion"] != "v2" or host["os"] != "linux"
            or host.get("cgroupManager") != "systemd"):
        raise DeploymentError("rootful local Linux Podman with cgroup v2 and systemd cgroupManager is required")
    arch = {"x86_64": "amd64", "aarch64": "arm64", "amd64": "amd64", "arm64": "arm64"}.get(host["arch"])
    if arch is None:
        raise DeploymentError("unsupported host architecture")
    return arch


class Deployment:
    def __init__(self, config_path):
        self.config_path = Path(config_path).resolve()
        try:
            self.config = json.loads(self.config_path.read_text())
        except (ValueError, OSError) as exc:
            raise DeploymentError("cannot read JSON configuration") from exc
        c = self.config
        if c.get("schema_version") != 1:
            raise DeploymentError("unsupported config schema_version")
        allowed = {"schema_version", "name", "cluster", "node", "state_dir", "metrics", "logs", "loki",
                   "ports", "ui_bind", "telegram", "resources", "retention", "podman_binary"}
        if set(c) - allowed:
            raise DeploymentError("unknown configuration fields: " + ", ".join(sorted(set(c) - allowed)))
        self.name = c.get("name", "")
        if not re.fullmatch(r"[a-z][a-z0-9-]{2,39}", self.name):
            raise DeploymentError("name must be3-40 lowercase letters/digits/hyphens")
        for field in ("cluster", "node"):
            if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,63}", c.get(field, "")):
                raise DeploymentError(f"invalid {field} label")
        self.state = safe_path(c.get("state_dir"), "state_dir")
        self.generated = self.state / "generated"
        self.private = self.state / "secrets"
        self.mount = safe_path(c.get("loki", {}).get("mount_path"), "loki.mount_path")
        if self.mount == self.state or self.mount in self.state.parents or self.state in self.mount.parents:
            raise DeploymentError("Loki mount and deployment state must be separate paths")
        self.max_bytes = c["loki"].get("max_bytes", 0)
        if not isinstance(self.max_bytes, int) or self.max_bytes < 64 * 1024 * 1024:
            raise DeploymentError("loki.max_bytes must be at least64MiB")
        self.logs = safe_path(c.get("logs", {}).get("directory"), "logs.directory")
        for key in ("uid", "gid"):
            if not isinstance(c["logs"].get(key), int) or c["logs"][key] < 0:
                raise DeploymentError("logs.uid/gid must be nonnegative integers for the real producer")
        self.ports = {**PORTS, **c.get("ports", {})}
        if (set(self.ports) != set(PORTS) or any(not isinstance(p, int) or not 1024 <= p <= 65535
                                               for p in self.ports.values())
                or len(set(self.ports.values())) != len(self.ports)):
            raise DeploymentError("service ports must be unique nonprivileged TCP ports")
        try:
            self.ui_bind = str(ipaddress.IPv4Address(c.get("ui_bind", "127.0.0.1")))
        except ValueError as exc:
            raise DeploymentError("ui_bind must be an explicit IPv4 address") from exc
        if not c.get("metrics", {}).get("core"):
            raise DeploymentError("a genuine core metrics URL is required")
        if set(c["metrics"]) - {"core", "gateway", "edge"}:
            raise DeploymentError("unknown metrics input")
        for name, target in c["metrics"].items():
            if target is None:
                continue
            if not isinstance(target, dict) or set(target) - {"url", "bearer_token_file"}:
                raise DeploymentError("metrics inputs require url and optional bearer_token_file")
            parsed = urllib.parse.urlsplit(target.get("url", ""))
            if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username
                    or parsed.password or parsed.query or parsed.fragment or any(ch in target["url"] for ch in "\r\n")):
                raise DeploymentError("metrics URL must not contain credentials, query secrets, or fragments")
            try:
                parsed.port
            except ValueError as exc:
                raise DeploymentError("invalid metrics URL port") from exc
        self.resources = {k: list(v) for k, v in RESOURCES.items()}
        for name, values in c.get("resources", {}).items():
            if (name not in self.resources or not isinstance(values, list) or len(values) != 3
                    or not isinstance(values[0], int) or not 32 <= values[0] <= 65536
                    or not isinstance(values[1], (int, float)) or not 0.05 <= values[1] <= 64
                    or not isinstance(values[2], int) or not 32 <= values[2] <= 4096):
                raise DeploymentError("resources map service to [memoryMiB, cpus, pidsLimit]")
            self.resources[name] = values
        self.lock = json.loads((ROOT / "deploy/observability/versions.lock.json").read_text())
        self.owner = None
        self.manifest = None
        self.observation_after = {}

    def immutable(self):
        return {"name": self.name, "state_dir": str(self.state), "loki_mount": str(self.mount),
                "loki_max_bytes": self.max_bytes}

    def load_state(self):
        try:
            manifest = self.state / "manifest.json"
            if manifest.is_symlink():
                raise DeploymentError("unsafe manifest path")
            self.manifest = json.loads(manifest.read_text())
        except (OSError, ValueError) as exc:
            raise DeploymentError("installation state is absent; run init/up first") from exc
        if self.manifest.get("immutable") != self.immutable():
            raise DeploymentError("immutable deployment/storage identity changed")
        self.owner = self.manifest["owner"]

    def initialize(self):
        if self.state.exists() and not (self.state / "manifest.json").exists():
            if any(self.state.iterdir()):
                raise DeploymentError("refusing unowned nonempty state directory")
        self.state.mkdir(parents=True, exist_ok=True)
        self.state.chmod(0o700)
        manifest_path = self.state / "manifest.json"
        if manifest_path.exists():
            self.manifest = json.loads(manifest_path.read_text())
            if self.manifest.get("immutable") != self.immutable():
                raise DeploymentError("immutable deployment/storage identity changed; migrate explicitly")
            self.owner = self.manifest["owner"]
        else:
            self.owner = str(uuid.uuid4())
            self.manifest = {"schema_version": 1, "owner": self.owner, "immutable": self.immutable(), "initialized": False}
            # Persist a claim before other writes so a failed first configuration can be retried safely.
            write_if_changed(manifest_path, encoded(self.manifest), 0o600)
        self.private.mkdir(exist_ok=True)
        self.private.chmod(0o700)
        self.generated.mkdir(exist_ok=True)
        self.generated.chmod(0o755)
        for name in ("grafana-password", "grafana-secret-key"):
            path = self.private / name
            if not path.exists():
                if self.manifest.get("initialized", True):
                    raise DeploymentError("existing Grafana secret is missing; restore the original file from backup before up; reset/migration must be explicit")
                write_if_changed(path, secrets.token_urlsafe(36) + "\n", 0o600)
            elif path.is_symlink() or not path.is_file():
                raise DeploymentError("unsafe Grafana secret path")
            path.chmod(0o600)
        telegram = self.config.get("telegram")
        if telegram:
            if set(telegram) != {"bot_token_file", "chat_id_file"}:
                raise DeploymentError("Telegram requires bot_token_file and chat_id_file")
            for field, output in (("bot_token_file", "telegram-bot-token"), ("chat_id_file", "telegram-chat-id")):
                data = read_private(telegram[field])
                if field == "chat_id_file" and not re.fullmatch(rb"-?[0-9]+\s*", data):
                    raise DeploymentError("Telegram chat_id file must contain an integer")
                write_if_changed(self.private / output, data, 0o600)
        for name, target in self.config["metrics"].items():
            if target and target.get("bearer_token_file"):
                write_if_changed(self.private / (name + "-bearer-token"), read_private(target["bearer_token_file"]), 0o600)
        self.render()
        hashes = {str(p.relative_to(self.generated)): digest(p.read_bytes())
                  for p in sorted(self.generated.rglob("*")) if p.is_file()}
        self.manifest["generated_sha256"] = hashes
        self.manifest["config_path"] = str(self.config_path)
        self.manifest["initialized"] = True
        write_if_changed(manifest_path, encoded(self.manifest), 0o600)
        return self.manifest

    @contextlib.contextmanager
    def locked(self, initialize=False):
        # Lock before initialization: simultaneous first runs must share one owner/password.
        self.state.parent.mkdir(parents=True, exist_ok=True)
        lock = self.state.with_name("." + self.state.name + ".observability.lock")
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(descriptor, "a+") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise DeploymentError("unsafe deployment lock")
            os.fchmod(stream.fileno(), 0o600)
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            if initialize:
                self.initialize()
            else:
                self.load_state()
            yield

    def prometheus_config(self):
        p = self.ports
        labels = {"cluster": self.config["cluster"], "node": self.config["node"]}
        jobs = []
        for name in ("prometheus", "grafana", "loki", "alloy", "alertmanager", "node", "podman"):
            jobs.append({"job_name": name, "sample_limit": 50000, "label_limit": 30,
                         "static_configs": [{"targets": [f"127.0.0.1:{p[name]}"],
                                             "labels": {**labels, "service": "host" if name == "node" else name}}]})
        jobs.append({"job_name": "cgroup", "sample_limit": 50000, "label_limit": 30,
                     "static_configs": [{"targets": [f"127.0.0.1:{p['cgroup']}", f"127.0.0.1:{p['cgroup-systemd']}"],
                                         "labels": {**labels, "service": "monitoring"}}]})
        for name, target in self.config["metrics"].items():
            if not target:
                continue
            url = urllib.parse.urlsplit(target["url"])
            job = {"job_name": "glamdb-" + name, "sample_limit": 20000, "label_limit": 30,
                   "scheme": url.scheme, "metrics_path": url.path or "/metrics",
                   "static_configs": [{"targets": [url.netloc], "labels": {
                       **labels, "service": "glamdb", "service_instance": name}}]}
            if target.get("bearer_token_file"):
                job["authorization"] = {"credentials_file": "/run/secrets/" + name + "-bearer-token"}
            jobs.append(job)
        return {"global": {"scrape_interval": "15s", "scrape_timeout": "10s", "evaluation_interval": "15s",
                           "external_labels": {"cluster": self.config["cluster"]}},
                "scrape_configs": jobs, "rule_files": ["/etc/observability/rules/*.yml"],
                "alerting": {"alertmanagers": [{"static_configs": [{"targets": [f"127.0.0.1:{p['alertmanager']}"]}]}]}}

    def render_rules(self):
        spec = importlib.util.spec_from_file_location("glamdb_alert_generator", ROOT / "tools/alerting/generate.py")
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        config = json.loads((ROOT / "deploy/alerting/checks.json").read_text())
        extension = json.loads((ROOT / "deploy/monitoring/collectors/checks.extension.json").read_text())
        config["selectors"].update(extension["selectors"])
        config["scope"]["cluster"] = self.config["cluster"]
        for key, selector in config["selectors"].items():
            config["selectors"][key] = selector.replace('cluster="local"', "cluster=" + json.dumps(self.config["cluster"]))
        merged = {c["id"]: c for c in config["checks"]}
        for check in extension["checks"]:
            merged[check["id"]] = check
        for name, override in extension.get("overrides", {}).items():
            if name in merged:
                merged[name].update(override)
        selected = []
        for name, check in merged.items():
            if name.startswith(("pod_", "macos_")):
                continue
            if (name.startswith("gateway_") or name.startswith("http_")) and not self.config["metrics"].get("gateway"):
                continue
            if name.startswith("edge_") and not self.config["metrics"].get("edge"):
                continue
            if check.get("scope", {}).get("node") == "all":
                check["scope"]["node"] = self.config["node"]
            selected.append(check)
        config["checks"] = selected
        write_if_changed(self.generated / "checks.json", encoded(config))
        write_if_changed(self.generated / "rules/states.yml", encoded(generator.rules(config)))
        recording_source = ROOT / "deploy/monitoring/prometheus/rules/recording.yml"
        write_if_changed(self.generated / "rules/recording.yml", recording_source.read_bytes())

    def render(self):
        write_if_changed(self.generated / "prometheus.json", encoded(self.prometheus_config()))
        self.render_rules()
        loki = (ROOT / "deploy/logging/loki.yaml").read_text()
        for key, value in {"LOKI_LISTEN_ADDRESS": "127.0.0.1", "LOKI_HTTP_PORT": str(self.ports["loki"]),
                           "LOKI_GRPC_PORT": str(self.ports["loki-grpc"]), "LOKI_DATA_DIR": "/loki/data"}.items():
            loki = loki.replace("${" + key + "}", value)
        write_if_changed(self.generated / "loki.yaml", loki)
        alloy = (ROOT / "deploy/logging/config.alloy").read_text() + "\n" + (ROOT / "deploy/logging/sources/file.alloy").read_text()
        write_if_changed(self.generated / "config.alloy", alloy)
        if self.config.get("telegram"):
            # Read the current maintained native Telegram template, including grouping/rate controls.
            alertmanager = (ROOT / "deploy/alerting/alertmanager.yml").read_text()
        else:
            alertmanager = encoded({"global": {"resolve_timeout": "5m"},
                                    "route": {"receiver": "visible-only", "group_by": ["cluster", "service"],
                                              "group_wait": "30s", "group_interval": "5m", "repeat_interval": "4h"},
                                    "receivers": [{"name": "visible-only"}]})
        write_if_changed(self.generated / "alertmanager.yml", alertmanager)
        sources = {"apiVersion": 1, "datasources": [
            {"name": "Prometheus", "uid": "prometheus", "type": "prometheus", "access": "proxy",
             "url": f"http://127.0.0.1:{self.ports['prometheus']}", "isDefault": True, "editable": False},
            {"name": "Loki", "uid": "loki", "type": "loki", "access": "proxy",
             "url": f"http://127.0.0.1:{self.ports['loki']}", "editable": False, "jsonData": {"maxLines": 1000}}]}
        write_if_changed(self.generated / "grafana/provisioning/datasources/default.yml", encoded(sources))
        provider = {"apiVersion": 1, "providers": [{"name": "GlamDB", "orgId": 1, "folder": "GlamDB",
                    "type": "file", "disableDeletion": False, "editable": False, "updateIntervalSeconds": 15,
                    "options": {"path": "/etc/observability/grafana/dashboards"}}]}
        write_if_changed(self.generated / "grafana/provisioning/dashboards/default.yml", encoded(provider))
        for source in (ROOT / "deploy/monitoring/grafana/dashboards").glob("*.json"):
            if source.name == "macos-node.json":
                continue
            dashboard = json.loads(source.read_text())
            if source.name == "glamdb-overview.json":
                layers = ["Real Linux host", "Podman containers", "existing native DB"]
                for component in ("gateway", "edge"):
                    if self.config["metrics"].get(component):
                        layers.append("configured existing " + component)
                scope = " + ".join(layers) + ". Sources and layers stay separate. "
                scope += "States: **0 OK · 1 WARN · 2 ALARM · 3 NODATA**. "
                if not self.config["metrics"].get("gateway"):
                    scope += "Gateway metrics are not configured; gateway panels remain NoData. "
                if not self.config["metrics"].get("edge"):
                    scope += "Outer-edge metrics are not configured. "
                scope += "Kubernetes/PVC fields require a real cluster. Logs omit request bodies and credentials."
                for item in dashboard["panels"]:
                    if item["type"] == "text" and item["title"] == "Monitoring scope":
                        item["options"]["content"] = scope
                dashboard["links"] = [item for item in dashboard.get("links", [])
                                      if "glamdb-macos-node" not in item.get("url", "")]
                node_uid = json.loads((source.parent / "node-exporter.json").read_text())["uid"]
                dashboard["links"].insert(0, {"title": "Linux host", "type": "link", "url": "/d/" + node_uid})
            write_if_changed(self.generated / "grafana/dashboards" / source.name, encoded(dashboard))
        for directory in self.generated.rglob("*"):
            if directory.is_dir():
                directory.chmod(0o755)

    def image(self, name, arch):
        entry = self.lock["images"]["cgroup" if name == "cgroup-systemd" else name]
        image = entry.get("image") or entry["platform_images"].get(arch)
        if not image or not re.search(r"@sha256:[0-9a-f]{64}$", image):
            raise DeploymentError("no immutable image for this architecture")
        return image

    def service_specs(self, arch):
        if arch not in ("amd64", "arm64"):
            raise DeploymentError("supported Linux architectures: amd64/arm64")
        p = self.ports
        specs = {}
        common = ["--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                  "--log-driver", "k8s-file", "--log-opt", "max-size=10mb", "--restart", "unless-stopped"]
        mount_config = ["--volume", str(self.generated) + ":/etc/observability:ro,z"]
        for name in ORDER:
            options = common + mount_config
            if name in VOLUMES:
                options += ["--volume", self.name + "-" + name + "-data:" + VOLUMES[name] + ":U"]
            specs[name] = {"image": self.image(name, arch), "network": "host", "options": options, "command": []}
        retention = self.config.get("retention", {})
        size = retention.get("prometheus_size", "2GB")
        duration = retention.get("prometheus_time", "7d")
        if not re.fullmatch(r"[1-9][0-9]*(?:MB|GB)", size) or not re.fullmatch(r"[1-9][0-9]*(?:h|d)", duration):
            raise DeploymentError("invalid Prometheus retention size/time")
        specs["prometheus"]["command"] = ["--config.file=/etc/observability/prometheus.json", "--storage.tsdb.path=/prometheus",
            "--storage.tsdb.retention.size=" + size, "--storage.tsdb.retention.time=" + duration,
            f"--web.listen-address=127.0.0.1:{p['prometheus']}"]
        for name, target in self.config["metrics"].items():
            if target and target.get("bearer_token_file"):
                specs["prometheus"]["options"] += ["--volume", str(self.private / (name + "-bearer-token")) + ":/run/secrets/" + name + "-bearer-token:ro,Z"]
        env = {"GF_SERVER_HTTP_ADDR": self.ui_bind, "GF_SERVER_HTTP_PORT": str(p["grafana"]),
               "GF_PATHS_DATA": "/var/lib/grafana", "GF_PATHS_PROVISIONING": "/etc/observability/grafana/provisioning",
               "GF_SECURITY_ADMIN_USER": "admin", "GF_SECURITY_ADMIN_PASSWORD__FILE": "/run/secrets/grafana-password",
               "GF_SECURITY_SECRET_KEY__FILE": "/run/secrets/grafana-secret-key", "GF_USERS_ALLOW_SIGN_UP": "false",
               "GF_USERS_DEFAULT_THEME": "light",
               "GF_ANALYTICS_REPORTING_ENABLED": "false", "GF_ANALYTICS_CHECK_FOR_UPDATES": "false",
               "GF_PLUGINS_PREINSTALL_DISABLED": "true", "GF_PLUGINS_PREINSTALL_AUTO_UPDATE": "false", "GF_METRICS_ENABLED": "true"}
        for key, value in env.items():
            specs["grafana"]["options"] += ["--env", key + "=" + value]
        for name in ("grafana-password", "grafana-secret-key"):
            specs["grafana"]["options"] += ["--volume", str(self.private / name) + ":/run/secrets/" + name + ":ro,Z"]
        specs["grafana"]["options"] += ["--tmpfs", "/tmp:rw,size=64m,mode=1777"]
        specs["loki"]["options"] += ["--volume", str(self.mount) + ":/loki:rw,Z", "--tmpfs", "/tmp:rw,size=32m,mode=1777"]
        # A Podman autonomous restart must never bypass the mount/UUID/capacity guard.
        restart = specs["loki"]["options"].index("--restart") + 1
        specs["loki"]["options"][restart] = "no"
        specs["loki"]["command"] = ["-config.file=/etc/observability/loki.yaml"]
        specs["alloy"]["options"] += ["--user", str(self.config["logs"]["uid"]) + ":" + str(self.config["logs"]["gid"]),
            "--volume", str(self.logs) + ":/application-logs:ro", "--env", "GLAMDB_LOG_DIRECTORY=/application-logs",
            "--env", "GLAMDB_ENVIRONMENT=" + self.config["cluster"], "--env", f"LOKI_PUSH_URL=http://127.0.0.1:{p['loki']}/loki/api/v1/push",
            "--security-opt", "label=type:container_runtime_t"]
        specs["alloy"]["command"] = ["run", f"--server.http.listen-addr=127.0.0.1:{p['alloy']}",
                                             "--storage.path=/var/lib/alloy", "/etc/observability/config.alloy"]
        specs["alertmanager"]["command"] = ["--config.file=/etc/observability/alertmanager.yml", "--storage.path=/alertmanager",
            "--data.retention=120h", f"--web.listen-address=127.0.0.1:{p['alertmanager']}", "--cluster.listen-address="]
        if self.config.get("telegram"):
            for source, target in (("telegram-bot-token", "telegram_bot_token"), ("telegram-chat-id", "telegram_chat_id")):
                specs["alertmanager"]["options"] += ["--volume", str(self.private / source) + ":/run/secrets/" + target + ":ro,Z"]
        specs["node"]["options"] += ["--volume", "/:/host:ro,rslave", "--security-opt", "label=type:container_runtime_t"]
        specs["node"]["command"] = ["--path.rootfs=/host", "--path.procfs=/host/proc", "--path.sysfs=/host/sys",
            f"--web.listen-address=127.0.0.1:{p['node']}", "--collector.filesystem.mount-points-exclude=^/(dev|proc|sys|var/lib/containers/.+)($|/)"]
        specs["podman"]["options"] += ["--user", "0", "--volume", "/run/podman/podman.sock:/run/podman/podman.sock:ro",
            "--env", "CONTAINER_HOST=unix:///run/podman/podman.sock", "--security-opt", "label=type:container_runtime_t"]
        specs["podman"]["command"] = [f"--web.listen-address=127.0.0.1:{p['podman']}", "--web.disable-exporter-metrics", "--collector.enhance-metrics"]
        for name, glob in (("cgroup", "machine.slice/libpod-*.scope/container"),
                           ("cgroup-systemd", "system.slice/*.service/libpod-payload-*")):
            specs[name]["options"] += ["--user", "65534:65534", "--volume", "/sys/fs/cgroup:/sys/fs/cgroup:ro"]
            specs[name]["command"] = ["-listen-address", f"127.0.0.1:{p[name]}", "-cgroup", glob]
        return specs

    def run(self, args, allow_failure=False):
        result = subprocess.run([self.config.get("podman_binary", "podman"), "--remote=false", *args],
                                capture_output=True, text=True, timeout=300)
        if result.returncode and not allow_failure:
            # Do not echo argv or container logs: neither is needed to identify the failed operation.
            raise DeploymentError("Podman " + args[0] + " failed: " + result.stderr.strip()[:1000])
        return result

    def inspect(self, kind, name):
        if self.run([kind, "exists", name], True).returncode:
            return None
        value = json.loads(self.run([kind, "inspect", name]).stdout)
        return value[0] if isinstance(value, list) else value

    def platform(self):
        if platform.system() != "Linux" or os.geteuid() != 0:
            raise DeploymentError("up/check/status/down require root on the target Linux server")
        info = json.loads(self.run(["info", "--format=json"]).stdout)
        return validate_local_host(info["host"])

    def storage_info(self, prepare=False):
        result = subprocess.run(["findmnt", "--json", "--mountpoint", str(self.mount),
                                 "--output", "TARGET,SOURCE,FSTYPE,UUID,FSROOT"], capture_output=True, text=True)
        if result.returncode:
            raise DeploymentError("Loki dedicated mount is absent; refusing fallback writes to the root disk")
        filesystems = json.loads(result.stdout).get("filesystems", [])
        if len(filesystems) != 1:
            raise DeploymentError("ambiguous Loki mount")
        info = filesystems[0]
        fs = os.statvfs(self.mount)
        info["capacity_bytes"] = fs.f_blocks * fs.f_frsize
        validate_mount(info, self.mount, self.max_bytes)
        previous = self.manifest.get("loki_filesystem")
        identity = {k: info[k] for k in ("target", "fstype", "uuid")}
        if previous is not None and previous != identity:
            raise DeploymentError("Loki filesystem identity changed; refusing to adopt another mount")
        marker = self.mount / ".glamdb-observability-owner"
        if marker.exists():
            if marker.is_symlink() or marker.read_text().strip() != self.owner:
                raise DeploymentError("Loki mount is owned by another installation")
        elif prepare:
            unexpected = [p.name for p in self.mount.iterdir() if p.name != "lost+found"]
            if unexpected:
                raise DeploymentError("refusing unowned nonempty Loki filesystem")
            write_if_changed(marker, self.owner + "\n", 0o400)
        else:
            raise DeploymentError("Loki mount ownership marker is absent")
        data = self.mount / "data"
        if data.is_symlink():
            raise DeploymentError("unsafe Loki data directory")
        if prepare:
            data.mkdir(exist_ok=True)
            os.chown(data, 10001, 10001)
            data.chmod(0o700)
            self.manifest["loki_filesystem"] = identity
            write_if_changed(self.state / "manifest.json", encoded(self.manifest), 0o600)
        return info

    def preflight(self, include_unit=True):
        unit = Path("/etc/systemd/system") / (self.name + "-observability.service")
        if include_unit and unit.exists() and "# glamdb-observability-owner: " + self.owner not in unit.read_text():
            raise DeploymentError("systemd unit name belongs to another installation")
        existing = {}
        for name in ORDER:
            full = self.name + "-" + name
            record = self.inspect("container", full)
            if record:
                require_owned(record, self.owner, full)
                existing[name] = record
        for name in VOLUMES:
            full = self.name + "-" + name + "-data"
            record = self.inspect("volume", full)
            if record:
                require_owned(record, self.owner, full)
        # A new service must not collide with an unrelated loopback listener.
        for name, port in self.ports.items():
            owner = "loki" if name == "loki-grpc" else name
            if owner in existing and existing[owner]["State"]["Running"]:
                continue
            with socket.socket() as listener:
                # Match Go servers' normal restart behavior; TIME_WAIT is not an unowned listener.
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    listener.bind((self.ui_bind if name == "grafana" else "127.0.0.1", port))
                except OSError as exc:
                    raise DeploymentError(f"port {port} is already in use by an unowned listener") from exc
        return existing

    def own_secret_permissions(self):
        for p in self.private.iterdir():
            if p.is_symlink() or not p.is_file():
                raise DeploymentError("unsafe secret file")
            uid = 472 if p.name.startswith("grafana-") else 65534
            os.chown(p, uid, uid)
            p.chmod(0o600)

    def start_service(self, name, spec, existing):
        full = self.name + "-" + name
        secret_hashes = {p.name: digest(p.read_bytes()) for p in self.private.iterdir() if p.is_file()}
        fingerprint = digest(encoded({"spec": spec, "files": self.manifest["generated_sha256"],
                                      "secrets": secret_hashes, "resources": self.resources[name]}).encode())
        current = existing.get(name)
        if current and current["Config"]["Labels"].get(SPEC_LABEL) != fingerprint:
            self.run(["stop", "--time", "20", full])
            self.run(["rm", full])
            current = None
        if current:
            if not current["State"]["Running"]:
                self.run(["start", full])
            return "unchanged"
        if self.run(["image", "exists", spec["image"]], True).returncode:
            self.run(["pull", spec["image"]])
        memory, cpus, pids = self.resources[name]
        args = ["run", "-d", "--name", full, "--network", spec["network"], "--label", OWNER_LABEL + "=" + self.owner,
                "--label", SPEC_LABEL + "=" + fingerprint, "--label", "io.glamdb.observability.stack=" + self.name,
                "--memory", str(memory) + "m", "--cpus", str(cpus), "--pids-limit", str(pids),
                *spec["options"], spec["image"], *spec["command"]]
        self.run(args)
        return "created"

    def wait_http(self, url, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=3) as response:
                    if response.status == 200:
                        return
            except (OSError, urllib.error.HTTPError):
                pass
            time.sleep(1)
        raise DeploymentError("service readiness timed out at " + url)

    def install_boot_unit(self):
        unit_name = self.name + "-observability.service"
        path = Path("/etc/systemd/system") / unit_name
        marker = "# glamdb-observability-owner: " + self.owner
        if path.exists() and marker not in path.read_text():
            raise DeploymentError("systemd unit name belongs to another installation")
        def quote(value):
            return json.dumps(str(value).replace("%", "%%"))
        entry = ROOT / "scripts/observability.py"
        command = f"{quote(sys.executable)} {quote(entry)}"
        unit = (marker + "\n[Unit]\nDescription=GlamDB owned observability stack\nAfter=network-online.target podman.socket\n"
                "Wants=network-online.target\nRequires=podman.socket\nRequiresMountsFor=" + quote(self.mount) + "\n\n[Service]\n"
                "Type=oneshot\nRemainAfterExit=yes\nTimeoutStartSec=300\nTimeoutStopSec=240\n"
                f"ExecStart={command} up --config {quote(self.config_path)} --no-systemd\n"
                f"ExecStop={command} down --config {quote(self.config_path)} --no-systemd\n\n[Install]\nWantedBy=multi-user.target\n")
        write_if_changed(self.state / "systemd" / unit_name, unit)
        write_if_changed(path, unit)
        for args in (["systemctl", "daemon-reload"], ["systemctl", "enable", unit_name]):
            subprocess.run(args, check=True, capture_output=True, text=True)

    def up(self, no_systemd=False, timeout=120):
        arch = self.platform()
        self.initialize()
        existing = self.preflight(not no_systemd)
        self.storage_info(prepare=True)
        if not self.logs.is_dir():
            raise DeploymentError("real application log directory does not exist")
        if not Path("/run/podman/podman.sock").exists():
            raise DeploymentError("VM-local /run/podman/podman.sock is required; enable podman.socket, never a TCP API")
        self.own_secret_permissions()
        for name in VOLUMES:
            full = self.name + "-" + name + "-data"
            if self.inspect("volume", full) is None:
                self.run(["volume", "create", "--label", OWNER_LABEL + "=" + self.owner, full])
        specs = self.service_specs(arch)
        changed = {}
        for name in ORDER:
            if name == "alloy":
                # Do not exhaust Alloy's bounded retry queue on preexisting genuine log records.
                self.wait_http(f"http://127.0.0.1:{self.ports['loki']}/ready", timeout)
            changed[name] = self.start_service(name, specs[name], existing)
        self.wait_http(f"http://127.0.0.1:{self.ports['prometheus']}/-/ready", timeout)
        self.wait_http(f"http://127.0.0.1:{self.ports['grafana']}/api/health", timeout)
        if not no_systemd:
            self.install_boot_unit()
        return {"deployment": self.name, "services": changed,
                "grafana": f"http://{self.ui_bind}:{self.ports['grafana']}",
                "admin_password_file": str(self.private / "grafana-password"), "data_preserved": True}

    def status(self):
        self.platform()
        if not self.owner:
            self.load_state()
        result = []
        for name in ORDER:
            full = self.name + "-" + name
            record = self.inspect("container", full)
            if record:
                require_owned(record, self.owner, full)
            result.append({"service": name, "exists": record is not None,
                           "running": bool(record and record["State"]["Running"]),
                           "started_at": record["State"]["StartedAt"] if record else None,
                           "id": record["Id"] if record else None})
        return result

    def query(self, expr):
        url = f"http://127.0.0.1:{self.ports['prometheus']}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
        try:
            result = json.load(urllib.request.urlopen(url, timeout=10))
        except (OSError, ValueError) as exc:
            raise DeploymentError("cannot query the private Prometheus API") from exc
        if result.get("status") != "success":
            raise DeploymentError("Prometheus rejected a validation query")
        return result["data"]["result"]

    def observed(self, selector, job="prometheus"):
        # Instant API timestamps are evaluation times, so filter observation timestamps in PromQL.
        cutoff = self.observation_after.get(job, self.observation_after.get("prometheus", 0))
        expression = (f"({selector}) and (timestamp({selector}) >= {cutoff}) "
                      f"and (time() - timestamp({selector}) >= -5) "
                      f"and (time() - timestamp({selector}) < 90)")
        return self.query(expression)

    def logging_check(self):
        records = source_records(self.logs / "glamdb.jsonl")
        telemetry = {}
        for metric in ("loki_source_file_files_active_total", "loki_source_file_read_lines_total",
                       "loki_source_file_file_bytes_total", "loki_source_file_read_bytes_total",
                       "loki_write_sent_entries_total", "loki_write_dropped_entries_total",
                       "loki_process_dropped_lines_total", "loki_write_batch_retries_total"):
            values = self.observed(metric + '{job="alloy"}', "alloy")
            telemetry[metric] = sum(float(r["value"][1]) for r in values) if values else None
        if not records:
            return {"result": "FAIL", "reason": "no valid genuine schema1 record in the bounded source-file tail",
                    "matched_records": 0, "telemetry": telemetry}
        ids = "|".join(sorted({item["request_id"] for item in records}))
        query = '{service="glamdb",environment=' + json.dumps(self.config["cluster"]) + '} | json | request_id=~' + json.dumps(ids)
        now = time.time_ns()
        params = {"query": query, "start": now - 24 * 3600 * 10**9, "end": now, "limit": 1000, "direction": "backward"}
        url = f"http://127.0.0.1:{self.ports['loki']}/loki/api/v1/query_range?" + urllib.parse.urlencode(params)
        for attempt in range(8):
            try:
                result = json.load(urllib.request.urlopen(url, timeout=5))
                if result.get("status") == "success" and correlate_logs(records, result):
                    return {"result": "PASS", "matched_records": len(records), "telemetry": telemetry,
                            "meaning": "bounded genuine source IDs/events/timestamps match stored Loki schema1 records"}
            except (OSError, ValueError):
                pass
            if attempt != 7:
                time.sleep(1)
        return {"result": "FAIL", "reason": "genuine source records are not queryable from Loki",
                "matched_records": 0, "telemetry": telemetry}

    def check(self):
        services = self.status()
        starts = {}
        for row in services:
            if row["running"]:
                timestamp = re.sub(r"(\.[0-9]{6})[0-9]+", r"\1", row["started_at"])
                starts[row["service"]] = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
        if "prometheus" not in starts:
            raise DeploymentError("Prometheus is not running; no current observation generation is available")
        self.observation_after = {name: max(value, starts["prometheus"]) for name, value in starts.items()}
        self.observation_after["cgroup"] = max(self.observation_after.get("cgroup", 0),
                                              self.observation_after.get("cgroup-systemd", 0))
        storage = self.storage_info()
        problems = [s["service"] + " is not running" for s in services if not s["running"]]
        up = self.observed("up")
        problems += ["scrape down: " + r["metric"].get("job", "unknown") for r in up if float(r["value"][1]) != 1]
        expected = {"prometheus": ["prometheus_build_info"], "grafana": ["grafana_build_info"],
                    "loki": ["loki_build_info"], "alloy": ["alloy_build_info"], "alertmanager": ["alertmanager_build_info"],
                    "node": ["node_exporter_build_info", "node_memory_MemTotal_bytes"],
                    "podman": ["podman_container_info", "podman_scrape_collector_success"],
                    "cgroup": ["cgroup_memory_current_bytes", "cgroup_memory_anon_bytes", "cgroup_pids_current"],
                    "glamdb-core": ["glamdb_operations_total", "glamdb_server_ready"]}
        if self.config["metrics"].get("gateway"):
            expected["glamdb-gateway"] = ["glamdb_gateway_http_requests_total", "glamdb_gateway_ready"]
        if self.config["metrics"].get("edge"):
            expected["glamdb-edge"] = ["caddy_http_request_duration_seconds_count"]
        checks = []
        for job, families in expected.items():
            for family in families:
                extra = ',server="workbench",handler="subroute"' if job == "glamdb-edge" else ""
                rows = self.observed(f'{family}{{job="{job}"{extra}}}', job)
                checks.append({"job": job, "family": family, "samples": len(rows)})
                if not rows:
                    problems.append("missing expected metric samples: " + job + "/" + family)
        expected_targets = len(self.prometheus_config()["scrape_configs"]) + 1
        if len(up) != expected_targets:
            problems.append(f"expected {expected_targets} target series, observed {len(up)}")
        node_status = node_collector_status(self.observed('node_scrape_collector_success{job="node"}', "node"))
        if node_status["required_missing_or_failed"]:
            problems.append("required node collectors missing/failed: " + ", ".join(node_status["required_missing_or_failed"]))
        if any(float(r["value"][1]) != 1 for r in self.observed('podman_scrape_collector_success{job="podman"}', "podman")):
            problems.append("collector reports failure: podman")
        running = json.loads(self.run(["ps", "--format=json"]).stdout)
        actual = {row["Id"][:12]: row["Names"][0] for row in running}
        inventory = self.observed('podman_container_state{job="podman"}', "podman")
        inventory_ids = {row["metric"]["id"] for row in inventory if float(row["value"][1]) == 2}
        pattern = re.compile(r"(?:machine[.]slice/libpod-|system[.]slice/[^/]+/libpod-payload-)([0-9a-f]{12})[0-9a-f]+(?:[.]scope/container)?")
        coverage = {}
        for family in ("cgroup_memory_current_bytes", "cgroup_memory_anon_bytes", "cgroup_pids_current"):
            paths = self.observed(family + '{job="cgroup"}', "cgroup")
            coverage[family] = {match[1] for row in paths if (match := pattern.fullmatch(row["metric"]["cgroup"]))}
        covered = set.intersection(*coverage.values())
        missing = [name for identity, name in actual.items() if identity not in covered or identity not in inventory_ids]
        if missing:
            problems.append("running containers lack a supported exact leaf: " + ", ".join(sorted(missing)))
        if (self.logs / "glamdb.jsonl").is_file():
            active = self.observed('loki_source_file_files_active_total{job="alloy"}', "alloy")
            if not active or sum(float(r["value"][1]) for r in active) < 1:
                problems.append("Alloy has no active real application log reader")
        else:
            problems.append("real application glamdb.jsonl is absent; log source remains NoData")
        logging = self.logging_check()
        if logging["result"] != "PASS":
            problems.append(logging["reason"])
        result = {"result": "PASS" if not problems else "FAIL", "services": services,
                  "targets": len(up), "expected_metric_samples": checks, "loki_storage": storage,
                  "container_coverage": {"running": len(actual), "exact_leaves": len(set(actual) & covered), "missing": missing,
                                         "source": "actual local Podman inventory; fresh current/anonymous/task series"},
                  "node_collectors": node_status,
                  "logging": logging,
                  "problems": problems, "notifications": "Telegram configured" if self.config.get("telegram") else "visible-only; no external notification credentials"}
        write_if_changed(self.state / "last-check.json", encoded(result), 0o600)
        return result

    def down(self, no_systemd=False):
        self.platform()
        if not self.owner:
            self.load_state()
        # Check the entire inventory before stopping a single container.
        records = {}
        for name in ORDER:
            full = self.name + "-" + name
            record = self.inspect("container", full)
            if record:
                require_owned(record, self.owner, full)
                records[name] = record
        for name in reversed(ORDER):
            if name in records:
                full = self.name + "-" + name
                if records[name]["State"]["Running"]:
                    self.run(["stop", "--time", "20", full])
                self.run(["rm", full])
        if not no_systemd:
            unit = Path("/etc/systemd/system") / (self.name + "-observability.service")
            if unit.exists():
                if "# glamdb-observability-owner: " + self.owner not in unit.read_text():
                    raise DeploymentError("foreign systemd unit; left untouched")
                subprocess.run(["systemctl", "disable", unit.name], check=True, capture_output=True)
        return {"deployment": self.name, "stopped": list(records), "volumes_preserved": True,
                "loki_filesystem_preserved": True, "secrets_preserved": True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "up", "status", "check", "down"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--no-systemd", action="store_true", help="do not install/disable a boot unit (isolated tests)")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        deployment = Deployment(args.config)
        with deployment.locked(initialize=args.command in ("init", "up")):
            if args.command == "init":
                result = {"state_dir": str(deployment.state), "generated_config": str(deployment.generated),
                          "installation_id": deployment.owner,
                          "grafana_credential_fingerprint": digest((deployment.private / "grafana-password").read_bytes()),
                          "review_before_up": True, "services": list(ORDER)}
            elif args.command == "up":
                result = deployment.up(args.no_systemd, args.timeout)
            elif args.command == "down":
                result = deployment.down(args.no_systemd)
            elif args.command == "check":
                result = deployment.check()
            else:
                result = deployment.status()
        print(encoded(result), end="")
        if isinstance(result, dict) and result.get("result") == "FAIL":
            raise SystemExit(1)
    except (DeploymentError, OSError, subprocess.SubprocessError) as exc:
        print("observability: " + str(exc), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
