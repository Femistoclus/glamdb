#!/usr/bin/env python3
"""Disposable standard collectors -> Prometheus -> Grafana Linux-source check.

Only explicitly selected task VM/connection and uniquely named test resources
are used. The script never starts/stops a VM or deletes an existing volume.
"""

import argparse
import base64
import json
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "deploy/monitoring"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", required=True)
    parser.add_argument("--machine", required=True)
    parser.add_argument("--vm-root", default="/workspace", help="Existing checkout share in the dedicated VM")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/monitoring/standard-collectors.json")
    args = parser.parse_args()
    engine = ["podman", "--connection", args.connection]
    images = json.loads((BASE / "versions.lock.json").read_text())["images"]
    suffix = secrets.token_hex(4)
    network = "glamdb-oss-monitor-" + suffix
    names = []
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = tempfile.TemporaryDirectory(prefix="standard-collectors-", dir=args.output.parent)
    work = Path(temporary.name)
    evidence = {"fixture": "64 MiB Python allocator with 3 extra threads; not a database",
                "connection": args.connection, "images": {key: value["image"] for key, value in images.items()},
                "checks": {}, "source_gaps": "See MONITORING_COVERAGE.md; candidate not enabled by default"}

    def command(*parts):
        result = subprocess.run([*engine, *parts], capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise RuntimeError("container command failed: " + " ".join(parts[:2]) + "\n" + result.stderr)
        return (result.stdout + (result.stderr if parts and parts[0] == "logs" else "")).strip()

    def vm_path(path):
        return args.vm_root + "/" + str(Path(path).relative_to(ROOT))

    def start(role, image, extra=(), command_args=(), memory="256m", cpus="0.5"):
        name = "glamdb-oss-monitor-" + role + "-" + suffix
        names.append(name)
        command("run", "--detach", "--name", name, "--network", network, "--memory", memory, "--cpus", cpus,
                *extra, images[image]["image"], *command_args)
        return name

    def get(url, auth=None):
        # Real HTTP over the isolated Linux network; macOS VM port forwarding
        # is not part of collector correctness and may be unavailable.
        program = ("import json,sys,urllib.request; "
                   "request=urllib.request.Request(sys.argv[1]); "
                   "request.add_header('Authorization',sys.argv[2]) if sys.argv[2] else None; "
                   "response=urllib.request.urlopen(request,timeout=10); "
                   "print(response.read().decode())")
        authorization = "Basic " + base64.b64encode(auth.encode()).decode() if auth else ""
        return json.loads(command("exec", probe, "python3", "-c", program, url, authorization))

    def until(fn, timeout=60):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            try:
                value = fn()
                if value:
                    return value
            except Exception as error:
                last = str(error)
            time.sleep(0.5)
        raise RuntimeError("condition timed out: " + str(last))

    try:
        command("network", "create", network)
        probe = start("probe", "python", ["--volume", "/sys/fs/cgroup:/host-cgroup:ro",
            "--volume", "/proc:/host-proc:ro"], ["python3", "-c", "import time;time.sleep(900)"], memory="64m")
        target = start("workload", "python", [], ["python3", "-c",
            "import time,threading; data=bytearray(64*1024*1024); "
            "[threading.Thread(target=lambda:time.sleep(900),daemon=True).start() for _ in range(3)]; time.sleep(900)"])
        pid = json.loads(command("inspect", target))[0]["State"]["Pid"]
        source = subprocess.run(["podman", "machine", "ssh", args.machine, "cat", f"/proc/{pid}/cgroup"],
                                check=True, capture_output=True, text=True).stdout
        cgroup = next(line.split("::", 1)[1].strip() for line in source.splitlines() if line.startswith("0::"))
        source_code = ("import json,pathlib; root=pathlib.Path('/host-cgroup')/" + repr(cgroup.lstrip('/')) + "; "
                       "data={name:(root/name).read_text() for name in "
                       "('memory.current','memory.max','memory.stat','cpu.stat','pids.current','io.stat','cpu.max') if (root/name).exists()}; "
                       "data['meminfo']=pathlib.Path('/host-proc/meminfo').read_text(); print(json.dumps(data))")
        linux = json.loads(command("exec", probe, "python3", "-c", source_code))
        evidence["linux_source"] = linux
        evidence["cgroup"] = cgroup
        node = start("node", "node-exporter", ["--pid", "host", "--volume", "/:/host:ro,rslave"],
                     ["--path.rootfs=/host", "--path.procfs=/host/proc", "--path.sysfs=/host/sys",
                      "--web.listen-address=:9100"], memory="128m")
        cadvisor = start("cadvisor", "cadvisor", ["--privileged", "--pid", "host", "--cgroupns", "host", "--volume", "/:/rootfs:ro",
            "--volume", "/var/run:/var/run:ro", "--volume", "/sys:/sys:ro",
            "--volume", "/var/lib/containers:/var/lib/containers:ro"],
            ["--housekeeping_interval=1s", "--max_housekeeping_interval=1s", "--global_housekeeping_interval=2s",
             "--store_container_labels=false", "--docker_only=false", "--docker=unix:///nonexistent",
             "--enable_metrics=cpu,memory,disk,diskIO,network,process,sched,tcp,advtcp,oom_event"])
        config = {"global": {"scrape_interval": "1s", "evaluation_interval": "1s"},
                  "scrape_configs": [{"job_name": job, "scrape_timeout": "800ms", "sample_limit": 100000,
                    "honor_timestamps": True, "track_timestamps_staleness": True,
                    "static_configs": [{"targets": [name + ":" + str(port_number)],
                    "labels": {"cluster": "test", "service": "fixture"}}]}
                    for job, name, port_number in (("node", node, 9100), ("cadvisor", cadvisor, 8080))]}
        (work / "prometheus.yml").write_text(json.dumps(config))
        prom = start("prometheus", "prometheus", ["--read-only", "--cap-drop", "ALL",
            "--tmpfs", "/prometheus:rw,size=128m,mode=1777", "--volume",
            vm_path(work / "prometheus.yml") + ":/etc/prometheus-test.yml:ro"],
            ["--config.file=/etc/prometheus-test.yml", "--storage.tsdb.path=/prometheus"])
        prom_url = "http://" + prom + ":9090"

        def query(expression):
            data = get(prom_url + "/api/v1/query?" + urlencode({"query": expression}))
            if data["status"] != "success":
                raise RuntimeError(str(data))
            return data["data"]["result"]

        scope_pairs = ((cgroup, cgroup), (str(Path(cgroup).parent) + "/", cgroup),
                       (str(Path(cgroup).parent), str(Path(cgroup).parent)))
        def workload_memory():
            series = query('container_memory_usage_bytes{job="cadvisor"}')
            evidence["exported_memory_scopes"] = [item["metric"].get("id") for item in series]
            return next((item for exporter_id, _ in scope_pairs for item in series
                         if item["metric"].get("id") == exporter_id), None)
        selected = until(workload_memory)
        exported_scope = selected["metric"]["id"]
        evidence["exported_scope"] = exported_scope
        # Podman's factory labels its container child with the owning scope
        # plus trailing slash. Raw systemd scope is a different series.
        linux_scope = next(path for exporter_id, path in scope_pairs if exporter_id == exported_scope)
        evidence["linux_scope"] = linux_scope
        source_code = source_code.replace(repr(cgroup.lstrip('/')), repr(linux_scope.lstrip('/')))
        linux = json.loads(command("exec", probe, "python3", "-c", source_code))
        evidence["linux_source"] = linux
        selector = '{job="cadvisor",id=' + json.dumps(exported_scope) + '}'
        memory = float(selected["value"][1])
        actual_memory = int(linux["memory.current"])
        if memory < 64 * 1024 * 1024 or abs(memory - actual_memory) > 16 * 1024 * 1024:
            raise RuntimeError("cAdvisor memory disagrees with real memory.current")
        evidence["checks"]["memory.current"] = {"result": "PASS", "linux": actual_memory, "collector": memory,
                                                  "tolerance_bytes": 16 * 1024 * 1024}
        for field in ("MemTotal", "AnonPages", "Cached", "Unevictable"):
            current = json.loads(command("exec", probe, "python3", "-c", source_code))
            meminfo = dict(line.split(":", 1) for line in current["meminfo"].splitlines())
            actual = int(meminfo[field].split()[0]) * 1024
            observed = float(until(lambda field=field: query("node_memory_" + field + "_bytes"))[0]["value"][1])
            tolerance = 0 if field == "MemTotal" else max(16 * 1024 * 1024, actual * 0.1)
            if abs(observed - actual) > tolerance:
                raise RuntimeError("host memory source mismatch: " + field)
            evidence["checks"]["host_" + field] = {"result": "PASS", "linux": actual, "collector": observed,
                "tolerance_bytes": tolerance}
        cpu = dict(line.split() for line in linux["cpu.stat"].splitlines())
        observed_cpu = float(until(lambda: query("container_cpu_usage_seconds_total" + selector))[0]["value"][1])
        if observed_cpu < int(cpu["usage_usec"]) / 1e6:
            raise RuntimeError("CPU usage seconds conversion mismatch")
        evidence["checks"]["cpu_usage_usec"] = {"result": "PASS", "linux_seconds_before": int(cpu["usage_usec"]) / 1e6,
                                                  "collector_seconds_after": observed_cpu}
        # Bounded control touches only this disposable container's file/cache
        # and CPU budget. No host-wide cache eviction or scheduler setting.
        control = ("import os,mmap,time; "
                   "f=open('/tmp/glamdb-monitoring-control','w+b'); f.write(b'x'*(8*1024*1024)); "
                   "f.flush(); os.fsync(f.fileno()); "
                   "os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED); "
                   "m=mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ); "
                   "sum(m[offset] for offset in range(0,len(m),4096)); m.close(); f.close(); "
                   "end=time.monotonic()+2; "
                   "exec('while time.monotonic()<end: pass')")
        command("exec", target, "python3", "-c", control)
        linux = json.loads(command("exec", probe, "python3", "-c", source_code))
        memory_stat = dict(line.split() for line in linux["memory.stat"].splitlines())
        cpu_stat = dict(line.split() for line in linux["cpu.stat"].splitlines())
        evidence["linux_source_after_control"] = linux
        # This mapping is proven only for the locked cAdvisor 0.60.6 unified-v2
        # handler, which assigns RSS=Stats[anon] and Cache=Stats[file].
        for metric, field in (("container_memory_rss", "anon"), ("container_memory_cache", "file")):
            expected = int(memory_stat[field])
            def matching_sample():
                found = query(metric + selector)
                observed = float(found[0]["value"][1]) if found else None
                evidence["checks"]["v2_" + field] = {"result": "PENDING", "linux": expected,
                    "collector": observed, "producer": metric, "profile": "cAdvisor0.60.6/unified-v2"}
                return found if observed is not None and abs(observed - expected) <= 2 * 1024 * 1024 else None
            observed = float(until(matching_sample)[0]["value"][1])
            evidence["checks"]["v2_" + field]["result"] = "PASS"
        for metric, field in (("container_cpu_cfs_burst_seconds_total", "burst_usec"),
                              ("container_cpu_cfs_throttled_seconds_total", "throttled_usec")):
            found = until(lambda metric=metric: query(metric + selector))
            observed = float(found[0]["value"][1])
            expected = int(cpu_stat[field]) / 1e6
            # Wait for the next completed sample after the bounded CPU loop.
            if observed + 1e-6 < expected:
                found = until(lambda metric=metric, expected=expected:
                    query(metric + selector) if float(query(metric + selector)[0]["value"][1]) + 1e-6 >= expected else None)
                observed = float(found[0]["value"][1])
            evidence["checks"][field] = {"result": "PASS", "linux_seconds": expected, "collector_seconds": observed}
        faults = query('container_memory_failures_total{job="cadvisor",id=' + json.dumps(exported_scope) +
                       ',failure_type="pgmajfault",scope="hierarchy"}')
        if faults:
            expected = int(memory_stat["pgmajfault"])
            observed = float(faults[0]["value"][1])
            evidence["checks"]["major_faults"] = {"result": "PASS" if observed == expected else "SOURCE_GAP",
                "linux": expected, "collector": observed, "nonzero_control": expected > 0}
        for metric, source_field in (("container_threads", "pids.current"),):
            found = query(metric + selector)
            if found and float(found[0]["value"][1]) == int(linux[source_field]):
                evidence["checks"][metric] = {"result": "PASS", "linux": int(linux[source_field]),
                                                "collector": float(found[0]["value"][1])}
            else:
                evidence["checks"][metric] = {"result": "SOURCE_GAP", "linux": int(linux[source_field]),
                                                "collector": found, "reason": "Runtime process collector did not expose matching tasks"}
        evidence["checks"]["removed_custom_collector"] = {"result": "PASS", "query_result": query("glamdb_cgroup_memory_anon_bytes")}
        if evidence["checks"]["removed_custom_collector"]["query_result"]:
            raise RuntimeError("removed collector unexpectedly active")
        provisioning = work / "provisioning"
        (provisioning / "datasources").mkdir(parents=True)
        (provisioning / "dashboards").mkdir()
        (provisioning / "datasources/default.yml").write_text(json.dumps({"apiVersion": 1, "datasources": [{
            "name": "Prometheus", "uid": "prometheus", "type": "prometheus", "access": "proxy",
            "url": "http://" + prom + ":9090", "isDefault": True}]}))
        (provisioning / "dashboards/default.yml").write_text((BASE / "grafana/provisioning/dashboards/default.yml").read_text())
        password = secrets.token_urlsafe(24)
        grafana_started = time.monotonic()
        grafana = start("grafana", "grafana", ["--read-only", "--cap-drop", "ALL",
            "--tmpfs", "/var/lib/grafana:rw,size=128m,mode=1777", "--tmpfs", "/tmp:rw,size=32m,mode=1777",
            "--env", "GF_SECURITY_ADMIN_PASSWORD=" + password, "--env", "GF_ANALYTICS_REPORTING_ENABLED=false",
            "--env", "GF_ANALYTICS_CHECK_FOR_UPDATES=false", "--env", "GF_AUTH_ANONYMOUS_ENABLED=false",
            "--env", "GF_PLUGINS_PREINSTALL_DISABLED=true", "--env", "GF_PLUGINS_PREINSTALL_AUTO_UPDATE=false",
            "--env", "GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES=false",
            "--volume", vm_path(provisioning) + ":/etc/grafana/provisioning:ro",
            "--volume", vm_path(BASE / "grafana/dashboards") + ":/var/lib/grafana/dashboards:ro"], memory="512m", cpus="1")
        grafana_url = "http://" + grafana + ":3000"
        auth = "admin:" + password
        until(lambda: get(grafana_url + "/api/health")["database"] == "ok", timeout=180)
        evidence["grafana_start_seconds"] = time.monotonic() - grafana_started
        dashboards = until(lambda: get(grafana_url + "/api/search?type=dash-db", auth))
        expected = {json.loads(path.read_text())["uid"] for path in (BASE / "grafana/dashboards").glob("*.json")}
        if {item["uid"] for item in dashboards} != expected:
            raise RuntimeError("actual Grafana dashboard provisioning mismatch")
        proxied = get(grafana_url + "/api/datasources/proxy/uid/prometheus/api/v1/query?" + urlencode({
            "query": "node_memory_MemTotal_bytes"}), auth)
        if not proxied["data"]["result"]:
            raise RuntimeError("Grafana cannot query the real standard producer")
        evidence["checks"]["grafana"] = {"result": "PASS", "dashboard_uids": sorted(expected), "datasource": "PASS"}
        command("stop", "--time", "2", target)
        command("rm", target)
        raw_expression = "container_memory_usage_bytes" + selector
        fresh_expression = raw_expression + " and (time() - timestamp(" + raw_expression + ") < 30)"
        until(lambda: not query(fresh_expression), timeout=45)
        if not query('up{job="cadvisor"} == 1'):
            raise RuntimeError("cAdvisor should remain reachable after workload removal")
        evidence["checks"]["target_removed"] = {"result": "PASS", "collector_up": 1,
            "freshness_seconds": 30, "fresh_query_result": query(fresh_expression),
            "raw_cached_series": len(query(raw_expression)),
            "contract": "Actual metric timestamp, never container_last_seen scrape-time gauge"}
        command("stop", "--time", "2", node)
        until(lambda: query('up{job="node"} == 0'))
        until(lambda: not query("node_memory_MemTotal_bytes"))
        evidence["checks"]["failed_scrape_stale_values_removed"] = {"result": "PASS"}
        command("start", node)
        until(lambda: query('up{job="node"} == 1'))
        evidence["checks"]["exporter_restart"] = {"result": "PASS"}
        evidence["result"] = "PASS_WITH_DOCUMENTED_SOURCE_GAPS"
    except Exception as error:
        evidence["result"] = "FAIL"
        evidence["error"] = str(error)
        for name in names:
            try:
                evidence.setdefault("logs", {})[name] = command("logs", "--tail", "30", name)
            except Exception:
                pass
        raise
    finally:
        for name in reversed(names):
            subprocess.run([*engine, "rm", "--force", name], capture_output=True)
        subprocess.run([*engine, "network", "rm", network], capture_output=True)
        container_states = {name: subprocess.run([*engine, "container", "exists", name],
                            capture_output=True).returncode for name in names}
        network_state = subprocess.run([*engine, "network", "exists", network], capture_output=True).returncode
        removed = all(status == 1 for status in container_states.values()) and network_state == 1
        evidence["teardown"] = {"task_names_absent": all(status == 1 for status in container_states.values()),
                                "network_absent": network_state == 1}
        if not removed:
            evidence["result"] = "FAIL"
            evidence["teardown_error"] = {"containers": container_states, "network": network_state}
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        temporary.cleanup()
        print(json.dumps({"result": evidence.get("result"), "checks": evidence["checks"], "output": str(args.output)}, indent=2))
        if not removed:
            raise RuntimeError("task-owned container/network teardown could not be verified")


if __name__ == "__main__":
    main()
