#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 EXISTING_ROOTFUL_CONNECTION [EXISTING_NETWORK]" >&2
  exit 2
fi

connection=$1
network=${2:-glamdb-observability}
engine=(podman --connection "$connection")
profile=$("${engine[@]}" info --format '{{.Host.Security.Rootless}} {{.Host.CgroupsVersion}}')
if [[ "$profile" != 'false v2' ]]; then
  echo 'This profile requires an explicitly selected rootful cgroup-v2 Podman VM.' >&2
  exit 2
fi
architecture=$("${engine[@]}" info --format '{{.Host.Arch}}')
if [[ "$architecture" != arm64 && "$architecture" != aarch64 ]]; then
  echo 'This local collector profile pins Linux arm64 images.' >&2
  exit 2
fi
"${engine[@]}" network exists "$network"

if "${engine[@]}" container exists glamdb-collectors-podman; then
  echo 'glamdb-collectors-podman already exists; left intact.'
else
  "${engine[@]}" run -d --name glamdb-collectors-podman \
    --network "$network" --network-alias glamdb-collectors-podman \
    --restart unless-stopped --memory 128m --cpus 0.25 \
    --read-only --cap-drop ALL --security-opt no-new-privileges:true \
    --security-opt label=type:container_runtime_t --user root \
    --volume /run/podman/podman.sock:/run/podman/podman.sock:ro \
    --env CONTAINER_HOST=unix:///run/podman/podman.sock \
    --label app=glamdb-collectors \
    quay.io/navidys/prometheus-podman-exporter@sha256:0a9bfa9bd6260a47d3a0bbe2ce42f41ac98854f32dab5b4d7e28725075b0c187 \
    --web.listen-address=:9882 --web.disable-exporter-metrics --collector.enhance-metrics
fi

start_cgroup_collector() {
  local name=$1
  local cgroup_glob=$2
  if "${engine[@]}" container exists "$name"; then
    echo "$name already exists; left intact."
  else
    "${engine[@]}" run -d --name "$name" \
      --network "$network" --network-alias "$name" \
      --restart unless-stopped --memory 64m --cpus 0.25 \
      --read-only --cap-drop ALL --security-opt no-new-privileges:true \
      --user 65534:65534 --volume /sys/fs/cgroup:/sys/fs/cgroup:ro \
      --label app=glamdb-collectors \
      ghcr.io/arianvp/cgroup-exporter@sha256:3e427d53195d319a1aa5ff9ee65d76b5c6e6223d1d28db06f46e07c298c6228f \
      -listen-address :13232 -cgroup "$cgroup_glob"
  fi
}

start_cgroup_collector glamdb-collectors-cgroup 'machine.slice/libpod-*.scope/container'
start_cgroup_collector glamdb-collectors-cgroup-systemd 'system.slice/*.service/libpod-payload-*'
