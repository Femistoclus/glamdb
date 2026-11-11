#!/usr/bin/env bash
# Build and run a local GlamDB server: natively on Linux, in Docker elsewhere.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/glamdb-local.sh COMMAND [ARGS...]

Commands:
  up      Build what changed (pinned dependencies once), then (re)start the
          server in the background and wait until it answers
  down    Stop the server; data is kept
  status  Show whether the server is running, its URL, token and data location
  logs    Show server logs; extra arguments (e.g. -f) go to docker logs / tail
  cli     Run glamdb-cli against the server, e.g. cli request --path /databases

Environment:
  GLAMDB_PORT        Host port (default 8080)
  GLAMDB_TOKEN       Bearer token; when unset, a generated owner-only token in
                     $GLAMDB_HOME/token is created once and reused
  GLAMDB_LISTEN      Host address to listen on or publish to (default 127.0.0.1)
  GLAMDB_DATA        Data: absolute directory (native) or Docker volume
                     (default $GLAMDB_HOME/data, or volume glamdb-data)
  GLAMDB_HOME        Token, native PID and log (default ~/.local/share/glamdb)
  GLAMDB_USE_DOCKER  1 for Docker, 0 for a native build (default 0 on Linux)
  GLAMDB_JOBS        Build parallelism (default 2)
  Docker only: GLAMDB_CONTAINER (glamdb), GLAMDB_BUILD_VOLUME (glamdb-build),
               GLAMDB_DEPS_VOLUME (glamdb-deps-clang18)
EOF
}

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
port=${GLAMDB_PORT:-8080}
listen=${GLAMDB_LISTEN:-127.0.0.1}
home=${GLAMDB_HOME:-${XDG_DATA_HOME:-$HOME/.local/share}/glamdb}
jobs=${GLAMDB_JOBS:-2}
if [ -z "${GLAMDB_USE_DOCKER:-}" ]; then
    if [ "$(uname -s)" = Linux ]; then GLAMDB_USE_DOCKER=0; else GLAMDB_USE_DOCKER=1; fi
fi
docker_mode=$GLAMDB_USE_DOCKER
container=${GLAMDB_CONTAINER:-glamdb}
build_volume=${GLAMDB_BUILD_VOLUME:-glamdb-build}
deps_volume=${GLAMDB_DEPS_VOLUME:-glamdb-deps-clang18}
image=glamdb-deps:clang18
native_build=$root/build/server
stamp_name=.glamdb-local-deps

fail() {
    printf 'glamdb-local: %s\n' "$*" >&2
    exit 1
}
note() { printf '==> %s\n' "$*"; }

case "$port" in
    '' | *[!0-9]*) fail "GLAMDB_PORT must be a port number, got '$port'" ;;
esac
[ "$port" -ge 1 ] && [ "$port" -le 65535 ] || fail "GLAMDB_PORT must be 1..65535"
case "$jobs" in '' | *[!0-9]* | 0) fail "GLAMDB_JOBS must be a positive integer" ;; esac
case "$docker_mode" in 0 | 1) ;; *) fail "GLAMDB_USE_DOCKER must be 0 or 1" ;; esac

# The server reads its token only from GLAMDB_TOKEN; keep it out of argv.
load_token() {
    token_source=GLAMDB_TOKEN
    if [ -n "${GLAMDB_TOKEN:-}" ]; then
        export GLAMDB_TOKEN
        return
    fi
    token_source=$home/token
    if [ ! -s "$token_source" ]; then
        mkdir -p "$home"
        (umask 077 && od -An -tx1 -N16 /dev/urandom | tr -d ' \n' >"$token_source")
        note "Generated a token in $token_source"
    fi
    GLAMDB_TOKEN=$(cat "$token_source")
    export GLAMDB_TOKEN
}

url() {
    local host=${listen#[}
    host=${host%]}
    case "$host" in
        0.0.0.0 | ::) printf 'http://127.0.0.1:%s' "$port" ;;
        *:*) printf 'http://[%s]:%s' "$host" "$port" ;;
        *) printf 'http://%s:%s' "$host" "$port" ;;
    esac
}

healthy() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsS --max-time 2 "$(url)/healthz" >/dev/null 2>&1
    else
        python3 -c 'import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=2)' \
            "$(url)/healthz" >/dev/null 2>&1
    fi
}

# Recipe fingerprint of the pinned dependency prefix; a change rebuilds it.
deps_stamp() {
    (
        cd "$root"
        cat third_party/lock.json .gitmodules
        find cmake/dependencies -type f | LC_ALL=C sort | while IFS= read -r file; do cat "$file"; done
        if [ "$docker_mode" = 1 ]; then cat docker/dev/Dockerfile; fi
    ) | cksum | cut -d' ' -f1
}

ensure_submodules() {
    local missing=0 name
    for name in "$@"; do
        [ -n "$(ls -A "$root/third_party/$name" 2>/dev/null)" ] || missing=1
    done
    if [ "$missing" = 1 ]; then
        note "Initializing submodules"
        git -C "$root" submodule update --init --recursive
    fi
}

# Writes need max(1 GiB, 5% of the filesystem) + 512 MiB free (core storage preflight).
check_space() {
    local line size available need
    line=$1
    size=$(printf '%s\n' "$line" | awk 'NR==2 {print $2}')
    available=$(printf '%s\n' "$line" | awk 'NR==2 {print $4}')
    [ -n "$size" ] && [ -n "$available" ] || return 0
    need=$((size / 20 > 1048576 ? size / 20 : 1048576))
    need=$((need + 524288))
    if [ "$available" -lt "$need" ]; then
        printf 'glamdb-local: warning: %s MiB free on the data filesystem, writes need %s MiB;\n' \
            "$((available / 1024))" "$((need / 1024))" >&2
        printf '  creating collections and appending will fail with 507 RESOURCE_EXHAUSTED.\n' >&2
        printf '  Free space or enlarge the disk (for Docker, the Docker VM disk).\n' >&2
    fi
}

wait_healthy() {
    local attempt
    for attempt in $(seq 1 100); do
        if healthy; then return 0; fi
        sleep 0.2
    done
    return 1
}

started() {
    note "GlamDB is up at $(url)"
    printf '    token: %s\n' "$([ "$token_source" = GLAMDB_TOKEN ] && echo 'from GLAMDB_TOKEN' || echo "cat $token_source")"
    printf '    data:  %s\n' "$1"
    case "$listen" in
        127.* | localhost | ::1 | '[::1]') ;;
        *) printf 'glamdb-local: warning: plain HTTP on %s; use a TLS reverse proxy for remote access\n' \
            "$listen" >&2 ;;
    esac
}

# --- Docker -----------------------------------------------------------------

docker_ready() {
    command -v docker >/dev/null 2>&1 || fail "docker is not installed"
    docker info >/dev/null 2>&1 || fail "docker is not running (start Docker Desktop or colima)"
}

docker_running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" = true ]
}

docker_up() {
    local data stamp
    docker_ready
    data=${GLAMDB_DATA:-glamdb-data}
    case "$data" in */*) fail "GLAMDB_DATA must name a Docker volume in Docker mode" ;; esac
    ensure_submodules arrow iceberg-cpp googletest cli11 asio json
    note "Preparing build images"
    docker build -q --tag glamdb-dev:clang18 - <"$root/docker/dev/Dockerfile" >/dev/null
    docker build -q --tag "$image" - <"$root/docker/deps/Dockerfile" >/dev/null
    stamp=$(deps_stamp)
    if [ "$(docker run --rm --network none -v "$deps_volume:/deps:ro" "$image" \
        cat "/deps/install/$stamp_name" 2>/dev/null || true)" != "$stamp" ]; then
        note "Building pinned dependencies into volume $deps_volume (first run: 30-60 minutes)"
        if ! GLAMDB_DEPS_VOLUME=$deps_volume "$root/scripts/build-deps-in-docker.sh"; then
            note "Incremental dependency build failed; rebuilding $deps_volume from scratch"
            GLAMDB_DEPS_CLEAN=1 GLAMDB_DEPS_VOLUME=$deps_volume "$root/scripts/build-deps-in-docker.sh"
        fi
        docker run --rm --network none -v "$deps_volume:/deps" "$image" \
            sh -c 'printf "%s\n" "$1" >"/deps/install/$2"' sh "$stamp" "$stamp_name"
    fi
    note "Building glamdb and glamdb-cli"
    docker run --rm --network none -v "$root:/src:ro" -v "$build_volume:/build" \
        --mount "type=volume,src=$deps_volume,dst=/prefix,readonly,volume-subpath=install" \
        --workdir /src "$image" sh -eu -c '
            cmake --preset release -B /build -DCMAKE_PREFIX_PATH=/prefix -DBUILD_TESTING=OFF \
                -DGLAMDB_BUILD_STORAGE=ON -DGLAMDB_IO_ONLY=OFF >/dev/null
            cmake --build /build --parallel "$1" --target glamdb_server glamdb_cli
        ' sh "$jobs"
    check_space "$(docker run --rm --network none -v "$data:/data" "$image" df -Pk /data)"
    docker_down >/dev/null
    load_token
    note "Starting container $container"
    docker run -d --name "$container" --restart unless-stopped -e GLAMDB_TOKEN \
        -p "$listen:$port:8080" -v "$root/ui:/ui:ro" -v "$build_volume:/build:ro" -v "$data:/data" \
        "$image" /build/apps/server/glamdb --data-dir /data/glamdb --listen 0.0.0.0 --port 8080 \
        --ui-root /ui >/dev/null
    if ! wait_healthy; then
        docker logs --tail 30 "$container" >&2 || true
        fail "the server did not become healthy at $(url)"
    fi
    started "Docker volume $data"
}

docker_down() {
    docker_ready
    if docker inspect "$container" >/dev/null 2>&1; then
        # SIGTERM lets admitted calls finish before the store is released.
        docker stop -t 60 "$container" >/dev/null
        docker rm "$container" >/dev/null
        note "Stopped $container"
    else
        note "$container is not running"
    fi
}

# --- Native -----------------------------------------------------------------

native_data() {
    local data parent
    data=${GLAMDB_DATA:-$home/data}
    case "$data" in /*) ;; *) data=$PWD/$data ;; esac
    parent=$(dirname -- "$data")
    mkdir -p "$parent"
    printf '%s/%s' "$(cd "$parent" && pwd -P)" "$(basename -- "$data")"
}

native_pid() {
    local pid
    pid=$(cat "$home/server.pid" 2>/dev/null || true)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && [ "$(ps -p "$pid" -o comm= 2>/dev/null)" = glamdb ] &&
        printf '%s' "$pid"
}

native_deps() {
    (cd "$root" && cmake --preset deps-local && cmake --build --preset deps-local --parallel "$jobs")
}

native_up() {
    local data stamp pid
    data=$(native_data)
    ensure_submodules arrow iceberg-cpp googletest cli11 asio json
    stamp=$(deps_stamp)
    if [ "$(cat "$root/.deps/install/$stamp_name" 2>/dev/null || true)" != "$stamp" ]; then
        note "Building pinned dependencies into .deps/install (first run: 30-60 minutes)"
        if ! native_deps; then
            note "Incremental dependency build failed; rebuilding .deps/install from scratch"
            rm -rf "$root/.deps/install" "$root/build/deps-local"
            native_deps
        fi
        printf '%s\n' "$stamp" >"$root/.deps/install/$stamp_name"
    fi
    note "Building glamdb and glamdb-cli"
    (cd "$root" && cmake --preset release -B "$native_build" -DBUILD_TESTING=OFF \
        -DGLAMDB_BUILD_STORAGE=ON -DGLAMDB_IO_ONLY=OFF >/dev/null)
    cmake --build "$native_build" --parallel "$jobs" --target glamdb_server glamdb_cli
    check_space "$(df -Pk "$(dirname -- "$data")")"
    native_down >/dev/null
    load_token
    mkdir -p "$home"
    note "Starting $native_build/apps/server/glamdb"
    nohup "$native_build/apps/server/glamdb" --data-dir "$data" --listen "$listen" --port "$port" \
        --ui-root "$root/ui" >>"$home/server.log" 2>&1 </dev/null &
    pid=$!
    printf '%s\n' "$pid" >"$home/server.pid"
    printf '%s %s\n' "$listen" "$port" >"$home/server.addr"
    if ! wait_healthy; then
        tail -n 30 "$home/server.log" >&2 || true
        fail "the server did not become healthy at $(url)"
    fi
    started "$data"
}

native_down() {
    local pid attempt
    pid=$(native_pid || true)
    if [ -z "$pid" ]; then
        rm -f "$home/server.pid"
        note "glamdb is not running"
        return
    fi
    # SIGTERM lets admitted calls finish before the store is released.
    kill -TERM "$pid"
    for attempt in $(seq 1 600); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done
    kill -0 "$pid" 2>/dev/null && fail "glamdb (pid $pid) is still draining; retry later"
    rm -f "$home/server.pid" "$home/server.addr"
    note "Stopped glamdb (pid $pid)"
}

# --- Commands ---------------------------------------------------------------

# Status and cli follow the running server's endpoint, not the current GLAMDB_PORT.
adopt_endpoint() {
    local address
    if [ "$docker_mode" = 1 ]; then
        address=$(docker port "$container" 8080/tcp 2>/dev/null | head -n 1 || true)
        [ -n "$address" ] || return 0
        port=${address##*:}
        listen=${address%:*}
    elif [ -s "$home/server.addr" ]; then
        read -r listen port <"$home/server.addr"
    fi
}

status() {
    local running=no where
    if [ "$docker_mode" = 1 ]; then
        docker_ready
        docker_running && running=yes && adopt_endpoint
        where="container $container, data volume ${GLAMDB_DATA:-glamdb-data}"
    else
        [ -n "$(native_pid || true)" ] && running=yes && adopt_endpoint
        where="data $(native_data), log $home/server.log"
    fi
    if [ "$running" = no ]; then
        note "GlamDB is not running ($where)"
        return 3
    fi
    if healthy; then note "GlamDB is up at $(url) ($where)"; else note "GlamDB is starting or unhealthy ($where)"; fi
    if [ -n "${GLAMDB_TOKEN:-}" ]; then echo '    token: from GLAMDB_TOKEN'; else echo "    token: cat $home/token"; fi
}

logs() {
    if [ "$docker_mode" = 1 ]; then
        docker_ready
        docker logs "$@" "$container"
    else
        tail -n 100 "$@" "$home/server.log"
    fi
}

cli() {
    local endpoint
    [ "$docker_mode" = 1 ] || adopt_endpoint
    endpoint=(--host 127.0.0.1 --port "$port")
    case " $* " in *' --url'* | *' --host'* | *' --port'*) endpoint=() ;; esac
    load_token
    if [ "$docker_mode" = 1 ]; then
        docker_ready
        docker_running || fail "$container is not running; run: $0 up"
        [ "${#endpoint[@]}" -gt 0 ] && endpoint=(--host 127.0.0.1 --port 8080)
        exec docker exec -i -e GLAMDB_TOKEN "$container" /build/apps/cli/glamdb-cli "$@" \
            ${endpoint[@]+"${endpoint[@]}"}
    fi
    [ -x "$native_build/apps/cli/glamdb-cli" ] || fail "glamdb-cli is not built; run: $0 up"
    exec "$native_build/apps/cli/glamdb-cli" "$@" ${endpoint[@]+"${endpoint[@]}"}
}

command=${1:-}
[ "$#" -gt 0 ] && shift
case "$command" in
    up)
        [ "$#" -eq 0 ] || fail "up takes no arguments"
        if [ "$docker_mode" = 1 ]; then docker_up; else native_up; fi
        ;;
    down)
        [ "$#" -eq 0 ] || fail "down takes no arguments"
        if [ "$docker_mode" = 1 ]; then docker_down; else native_down; fi
        ;;
    status) status ;;
    logs) logs "$@" ;;
    cli) cli "$@" ;;
    -h | --help | help) usage ;;
    *)
        usage >&2
        exit 2
        ;;
esac
