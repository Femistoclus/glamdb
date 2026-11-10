# Build and test scaffold validation

Date: 2026-11-10. Scope: the initial build and test scaffold described in the
[build guide](BUILDING.md).
No storage behavior or durability gate is implemented by this step.

## Environment

- Apple Silicon host, Colima 0.10.3; isolated `glamdb` VM, 4 CPUs / 4 GiB RAM.
- Ubuntu 24.04 container, Linux aarch64, Clang 18.1.3, CMake 3.28.3,
  Ninja 1.11.1, Python 3.12.3.
- Base image digest: [Dockerfile](../../docker/dev/Dockerfile).
  GoogleTest revision: [dependency lock](../../third_party/lock.json).
- No Arrow, Iceberg, or SQLite dependency build. Sources mounted read-only;
  generated files stay in the disposable container.

## Executed checks

After the isolated Docker setup in [BUILDING](BUILDING.md#linux-tests-with-docker),
run from the repository root:

```sh
export DOCKER_CONFIG="$PWD/artifacts/docker-client"
./scripts/test-in-docker.sh
./scripts/test-in-docker.sh release
./scripts/test-in-docker.sh asan-ubsan
```

All three commands exited **0**. Each run passed **4/4 CTest entries**,
including the required nonempty `unit` selection, and all three quality targets.

| Component | Test | Checked invariant | Result |
| --- | --- | --- | --- |
| Core/unit scaffold | `glamdb.unit.CoreScaffold.UsesCpp23` | Unit target links the core scaffold and uses C++23. | PASS |
| CMake configuration | `glamdb.tooling.cmake-configuration` (2 cases) | No GoogleTest required when testing is off; missing GoogleTest has an actionable diagnostic when testing is on; core stays static with `BUILD_SHARED_LIBS=ON`; only the executable is installed and runs. | PASS |
| Docker runner | `glamdb.tooling.docker-runner` (6 cases) | Profiles, paths with spaces, read-only sources, temporary builds, missing submodule, and failure propagation at every check boundary. | PASS |
| Existing executable | `glamdb.smoke` | No-op application starts and exits successfully. | PASS |
| C++ quality | `format-check`, `tidy-check`, `quality-negative-check` | LLVM 18 checks pass and reject intentional formatting/analyzer violations. | PASS |

The tooling fixtures are deterministic temporary directories and fake commands;
they do not substitute for any storage component. Local raw output is kept in
ignored `artifacts/e0-linux-{local,release,asan-ubsan}.log` and
`artifacts/e0-linux-environment.log`.

Independent reviews covered CMake/C++ wiring, Docker tooling, configuration
tests, and documentation. Documentation corrections were applied; no remaining
actionable findings were reported. The CMake reviewer also independently passed
macOS builds with tests disabled and ASan/UBSan unit tests enabled, and checked
that GoogleTest is absent from core include paths and the installed prefix.

E1–E9, real IO fault tests, library integration, Linux x86-64/ext4 qualification,
and power-loss tests are **NOT RUN**. E0 establishes development infrastructure
only; `core/core.cpp` and its unit test remain scaffolding for the next layer.
