# Storage qualification preparation

The published storage module has component and process-recovery evidence. Full
engine integration, capacity measurements and power-loss qualification remain
incomplete. This preparation kit provides a read-only environment collector and
a bounded I/O probe using disposable stores. Neither changes mounts, formats
devices or switches power.

## Test machines

Prepare a Linux x86-64 host with a dedicated, disposable local ext4 volume.
Use `data=ordered` with barriers and flush enabled. Keep the catalog and warehouse
on that same volume. The operating system, source checkout and evidence directory
must survive interruption of the test volume. Docker on macOS remains useful for
component tests, but does not qualify this profile.

A separate controller must keep the received-ACK ledger and control the power
switch. Its storage and power supply must remain available during every cut.
Record the actual host, device, controller, firmware and cache settings. Mount
options alone cannot prove that hardware honors flush/FUA or that the controller
has an independent power supply. These require explicit supporting evidence.

Keep SQLite's temporary directory on local disk with room for the 256 MiB
recovery visited index. Its database is disposable and separate from the
persistent catalog; failure to create or grow it must fail recovery.

Install Python 3, util-linux (`findmnt`, `lsblk`), Git, the pinned C++ toolchain and
the project's dependency prefix. Do not reuse production stores. Choose a new
evidence directory for each campaign and retain raw outputs as well as summaries.

## Read-only preflight

Run from the source checkout on the future test host. The output directory must
already exist on a different filesystem from the disposable volume:

```sh
python3 probes/qualification/environment.py \
    --volume /mnt/glamdb-test --dedicated-test-volume \
    --output /srv/glamdb-evidence/environment.json
```

The volume argument is the mountpoint itself, not a store subdirectory. The
dedicated-volume flag is the operator's assertion that the volume is disposable;
the program cannot infer that from its contents. Existing output files are never
overwritten. Omitting `--output` prints JSON to stdout. Keep any redirected output
off the test volume as well.

Exit 0 means the automated environment checks found no incompatibility; exit 1
means a mismatch or an unavailable observation; exit 2 means arguments or output
failed. Every report retains `power_durability: NOT_VALIDATED` and a list of
manual checks. The collector records source revision and dependency pins, but
does not substitute them for the actual loaded engine/library versions.

The collector's bounded-command, parser, filesystem and output checks are tested
without a dedicated volume:

```sh
python3 tests/tooling/test_qualification_environment.py
```

These tests are included in the `glamdb.tooling` CTest suite.
All sixteen collector tests passed in Linux ARM64 Docker. They exercise fixture
mount/device observations, malformed and unavailable command output, output
isolation and bounded child-process cleanup; no test volume was qualified.

## Bounded file-I/O probe

Build the pinned dependency prefix as described in [Building](BUILDING.md), then
enable the optional probe on Linux:

```sh
cmake --preset local -DGLAMDB_IO_ONLY=ON \
    -DGLAMDB_BUILD_STORAGE=ON -DGLAMDB_BUILD_IO_PROBE=ON
cmake --build --preset local --target io-probe
python3 probes/io/run.py --probe build/local/probes/io/io-probe \
    --stores /tmp/glamdb-probe-stores --output /tmp/glamdb-probe-evidence --strace
```

Both directories must be new, disjoint paths with existing parents. Each case
runs in a separate subprocess and disposable store, with a default 60-second
timeout. The runner never removes the stores or evidence. Install `strace` for
the native syscall audit. Docker on macOS can run this component check; it is
not a replacement for the dedicated qualification host.

The success case creates a table and appends two batches of 1000 rows through
real Parquet/Avro writers and Iceberg SqlCatalog. Its CatalogStore is explicitly
an in-memory test double. The other 32 cases cover writes, short writes, sync,
close and directory-sync failures for all four file kinds, calibrated footer
and trailer writes, repeated close, the Avro destructor error path, inactive
attempts, exclusive creation, EINTR and zero writes.

`summary.json` records case results and the native open audit separately. Without
`--strace`, passing cases leave `p_io: INCOMPLETE`; process exit 0 alone is not a
complete probe result. With strace, the verifier checks registered file accesses,
exclusive creation, absence of unexpected mutations, and write/sync/close/directory
ordering. The trace is audited only for the success case. A traced ASan process
disables LeakSanitizer because it cannot operate under ptrace; other cases retain
the supplied sanitizer configuration.

The audit includes the store parent's directory sync and rejects unsupported
native content mutations. Negative controls remove or fail the parent sync and
inject writes that bypass the tracked stream.

Per-case evidence includes native results, I/O events, file lengths, format
checks, raw metadata/Avro files and decoded manifest entries. Inventory
`latest_root` identifies the checked reachable closure; `published_roots` is
the history of successful test-catalog publications, whose old root files need
not remain reachable from the latest root. Orphans are reported separately.

The runner/parser has negative-control and fixture tests, included in the
`glamdb.tooling` CTest suite:

```sh
python3 tests/tooling/test_io_probe.py
```

All 33 native cases and the success syscall audit passed in Linux ARM64 Docker
with the storage ASan/UBSan profile. The run recorded 3076 syscalls and calibrated
the second-append data/manifest/list final writes to ordinals 7/4/4. These are
observations of the pinned build, not hard-coded write positions. This evidence
does not establish production catalog integration or power-loss durability.

## Evidence to retain

| Record | Required contents |
| --- | --- |
| Environment | Kernel, architecture, mount source/options, block-device stack, device/firmware/cache and flush evidence, compiler and dependency revisions, actual SQLite build/VFS and working connection settings. |
| Execution | Source revision, exact command and exit status, configuration, fixture generator, seed, start/end times, raw event trace and outcome. |
| Acknowledgements | Operation ID, canonical hash and complete received receipt, persisted by the independent controller before the cut. |
| Recovery | Authoritative catalog pointers and receipts, retained snapshots and file inventory, independently checked row results, missing/duplicate operations and corruption findings. |
| Resources | Continuous RSS, allocator domains, metadata materialization, physical orphan bytes, file/footer I/O, fsync counts, open time and latency distribution. |

The environment collector reports observable mismatches and outstanding manual
checks. A successful preflight is not power-loss validation. Its report must keep
that distinction even when every automated check passes.

## Remaining campaigns

1. Connect the production catalog/coordinator to the storage primitives and the
   scanner. Run create, two appends, historical/current reads, duplicate keys,
   orphan exclusion and receipt lookup in a new process. A test catalog or raw
   Parquet reader cannot replace this integrated check.
2. Calibrate one shared POSIX/SQLite durability model. It must detect the three
   broken variants omitting data sync, directory sync and catalog sync, and
   preserve the positive protocol across twenty fixed seeds.
3. Run at least 100 deterministic modeled cuts and 100 seeded schedules across
   file, directory, SQLite journal/database, outer-commit and ACK boundaries.
   Keep process-kill results separate: they leave the OS cache alive.
4. Once the environment and controller are approved for the disposable volume,
   run at least twenty actual power-cut cycles near ACK and SQL boundaries.
   Every acknowledged operation must survive exactly once; an unacknowledged
   operation may be absent or fully committed, never partially published.
5. Measure independent capacity axes: 4096 snapshots, 16384 data files / 4096
   manifests, maximum append change-set and +1, and cold recovery with 4096
   receipts under a 1 GiB cgroup. The 16 GiB decoded scanner fixture uses 64
   component-generated 256 MiB files; it is not public-ingress evidence.
6. Measure 5000 real ingress attempts of 1000 rows (about 1 MiB decoded) using a
   fixed seed. Record checkpoints every 100 successes and test sixteen competing
   callers. Capacity exhaustion must preserve old retries and prevent partial
   publication. Accepted boundaries must reopen with the same limits within
   120 seconds on the recorded reference SSD; lower limits must fail explicitly.

The production backend/scanner, shared power model, controller integration and
these campaigns are not supplied by the environment collector. Keep their
status NOT RUN until their actual commands and results exist. Hardware and
configuration changes invalidate transferred qualification claims.
