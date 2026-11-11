# Shared resource validation

Recorded on 2026-11-11 for [issue #99](https://github.com/glamourdb/glamdb/issues/99).
Checks updated on 2026-11-12 after integration with official main.
Environment: Ubuntu 24.04.4, Linux x86-64, Clang 18.1.3, Debug builds,
pinned libc++ 18 and repository dependencies. The test filesystem reports
`ext2/ext3` through `stat -f`; this is not the dedicated G4 qualification volume.
The checks below cover official main through `d61aeaa`, including the later scan
ownership and fixed-view paging integrations. They were run locally after the
merge. The HTTP measurements below record the earlier three-database campaign.

## Checks

| Check | Result |
| --- | --- |
| Storage-enabled build and full native CTest suite | PASS: 674 passed, three skipped, one disabled; 678 discovered |
| Resource owner/pool, shared-store, compound predicate, native route, file metadata, fixed-view pages, scan lifetime, workspace recovery, HTTP admission/lifecycle and profile tests under ASan/UBSan/LSan | PASS: 130 tests, leak detection enabled |
| Storage-disabled full CTest suite | PASS: 332 passed, one existing disabled test |
| LLVM 18 formatting | PASS: 211 project files |
| LLVM 18 static analysis with configured warnings as errors | PASS: all 14 integration units changed since `a0b6e8f`, including project headers |
| Quality negative checks and `git diff --check` | PASS |

The earlier SDK unit run at `a0b6e8f` passed 97 outcome, scan-page and cleanup
tests. The current native suite also runs real SDK HTTP scenarios.

The skipped cases are the standalone `CapacityRecoveryChild.RejectsOverBudgetStore`,
`RecoveryChildProcess.ReopensPublishedRoot` and
`RecoveryWorkspaceChild.CrashesDuringVerification` subprocess helpers. The disabled
case is `TerminalRetryCost.CoordinatorWorkScalesWithStoreHistory`. The crash helper
is invoked by its parent regression; no disabled cases were added. Full-repository
static analysis, full sanitizer suites, TSan and MSan were not run locally.

An earlier parallel run during static analysis failed the unchanged metadata
serialization timing ratio (3.34 against a limit of 3). Three isolated repeats
passed with ratios 1.90, 2.01 and 1.87; the final full suite also passed without
changing that test.

Commands, with the normal and instrumented build directories configured as
described in [BUILDING.md](BUILDING.md):

```sh
cmake --build build/local --parallel 6
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
  ctest --test-dir build/local --output-on-failure --parallel 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 ARROW_DEFAULT_MEMORY_POOL=system \
  GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
  ctest --test-dir build/issue-43-asan --output-on-failure --parallel 2 \
  -R 'ResourceOwner|ResourcePool|MetadataResources|SharedResources|SharedPressure|TFileMetadataTest|RecoveryWorkspaces|ScanPredicate|ScanMockTest|Compound|Admission|ScanSessionsTest|ReplayAdmissionTest|TScanLifetimeTest|TMetadataPagesTest|SupersededRoot|glamdb\.(native_http|http_review|native_http_fault|scan_http|http_lifecycle|resource_profile)$'
ctest --test-dir build/nostorage --output-on-failure --parallel 2
cmake --build build/local --target format-check quality-negative-check
ctest --test-dir build/local --output-on-failure -V -R '^glamdb.resource_profile$'
```

The instrumented build enables address and undefined-behavior sanitizers;
LeakSanitizer runs with its default Linux leak detection. Static analysis uses
`clang-tidy-18`, `build/local/compile_commands.json`, the checked-in
`.clang-tidy`, the project header filter and `--warnings-as-errors=*` for each
changed C++ translation unit.

## Behavior and independent review

Owner tests check concurrent admission, separate protected classes, shared
lease lifetime, same-device byte/inode competition and overflow refusal. Pool
tests check upstream refusal, retained old buffers on failed reallocation and
the simultaneous old/replacement charge. Zero-size reallocation refusal also
retains the old buffer and its reservation. Real-store tests check cross-database
pressure, pre-binding refusal, original receipt reuse, uncertain append/create
reconciliation, retained slices/C Data exports and original metadata results.
All reservations return to zero after their final owners disappear.

A fresh independent reviewer inspected the patch and surrounding code read-only.
The review found three issues, all corrected before publication:

- Staged-object destruction could refund a candidate root before an uncertain
  COMMIT was classified. Only confirmed publication/rollback now releases it;
  append and create recover while ordinary classes are full.
- Returned metadata outlived its working reservation. Original DTOs now retain
  capacity-sized leases, and an extracted explanation schema retains its lease.
- HTTP response members could outlive their reservation during destruction.
  The reservation now outlives the response payload and headers.

The first two findings were reproduced by failing tests before the fixes. The
final tests pass with sanitizers. The reviewer found no critical issues; broader
envelope calibration and physical fault qualification remain explicit limits.
A scoped independent review covers the later integration of snapshot file APIs
from main. Their listing and file-detail DTOs retain separate capacity-sized
leases for file metrics, bounds and row-group/column data; a new regression
reproduced their missing charges after close. This review found that many small
bounds allocations needed per-allocation overhead beyond the fixed allowance.
An 8,192-bound refusal test reproduced it; retained charges now include allocator
headers and rounding per allocation.

Final self-inspection also reproduced and fixed premature release on a rejected
zero-size reallocation; its regression checks both refusal and successful release.

## Review follow-up, 2026-11-12

[The PR review](https://github.com/glamourdb/glamdb/pull/119#discussion_r4543069093)
identified named workspaces surviving process death. Three regressions first
failed, then passed after cleanup moved into exclusive, identity-validated open
and device admission moved after cleanup. Memory remains reserved before open.

A fresh child is killed by SIGKILL after a real recovery workspace write. Three
successive interrupted recoveries leave one file each; reopen removes it and
preserves the original receipt and rows. Other cases check busy/wrong-identity
stores, unrelated names, directories and symlinks, plus cleanup before device
refusal and release of the refused open's reservations. These pass under
ASan/UBSan/LSan as well as the native build. SIGKILL does not qualify power loss.

## Main integration, 2026-11-12

The compound predicate merge preserves tracked Arrow builders and SQL
three-valued scan behavior. Independent review found that large compiled
literal sets and nested explanation trees needed additional accounting.
Compilation now reserves measured capacity before copying literals and keeps
that lease with the program. Explanation nodes and deferred child lists retain
their result lease through extracted subtrees. Explain does not compile a
second literal set.

Four regressions failed before these fixes and passed afterward. They cover
nested literals, unused vector capacity, extracted subtree lifetime and a
32 MiB predicate retained by a real engine cursor. The engine case also checks
refusal without leaked reservations and an explanation subtree surviving engine
close. The scoped independent review finished with no remaining findings.

The later HTTP admission merge preserves queued body leases and separate
replay/outgoing leases. Shared-parent refusal precedes row consumption and does
not invalidate an existing cursor; both first-page and next-page cases fill the
Data class and check the original replay and fixed view after release.

Logical quota tests use explicit finite profiles. Thirty-three retained Parquet
readers require more than the default 8 GiB Metadata envelope, so their dedicated
fixture uses 16 GiB Metadata. The eight-store, 64-session HTTP quota cases use
4 GiB Data, 16 GiB Metadata and 8 GiB Recovery. These cases first refused under
the smaller default profile. Production defaults and the three-database measured
profile remain 2 GiB Data, 8 GiB Metadata and 4 GiB Recovery.

Fixed-view metadata pages now reserve construction, cached descriptors/tokens
and returned copies from the engine's shared metadata parent. Every returned
page retains its own capacity-sized lease after engine close. Two regressions
first failed on retained-page charges and continuation/replay admission, then
passed after accounting was integrated.

Independent review found that working admission could prevent expired cache
leases from being reclaimed, and vector compaction could release leases before
overwritten payloads. A third regression reproduced expiry under saturation.
Expiry now precedes working admission, and sessions use separately freed list
nodes. The reviewer confirmed both findings resolved; normal and sanitizer
runs cover these changes.

## Measured HTTP profile

`glamdb.resource_profile` creates three managed databases with one collection
each. Every collection receives two 2,000-row appends, including duplicate keys
and 128-byte string payloads. Three writers and three scanners overlap; final
scans check all 4,000 rows per database and retries check original receipts.
The service reconciles, shuts down and reopens in a fresh process with the same
limits. A third process fills data, metadata and ordinary control reservations,
then verifies outcome lookup, readiness and reconciliation for every database.

One normal run recorded these reservation high-water marks, in bytes:

| Process phase | Data | Metadata | Control | Completion | Recovery |
| --- | ---: | ---: | ---: | ---: | ---: |
| Concurrent load | 1,446,772,098 | 2,651,070,720 | 98,304 | 4,588,556 | 2,432,696,320 |
| Fresh-process reopen | 404,468,630 | 1,057,240,832 | 49,152 | 262,144 | 2,432,696,320 |
| Ordinary classes full | 2,147,483,648 | 8,589,934,592 | 16,777,216 | 4,588,670 | 2,432,696,320 |

Actual Arrow allocator peaks and process RSS are measured separately from those
conservative reservations:

| Process phase | Arrow peak bytes | Peak RSS bytes | Pending device bytes peak | Pending inodes peak |
| --- | ---: | ---: | ---: | ---: |
| Concurrent load | 1,729,984 | 130,867,200 | 3,280,994,304 | 304 |
| Fresh-process reopen | 417,792 | 85,348,352 | 2,475,687,936 | 112 |
| Ordinary classes full | 32,832 | 36,433,920 | 2,475,687,936 | 112 |

The load completed without `SERVER_BUSY` refusals under the separate reader
and publisher pools. A 5 ms sampler observed physical tree peaks of 401,408 bytes and
49 entries, including an 8,192-byte recovery workspace. Sampling may miss shorter
peaks; it is not a physical upper bound. The Arrow metric is the default upstream
pool's high-water allocation count; RSS includes non-Arrow memory and allocator
retention. Every process reported zero remaining memory/device/inode reservations
after shutdown.

The same profile passed under ASan/UBSan/LSan. That run recorded peak RSS of
396,693,504 bytes during load, 215,920,640 during reopen and 144,576,512 with
ordinary classes full. Sanitizer overhead makes these separate measurements.
Scheduling changes reservation peaks and retry counts between runs.

## Limits

This small campaign exercises shared admission and lifetimes; it does not
certify an RSS ceiling for every valid or corrupt input. Non-Arrow dependency
work uses deliberately coarse envelopes. Broader workloads and dependency
changes require recalibration. Caller-created copies of inputs or metadata DTOs
belong to the embedding application's budget.

Device tests use deterministic reservation caps on real filesystem paths.
Existing injected I/O/fault suites pass, but no dedicated physical full-volume,
inode-exhaustion or power-loss campaign was run. External writers can still
cause ENOSPC after admission. No G4 qualification or network durability claim
follows from these results; [G4 remains NOT RUN](../ROADMAP.md#current-status).
