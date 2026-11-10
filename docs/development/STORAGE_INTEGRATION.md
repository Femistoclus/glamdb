# Storage integration checks

These checks exercise the production `TLocalStorageBackend`, SQLite catalog and
scanner together. The file-layer tests remain independent; passing them alone
does not verify the engine boundary. See [the storage guide](STORAGE_MODULE.md),
[coordinator guide](COORDINATOR.md) and [scanner guide](SCANNER.md) for the contracts.

## Connected interfaces

| Boundary | Actual path |
| --- | --- |
| Normalized input to immutable files | `PrepareAppend` calls `WriteDataFile`, `StageAppend` and durable root writing. |
| Prepared state to catalog pointer | `StagedAppend::Publish` calls `CatalogStore::UpdateTableMetadataLocation` with the prepared metadata location. |
| Publication validation | `DurableSqliteCatalogStore::PublishAppend` checks the bound base and invokes `VerifyClosure`, which calls `TStoragePublicationGuard`. |
| Tentative candidate to confirmed base | The coordinator calls `ConfirmPublication` after the outer SQLite commit. |
| Reopen | The coordinator validates catalog state; `VerifyRetained` checks reachable storage and committed receipts. |
| Fixed snapshot reads | `Engine::Scan` resolves the catalog view and uses `ReadOnlyFileIO`; cursors and returned buffers retain the store lease. |

The production publication path uses a direct catalog CAS. It does not call
`Transaction::Commit` or emit Iceberg `AssertRefSnapshotID` requirements. The
separate `glamdb.durable_storage_catalog` test observes those requirements on the
SqlCatalog path; that evidence cannot be attributed to this backend. This
interface difference remains explicit until the integration contract is reconciled.

## Public history and restart

`PublicHistoryAndReceiptsSurviveFreshProcessWithOrphans` creates a store and
collection through the public API, rejects an empty append without binding its
operation ID, and appends 1,000 A rows followed by 10 B rows. All rows have the
same key; a literal multiset oracle preserves their values and multiplicity.

An initial empty cursor and the A cursor retain their views across B. Valid
orphan Parquet and higher-numbered metadata files remain invisible. The parent
saves the caller's outcome bytes outside the store and closes the engine; an
exec-created process reopens the store, checks initial/A/latest reads and both
original receipts. Retrying A after B creates no files and leaves B current.
This checks a fresh-process reopen after orderly shutdown, not a process crash
or power loss.

## Exact prepared base

`TPreparedBaseTest` prepares a real backend append and changes the authoritative
catalog before publication. Four cases cover empty-to-first snapshot, A-to-B,
and metadata-only pointer changes with either no snapshot or the same snapshot.
The additional producer and SQL connection are sequential disposable test actors;
they do not introduce a supported multiple-writer mode. Metadata-only roots are
durably copied and verified before the actor updates the pointer.

The real catalog rejects each prepared operation with `STALE_BASE` and
`uncertain=false` before calling either the publication body or closure guard.
The winning root bytes, pointer and receipt remain unchanged; no files disappear
or appear on rejection. The prepared receipt is unchanged and its operation
remains `Bound`, without an invented terminal outcome. This checks the production
base barrier, while the upstream requirement-tracing difference above remains open.

## Installed public API

`glamdb.installed_consumer` installs the library into a disposable prefix and
builds a separate application with installed `storage/engine.h` and public Arrow
headers. Storage-enabled runs create an empty collection, reopen the store,
resolve its exact name and check a literal schema. They construct a public
predicate and a zero-limit scan, verify the fixed initial-empty identity and
schema before `Next()`, and check end-of-stream.

The root build passes its expected storage capability explicitly. An OFF run
keeps the empty-store scenario; missing or invalid capability arguments fail
instead of silently skipping storage assertions. The installed test runs in
unsanitized configurations; the sanitizer suite exercises the vertical and
lifetime paths separately.

## Reproduction and recorded evidence

The recorded baseline uses Linux ARM64 Docker, Clang/libc++ 18, ASan/UBSan and the
pinned dependency installation. The checkout is mounted read-only at `/src`,
the installed dependencies read-only at `/prefix`, and a build volume at `/build`.
Inside `glamdb-deps:clang18` the commands are:

```sh
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
export ARROW_DEFAULT_MEMORY_POOL=system
export PYTHONDONTWRITEBYTECODE=1
cmake -S /src -B /build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_PREFIX_PATH=/prefix -DGLAMDB_IO_ONLY=OFF \
    -DGLAMDB_BUILD_STORAGE=ON -DGLAMDB_BUILD_IO_PROBE=ON -DBUILD_TESTING=ON \
    -DGLAMDB_ENABLE_ASAN=ON -DGLAMDB_ENABLE_UBSAN=ON \
    '-DCMAKE_CXX_FLAGS=-stdlib=libc++ -D_LIBCPP_HAS_NO_ASAN' \
    -DCMAKE_EXE_LINKER_FLAGS=-static-libstdc++ -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
cmake --build /build --parallel 2
ctest --test-dir /build --output-on-failure --no-tests=error --parallel 2
```

The production baseline is `64cc7d49500bc29118082be30614eecebeb19112`.
The full ASan/UBSan suite registered 412 tests: 410 passed and two dispatchers
skipped standalone invocation while executing from their parent tests. After
optional-access guard refinements, the vertical suite registered 23 tests:
22 passed and one dispatcher skipped. LLVM 18 formatting, clang-tidy for both
changed C++ files and independent review passed. Local logs are
`artifacts/engine-integration-64cc-full.log`,
`artifacts/engine-integration-64cc-test-details.log` and
`artifacts/engine-integration-64cc-guards.log`.

The installed consumer passed in Release with storage ON and OFF, and Debug
with storage ON. Invoking the storage-disabled consumer with expected capability
ON failed with the intended exit code 7. LLVM 18 consumer formatting and
clang-tidy passed. These checks are recorded in `artifacts/engine-package-64cc-matrix.log`.
A further Release consumer configure/build/run passed in a container without
`/src` or vendor-source mounts. Its only project include paths were the installed
GlamDB and dependency prefixes; it used only the two copied consumer fixture
files. The original compile/link evidence is in `artifacts/engine-package-source-free.log`;
relinking with the updated installed libraries and running the consumer again
passed, as recorded in `artifacts/engine-package-64cc-source-free.log`.

For the standard Docker setup and sanitizer compatibility limits, see
[Building](BUILDING.md#storage-adapters). The dependency prefix in this local
run is not fully sanitizer-instrumented.

## Remaining boundaries

These results do not establish compound predicates, pruning, runtime memory
quotas, unified filesystem/SQLite fault recovery, quiesced whole-store backup,
capacity/ingestion limits or power-loss durability. Scanner decoding still needs
to share the module's bounded Avro/footer and strict field-ID validation.
Long qualification campaigns and the dedicated Linux x86-64/ext4 environment
remain separate from these bounded integration checks.
