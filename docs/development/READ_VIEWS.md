# Native pinned query read views

Contract **1.1** adds `IndependentReadView` for the local storage backend. Default
contract requests still negotiate **1.0** and do not advertise this feature.
Native callers use the installed `storage/read_view.h` types through `Engine`;
there is no HTTP route, remote token, split executor or stable ABI for views.

## Capture and scan

`AcquireReadView({Entries, AuthorizationScope, Lifetime})` returns an opaque
`shared_ptr<TReadView>`. Each ordered entry specifies a collection and
`Latest`, `Id(snapshot)` or `InitialEmpty`. Duplicate entries are allowed, including
several selectors for one collection. Acquisition validates every entry or fails
as a whole. It captures collection identity, full fixed schema, original root
identity, resolved selector and the verified membership generation under one
engine `state_mutex`. It retains root readers before releasing that mutex.

The vector is a coherent cut of confirmed publications in **one owning store**.
With serialized writes A then B, it can see both before, A after/B before, or
both after; it cannot see A before/B after. This is neither an atomic multi-table
write nor a transaction/timestamp spanning stores. Recovery/readiness fencing
still applies on acquisition and new scan admission.

```cpp
glamdb::TReadViewRequest request;
request.AuthorizationScope = "principal-and-policy-generation";
request.Entries = {{collectionA}, {collectionB}};
auto view = engine.AcquireReadView(request);
// Check each Result before dereferencing.
auto info = engine.GetReadViewInfo(**view, request.AuthorizationScope);
auto scan = engine.ScanReadView(**view, 0, {.Limit = 100}, request.AuthorizationScope);
```

`GetReadViewInfo` returns independently accounted immutable information in entry
order. `Identity` is the root identity **at capture**; its `current_snapshot_id`
can differ from the selected historical `SelectorResolved`. Schema field IDs
remain original ordinal + 1, as in the native storage contract. Initial and
no-data views retain real collection/schema identities. `ScanReadView` takes an
entry index and projection/filter/limit/batch size, with no collection or
selector override. It reuses captured state without consulting Latest, including
when planning is delayed until after retention, upsert, delete or compaction.
Zero-limit and zero-column scans keep existing scanner semantics.

## Ownership, bounds and lifetime

All methods require the exact nonempty acquisition `AuthorizationScope` (at most
256 bytes). This is a binding supplied by a **trusted embedding adapter**, not
authentication. The adapter must reauthorize on each use and include an appropriate
principal/tenant/policy generation in the scope. Possession of a native pointer
is not a remotely transferable grant. A scope mismatch cannot renew or cancel
another scope's view.

The fixed limits are 32 active views per engine, 8 per scope, 64 entries per view,
and 256 entries total. Entries, not distinct collections, consume the quota.
Acquisition also reserves shared metadata/control memory; original verified
state retains its backend generation reservation. Shared admission can refuse
before any quota ceiling. Limits are exposed in contract 1.1's optional
`EffectiveLimits.ReadViews`; 1.0 and unsupported backends leave it absent.

TTL defaults to 30 seconds. Requests must be positive and at most five minutes.
`RenewReadView(view, scope, ttl)` sets the deadline to the earlier of now + ttl
and acquisition + five minutes. It never revives an expired handle or selects a
new root. Renewal can shorten a deadline. The deadline uses monotonic process
time, never a wall clock or externally supplied timestamp.

Expiration is observed and reclaimed on view API calls and writer root/retention
cleanup. There is no timer thread: an idle expired view may retain its bounded
resources until one of these boundaries, final handle destruction, engine
destruction or shutdown. It cannot admit a new operation after its deadline.
Expired pins are swept before acquisition reserves shared memory, then expiry,
readiness and quotas are rechecked under the coherent-capture mutex. Retained
info and terminal handles keep their independent reservations. Last shared-handle destruction releases the
pin; `ReleaseReadView` releases it explicitly. `CancelReadView` also releases it,
with a distinct terminal error. A terminal handle keeps only its small, charged
control record until the caller releases it; retained info keeps its own metadata
reservation. Repeating the same terminal operation is
idempotent. Neither terminal operation revives a handle.

Release, cancellation and expiration stop **future** view admissions. A scan
already admitted under the view owns its fixed state and follows its ordinary
cursor lifetime; cancel that cursor explicitly when required. Shutdown cancels
all cursors through the existing engine closing flag and clears view pins even
when waiting for the writer times out. A racing scan either obtains its own root
reader before the pin is removed or gets the terminal error. An in-flight I/O
may finish before ordinary cursor cancellation is observed.

Retained Arrow batches, arrays, slices, buffers and C Data exports keep their
allocator, accounting and store ownership independently of the view. Information
returned by `GetReadViewInfo` keeps schema/identity accounting, without retaining
roots or the engine. A bare live or terminal read-view handle does not keep a
shut-down/destroyed engine's store lock alive. Native handles never survive reopen;
using an old handle against a new instance for the same store fails distinctly,
and never falls back to Latest.

## Errors

| Condition | Code |
| --- | --- |
| Invalid scope/TTL/vector/index/scan parameters | `INVALID_ARGUMENT` |
| View quota or shared reservation exhausted | `RESOURCE_EXHAUSTED` |
| Scope differs | `READ_VIEW_SCOPE_MISMATCH` |
| Other store's engine | `READ_VIEW_WRONG_ENGINE` |
| Different engine incarnation for the same store | `READ_VIEW_RESTARTED` |
| Deadline elapsed | `READ_VIEW_EXPIRED` |
| Explicit cancellation / release | `READ_VIEW_CANCELLED` / `READ_VIEW_RELEASED` |
| Owning engine shutdown | `READ_VIEW_CLOSED` |
| New acquisition selects removed / unknown history | `SNAPSHOT_EXPIRED` / `NOT_FOUND` |
| Backend lacks native read views | `UNSUPPORTED_FEATURE` |

Identity and scope checks precede terminal-state checks. These input/lifetime
errors require a corrected request or explicitly acquired new view; retrying a
new view may select newer data and is the caller's decision. Existing recovery,
corruption, I/O and resource errors remain unchanged.

## Private planning bridge and reclamation

`core/engine/read_views.*` owns native handle state, quotas, deadlines, immutable
entry information and retained verified states. Its methods run only while the
owning engine holds `state_mutex`; it has no second lock. Root readers use that
same lock. Destruction callbacks hold only weak engine ownership and enter the
same serialized release path.

The registry's checked entry resolver is the private bridge for future split
planning. A consumer must validate engine/scope/deadline and take a separate root
reader under `state_mutex`, copy the original `shared_ptr<const TBaseState>`
**with its aliasing ownership intact**, then plan from that state and the captured
resolved selector outside the lock. The existing `TScanContext` receives the
captured collection, base and verified state plus normal FileIO/cache/engine
leases. No private Iceberg/SQLite/core headers cross the installed API boundary.

Pin release only decrements readers. Physical root unlink and expiration queue
draining stay on the existing writer path; this preserves inventory/capacity
ordering. Current data and KV historical intervals are already retained by the
storage profile. Future data-file reclamation must honor these leases before it
can remove any file used by an admitted view or scan.

`ReadView.*` native regressions cover repeated/delayed scans, coherent store cuts,
retention and actual four-file KV compaction, initial/historical/zero-limit views,
quota/resource release, scope checks, expiry/renewal, explicit release/cancel,
shutdown/reopen and independent retained-buffer lifetime. Validation commands and
results belong in `artifacts/modularity/task-2-report.md`; implementation and
sanitizer checks do not establish physical power-loss durability.

Contract 1.2 [scan splits](SCAN_SPLITS.md) retain the original shared view handle
and use this same private planning bridge for bounded pages and repeatable
physical file ranges. View release, cancellation and expiry stop new split
admissions; already admitted cursors retain their independent leases.
