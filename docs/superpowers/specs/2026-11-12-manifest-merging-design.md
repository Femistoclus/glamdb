# Bounded append manifest merging

Issue: https://github.com/glamourdb/glamdb/issues/169

Use the pinned Iceberg C++ `NewMergeAppend` in the existing append transaction,
including bulk appends. Its existing defaults merge bins of small manifests at
100 entries, targeting 8 MiB. Table properties and persistent format remain
unchanged, so existing FastAppend histories remain readable. A separate
maintenance transaction would add publication and retry complexity; a custom
manifest writer would duplicate supported upstream behavior.

Publication and recovery must use the same closure verifier. Match unchanged
manifest descriptors exactly by path. For removed parent manifests, read their
bounded entries and require that new manifests preserve each live entry as
EXISTING with identical file metadata, original snapshot and sequence numbers.
Reject omissions, duplicates, deletes, changed descriptors and invented existing
entries. Only ADDED entries may contribute new data, receipt counts and scan
membership; keep ordinary and bulk input limits distinct.

Preserve the eight-new-manifest and 32 MiB commit verification guards. Bound
removed-manifest reads and accumulated entries as well as new manifests, and
charge only new reachable manifests to retained storage usage. Preserve old
manifests and lists for historical snapshots, and count unreferenced staging
files in physical warehouse usage. No deletion or snapshot expiration is added.

For a fixed maximum reachable metadata volume, target-sized bins plus at most
99 small manifests bound the latest manifest list independently of the append
count. On the #63 workload the target fits the complete current membership and
the list cycles between 1 and 99 manifests. With all snapshots retained, merging
every K appends retains O(N²/K) file descriptors in merged manifests in the
single-bin regime; manifest-list entries become O(KN). Root history still grows
with N. This change does not replace #168 or qualify physical power-loss safety.

Prove behavior with real native storage, independently checked row counts,
historical and initial views, retries, reopening, malformed merges and file/SQL
faults at a merge boundary. Rerun the default #63 campaign before and after on
the same build profile and host, publish compact evidence, and obtain a fresh
independent review before opening the pull request.
