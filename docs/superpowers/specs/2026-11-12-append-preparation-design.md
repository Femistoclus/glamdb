# Bounded append preparation follow-up

Issue: https://github.com/glamourdb/glamdb/issues/107

PR #134 already replaced the Arrow → rows → Arrow round trip with owned
canonical columns. This follow-up profiles that implementation before making
further changes, including highly fragmented input, and closes remaining
unbounded field-mapping scratch on rejected input.

## Approach

Retain the existing columnar representation and reservation formula. Reuse one
schema mapping at a time across consecutive batches sharing an immutable Arrow
schema. Reuse per-column string sizes from the same inspection that validates
the owned copy, rather than traverse all input batches again to count strings.
Reject unknown/oversized field names and incompatible types before full Arrow
validation; validate every batch's actual layout even when its schema mapping
is cached. Scratch must remain O(fields), including 10,000 batches.

Alternatives considered: retaining the current repeated mapping/sizing leaves
avoidable work for fragmented input; a new preparation queue or moving work
outside the writer changes admission/outcome contracts and is a separate design.
Removing the tracked backend copy also changes allocation-refusal and allocator
lifetime contracts and is excluded here. These choices follow source inspection;
retain the optimization only if repeated measurements support it.

## Invariants

- Hash bytes, field IDs, values, NULLs, duplicates and limits stay unchanged.
- Full validation still precedes raw value reads and validates every batch.
- Caller memory is copied under admitted ownership and the owned hash is checked
  before binding. Mutation between fingerprinting and copying still rejects.
- Old receipts precede new execution reservations and snapshot checks.
- Writer ownership, exact base, unknown barrier, durable acknowledgment and
  selected historical reads retain their current ordering.
- No persistent-format, public API, dependency or default-limit changes.

## Evidence

Record the upstream baseline and changed Release stage profiles, allocation
peaks, HTTP timings and exact commands in a development report. Distinguish
microbenchmarks, real public API storage tests, HTTP and sanitizer results.
Do not infer a process-wide preparation queue or RSS guarantee from these runs:
fingerprinting currently precedes pending-call admission, as documented in
RESOURCE_BUDGETS.md. Immediate refusal before computing a digest needs a separate
outcome-protocol decision; this change does not move more work there.
