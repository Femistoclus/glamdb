# Append preparation implementation plan

> Execute inline, with an independent whole-change review after verification.

**Goal:** Reduce repeated append mapping/sizing work and bound rejected-field
scratch while preserving the existing columnar ownership and outcome protocol.

**Spec:** ../specs/2026-11-12-append-preparation-design.md

**Architecture:** Keep one schema mapping per traversal, retain inspected string
sizes in O(fields) scratch, and retain full validation of every actual batch.

**Constraints:** C++23, LLVM 18 formatting, unchanged canonical bytes, limits,
admission, binding, publication and persistent formats. Work only in this worktree.

**Review focus:** Caller field names; incompatible arrays behind a shared schema;
alternating/reordered/omitted fields; many tiny/empty batches; mutation between
fingerprinting and copying. Existing independent canonical and lifetime tests
remain the primary value/ownership oracle.

### Task 1: Establish baseline and regression evidence

- [x] Extend the opt-in stage benchmark with a batch-row-size selector.
- [x] Measure current single-batch and fragmented Release appends repeatedly.
- [x] Add a bounded-heap test for rejected large Arrow field names; run it and
  confirm failure on the unchanged production implementation.
- [x] Add cases ensuring a shared schema never skips actual batch validation.

### Task 2: Reuse bounded preparation work

- [x] Update core/api/arrow_input.cpp: reject incompatible fields before full
  validation; reuse one mapping at a time; retain inspection's string sizes for
  the owned copy. Preserve the second inspection and owned-hash comparison.
- [x] Run regression, independent canonical, ownership, admission and resource
  cases; repeat stage measurements and keep only a measured improvement.

### Task 3: Verify, critique and publish

- [x] Run the built native suite, HTTP/SDK coverage and sanitizer lifetime cases.
- [x] Record measured results and remaining boundaries in a development report;
  keep AGENTS.md and coordinator guidance consistent with the implementation.
- [x] Request a fresh independent review of the final patch, including alternative
  approaches and invariants; resolve actionable findings and verify any fixes.
Publication follows verification: commit in English, push to the existing fork
and create an English PR to official main using the repository template, with
no tool attribution. The remaining shared preparation-admission work keeps #107
open, as documented in the measured report.
