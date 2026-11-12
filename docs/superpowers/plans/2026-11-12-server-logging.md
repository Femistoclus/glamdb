# Server logging implementation plan

**Goal:** Make PR 14 emit useful operational events from the native server, engine and offline administrator.

**Architecture:** Keep the existing bounded Quill stderr sink. The process owns the logger until workers and engines finish. Each request has a generated correlation ID; an explicit thread-local scope carries only typed operation/correlation into synchronous core calls. HTTP completion describes transport, while core recovery/storage events describe engine observations. Embedded callers without a logging scope remain silent.

**Tech stack:** C++23, Quill 13, Asio, Python integration tests.

## Constraints and review focus

- Preserve current main, storage outcomes, machine-readable stdout and installed static-library consumers.
- Never record arbitrary URLs, headers, exceptions, paths or database values.
- Log failed/disconnected sessions exactly once, including early authentication/framing failures.
- Keep the logger alive through worker draining; bound producer registration to the existing stable worker pools.
- Test actual server requests, controlled storage failures, admin commands, sampling and broken sinks.

## Tasks

1. Rebase the existing PR onto main, resolving conflicts without dropping newer functionality.
2. Add failing process tests for server lifecycle, successful/error requests, redaction and admin operations; demonstrate missing events against the current binaries.
3. Connect server lifecycle and session completion to the logger. Add typed request scopes and core storage/recovery events, preserving correlation across worker execution. Export the logging archive needed by installed core consumers.
4. Extend admin operation classification to read and quiesce, preserve stdout, and verify failures. Exercise core events with the existing native VFS fault driver.
5. Run focused integration, adapter, installed-consumer and formatting checks; add a short English server log guide and update implementation/validation documentation.
6. Commit and push to the existing PR with an exact force-with-lease after rebase. Request an independent review, fix actionable findings, test and push again.

## Progress

- Rebase complete on `0c5df05`; retained newer admin commands, resource tests,
  reproc and paged/legacy stream fixtures.
- Server and SQLite VFS logging regressions observed failing on old binaries.
- Server/core/admin integration and guide implemented; focused checks pass.
- Complete build and CTest pass: 698 passed, three skipped child entrypoints,
  one previously disabled test; focused static analysis and quality checks pass.
- Initial integration committed and pushed to the existing PR after rebase.
- Independent review found missing recovery/reopening failure events. Added a
  failing real-server regression, fixed the gap, and verified the full suite.
- Final recovery fix and validation record are ready for the follow-up push.
