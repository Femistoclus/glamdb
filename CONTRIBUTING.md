# Contributing to GlamDB

## Fork-based development

1. Fork `glamourdb/glamdb` into your account.
2. Create a working branch in your fork, commit your changes, and push it there.
3. Open a pull request targeting `glamourdb/glamdb:main`.

The upstream repository keeps only `main`. Creation of upstream working branches
is prohibited for maintainers and automation as well as external contributors.
Dependency updates follow the same fork and pull request process.

## Review and merge requirements

Every pull request needs at least **one approval from a member of
`@glamourdb/maintainers` other than its author**.

Organization membership does not automatically add someone to this team.
Organization owners or team maintainers add members explicitly. When inviting
someone new, they can invite that person through the team so membership is
assigned when the invitation is accepted.

- New changes dismiss earlier approvals. The latest reviewable push also needs
  approval from someone other than its pusher.
- Resolve review conversations and pass all required checks before merging.
- Keep the pull request up to date with `main` so checks cover the current base.
- Use squash merge. Direct pushes, force pushes, and deleting `main` are blocked.
- These requirements also apply to workflow, dependency, release, policy, and
  CODEOWNERS changes.

The repository ruleset enforces the required maintainer approval. CODEOWNERS
requests the maintainers' review but does not enforce that count on its own.

### Maintainer force-merge

Members of `@glamourdb/maintainers` can explicitly bypass the review and CI
requirements when merging a pull request. This is a PR-only bypass: it does not
permit direct pushes, force pushes, deletion of `main`, or creation of upstream
branches. Ordinary merges and auto-merge still use the requirements above.

Explain the reason in the pull request when using a bypass. GitHub records the
bypass in the pull request and ruleset audit history.

## Checks and testing

The `Workflow security` check runs actionlint and zizmor against GitHub
Actions workflows. The `C++ quality` workflow runs separate `clang-format` and
`clang-tidy` checks using LLVM 18 and the checked-in configurations. Formatting
covers project C++ sources and headers; tidy includes the engine, storage adapters,
CLI, and tests using the pinned dependencies. Tidy runs two files concurrently
with grouped diagnostics and per-file timings. Both workflows run on every pull
request, including documentation changes, and pushes to `main`.
The `Native tests` workflow runs independent normal, ASan, UBSan, MSan, TSan,
and LSan jobs. `Workbench checks` runs independent Python, Node, and browser
jobs. These test workflows skip documentation-only changes; new code paths
trigger them automatically. See [test CI](docs/development/BUILDING.md#test-ci)
for discovery, sanitizer scope, and failure logs.
External contributors' workflow runs require a maintainer to approve execution;
approval to run CI does not approve the pull request for merging.

If a run reports **Internal server error** before any job starts, use
**Re-run all jobs** to retry the same commit. A job that never starts has no
step logs; the error appears in the run's annotations. Retrying only failed
jobs may be unavailable when GitHub did not create any jobs. Keep workflow
validation enabled while investigating the platform error.

Explain how you tested each change in its pull request. As engine code is added,
include runnable tests for behavior changes. For storage changes, consider
corrupted input, crash recovery, concurrency, persistent-format compatibility,
and the possibility of data loss. Record relevant performance measurements.

## Dependencies and CI

- Use reviewed upstream sources and pin dependencies to explicit versions or
  immutable revisions. Preserve and verify checksums or lockfiles where supported.
- Actions must use a full commit SHA. The repository permits only the Actions
  explicitly selected in its settings; additions need maintainer review.
- PR workflows run on GitHub-hosted runners with read-only repository access,
  without release credentials or other secrets.
- Keep privileged release jobs separate from PR code. Do not execute fork code
  through `pull_request_target` or consume untrusted PR artifacts in release jobs.
- Dependabot alerts are enabled, but automatic update PRs in the upstream
  repository are disabled because they would create upstream branches. Submit
  reviewed updates from a fork, including updates to Actions and CI tools.

The pinned CI tool package and its hashes are in `.github/requirements-ci.txt`.
Update its version and hashes together in a pull request, and verify both linters.

## Security issues

Follow [SECURITY.md](SECURITY.md) to report vulnerabilities privately. Use public
issues for ordinary bug reports and feature discussions.
