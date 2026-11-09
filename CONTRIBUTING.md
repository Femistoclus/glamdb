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
  CODEOWNERS changes. There is no normal administrator or bot bypass.

The repository ruleset enforces the required maintainer approval. CODEOWNERS
requests the maintainers' review but does not enforce that count on its own.

## Checks and testing

The initial `Workflow security` check runs actionlint and zizmor against GitHub
Actions workflows. It runs on every pull request, including documentation changes.
External contributors' workflow runs require a maintainer to approve execution;
approval to run CI does not approve the pull request for merging.

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
