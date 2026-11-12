# CLI data and inspection implementation plan

**Goal:** Complete issue #52 against upstream `main`, including the final PR #19
baseline and the completed native server, codec and route dependencies (#48, #49,
#51).

**Architecture:** Keep `glamdb-cli` independently buildable. Share named command
definitions between it and `glamdb-admin`; remote commands use the existing HTTP
client and offline commands use only the public Engine facade. Share native
metadata serializers with HTTP. Keep existing server invocation and add `serve`.

**Constraints:** Store creation is explicit. Remote access never opens an engine.
Offline opens retain exclusive ownership. Preserve exact IDs, schema field IDs,
int64/timestamp values, duplicate rows, snapshot selection and original receipts.
Never retry writes automatically or resolve uncertainty as failure. JSON belongs
on stdout; diagnostics belong on stderr. Keep request and result sizes bounded.

**Review focus:** Lost write replies and unresolved outcomes; mixed local/remote
options; missing stores and concurrent owners; large or malformed JSON; historical
file membership, fixed scans and precise scalar round trips.

- [x] Add native CLI acceptance tests, observe missing named commands fail, and
  retain the existing CLI wire/output and admin parsing tests as regression checks.
- [x] Add shared command definitions and remote route construction for collection
  creation/list/resolve/get/schema, append, scan, operation, snapshots and files.
  Preserve standalone client builds and preflight output before requests.
- [x] Implement offline facade dispatch and reuse transport codecs/metadata JSON.
  Add `serve`, preserve `read`/quiesce compatibility, and verify local/remote parity
  across restart, idempotent retries, duplicate rows and exclusive ownership.
- [x] Document commands, limits, failure/unresolved exit statuses and validation;
  update the agent guide and roadmap. Run relevant native and quality checks.
- [x] Independently review and critique the full change, brainstorm failure cases,
  fix actionable findings and verify the final patch.

Publication: commit in English and open a fork pull request targeting the official
`glamourdb/glamdb:main`, as requested.
