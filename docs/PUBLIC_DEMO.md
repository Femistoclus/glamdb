# Public browser demo

The [hosted demo](https://nnrpi.github.io/glamdb/) is a separate static
publication and may differ from the current source checkout.

This mode publishes the same pink workbench as static HTML/CSS/ES modules on
GitHub Pages. A visitor needs only a browser. There is no token prompt, shared
API, account database or local Python prerequisite.

For restricted access, use the separate [OIDC HTTPS deployment](SSO_OPERATIONS.md).
It checks every resource on the server and needs hosting that can run Caddy.
The existing GitHub Pages publication stays anonymous until it is explicitly
retired after a protected replacement has been deployed and verified.

Every page instance owns an isolated bounded BrowserDemoAPI simulation. Visitors
can create fixed-schema collections, append duplicate rows, build typed filters,
read initial/historical snapshots, inspect synthetic metadata and export their
result as JSON. Reset demo or reload recreates the two sample collections.
There is no localStorage/sessionStorage persistence and no API network request;
data is not sent to the project owner's machine or another visitor.

This is a product demo. It is not a network database endpoint, durable storage,
native query/pruning evidence or an SDK server. Full Arrow IPC and network SDK
remain available through the existing authenticated server mode. Browser demo
outcomes are UI simulation records, not C-RESULT durability receipts/canonical
content hashes. Its JSON fingerprints use SHA256 internally only to freeze
request identity within the page.

Limits: 16 collections, 10,000 current rows in total, 128 operation records per
collection, 1,000 preview rows per query, 128 metadata cursor records and a
16MiB conservative retained row/history estimate, plus bounded control records.
This is not an RSS guarantee. Capacity rejection is authoritative locally and
returns definitely_not_committed, allowing a corrected batch. At saturated
control capacity, further failures need not allocate an outcome record; existing
reads/retries remain available and Reset demo starts a new simulation.

## Brand assets

The owner supplied two PNGs, copied byte-for-byte:

- `ui/assets/brand/wordmark.png`: original full logo with its background;
  SHA256 `a2222b67971f388fe2948c18ec9ca9f098a3d25c83a4b3652c10fbfd8eca038e`.
- `ui/assets/brand/mark.png`: transparent database/sparkles mark, favicon/avatar;
  SHA256 `4656feea9c748658ab75b4b36244ccb6822c1869e10cf029e872d13724a7e010`.

`ui/assets/brand/wordmark-transparent.png` is the background-removed version
used by the README, login screen and sidebar. It retains PNG alpha transparency;
the supplied originals above remain unchanged.

Configure paths through `brand.wordmark`/`brand.mark` and sizes through
`sizing.logo_width`, `login_logo_width`, `logo_mark_size` in theme.json. Assets
stay local; unsafe paths fail validation. Old custom themes without logos work.
Colors, decoration and all earlier theme controls remain available.

## Package and preview

From the repository root:

```sh
.venv/bin/python scripts/build-public-demo.py
.venv/bin/python -m http.server 8768 --bind 127.0.0.1 --directory artifacts/public-demo
```

Open http://127.0.0.1:8768/. The build copies only index.html, public assets and
the two explicit theme/sample JSON files. It adds a public-demo bootstrap flag
and .nojekyll. It does not copy .venv, node_modules, .git, tests, server state,
tokens, logs or environments. Output must be a dedicated artifacts/ subdirectory;
rebuilding refuses an existing unmarked directory.

The browser tests also serve the package at /glamdb/, matching project Pages:

```sh
npm --prefix ui test
npm --prefix ui run test:e2e
```

To run only the public scenarios against a deployed URL, set GLAMDB_PUBLIC_URL
and use `-- --grep public` or the public-demo.spec.mjs file. These tests mutate
only their own page instances, never shared remote data.

## Publish

Publish only the generated artifact to the Pages branch of the hosting fork.
The current demo uses `nnrpi/glamdb:gh-pages`, path `/`. Upstream development
branches belong in forks as described in [CONTRIBUTING](../CONTRIBUTING.md).

Build from reviewed source, copy only the packaged static assets, preserve the
deployment history, and check the deployment and anonymous browser behavior.
Theme changes become public only after publishing the changed public theme.
