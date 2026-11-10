# Public demo and supplied brand assets

The owner requested both supplied logos and access for external visitors.
Existing autonomous authorization covers implementation, tests, fork pushes,
upstream PR updates and publication. No new approval checkpoint is needed.

## Design and choices

Use GitHub Pages on the existing public fork for a durable HTTPS demo URL.
Sites hosting was also considered, but GitHub matches the requested repository
workflow and needs no additional hosting account. A public Python service would
require a separate server and authentication/tenant setup; the existing
disposable development server must not become a shared production database.

The published UI therefore boots a separate browser-only mock, with one bounded
in-memory dataset per page instance. No token, shared remote writer, telemetry,
browser persistence or production database is used. Mark it clearly as a public
demo; reload/reset restores the sample data. Preserve the existing authenticated
Python/HTTP mode for local/real-server work. Browser demo exports JSON; actual
Arrow IPC remains available through the connected-server mode and SDK.

Copy supplied PNGs unchanged. Use the full wordmark in sidebar/login and the
transparent mark in favicon/avatar. Brand paths and major logo sizes live in
the runtime theme and are restricted to local brand assets. Relative asset URLs
must work both at origin root and /glamdb/ on project Pages.

## Tasks and checks

- [x] Add failing unit cases for browser isolation, append identity, duplicates,
  precision/NULL/timestamps, initial/history/preconditions, typed predicates,
  pagination and bounds; implement the browser-only adapter.
- [x] Add logos, validated configurable paths/sizes and base-relative assets.
- [x] Add deterministic static packaging with no credentials/private artifacts.
- [x] Test the generated site from a /glamdb/ prefix in Chromium: no token/API,
  logos loaded, create/append/filter/history/JSON, isolated visits and reset,
  mobile and config. Retest connected-server paths and full Python suite.
- [x] Document both modes and publication; update AGENTS/README/status.
- [x] Publish tested static output on fork gh-pages, enable HTTPS Pages, verify
  returned public URL and browser behavior, update fork branch and PR #4.

The browser mock is for product demonstration, not native engine correctness,
HTTP/SDK interoperability or durability evidence. All real engine gates stay open.

Published: https://nnrpi.github.io/glamdb/. Pages reports public=true,
https_enforced=true, source gh-pages:/, status built. Five live-site Chromium
scenarios and anonymous HTTPS/logo SHA256 checks passed. Local full validation:
374 Python, 26 Node unit and 23 Chromium tests PASS. Fresh review findings were
fixed with observed failing regressions; the final narrow review found no
important regressions.
