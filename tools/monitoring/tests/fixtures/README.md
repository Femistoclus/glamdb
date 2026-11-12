# Captured gateway metrics

`caddy-2.11.9.prom` contains the `caddy_http_*` subset of a real private Caddy
2.11.9 metrics scrape after authenticated gateway, rejected authentication,
unknown path and nonstandard-method requests. It contains no request IDs,
credentials, URLs or backend addresses.

The configured workbench route has `handler="subroute",server="workbench"`.
Caddy's route-level instrumentation does not recursively instrument nested
subroutes; `handler="reverse_proxy"` is absent in this configuration. The
counter includes workbench UI/authentication/helper/API requests. Unknown HTTP
methods normalize to `OTHER`.

The PromQL scenario replays these captured label sets with known synthetic
increments to verify the recording contract. The timestamps/increments in that
scenario are a deterministic rule test, not a measured database load result.
