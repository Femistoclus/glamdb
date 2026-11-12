# Gateway replay fixture

`caddy-2.11.9.prom` is the request-duration histogram subset of a real Caddy
2.11.9 scrape from the packaged workbench gateway. Only metric names, bounded
status/method/handler/server/bucket labels and numeric values are retained;
there are no credentials, request IDs, URLs, addresses or user data.

The actual outer route is `handler="subroute",server="workbench"`. Its
histogram counts all gateway traffic, including UI, authentication, helpers and
API calls. It does not separate database operations. The live-process replay
scrapes these captured, unchanged values as idle initialized counters and
verifies that the authentication-failure check becomes fresh OK rather than
NODATA. Synthetic rate/threshold transitions remain separate PromQL fixtures.
