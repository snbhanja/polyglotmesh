# Future improvements

A working list of enhancements, ordered roughly by impact and effort. Each
item is self-contained — pick the ones that matter for your deployment.

## Observability (passes 8–12 follow-ups)

- **Per-upstream latency breakdown** — `upstream_duration_seconds` and
  `request_duration_seconds` are currently rolled up across all upstreams.
  Add a labeled variant (`{upstream_id="openai-prod",...}`) so dashboards
  can compare providers. ~50 LOC in `metrics.rs` + a new field on the
  histogram struct.
- **Sliding-window rate metrics** — RPS, RPM, TPM/min over the last 60s
  (and 5m, 1h) using a bucketed ring. Surface as `rate(rps, rpm, tpm)`
  in JSON + as new Prom gauges. The current counters are all-time only.
- **OpenTelemetry-compatible trace export** — `GET /v1/admin/traces/recent`
  returning the last N requests as OTLP-shaped spans (with
  `span.kind=client`, attributes for model/upstream/status). Wire up a
  `tracing-opentelemetry` layer when needed; the in-memory span buffer
  can be the same `DashMap` shape used today.
- **Live tail on the dashboard** — WebSocket / SSE stream of the most
  recent 100 events with live latency + cost. The dashboard already
  polls every 5–60s; tail mode would be true live.
- **Operational reset endpoint** — `POST /v1/admin/metrics/reset` with
  an audit log row, useful for staging environments. Should require
  admin auth and refuse to run on a config flag for production.
- **Per-model error breakdown** — current `error_total` is labeled by
  upstream+reason; add a `model` label so the dashboard can show
  "which model is failing for which provider". One line in the
  dispatcher.

## Persistence (passes 4, 6, 11 follow-ups)

- **`usage_events` retention cron** — the daily prune exists, but a
  real cron with hour-of-day scheduling + an admin endpoint to force
  an immediate prune would be friendlier.
- **Streaming cost accrual** — `cost_micros_total` is only incremented
  on non-stream success today. Streams record tokens async via the
  watcher, so cost totals can be slightly stale during heavy traffic.
  The fix is to write directly from the watcher (already done for
  tokens; cost just needs to follow).
- **Per-key spend history** — `key_usage` is a single row per key. A
  `key_spend_history(alias, window_start, spend, window_end)` table
  would enable historical budget-vs-actual charts.

## Operations

- **Config rollback** — currently `/v1/admin/reload` overwrites in
  place. A `POST /v1/admin/reload?from=path/to/snapshot.toml` would
  let operators stage a config, validate it, then atomically swap.
  Plus a `cp config.toml config.toml.bak` in `cmd_serve` startup
  (last-known-good) so a bad config falls back automatically.
- **Health endpoint with upstream detail** — `/healthz` currently
  returns "ok" if the process is up. A `/healthz?strict=1` mode
  should return 503 if any *required* upstream is unhealthy (configurable
  per upstream with a `critical: true` flag).
- **`polyglotmesh doctor` CLI** — standalone diagnostic command that
  reads `config.toml`, parses the upstreams, optionally does a
  `/v1/models` GET against each (with a short timeout), and prints a
  per-upstream health report. ~200 LOC, no runtime impact.
- **Graceful shutdown** — SIGTERM should drain in-flight requests
  (with a configurable drain timeout) before exiting. Today the
  process exits immediately on Ctrl-C.

## API surface

- **`POST /v1/admin/keys/:alias/rotate`** — issue a new raw token for
  the same alias, mark the old one revoked (with a configurable grace
  window). Useful for secret rotation.
- **`POST /v1/admin/upstreams/:id/test`** — issue a synthetic
  `/v1/models` GET against one upstream, return the response time +
  status. Operator's "is this upstream actually working" smoke test.
- **Per-upstream circuit breaker** — config field
  `circuit_breaker: { failure_threshold: 5, open_duration_s: 30 }`.
  When consecutive failures hit the threshold, mark the upstream
  `paused` for `open_duration_s`, then half-open for one probe.
- **WebSocket streaming** — `/v1/chat/completions` already supports
  SSE via the `stream: true` flag. A native WebSocket transport
  (`/v1/ws/chat/completions`) would let browser clients connect
  without needing to construct EventSource.
- **Batch endpoint** — `/v1/batches` (Anthropic-style) for
  asynchronous multi-request processing. Probably overkill for a
  router; defer until asked.

## Security & auth

- **OIDC / OAuth admin login** — today the admin token is a static
  bearer. A `POST /v1/admin/login` with OIDC code exchange would
  replace the static token with a short-lived JWT. Useful for SSO
  orgs.
- **Per-key IP allowlist** — `ApiKeyConfig.allowed_cidrs: Vec<String>`.
  Enforce in the auth middleware, before the per-key RPM check.
- **Audit log** — DONE. `audit_events` SQLite table +
  `GET /v1/admin/audit?limit=N` endpoint + `Storage::append_audit` /
  `Storage::recent_audit` helpers. Currently logs `metrics.reset`;
  the next step is to wire it into key-create / key-revoke /
  config-reload / retention-set handlers.
- **mTLS for upstream calls** — `UpstreamConfig.client_cert_path`
  + `client_key_path` + `ca_bundle_path`. Useful for private
  deployments of OpenAI-compatible gateways (vLLM, llama.cpp, etc.).

## Performance

- **Connection pool warm-up** — `reqwest::Client` is created once at
  startup but no upstream connections are pre-warmed. A background
  task that does an OPTIONS or `/v1/models` against each upstream at
  boot would shave the first-request latency.
- **Streaming token pipeline parallelism** — today the stream body
  wrapper yields bytes to the client and extracts usage in lockstep.
  Splitting those into two independent tasks (one for the client, one
  for the parser) would let SSE parsing happen off the critical path
  for high-throughput streams.
- **LRU cache for `model_info` lookups** — `cost_for()` does a
  `BTreeMap` lookup per request. For ~10 models × N upstreams, the
  cost is negligible. Defer until a real profile shows it.

## UI

- **Dashboard → embedded charts** — DONE. Chart.js via CDN, P50/P95/P99
  cards, per-upstream sparkline, rates panel, SSE live-tail, dark/light
  toggle. Single-file at `static/dashboard.html`.
- **Per-upstream sparkline** — DONE. Shows the per-upstream RPS for
  the last 60s of observations.
- **Dark/light theme toggle** — DONE (dashboard header button).
- **Multi-router view** — if you have multiple routers behind a load
  balancer, the dashboard could aggregate by adding a
  `?peer=http://other-router:8080` param. Useful for fleet operators.

## Out of scope (deliberately)

- **Built-in semantic cache** — Bifrost has one, but it adds a
  vector-store dependency. If you want it, point to a separate
  Redis / Qdrant; the proxy path doesn't need to know.
- **Built-in rate-limit middleware** — we have per-key RPM/TPM
  already; global rate limits can be done with `iptables` or a
  sidecar. Defer.
- **Provider-specific prompt caching logic** — the metrics layer
  records `cache_read_input_tokens_total` and
  `cache_write_input_tokens_total` labels when the upstream returns
  them, but we don't have a strategy for re-injecting cache_control
  markers in requests. Add per-provider when the use case is real.


## Future observability & dashboard work

The current observability stack (per-upstream histograms, rate rings,
trace ring, SSE event bus, single-file dashboard) is intentionally
lightweight: no Prometheus, no Jaeger, no OTel collector. The next
moves in this area:

- **Usage dashboard for non-admin operators** — today
  `GET /v1/admin/usage` returns raw buckets; a per-key usage view
  (last 24h requests / tokens / cost, broken down by model) would
  be useful for app owners who only have an API key, not an admin
  token. Read-only, no new write paths.
- **Per-token pricing UI** — prices are entered as
  USD per million tokens (input / output / cached). A small
  `POST /v1/admin/upstreams/:id/prices` already exists; the missing
  piece is a dashboard form to edit them with validation feedback
  (must be > 0, must have all three rates to be useful for cost
  accounting).
- **SSE reconnect with backoff** — the dashboard's live tail silently
  drops events on SSE disconnect. A reconnect-with-jitter that
  resumes from `last-event-id` (a span/trace id) would be
  nicer than the current "reload to see recent activity" workaround.
- **Long-term metrics retention** — `metric_samples` is overwritten
  every 10s with the cumulative counters. A separate `metric_samples_history`
  table with downsampled rollups (1m / 5m / 1h averages) would let
  the dashboard plot "requests per second over the last 7 days"
  without depending on an external TSDB.
- **gRPC streaming export** — for operators who want to push to an
  existing observability stack without running a Prometheus scraper.
  Optional, behind a `metrics_export_grpc: { addr: "..." }` config
  field; off by default.
- **Trace sampling** — the trace ring holds 1000 spans; under heavy
  load the oldest are evicted. A `trace_sample_ratio: f64` config
  (e.g. 0.1 for 10% sampling) would trade fidelity for retention
  on long-running processes.
