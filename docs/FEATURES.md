# Features

The complete feature surface: every CLI subcommand, every HTTP endpoint,
every config field, with examples. For *how it works* see
[`ARCHITECTURE.md`](./ARCHITECTURE.md); for a working config see
[`examples/config.sample.toml`](../examples/config.sample.toml).

---

## CLI

```
polyglotmesh init [--bind ADDR] [--no-key]            first-time setup
polyglotmesh key  [--role api|admin]                  generate a new key
polyglotmesh upstream add [flags]                      register a deployment
polyglotmesh upstream remove --id ID
polyglotmesh upstream list
polyglotmesh show                                      dump active config.toml
polyglotmesh where                                     print config path
polyglotmesh serve [--bind ADDR]                       run the HTTP server
```

Global flag: `--config <PATH>` (defaults to `$POLYGLOTMESH_HOME/config.toml`
or `~/.polyglotmesh/config.toml`).

### `init`

Generates the config directory, the first API key, and prints the URLs.

```
$ polyglotmesh init --bind 0.0.0.0:8080
Config written to: /home/you/.polyglotmesh/config.toml
Bind: 0.0.0.0:8080

OpenAI-compatible base URL:    http://0.0.0.0:8080/v1
Anthropic-compatible base URL: http://0.0.0.0:8080/v1

Your self-issued API key (Bearer token): pgm-…
```

### `key --role api|admin`

Generates a single new key and appends it to the config. Output is the
raw token — there is no way to recover it later (the router stores only a
sha256 hash in memory; the file stores the raw token in `api_keys_legacy`).

```
$ polyglotmesh key --role admin
Admin token: pgm-admin-…
```

### `upstream add`

Registers or updates a deployment.

```
--id            stable id (e.g. "openai-primary")
--kind          "openai" or "anthropic"
--base-url      base URL (no trailing slash needed)
--api-key       upstream key
--models        comma-separated list of model names (empty = pass-through)
--priority      higher = preferred
--weight        weighted-round-robin weight
--timeout-ms    per-request timeout (default 60000)
--max-concurrency   in-flight cap (0 = unlimited)
--rate-limit-rpm    per-upstream RPM cap
--rate-limit-tpm    per-upstream TPM cap
--max-budget        USD budget for this deployment
--budget-duration   "30s" "5m" "1h" "1d" "7d" "30d" "1w" "1mo"
--region            region tag
--tags              comma-separated tags
```

### `show` / `where`

`show` prints the *active* (merged with defaults) config. `where` prints
the config file path so scripts can edit it.

```
$ polyglotmesh where
config: /home/you/.polyglotmesh/config.toml
```

### `serve`

Starts the HTTP server. `Ctrl-C` to stop. The address comes from
`server.bind` in the config (or `--bind`).

---

## HTTP endpoints

### Public

| Method | Path       | Auth   | Body |
|--------|------------|--------|------|
| GET    | `/healthz` | none   | none |

`/healthz` returns:
```json
{
  "status": "ok",
  "upstreams":  [ { "id": "...", "health": "...", "in_flight": 0, ... } ],
  "queue":      { "total_dispatched": 0, "total_rejected": 0, ... },
  "pending":    { "openai": 0, "anthropic": 0 },
  "keys":       [ { "key_alias": "...", "rpm_limit": 60, "usage": { ... } } ]
}
```

### OpenAI-compatible (require API key)

| Method | Path                       | Description |
|--------|----------------------------|-------------|
| POST   | `/v1/chat/completions`     | OpenAI chat completions; supports `stream: true` (SSE) |
| GET    | `/v1/models`               | List logical model names (union of upstreams + aliases) |
| GET    | `/v1/models/{model}`       | Single model metadata |

`/v1/chat/completions` accepts the full OpenAI body. Headers forwarded:
`x-request-id`, `x-correlation-id`, `user-agent`. The `model` field is
optionally rewritten if it matches a `model_list` or `model_aliases` entry.

### Anthropic-compatible (require API key)

| Method | Path            | Description |
|--------|-----------------|-------------|
| POST   | `/v1/messages`  | Anthropic `/v1/messages`; supports `stream: true` |

Headers forwarded: `x-request-id`, `anthropic-beta`, `user-agent`.
`anthropic-version: 2023-06-01` is set by the router.

### Admin (require admin token)

| Method | Path                                                | Description |
|--------|-----------------------------------------------------|-------------|
| GET    | `/v1/admin/status`                                  | Counts, queue stats, config path |
| GET    | `/v1/admin/upstreams`                               | List all deployments |
| POST   | `/v1/admin/upstreams`                               | Create a deployment (full LiteLLM body) |
| GET    | `/v1/admin/upstreams/{id}`                          | Get one deployment |
| PUT    | `/v1/admin/upstreams/{id}`                          | Update a deployment |
| DELETE | `/v1/admin/upstreams/{id}`                          | Delete a deployment |
| POST   | `/v1/admin/upstreams/{id}`                          | Control: `{"action":"enable"\|"disable"\|"reset"}` |
| POST   | `/v1/admin/upstreams/{id}/pause`                    | Pause a deployment (admin override) |
| POST   | `/v1/admin/upstreams/{id}/resume`                   | Resume a paused deployment |
| GET    | `/v1/admin/keys`                                    | List every key with config + live usage |
| POST   | `/v1/admin/keys`                                    | Create a key (full LiteLLM `ApiKeyConfig` body) |
| DELETE | `/v1/admin/keys/{alias}`                            | Revoke a key by alias |
| GET    | `/v1/admin/aliases`                                 | List `model_aliases` map |
| PUT    | `/v1/admin/aliases`                                 | Set/clear one alias entry |
| DELETE | `/v1/admin/aliases/{name}`                          | Delete an alias |
| GET    | `/v1/admin/model_list`                              | List `[[model_list]]` entries |
| PUT    | `/v1/admin/model_list`                              | Replace `[[model_list]]` |
- `POST /v1/admin/metrics/reset` — zero in-memory counters, histograms, gauges, rate rings, and trace ring.
- `GET  /v1/admin/audit?limit=N` — newest-first list of admin actions (reset, key ops, config reload, ...).


`POST /v1/admin/keys` body (LiteLLM parity):

```json
{
  "generate": true,                // if true, the router generates the raw token
  "role": "api",                   // optional override, default "api"
  "key_alias": "dev",
  "models": ["gpt-4o-mini"],       // allow-list (empty = any)
  "allowed_providers": ["openai"], // empty = both
  "rpm_limit": 60,
  "tpm_limit": 200000,
  "max_parallel_requests": 5,
  "max_budget": 5.0,
  "soft_budget": 4.0,
  "budget_duration": "1d",
  "expires": "7d",                 // absolute ISO 8601 or relative
  "allowed_model_region": "us",
  "blocked": false
}
```

Returns:
```json
{
  "key": "pgm-…",                  // shown ONCE, store it
  "key_alias": "dev",
  "role": "api",
  "usage": { "total_requests": 0, "in_flight": 0, ... }
}
```

---

## Per-key limits (LiteLLM parity)

Each `[[api_keys]]` entry supports the same set of fields LiteLLM uses
on virtual keys. Limits are evaluated at request time in this order:

1. **blocked** — return 401 if true.
2. **expires** — return 401 if past the absolute/relative time.
3. **rpm_limit** — 1-minute rolling window. Return 429 if hit.
4. **max_parallel_requests** — in-flight cap. Return 429 if hit.
5. **max_budget** — USD. Reset after `budget_duration` (e.g. "1d" = 24h).
   Return 402 if `spend >= limit`.
6. **soft_budget** — when crossed, the key is throttled (not blocked).

`models` is an allow-list. If non-empty, requests for a model not in the
list return 401 ("key not allowed for this provider/model"). Similarly,
`allowed_providers: ["openai"]` blocks Anthropic-shaped calls.

---

## Per-upstream limits

Each `[[upstreams]]` entry supports the same deployment-level caps as
LiteLLM's `litellm_params`:

- `max_concurrency` — in-flight cap on the deployment.
- `rate_limit_rpm` — token-bucket at 60 s.
- `rate_limit_tpm` — token-bucket (counts `prompt_tokens + completion_tokens`).
- `timeout_ms` — soft per-request timeout.
- `max_budget` + `budget_duration` — USD budget reset on a schedule.
- `region` + `tags` — opaque metadata; the router doesn't currently
  act on these but they're persisted and exposed via the admin API.

The upstream is also subject to the **global health checker**: every
`healthcheck_interval_ms` the router probes `/models` (OpenAI) or
`/v1/messages` (Anthropic). `healthcheck_failure_threshold` consecutive
failures flip it to Unhealthy; the next success flips it back.

---

## Model aliases

Three layers, evaluated in order:

1. **`[[model_list]]`** — `{ model_name, upstream_id, upstream_model? }`.
   Single-upstream alias. `upstream_model` defaults to the alias name.
2. **`[model_aliases]`** — `name = [ {upstream_id, upstream_model}, … ]`.
   Multi-upstream failover; tried in list order.
3. **Direct match** — the model's name is forwarded as-is to upstreams
   that declare it (if their `models` list is non-empty).

The first `model_name → upstream` mapping whose upstream is `is_usable()`
wins. If no alias matches and no upstream declares the model, the router
falls back to passing the request to upstreams with empty `models`
(treated as "any model").

---

## Queue semantics

Per provider kind (`openai`, `anthropic`):

- **Greedy** in priority order: the first upstream with a free slot AND
  a token-bucket token wins.
- **Queue wait** (Notify + exponential backoff up to 200ms) if all
  eligible upstreams are saturated.
- **`max_queue_per_provider`** — soft cap on concurrent waiters; requests
  beyond it get 503 immediately.
- **`queue_wait_timeout_ms`** — hard cap on how long a single request
  can wait. Past that, 503.

The router wakes the queue when an upstream is paused/resumed, when the
health checker probes, and when admin actions change state.

---

## Health checking

Background task in `admin::spawn_health_checker`. Runs every
`healthcheck_interval_ms`:

1. For each enabled upstream, build a GET to `/models` (OpenAI) or
   `/v1/messages` (Anthropic).
2. 2xx, 404, 405 → `record_success()`. Anything else → `record_failure()`.
3. After all upstreams, call `queue.notify_change()` so any queued
   requests get a chance to retry.

`consecutive_failures >= healthcheck_failure_threshold` → Unhealthy.
The deployment is then skipped during acquisition. A single success
flips it back to Healthy.

`POST /v1/admin/upstreams/{id}` with `{"action":"reset"}` clears the
failure counter (manual recovery).

---

## Operational views

### `GET /healthz` (public)
Snapshot of every upstream + queue + per-key usage. Cheap; safe to scrape.

### `GET /v1/admin/status` (admin)
Same data, but counts only (no per-key details). For dashboards.

### `GET /v1/admin/upstreams` (admin)
Full snapshot of every deployment including live state, success/failure
counters, rate-bucket remaining.

### `GET /v1/admin/keys` (admin)
Every key with its config and live usage (requests, tokens, in-flight).

---

## Examples

### Generate a budgeted dev key

```bash
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "generate": true,
    "key_alias": "dev",
    "rpm_limit": 60,
    "tpm_limit": 200000,
    "max_budget": 5.0,
    "budget_duration": "1d",
    "models": ["gpt-4o-mini", "claude-3-5-sonnet-20241022"]
  }' \
  http://localhost:8080/v1/admin/keys
```

### Failover across three OpenAI providers

```toml
[[upstreams]]
id = "openai-primary"
kind = "openai"
base_url = "https://api.openai.com/v1"
api_key = "sk-…"
priority = 30

[[upstreams]]
id = "openai-fallback"
kind = "openai"
base_url = "https://api.openrouter.ai/v1"
api_key = "sk-or-…"
priority = 10

[[upstreams]]
id = "openai-local"
kind = "openai"
base_url = "http://gpu.local:8000/v1"
api_key = "EMPTY"
priority = 5
```

The router picks `openai-primary` first; on 5xx or timeout it retries the
next.

### Multi-upstream alias

```toml
[model_aliases]
"my-gpt-mini" = [
  { upstream_id = "openai-primary", upstream_model = "gpt-4o-mini" },
  { upstream_id = "openai-fallback", upstream_model = "gpt-4o-mini" },
  { upstream_id = "openai-local", upstream_model = "gpt-4o-mini" },
]
```

A request to `model: "my-gpt-mini"` is rewritten to the first usable
upstream's model name and dispatched.

### Per-deployment budget

```toml
[[upstreams]]
id = "openai-primary"
kind = "openai"
base_url = "https://api.openai.com/v1"
api_key = "sk-…"
max_budget = 100.0
budget_duration = "1d"
```

After $100 of spend in 24 h, the router stops routing to this deployment
(it's still in the registry; the queue just skips it). Reset at the start
of the next 24 h window.

## Persistence (SQLite + hot reload)

* **Bundled SQLite (WAL mode)** at `$POLYGLOTMESH_HOME/state.db` — no external
  service, no schema migration tooling required.
* **Per-key counters** survive process restarts: total requests, total input /
  output tokens, total spend (USD, micros-precision), in-flight slot count.
* **Rolling 1-minute windows** for RPM and TPM are persisted so a key that
  hit its limit just before a restart does not get a fresh quota after restart.
* **Audit log** — every successful request is appended to `usage_events` with
  alias, upstream id, model, token counts, cost, and HTTP status.
* **Hot reload** — `POST /v1/admin/reload` re-reads `config.toml` and rebuilds
  the in-memory registry + auth store in place, with counters re-hydrated from
  SQLite. Adding an upstream, raising a budget, or revoking a key can be done
  with zero request drop.
* **No write amplification** — storage I/O is batched per request; the proxy
  path issues at most one transactional write per successful response.

## Pricing & cost accounting

* Built-in default price table for OpenAI (gpt-4o*, gpt-3.5-turbo, o1*, o3-mini)
  and Anthropic (claude-3-5-*, claude-3-*) models, USD per token.
* Per-upstream override via `model_info.<model>` in `config.toml`.
* Real `cost_usd` recorded on every successful response, persisted to both
  `key_usage.total_spend_micros` and the `usage_events.cost_usd` audit row.

## Stream-path token accounting

* SSE bytes flow through a wrapper that fills an `Arc<Mutex<Option<(in, out)>>>`
  accumulator as the body is yielded to the client.
* After the response is fully drained, a background task (`spawn_stream_usage_watcher`)
  reads the accumulator, computes real cost from the price table, and persists
  via `record_key_usage` — so streaming and non-streaming requests are
  accounted identically in the `key_usage` and `usage_events` tables.
* Supports both OpenAI `chat.completion.chunk` and Anthropic `message_delta`
  usage shapes.

## Usage analytics (`GET /v1/admin/usage`)

* `group_by=alias|upstream|model|all`, optional `since`/`until` Unix-seconds
  window, optional `limit` (default 50, max 500).
* Response: `{ "group_by", "totals", "buckets": [{ key, requests, input_tokens, output_tokens, cost_usd }] }`
  sorted by `cost_usd DESC`.
* `GET /v1/admin/usage/recent?limit=N` returns the raw audit log rows.

## Config auto-reload (file watcher)

* Background task polls `config.toml` mtime every 2s.
* On change: `AppState::reload_from_disk()` — rebuilds registry, swaps auth
  store, re-hydrates counters from SQLite.
* First observation is a no-op; subsequent changes always reload.
* Complements the explicit `POST /v1/admin/reload` endpoint.

## Retention policy for `usage_events`

* `server.usage_retention_days` config field, default 0 (keep forever).
* `POST /v1/admin/usage/retention` body `{"days": N}` to change live.
* Background task runs once per day, deletes rows older than the window,
  runs `PRAGMA wal_checkpoint(TRUNCATE)` to reclaim disk.

## Background budget reset

* 60s loop checks all keys with `max_budget` + `budget_duration`.
* Resets in-memory `total_spend_usd` to 0 when the window expires.
* Persists via `Storage::reset_spend` so it survives a restart.
* Eliminates the "key stuck at 100% budget after the window expired but no
  traffic" edge case.

## Per-upstream pricing overrides

* `POST /v1/admin/upstreams/:id/prices` — body `{ merge, prices: { model: ModelCost } }`.
* `GET` to inspect the current price map and known models.
* Stored in `config.toml` and mirrored in SQLite, so a hot-reload restores them.
* Always beats the built-in default price table.

## Kernel-level config watcher (inotify / FSEvents)

* Replaces the 2s polling loop with a `notify::RecommendedWatcher`.
* ~150 ms detection latency, with debounce so editor save-bursts collapse to one reload.
* Graceful fallback to 2s `stat()` poll if the platform watcher can't initialize.
* The `config watcher active (inotify/FSEvents)` log line confirms the fast path is in use.

## Built-in observability (no Prom crate)

* Counters + histograms + gauges, all `Arc<AtomicU64>`-backed, `Send + Sync`.
* Histograms: 14 log-scale buckets, P50/P95/P99 computed on demand from
  cumulative counts.
* `RequestTimer` captures `request_duration`, `upstream_duration`, and
  (for streams) `time_to_first_token_seconds` + `stream_inter_token_seconds`.
* Stream body wrapper tees bytes into a usage accumulator AND an inter-token
  histogram, so streaming and non-streaming are accounted identically.
* `GET /v1/admin/metrics` returns JSON (counters + P50/P95/P99 + gauges).
* `GET /v1/admin/metrics/prom` returns Prometheus text format — drop into
  any existing Grafana/Prom scraper without code changes.
* `GET /dashboard` is a single-file HTML dashboard with auto-refresh, no
  JS framework, no build step. Shows totals, upstream health, top talkers
  (by model / upstream / alias), recent events, latency P50/P95/P99,
  TTFT, and error rate.
* Persisted to SQLite every 10s, restored on startup. Counters and
  histogram sum/count survive a restart; the first persist run after
  startup skips bucket writes to avoid clobbering restored values.
* Zero external deps — no `prometheus`, `metrics-exporter-prometheus`,
  `tracing-opentelemetry`, etc.

### Per-upstream / per-model latency breakdown

Every duration histogram is also exposed as a `LabeledHistogram` keyed
by `(upstream_id, model)`. Useful for comparing providers side-by-side
without losing the global rolled-up view. Available under
`histograms.request_duration_by_upstream` and
`histograms.upstream_duration_by_upstream` in the JSON snapshot.

### Sliding-window rates (`GET /v1/admin/rates`)

Pre-computed RPS / TPS / cost-per-second over 1m, 5m, and 1h windows.
Each rate is a 1-second-bucket ring buffer so the values are cheap to
read on every dashboard refresh.

### Recent traces (`GET /v1/admin/traces/recent?limit=N`)

OTLP-shaped JSON of the most recent request spans. Each span carries
`name`, `startTimeUnixNano`, `endTimeUnixNano`, `attributes` (status,
upstream_id, model, tokens, cost), and a `status` field. No
`tracing-opentelemetry` dependency — the router emits spans directly.

### Live event SSE (`GET /v1/admin/events/stream`)

`text/event-stream` of every completed request, used by the dashboard
live-tail. The browser consumes it via `fetch + reader` so the
`Authorization` header can be attached (browsers' native `EventSource`
cannot set headers).

### Operational reset (`POST /v1/admin/metrics/reset`)

Zeros every in-memory counter, histogram, gauge, rate ring, and the
trace ring. Returns `{ok, reset_at, actor}`. The next metrics
persistence cycle overwrites the SQLite `metric_samples` snapshot with
the new (zero) values, completing the reset end-to-end. **Does NOT**
touch the long-term `usage_events` or `audit_events` tables.

### Audit log (`GET /v1/admin/audit?limit=N`)

Append-only log of every admin action (`metrics.reset` today; more
hooks planned for `key.create`, `key.revoke`, `config.reload`,
`retention.set`). Newest-first; `limit` defaults to 100, max 1000.
Persisted to the `audit_events` SQLite table.
