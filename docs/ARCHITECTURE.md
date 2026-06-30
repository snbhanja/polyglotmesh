# Architecture

A tour of `polyglotmesh`'s internals: the modules, the request lifecycle,
the queue, the auth/limits pipeline, and the runtime state machine. The goal
of this document is that a new contributor can find any behavior in the code
within ~30 seconds.

## 1. Bird's-eye view

```
                   ┌─────────────────────────────────────────────────┐
                   │                  axum 0.7                        │
                   │  ┌──────────┐  ┌─────────────┐  ┌──────────────┐  │
   HTTP request ──▶│  │  auth_   │─▶│ proxy/      │─▶│ admin/       │  │
   /v1/*  /admin/* │  │ middleware│  │ openai_…    │  │ admin_router │  │
                   │  └────┬─────┘  │ anthropic_… │  └──────┬───────┘  │
                   │       │        │  (per-route)│         │          │
                   │       │        └──────┬──────┘         │          │
                   │       │               │                │          │
                   │       ▼               ▼                ▼          │
                   │   AuthStore       QueueManager     AppState       │
                   │   (KeyRecord[])   (Notify)         (Arc<…>)       │
                   │                       │                │          │
                   │                       ▼                ▼          │
                   │                 UpstreamRegistry  Config (Arc<RwLock>)
                   │                       │
                   │                       ▼
                   │                  Upstream[] (per kind)
                   │                       │
                   │                       ▼
                   │                  reqwest::Client  ──▶ upstream HTTP
                   └─────────────────────────────────────────────────┘
```

- **One process, one binary**, one Tokio multi-thread runtime.
- **One HTTP server** bound to `server.bind`. Routes are merged from three
  sub-routers: `public` (`/healthz`), `api` (`/v1/*`, behind auth middleware),
  and `admin` (`/v1/admin/*`, behind admin-only middleware).
- **One shared `AppState`** cloned as `Arc<AppState>` into every handler.
  It holds the config, the upstream registry, the queue manager, the auth
  store, and the shared `reqwest::Client`.

## 2. Source layout

```
src/
├── main.rs          CLI parser, subcommand dispatch, server bootstrap
├── error.rs         RouterError + axum IntoResponse mapping
├── state.rs         AppState (the shared state struct)
├── config/
│   ├── mod.rs       load/save + path discovery
│   └── types.rs     Config, UpstreamConfig, ApiKeyConfig, QueueConfig, …
├── auth/mod.rs      KeyRecord, KeyUsage, AuthStore, sha256 hashing, key gen
├── upstream/mod.rs  Upstream (per-deployment live state), UpstreamRegistry
├── queue/mod.rs     QueueManager (Notify-based wait loop)
├── proxy/mod.rs     OpenAI + Anthropic forwarders, model rewriting, filter
├── admin/mod.rs     /v1/admin/* routes, health-check background task
```

Module dependency graph (lower depends on nothing of the others; upper depend
on the lower):

```
main ─▶ admin ─▶ proxy ─▶ state ─▶ auth
                    │            └▶ upstream ─▶ config
                    └▶ queue
                    └▶ error
```

## 3. The shared state

`AppState` (in `src/state.rs`) is a single struct cloned into an `Arc` and
shared by every handler:

```rust
pub struct AppState {
    pub config: Arc<parking_lot::RwLock<Config>>,   // live config
    pub registry: Arc<UpstreamRegistry>,            // live upstreams
    pub queue: Arc<QueueManager>,                   // live queue
    pub auth: Arc<AuthStore>,                       // live keys
    pub http: reqwest::Client,                     // shared HTTP client
}
```

It is constructed once at startup by `AppState::from_config(cfg)`, which:

1. Builds a `QueueManager` from `cfg.queue`.
2. Builds an `UpstreamRegistry` from `cfg.upstreams`.
3. Builds an `AuthStore` and loads every `ApiKeyConfig` (rich) and every
   legacy bare-string key.
4. Builds the shared `reqwest::Client` (one pool, all upstreams).
5. Wraps the config in an `Arc<RwLock>` so admin handlers can mutate it
   live (e.g. add a key, edit a deployment).

`AppState::save_to_disk()` re-serializes the config to `~/.polyglotmesh/config.toml`
after any admin write. There is no live-reload — config changes require a
restart, but the admin API does the right thing: it mutates in-memory state
immediately and persists to disk so the next start picks up the same state.

## 4. Request lifecycle

### 4a. Public route (`/healthz`)

```
client ─▶ axum ─▶ proxy::health
                     │
                     ├─▶ registry.all()   →  snapshot
                     ├─▶ queue.snapshot() →  queue stats
                     └─▶ auth.all_keys()  →  per-key usage
                     ▼
                  JSON 200
```

No auth, no upstream calls, no queue. Just a JSON snapshot of in-memory
state.

### 4b. OpenAI / Anthropic route (`/v1/chat/completions`, `/v1/messages`)

```
client ─▶ axum ─▶ auth_middleware ─▶ openai_chat_completions
                       │                     │
                       │                     ├─▶ read body, model, stream flag
                       │                     ├─▶ state.authorize()  (auth + limits)
                       │                     │     │
                       │                     │     ├─▶ AuthStore.lookup(token)
                       │                     │     ├─▶ check expires / blocked
                       │                     │     ├─▶ check rpm_limit (1m window)
                       │                     │     ├─▶ check max_parallel_requests
                       │                     │     ├─▶ check max_budget
                       │                     │     └─▶ return Arc<KeyRecord>
                       │                     │
                       │                     ├─▶ check key.allowed_models
                       │                     │   (401 if model not in allow-list)
                       │                     │
                       │                     ├─▶ resolve_alias(model)
                       │                     │     ├─▶ model_list first
                       │                     │     └─▶ model_aliases fallback
                       │                     │
                       │                     ├─▶ filter_upstreams_for_model()
                       │                     │   (only upstreams declaring that
                       │                     │    model are eligible)
                       │                     │
                       │                     ├─▶ queue.acquire(upstreams, kind)
                       │                     │     ├─▶ try_acquire (greedy: highest
                       │                     │     │   priority upstream with a free
                       │                     │     │   slot + token-bucket tokens)
                       │                     │     └─▶ else: wait on Notify
                       │                     │         (with backoff up to 200ms)
                       │                     │
                       │                     ├─▶ forward_openai / forward_anthropic
                       │                     │     ├─▶ build upstream request
                       │                     │     ├─▶ reqwest.send (with stream or not)
                       │                     │     └─▶ build client response
                       │                     │
                       │                     ├─▶ upstream.release() (in-flight)
                       │                     ├─▶ upstream.record_success/failure
                       │                     ├─▶ key.usage.record_*  (post-response)
                       │                     └─▶ state.release_key(rec) (parallel slot)
                       ▼
                  response
```

**Stream and non-stream diverge only inside `forward_*`**:
- non-stream: `reqwest.send().await?.bytes().await?` → build a single `Body`.
- stream: `resp.bytes_stream()` → wrap in `Body::from_stream` and pipe through.

### 4c. Admin route (`/v1/admin/*`)

```
client ─▶ axum ─▶ admin_auth_middleware ─▶ admin::…
                       │
                       ├─▶ AuthStore.authorize(headers)
                       ├─▶ rec.role == "admin"?
                       └─▶ if yes, next.run(req)
                          if no,  401 "admin token required"
```

## 5. The queue

`QueueManager` (in `src/queue/mod.rs`) implements per-provider greedy
acquisition with Notify-based wait.

### Acquisition algorithm
1. **Sort** upstreams by `(priority desc, id asc)` at registry build time.
2. **Try greedy**: iterate in order, return the first upstream that
   `try_acquire()` accepts. `try_acquire()` checks:
   - `enabled`
   - `paused == false`
   - `health.allows_traffic()` (not Unhealthy)
   - `in_flight < max_concurrency` (if cap > 0)
   - `rate_tokens >= 1.0` (token-bucket, refilled per-second)
3. **Wait** if no slot is free, up to `queue.queue_wait_timeout_ms`:
   - Insert a per-provider wait counter (`pending_waits` map).
   - `tokio::select!` between `queue.notify.notified()` and an exponential
     backoff sleep starting at 5ms, capped at 200ms.
   - On notify: re-attempt greedy acquisition. On backoff timeout: re-attempt.
   - On deadline: decrement the counter and return `NoHealthyUpstream`.
4. **Reserve** the slot before returning. The caller MUST call
   `upstream.release()` when done, which is enforced by RAII-style calls in
   the proxy layer.

### Notify triggers
The health-check background task calls `queue.notify_change()` after every
probe round, so a waiting request wakes up immediately when an upstream flips
back to healthy.

### Stats
`QueueStats` is `Arc<QueueManager>`, so every counter is atomic:
- `total_waited` — requests that had to wait at least once
- `total_rejected` — requests that timed out waiting
- `total_dispatched` — requests that got a slot
- `total_no_upstream` — requests that arrived with no eligible upstreams

## 6. Upstream live state

Each `Upstream` (in `src/upstream/mod.rs`) is a per-deployment state object
that lives for the lifetime of the process:

```rust
pub struct Upstream {
    pub cfg: RwLock<UpstreamConfig>,       // mutable config
    pub health: RwLock<Health>,            // healthy/degraded/unhealthy
    pub in_flight: AtomicU32,              // active requests
    pub consecutive_failures: AtomicU32,   // for health flip
    pub rate_tokens: Mutex<RateBucket>,    // token-bucket
    pub success_count: AtomicU64,
    pub failure_count: AtomicU64,
    pub last_success: RwLock<Option<Instant>>,
    pub last_error: RwLock<Option<String>>,
    pub models: RwLock<Vec<String>>,
    pub paused: AtomicBool,
}
```

Health flip is per-upstream:
- `consecutive_failures >= 3` → Unhealthy (default threshold, override via
  `AI_LLM_ROUTER_FAIL_THRESHOLD`).
- `consecutive_failures >= 2` → Degraded (still routes traffic).
- `record_success()` resets to 0; any success flips back to Healthy.

The background health checker (in `admin/mod.rs`) runs every
`queue.healthcheck_interval_ms`, probes each upstream's `/models` (OpenAI) or
`/v1/messages` (Anthropic), and calls `record_success` / `record_failure`.
After every probe round it calls `queue.notify_change()` so queued requests
pick up recovered upstreams.

## 7. Auth and per-key limits

`AuthStore` (in `src/auth/mod.rs`) holds `Arc<KeyRecord>` items.

### KeyRecord
```rust
pub struct KeyRecord {
    pub raw: String,                                    // the token itself
    pub alias: String,                                  // friendly label
    pub role: String,                                   // "api" or "admin"
    pub models: Vec<String>,                            // allow-list
    pub allowed_providers: Vec<ProviderKind>,           // openai / anthropic
    pub rpm_limit: u32,                                 // 0 = unlimited
    pub tpm_limit: u32,                                 // 0 = unlimited
    pub max_parallel_requests: u32,                     // 0 = unlimited
    pub max_budget: Option<f64>,                        // USD
    pub soft_budget: Option<f64>,                       // USD
    pub budget_duration: Option<String>,                // "1d" etc.
    pub budget_window_start: Mutex<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub allowed_model_region: Option<String>,
    pub blocked: bool,
    pub created_at: DateTime<Utc>,
    pub usage: KeyUsage,
}
```

### KeyUsage (atomic counters)
```rust
pub struct KeyUsage {
    pub total_requests: AtomicU64,
    pub total_input_tokens: AtomicU64,
    pub total_output_tokens: AtomicU64,
    pub total_spend_usd: AtomicU64,                     // f64 cents*1000
    pub rpm_window_start: Mutex<DateTime<Utc>>,
    pub rpm_window_count: AtomicU32,                    // rolling 1m
    pub tpm_window_start: Mutex<DateTime<Utc>>,
    pub tpm_window_tokens: AtomicU32,                   // rolling 1m
    pub in_flight: AtomicU32,
}
```

### Authorization flow (`auth::authorize`)
```
bearer header ─▶ lookup raw in AuthStore
              ├─▶ not found       ─▶ 401
              ├─▶ blocked         ─▶ 401
              ├─▶ expires_at past ─▶ 401
              ├─▶ rpm_limit hit   ─▶ 429
              ├─▶ max_parallel    ─▶ 429
              ├─▶ max_budget      ─▶ 402
              └─▶ OK              ─▶ Arc<KeyRecord>
```

`max_parallel_requests` is incremented in `try_fetch_update` (compare-and-
swap). The handler MUST call `state.release_key(rec)` after the response
is built; the middleware in `auth_middleware` doesn't do it for you
(handlers own the response lifecycle so the slot is held for the entire
upstream round-trip, not just the auth check).

Token usage (`record_usage(rec, input, output, cost)`) is currently a
no-op-from-the-proxy-side: the proxy returns the upstream body without
parsing token counts (avoids a full body buffer for streams). Cost can
be supplied by the upstream's response `_cost_usd` or `usage.cost` field.
The hook is there for future enrichment; counters currently advance only
on the RPM window (per-request increment).

## 8. Configuration

See `examples/config.sample.toml` for the annotated reference. Schema
lives in `src/config/types.rs`.

Key shape:

```toml
[server]                                    # bind, admin_token, limits
api_keys_legacy = ["pgm-…"]                # legacy bare strings
[[api_keys]]                                # rich entries
key = "pgm-…"
key_alias = "dev"
rpm_limit = 60
tpm_limit = 200000
max_budget = 5.0
budget_duration = "1d"
models = ["gpt-4o-mini", "claude-3-5-sonnet-20241022"]

[[upstreams]]                               # OpenAI / Anthropic deployments
id = "openai-primary"
kind = "openai"
base_url = "https://api.openai.com/v1"
api_key = "sk-…"
priority = 30
weight = 3
models = ["gpt-4o-mini", "gpt-4o"]
max_concurrency = 50
rate_limit_rpm = 500
rate_limit_tpm = 200000
max_budget = 100.0
budget_duration = "1d"
region = "us"
tags = ["prod", "fast"]

[[model_list]]                              # LiteLLM-style top-level alias
model_name = "gpt4-mini"
upstream_id = "openai-primary"

[model_aliases]                             # multi-upstream failover
"gpt-mini-fanout" = [
  { upstream_id = "openai-primary",  upstream_model = "gpt-4o-mini" },
  { upstream_id = "openai-fallback", upstream_model = "gpt-4o-mini" },
]

[queue]                                     # global queue + health tuning
max_queue_per_provider = 0
queue_wait_timeout_ms = 30000
healthcheck_interval_ms = 15000
healthcheck_timeout_ms = 5000
healthcheck_failure_threshold = 3
```

## 9. Failure modes and guarantees

- **All upstream calls have a `timeout_ms` cap** (default 60 s). Past that,
  the proxy returns a `BadGateway` mirroring the upstream body if any.
- **Stream bodies are piped through directly** — no buffering. Backpressure
  is enforced by `reqwest` and `axum::Body::from_stream`.
- **Authorization never blocks the queue** — the auth check is purely
  in-memory and O(1) on the number of keys.
- **Config writes through the admin API are atomic** at the in-memory
  state (RwLock), then persisted to disk. If disk write fails, the change
  is still in effect for the running process but will be lost on restart.
- **A panicking handler returns 500** but does not bring down the process;
  axum's per-request task isolates it.
- **The health checker is best-effort** — a probe failure flips the upstream
  Unhealthy but does not affect the probe loop. The next interval retries.

## Storage layer (SQLite)

`src/storage.rs` wraps `rusqlite` (bundled, no system dependency). The database
file is `$POLYGLOTMESH_HOME/state.db`, opened in WAL mode for concurrent
readers + a single writer.

### Schema (v1)

| Table | Purpose |
| --- | --- |
| `api_keys` | `(alias PK, raw_hash, role, config_json, created_at)` — every API key ever issued, including the legacy `api_keys_legacy` form and the admin token. |
| `upstreams` | `(id PK, config_json, created_at)` — mirror of the upstreams in `config.toml`. |
| `key_usage` | `(alias PK FK, total_requests, total_input_tokens, total_output_tokens, total_spend_micros, in_flight, rpm_window_start, rpm_window_count, tpm_window_start, tpm_window_tokens)`. |
| `usage_events` | `(id PK auto, alias, upstream_id, model, input_tokens, output_tokens, cost_usd, status, at)` — append-only audit log. |

### When writes happen

* `AppState::authorize` — on a successful RPM check, the rolling window is
  upserted via `Storage::update_rpm_window`.
* `AppState::release_key` — decrements `in_flight` and persists it via
  `Storage::set_in_flight`.
* `AppState::record_key_usage` — after each successful (non-stream) response
  the counter delta, the rolling TPM window, and a `usage_events` row are
  written in a single transaction via `Storage::apply_counter_delta`.

Storage failures are logged at `debug` level and **do not fail the request** —
the in-memory counters remain authoritative, and the next event will re-sync
the database.

### Hot reload (`POST /v1/admin/reload`)

`AppState::reload_from_disk` re-reads `config.toml`, rebuilds the upstream
registry in place, atomically replaces the `AuthStore`, and re-hydrates every
key's `KeyUsage` from SQLite. Reload never drops in-flight requests, never
double-charges counters, and never requires a process restart.


## Pricing & cost accounting

Each `Upstream` exposes `cost_for(model, in, out)` that returns USD cost plus
a `cost_unknown` flag. Resolution order:

1. The upstream's own `model_info.<model>` override (per-upstream TOML config).
2. A built-in default price table for ~12 popular models.
3. `0.0` with `cost_unknown = true` (operator can configure a price later).

The proxy invokes this from `extract_usage(...)` after a non-stream response,
and from the `spawn_stream_usage_watcher(...)` background task after the
client drains a streaming response (the watcher polls an `Arc<Mutex<...>>`
filled in as SSE bytes flow through the body).

## Config auto-reload watcher

`admin::spawn_config_watcher(state)` runs every 2s. It `stat()`s
`$POLYGLOTMESH_HOME/config.toml`, compares mtime, and on change calls
`AppState::reload_from_disk()`. The first observation only records the
mtime (no reload). Subsequent changes always trigger a reload.

This complements — never replaces — the explicit `POST /v1/admin/reload`
endpoint, which is what scripted deploys should use.

## Usage aggregation

`Storage::usage_summary(group_by, since, until)` runs a parameterized
`GROUP BY` over the `usage_events` table. The `usage_aggregation` free
function is a pure SQL function exposed for future cron-style rollups.


## Retention pruning

`admin::spawn_retention_task(state)` sleeps 30s after startup, then loops
every 24h. Each iteration reads `server.usage_retention_days` from config;
if > 0, it computes `now - days * 86400` and calls
`Storage::delete_events_older_than(cutoff)`, which executes a `DELETE FROM
usage_events WHERE at < ?` and runs a `PRAGMA wal_checkpoint(TRUNCATE)` to
reclaim disk.

## Budget reset task

`admin::spawn_budget_reset_task(state)` loops every 60s. For each loaded
key with a `max_budget` + `budget_duration`, it checks whether the current
budget window has expired (using the same `parse_duration_to_chrono` helper
as the in-request path). On expiry it resets the in-memory `total_spend_usd`
to 0 and persists the reset via `Storage::reset_spend`. This means a key
that accrues spend then goes idle for a full window is automatically
re-enabled without needing a request to trigger the reset.

## Pricing overrides

`POST /v1/admin/upstreams/:id/prices` accepts a JSON body
`{ "merge": bool, "prices": { "<model>": ModelCost } }` and atomically:
1. Updates the in-memory `Upstream::cfg.model_info` (merge or replace).
2. Persists to `config.toml` via `AppState::save_to_disk`.
3. Mirrors into SQLite via `Storage::upsert_upstream` so a future reload
   picks up the override.

The `cost_for()` method checks the per-upstream map FIRST, so an operator-
set override always beats the built-in default price table.

## inotify-based config watcher

`admin::spawn_config_watcher(state)` uses `notify::recommended_watcher` to
subscribe to the parent directory of `config.toml`. Any `Modify`/`Create`
event on a child whose name matches triggers an `Arc`-channel send. The
task debounces (~150 ms) and then calls `AppState::reload_from_disk()`.

If `notify` fails to install (sandbox, missing inotify on the platform),
the task logs a warning and falls back to a 2-second `stat()` poll that
behaves identically. Both paths coexist — the fallback is a safety net, not
the primary path on supported platforms.


## Observability layer (`src/metrics.rs`)

Three primitive types, all `Arc`-wrapped and `Send + Sync`:

- **`Counter`** — labeled `AtomicU64`, stored in a `parking_lot::RwLock<BTreeMap>`.
  Iteration order is deterministic (good for tests + dashboards).
- **`Histogram`** — fixed 14-bucket log-scale latency histogram (1ms..30s).
  Each bucket is an `AtomicU64`; the `snapshot()` method computes cumulative
  counts and the `quantiles_us()` helper returns P50/P95/P99.
- **`Gauge`** — labeled `AtomicU64` with `set/inc/dec` and snapshot.

The `Metrics` struct owns one of each, plus 9 named counters. Two snapshot
methods produce flat `MetricSampleRow` records for persistence:

- `snapshot_for_persist()` — counters + bucket counts + sum + count
- `snapshot_for_persist_counters_only()` — counters + sum + count, no buckets

The counter-only variant is used for the **first** persist run after
startup, to avoid overwriting restored bucket values with zero before any
new requests have been observed.

### Request lifecycle

The `RequestTimer` struct in `proxy/mod.rs` captures four timestamps:
`start` (handler entry), `upstream_start` (after `queue.acquire`),
`first_byte` (set by the stream body wrapper when the first SSE chunk
arrives), and a `method`/`model`/`stream` triple. `RequestTimer::finalize`
emits the full set of metric observations: `request_duration_seconds`,
`upstream_duration_seconds`, `time_to_first_token_seconds` (if stream),
plus the labeled `requests_total` / `success_total` / `error_total` /
`input_tokens_total` / `output_tokens_total` / `cost_micros_total` /
`upstream_up` observations.

The `upstream_stream_body` wrapper, in addition to the SSE usage
accumulator, also records `inter_token` observations on every chunk
after the first.

### Prometheus text export

`Metrics::prometheus_text()` produces the standard
`text/plain; version=0.0.4` content. Histograms emit one
`{name}_bucket{le=…}` line per bucket (cumulative), a `{name}_sum` line
in seconds, and a `{name}_count` line. No external Prom crate — the
formatter is ~30 lines.

### Persistence

A 10 s `spawn_metrics_persister` task snapshots the registry and writes
to `metric_samples(name, labels_json, value, updated_at)` via
`INSERT OR REPLACE`. Counters are monotonic; bucket values are cumulative
(Prometheus convention); the `restore_histogram` helper in `state.rs`
converts cumulative → per-slot on load. Histogram `_sum` and `_count`
are stored as synthetic rows named `_hist_sum::{path}` /
`_hist_count::{path}` with `value` holding the absolute number.


### Per-upstream / per-model labeled histograms

`LabeledHistogram` is a `parking_lot::RwLock<BTreeMap<Vec<(&'static str, String)>, Histogram>>`:
one inner `Histogram` per unique label tuple. `observe()` lazily
allocates a new inner histogram on first use; `snapshot()` returns a
`Vec<(labels, HistogramSample)>` for serialization. Used for
`request_duration_by_upstream` and `upstream_duration_by_upstream`,
both labeled by `(upstream_id, model)`.

### Sliding-window rate rings

`RateRing` is a fixed-size ring of 1-second buckets with a "head" pointer
to the current second. `add(n, now_s)` zeroes any buckets between the
old `head` and `now_s`, then increments the current bucket. `sum(now_s)`
walks the last `window_s` seconds. `Rates` is a struct of eight rings
covering RPS / TPS / cost over 1m / 5m / 1h.

### Trace spans and event bus

`TraceSpan` is a flat struct with `name`, `start/end_time_unix_nano`,
`attributes: BTreeMap<String, String>`, and `status: { code, message }`.
`TraceRing` is a `Mutex<Vec<TraceSpan>>` with FIFO eviction at a fixed
capacity (1000). `EventBus` is a `Mutex<Vec<UnboundedSender>>` with
lazy-disconnect on send failure. Both are appended to in
`RequestTimer::finalize` so they capture every request, success or
error.

### Operational reset and audit log

`Metrics::reset()` zeros every counter, histogram, gauge, rate ring, and
the trace ring. The SQLite `metric_samples` snapshot is NOT touched
directly — the next 10s persistence cycle re-snapshots the (now-zero)
in-memory state and UPSERTs it, completing the reset end-to-end. The
`audit_events` table is append-only and never cleared.
`Storage::append_audit(action, actor, detail)` is called from the
admin handlers; `Storage::recent_audit(limit)` reads them back
newest-first.
