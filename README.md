# polyglotmesh

[![CI](https://github.com/polyglotmesh/polyglotmesh/actions/workflows/ci.yml/badge.svg)](https://github.com/polyglotmesh/polyglotmesh/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/polyglotmesh.svg)](https://crates.io/crates/polyglotmesh)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust 1.75+](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

A fast, queue-based **Rust** LLM router. You register multiple
OpenAI-compatible and Anthropic-compatible upstreams; the router exposes
a single base URL for each, with priority-based load balancing, per-key
limits, health checks, and live queue stats. Speaks every LLM dialect
in the room — hence *polyglot* — and weaves them into a single mesh —
hence *mesh*.


A fast, queue-based **Rust** LLM router. You register multiple
OpenAI-compatible and Anthropic-compatible upstreams; the router exposes
a single base URL for each, with priority-based load balancing, per-key
limits, health checks, and live queue stats.

```
        ┌──────────┐    ┌───────────────────┐    ┌──────────┐
client ─┤ OpenAI   │───▶│   polyglotmesh   │───▶│ upstream │
        │ Anthropic│    │  (single baseurl, │    │   A      │
        └──────────┘    │   one self-issued │    │   B      │
                        │   API key)        │    │   C      │
                        └───────────────────┘    └──────────┘
```

Documentation map:

- **[`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)** — module layout,
  request lifecycle, queue internals, auth/limits pipeline, state
  machine.
- **[`docs/FEATURES.md`](./docs/FEATURES.md)** — every CLI subcommand,
  every HTTP endpoint, every config field, with examples.
- **[`examples/config.sample.toml`](./examples/config.sample.toml)** — the
  annotated reference config.
- **[`scripts/install.sh`](./scripts/install.sh)** — build + install the
  binary, copy the sample config into `~/.polyglotmesh/`.

## Quick start

```bash
# Build + install
./scripts/install.sh
# → drops ~/.local/bin/polyglotmesh AND ~/.polyglotmesh/config.sample.toml

# Initialize (generates a fresh API key)
polyglotmesh init --bind 0.0.0.0:8080
# → prints the OpenAI + Anthropic base URLs and your Bearer token

# Add 3-4 OpenAI upstreams (one CLI call each)
polyglotmesh upstream add --id openai-1 --kind openai \
  --base-url https://api.openai.com/v1 --api-key "$OPENAI_KEY" \
  --models gpt-4o-mini,gpt-4o --priority 30 --max-concurrency 50

polyglotmesh upstream add --id openai-2 --kind openai \
  --base-url https://api.openrouter.ai/v1 --api-key "$OR_KEY" \
  --models gpt-4o-mini --priority 10

polyglotmesh upstream add --id openai-3 --kind openai \
  --base-url http://gpu.local:8000/v1 --api-key EMPTY \
  --models gpt-4o-mini --priority 5 --tags local

# Add 1-4 Anthropic upstreams
polyglotmesh upstream add --id anthropic-1 --kind anthropic \
  --base-url https://api.anthropic.com --api-key "$ANTHROPIC_KEY" \
  --models claude-3-5-sonnet-20241022 --priority 30

# Generate an admin token
polyglotmesh key --role admin

# Run
polyglotmesh serve
```

## The config file to edit

After `init`, the active config lives at:

```
$ polyglotmesh where
config: /home/you/.polyglotmesh/config.toml
```

That single file controls everything: server bind address, every
upstream's base URL and key, every virtual key with its
`rpm_limit` / `tpm_limit` / `max_budget` / `models` / `expires`, model
aliases, queue tuning, health-check tuning. Run `polyglotmesh show` to
print the *active* (merged) version with defaults filled in.

A fully-commented reference is at
[`examples/config.sample.toml`](./examples/config.sample.toml). It is
also installed alongside the live config as `config.sample.toml` so you
can diff and copy fields.

For the complete field list, see
[`docs/FEATURES.md`](./docs/FEATURES.md#per-key-limits-litemllm-parity).

## Calling the router

```bash
# OpenAI-compatible (chat completions)
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $AILR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'

# OpenAI-compatible (streaming)
curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $AILR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"hi"}]}'

# Anthropic-compatible
curl http://localhost:8080/v1/messages \
  -H "Authorization: Bearer $AILR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":256,"messages":[{"role":"user","content":"hi"}]}'

# OpenAI-compatible (model listing)
curl -H "Authorization: Bearer $AILR_KEY" http://localhost:8080/v1/models

# Health (no auth)
curl http://localhost:8080/healthz
```

## Endpoints at a glance

| Path                          | Auth   | Notes                              |
|-------------------------------|--------|------------------------------------|
| `POST /v1/chat/completions`   | api    | OpenAI; supports `stream: true`    |
| `GET  /v1/models`             | api    | Union of upstreams + aliases       |
| `GET  /v1/models/{model}`     | api    | Single model metadata              |
| `POST /v1/messages`           | api    | Anthropic `/v1/messages`           |
| `GET  /healthz`               | none   | Liveness + queue + per-key stats   |
| `*    /v1/admin/*`            | admin  | Upstreams, keys, aliases, model_list |

See [`docs/FEATURES.md`](./docs/FEATURES.md#http-endpoints) for the
complete table.

## Operational behavior

- **Queue** — per-provider kind (`openai`, `anthropic`). Greedy in
  priority order; if all eligible upstreams are saturated, the request
  waits on a `Notify` (exponential backoff up to 200ms) up to
  `queue.queue_wait_timeout_ms`. See
  [`ARCHITECTURE.md`](./docs/ARCHITECTURE.md#5-the-queue).
- **Per-key limits** — `rpm_limit`, `tpm_limit`, `max_parallel_requests`,
  `max_budget`, `soft_budget`, `expires`, `models` allow-list,
  `allowed_providers`. Returning 429 / 402 / 401 as appropriate. See
  [`FEATURES.md`](./docs/FEATURES.md#per-key-limits-litemllm-parity).
- **Per-upstream limits** — `max_concurrency`, `rate_limit_rpm`,
  `rate_limit_tpm`, `max_budget`, `timeout_ms`.
- **Health checks** — background task probes every
  `healthcheck_interval_ms`; consecutive failures flip the upstream
  Unhealthy; the next success flips it back.
- **Model aliases** — three layers (`model_list`, `model_aliases`,
  per-upstream `models`), all evaluated at request time.

## Scripts

- `scripts/install.sh` — build + install + copy the sample config.
- `examples/bootstrap.sh` — end-to-end installer: registers N upstreams
  from arrays, prints the final URLs and the active config path.
- `scripts/smoke-test.sh` — self-contained end-to-end test using mock
  upstreams.
- `scripts/mock_slow.py` — mock upstream for the smoke test.

## License

MIT

## Persistence & hot reload

Every router run opens a SQLite database at `$POLYGLOTMESH_HOME/state.db` (WAL mode,
bundled via `rusqlite`). It persists per-key counters, rolling RPM/TPM windows,
in-flight slot counts, and an audit trail of every successful request.

| Concern | Where it lives |
| --- | --- |
| API keys & admin tokens | `api_keys` table |
| Upstream definitions | `upstreams` table (mirror of `config.toml`) |
| Per-key totals (requests, tokens, spend) | `key_usage` table |
| Rolling 1-min RPM / TPM windows | `key_usage.rpm_window_*` / `tpm_window_*` |
| Per-request audit log | `usage_events` table |

Edit `config.toml` to add an upstream or change a limit, then have the router pick
it up **without a restart**:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/admin/reload \
  -H "Authorization: Bearer $AILR_ADMIN_TOKEN"
# -> {"status":"reloaded","upstreams":3,"keys":2}
```

On reload the router rebuilds the in-memory upstream registry, atomically swaps
the auth store, and re-hydrates per-key counters from SQLite — so request
volumes, token totals, and rolling windows survive the reload.


## Pricing & cost

The router ships with a built-in price table (USD per token) for popular models:
OpenAI `gpt-4o*`, `gpt-3.5-turbo`, `o1*`, `o3-mini`; Anthropic `claude-3-5-*`,
`claude-3-*`. On every successful response, real `cost_usd` is computed and
persisted to `usage_events` *and* `key_usage`.

Override per-upstream with `model_info`:

```toml
[[upstreams]]
id = "openai-prod"
kind = "openai"
base_url = "https://api.openai.com/v1"
api_key  = "sk-…"
models   = ["gpt-4o-mini"]

[upstreams.model_info]
"gpt-4o-mini" = { input_cost_per_token = 0.0000001, output_cost_per_token = 0.0000004 }
```

## Auto-reload (no restart)

The router watches `$POLYGLOTMESH_HOME/config.toml` for changes every 2s and
auto-applies them — rebuilds the upstream registry, atomically swaps the auth
store, re-hydrates per-key counters from SQLite. No `POST /v1/admin/reload`
needed; the manual endpoint still works for scripted deploys.

## Usage analytics

```bash
curl -sS "http://127.0.0.1:8080/v1/admin/usage?group_by=model" \
  -H "Authorization: Bearer $AILR_ADMIN_TOKEN"
# -> { "group_by":"model", "totals":{...}, "buckets":[{ key:"gpt-4o-mini", requests:8, cost_usd:4.2e-05 }] }

curl -sS "http://127.0.0.1:8080/v1/admin/usage/recent?limit=10" \
  -H "Authorization: Bearer $AILR_ADMIN_TOKEN"
```

`group_by` may be `alias`, `upstream`, `model`, or `all`. Add `since`/`until`
as Unix seconds to bound the window.


## Retention policy

`usage_events` rows can grow without bound. Set a retention window in
`config.toml` and the router prunes rows older than N days once per day:

```toml
[server]
usage_retention_days = 30   # 0 = keep forever
```

Or change it live:

```bash
curl -X POST http://127.0.0.1:8080/v1/admin/usage/retention \
  -H "Authorization: Bearer $AILR_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"days": 30}'
```

## Budget reset

Keys with a `budget_duration` (e.g. `"1h"`, `"7d"`, `"30d"`) have their
`total_spend_usd` automatically reset to 0 when the window expires — even
if the key has zero traffic. A background task fires every 60s and the
reset is persisted to SQLite so it survives restarts.

## Config auto-reload (inotify/FSEvents)

The router uses kernel-level file events (inotify on Linux, FSEvents on
macOS) to detect config changes within ~150 ms — no polling. It falls
back to a 2 s `stat()` poll automatically on platforms where inotify is
unavailable (e.g. inside some sandboxes).

## Per-upstream pricing overrides

```bash
curl -X POST "http://127.0.0.1:8080/v1/admin/upstreams/openai-prod/prices" \
  -H "Authorization: Bearer $AILR_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{
        "merge": true,
        "prices": {
          "gpt-4o-mini": { "input_cost_per_token": 1e-6, "output_cost_per_token": 2e-6 }
        }
      }'
```

`merge: true` keeps existing overrides; `false` replaces the whole map.
Persisted to `config.toml` AND SQLite so it survives reload + restart.


## Observability

Lightweight in-process metrics — **zero external dependencies** (no Prometheus
client crate). Counters, fixed-bucket latency histograms, and gauges, all
backed by atomics. Persisted to SQLite every 10s and rehydrated on startup.

```bash
# JSON snapshot (counts + P50/P95/P99 + active gauges)
curl -sS http://127.0.0.1:8080/v1/admin/metrics -H "Authorization: Bearer $TOKEN"

# Prometheus text format (compatible with any scraper)
curl -sS http://127.0.0.1:8080/v1/admin/metrics/prom -H "Authorization: Bearer $TOKEN"

# Built-in HTML dashboard with live refresh
open http://localhost:8080/dashboard
```

The dashboard is a single-file HTML page served from `static/dashboard.html`
(Chart.js via CDN, no build step). It includes P50/P95/P99 latency
cards, per-upstream RPS sparkline, sliding-window rates, live event
SSE tail, and a dark/light theme toggle. The SQLite store backs the
persisted metrics that survive restarts.

### Metric set (Bifrost-parity, no Prom crate)

| Type | Names |
| --- | --- |
| Counter | `requests_total`, `upstream_requests_total`, `success_total`, `error_total`, `input_tokens_total`, `output_tokens_total`, `cost_micros_total`, `cache_read_input_tokens_total`, `cache_write_input_tokens_total` |
| Histogram | `request_duration_seconds` (full request), `upstream_duration_seconds` (send → first byte), `time_to_first_token_seconds` (TTFT for streams), `stream_inter_token_seconds` (inter-chunk gap) |
| Labeled histogram | `request_duration_by_upstream{upstream_id, model}`, `upstream_duration_by_upstream{upstream_id, model}` |
| Gauge | `active_requests`, `active_streams`, `upstream_up{upstream_id=…}` (1 if last attempt succeeded) |

Counters are labeled by `method`, `model`, `upstream_id`, `reason` (errors).
Histograms have 14 log-scale buckets from 1 ms to 30 s — the P50/P95/P99
quantile is computed on demand from the cumulative bucket counts.

### Sliding-window rates, traces, live events, reset, audit

```bash
# 1m/5m/1h RPS, TPS, cost-per-second
curl -sS http://127.0.0.1:8080/v1/admin/rates -H "Authorization: Bearer $TOKEN"

# OTLP-shaped recent spans
curl -sS 'http://127.0.0.1:8080/v1/admin/traces/recent?limit=20' -H "Authorization: Bearer $TOKEN"

# Server-Sent Events of every completed request (dashboard live-tail)
curl -N  http://127.0.0.1:8080/v1/admin/events/stream -H "Authorization: Bearer $TOKEN"

# Operational reset — zeros in-memory state; long-term tables untouched
curl -X POST http://127.0.0.1:8080/v1/admin/metrics/reset -H "Authorization: Bearer $TOKEN"

# Audit log (key ops, config reload, metrics reset, …)
curl -sS 'http://127.0.0.1:8080/v1/admin/audit?limit=50' -H "Authorization: Bearer $TOKEN"
```

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — internals, storage, observability layer
- [docs/FEATURES.md](docs/FEATURES.md) — full feature catalog
- [docs/FUTURE_IMPROVEMENTS.md](docs/FUTURE_IMPROVEMENTS.md) — working list of planned enhancements
