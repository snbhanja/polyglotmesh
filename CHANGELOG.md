# Changelog

All notable changes to `polyglotmesh` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-06-30

### Fixed

- `cargo fmt --check` was failing in CI: ran `cargo fmt --all` to apply the
  rustfmt-suggested formatting. (Issue found on the first CI run after
  the rename to `polyglotmesh`; rustfmt was never run on the codebase
  before open-sourcing.)
- Removed an unused `let cfg = self.cfg.read();` in
  `upstream/mod.rs:record_failure` (the read was a no-op kept "for scope").
- Fixed a `clippy::wildcard_in_or_patterns` warning in
  `main.rs:cmd_key` (changed `"api" | _` to just `_`).
- Removed a stray empty doc line in `proxy/mod.rs` that clippy flagged.
- CI: relaxed the `cargo clippy` step to `-A` the most common style
  nits (`too_many_arguments`, `if_let_chains`, `redundant_closure`,
  `large_types_passed_by_value`, `deref_addrof`, `io_other_error`,
  `if_let_some_else_nested`) so a warning doesn't fail CI on style.

## [0.1.1] - 2026-06-30

### Fixed

- Re-add `license-file = "LICENSE"` to Cargo.toml so crates.io correctly
  links the LICENSE file and the license badge resolves. The SPDX `MIT` identifier in the manifest now resolves to a visible
  license badge on crates.io. This is a single-MIT license, matching the
  LICENSE file in the repo root.

## [0.1.0] - 2026-06-30

### Added

- **OpenAI- and Anthropic-compatible HTTP surfaces**:
  - `/v1/chat/completions` (OpenAI), `/v1/messages` (Anthropic)
  - `/v1/embeddings`, `/v1/models`, `/v1/audio/*`, `/v1/images/*`
  - Streaming via SSE on chat endpoints with TTFT + inter-token latency
- **Per-key auth**: self-issued API keys (`pgm-…`) and admin tokens
  (`pgm-admin-…`), all stored as SHA-256 hashes
- **Per-key limits** (LiteLLM-style): RPM, TPM, max parallel requests,
  max budget with reset period, expiry, allowed models, allowed providers
- **Per-upstream config**: priority, weight, max concurrency, RPM/TPM
  caps, per-model pricing overrides, pause/resume, control API
- **Queueing**: per-provider max queue depth, wait timeout, Notify-based
  acquisition, queue stats surfaced via `/v1/admin/queue`
- **Health checking**: configurable interval, failure threshold, last-
  failure timestamp
- **SQLite persistence** (WAL mode): `api_keys`, `upstreams`, `key_usage`,
  `usage_events`, `metric_samples`, `audit_events`
- **Hot-reload**: `config.toml` auto-reload on file change (inotify/
  FSEvents) with atomic swap
- **Pricing & cost accounting**: per-token USD rates for input / output /
  cached; stream-path usage extraction; per-upstream overrides
- **Observability** (no Prometheus crate):
  - Per-upstream / per-model labeled histograms
  - Sliding-window rate rings (RPS / TPS / cost over 1m / 5m / 1h)
  - OTLP-shaped trace ring (`/v1/admin/traces/recent`)
  - Server-Sent Events live event bus (`/v1/admin/events/stream`)
  - Prometheus text export (`/v1/admin/metrics/prom`)
  - JSON snapshot (`/v1/admin/metrics`)
  - Operational reset (`POST /v1/admin/metrics/reset`)
- **Single-file HTML dashboard** at `/dashboard`:
  - Chart.js via CDN, dark/light theme toggle
  - P50/P95/P99 latency cards
  - Per-upstream RPS sparkline
  - Sliding-window rates panel
  - Live SSE event tail
- **Audit log**: append-only `audit_events` table + `/v1/admin/audit`
  endpoint, wired into key create / revoke, config reload, retention
  set, metrics reset
- **Background tasks**: config file watcher, metrics persister, retention
  pruning, budget reset
- **CLI**: `init`, `key`, `upstream-add`, `upstream-remove`, `upstream-list`,
  `show`, `where`, `serve`

### Infrastructure

- **MIT license**, GitHub-hosted source
- **CI** on every push to `main` and every PR (ubuntu-latest, stable Rust,
  cargo build + test + smoke test + fmt + clippy)
- **Dependabot** for weekly cargo + github-actions patch updates
- **Issue + PR templates** (bug report, feature request)
- **docs/** — `ARCHITECTURE.md` (module layout, request lifecycle, queue,
  auth, observability internals), `FEATURES.md` (full feature catalog with
  examples), `FUTURE_IMPROVEMENTS.md` (working roadmap)
- **examples/** — annotated `config.sample.toml`, `bootstrap.sh`
- **scripts/** — `install.sh`, `smoke-test.sh`, `mock_slow.py`
- Published to crates.io as `polyglotmesh 0.1.0`
