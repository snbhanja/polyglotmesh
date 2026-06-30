//! Lightweight in-process metrics registry.
//!
//! Inspired by Bifrost's metric set, but with **zero external dependencies**:
//! atomic counters, fixed-bucket latency histograms, and gauges stored in a
//! central `Metrics` struct. Exposed via `/v1/admin/metrics` (JSON) and
//! `/v1/admin/metrics/prom` (text format compatible with Prometheus scrapers).
//!
//! The metric set is small by design. The exact list:
//!
//! Counters (labeled by provider / model / upstream / alias / status / cache):
//!   * `requests_total`           — every incoming proxy request
//!   * `upstream_requests_total`  — every request forwarded to an upstream
//!   * `success_total`            — 2xx upstream response
//!   * `error_total`              — non-2xx upstream response, labeled by reason
//!   * `input_tokens_total`       — prompt tokens
//!   * `output_tokens_total`      — completion tokens
//!   * `cost_micros_total`        — USD × 1e6
//!   * `cache_read_input_tokens_total` / `cache_write_input_tokens_total`
//!
//! Histograms (no labels; rolled up across all upstreams for the per-second
//! dashboards; per-upstream view in the JSON export):
//!   * `request_duration_seconds`       — full request (send + body read)
//!   * `upstream_duration_seconds`      — just the upstream call
//!   * `time_to_first_token_seconds`    — TTFT (streaming only)
//!   * `stream_inter_token_seconds`     — inter-token gap during a stream
//!
//! Gauges:
//!   * `active_requests`               — in-flight count
//!   * `active_streams`                — open streaming responses
//!   * `upstream_up{upstream_id=...}`   — 1 if last attempt to this upstream succeeded

use crate::storage::MetricSampleRow;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---- Atomic counter (labeled) ----

/// A single labeled counter. We use one `Counter` per (name, label-tuple) pair
/// stored in a `BTreeMap` so iteration is deterministic and we don't need
/// a hash combiner.
#[derive(Debug)]
pub struct Counter {
    pub name: &'static str,
    pub help: &'static str,
    pub values: RwLock<BTreeMap<Vec<(&'static str, String)>, AtomicU64>>,
}

impl Counter {
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self { name, help, values: RwLock::new(BTreeMap::new()) }
    }
    pub fn inc(&self, labels: Vec<(&'static str, String)>, by: u64) {
        let mut g = self.values.write();
        let entry = g.entry(labels).or_insert_with(|| AtomicU64::new(0));
        entry.fetch_add(by, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> Vec<CounterSample> {
        let g = self.values.read();
        g.iter().map(|(labels, v)| CounterSample {
            labels: labels.iter().map(|(k, val)| (k.to_string(), val.clone())).collect(),
            value: v.load(Ordering::Relaxed),
        }).collect()
    }
    /// Zero every label series. Used by operational `reset()` to clear the
    /// in-memory counters without touching the SQLite snapshot (which holds
    /// long-term cumulative totals).
    pub fn reset(&self) {
        let g = self.values.read();
        for v in g.values() { v.store(0, Ordering::Relaxed); }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CounterSample {
    pub labels: Vec<(String, String)>,
    pub value: u64,
}

// ---- Fixed-bucket histogram (atomic) ----

/// Latency histograms: 24 buckets on a log scale from 1ms to ~30s.
/// Each bucket is an `AtomicU64` count of observations that fell into it.
/// We also keep `_sum` (microseconds) and `_count` so we can compute averages
/// and P50/P95/P99 on demand.
pub struct Histogram {
    pub name: &'static str,
    pub help: &'static str,
    pub buckets_us: &'static [u64], // upper bounds in microseconds, sorted asc
    pub counts: Vec<AtomicU64>,     // one per bucket
    pub sum_us: AtomicU64,
    pub count: AtomicU64,
}

impl Histogram {
    /// Build the histogram with a runtime-known bucket set.
    pub fn with_buckets(name: &'static str, help: &'static str, buckets_us: &[u64]) -> Self {
        let counts = (0..buckets_us.len()).map(|_| AtomicU64::new(0)).collect();
        Self { name, help, buckets_us: Box::leak(buckets_us.to_vec().into_boxed_slice()), counts, sum_us: AtomicU64::new(0), count: AtomicU64::new(0) }
    }

    pub fn observe_us(&self, us: u64) {
        // Find the first bucket whose upper bound is >= us.
        let idx = self.buckets_us.iter().position(|&b| us <= b).unwrap_or(self.buckets_us.len() - 1);
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSample {
        let mut cumulative = Vec::with_capacity(self.buckets_us.len());
        let mut running: u64 = 0;
        for (i, b) in self.buckets_us.iter().enumerate() {
            running = running.saturating_add(self.counts[i].load(Ordering::Relaxed));
            cumulative.push(HistogramBucket { le_us: *b, count: running });
        }
        let count = self.count.load(Ordering::Relaxed);
        HistogramSample {
            buckets: cumulative,
            sum_us: self.sum_us.load(Ordering::Relaxed),
            count,
        }
    }

    /// Helper: compute P50 / P95 / P99 from a snapshot.
    pub fn quantiles_us(&self, qs: &[f64]) -> Vec<(f64, u64)> {
        let snap = self.snapshot();
        let total = snap.count as f64;
        if total == 0.0 { return qs.iter().map(|q| (*q, 0)).collect(); }
        qs.iter().map(|q| {
            let target = (total * q).ceil() as u64;
            let hit = snap.buckets.iter().find(|b| b.count >= target);
            (*q, hit.map(|b| b.le_us).unwrap_or(u64::MAX))
        }).collect()
    }

    /// Same as `quantiles_us` but takes a precomputed `HistogramSample`.
    /// Used by `LabeledHistogram` snapshot rendering.
    pub fn quantiles_from_sample(snap: &HistogramSample, qs: &[f64]) -> Vec<(f64, u64)> {
        let total = snap.count as f64;
        if total == 0.0 { return qs.iter().map(|q| (*q, 0)).collect(); }
        qs.iter().map(|q| {
            let target = (total * q).ceil() as u64;
            let hit = snap.buckets.iter().find(|b| b.count >= target);
            (*q, hit.map(|b| b.le_us).unwrap_or(u64::MAX))
        }).collect()
    }
    /// Zero every bucket, the running sum, and the total count.
    pub fn reset(&self) {
        for c in &self.counts { c.store(0, Ordering::Relaxed); }
        self.sum_us.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HistogramSample {
    pub buckets: Vec<HistogramBucket>,
    pub sum_us: u64,
    pub count: u64,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct HistogramBucket {
    pub le_us: u64,
    pub count: u64,
}

// ---- Labeled histogram (one Histogram per label tuple) ----

/// Multiple histograms, one per unique label set. Same bucket layout as
/// `Histogram` but the index is the label tuple. Used for per-upstream,
/// per-model latency breakdowns where you want to compare providers
/// side-by-side without losing the global rolled-up view.
pub struct LabeledHistogram {
    pub name: &'static str,
    pub help: &'static str,
    pub buckets_us: &'static [u64],
    pub series: RwLock<BTreeMap<Vec<(&'static str, String)>, Histogram>>,
}

impl LabeledHistogram {
    pub fn new(name: &'static str, help: &'static str, buckets_us: &'static [u64]) -> Self {
        Self { name, help, buckets_us, series: RwLock::new(BTreeMap::new()) }
    }
    pub fn observe(&self, labels: Vec<(&'static str, String)>, us: u64) {
        let mut g = self.series.write();
        let h = g.entry(labels).or_insert_with(|| Histogram::with_buckets(self.name, self.help, self.buckets_us));
        h.observe_us(us);
    }
    pub fn snapshot(&self) -> Vec<(Vec<(String, String)>, HistogramSample)> {
        let g = self.series.read();
        g.iter().map(|(labels, h)| {
            let labels_owned: Vec<(String, String)> = labels.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
            (labels_owned, h.snapshot())
        }).collect()
    }
    /// Zero every per-label histogram (buckets + sum + count).
    pub fn reset(&self) {
        let g = self.series.read();
        for h in g.values() { h.reset(); }
    }
}

// ---- Gauge (labeled) ----

pub struct Gauge {
    pub name: &'static str,
    pub help: &'static str,
    pub values: RwLock<BTreeMap<Vec<(&'static str, String)>, AtomicU64>>,
}

impl Gauge {
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self { name, help, values: RwLock::new(BTreeMap::new()) }
    }
    pub fn set(&self, labels: Vec<(&'static str, String)>, v: u64) {
        let mut g = self.values.write();
        g.entry(labels).or_insert_with(|| AtomicU64::new(0)).store(v, Ordering::Relaxed);
    }
    pub fn inc(&self, labels: Vec<(&'static str, String)>) {
        let mut g = self.values.write();
        g.entry(labels).or_insert_with(|| AtomicU64::new(0)).fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self, labels: Vec<(&'static str, String)>) {
        let mut g = self.values.write();
        g.entry(labels).or_insert_with(|| AtomicU64::new(0)).fetch_sub(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> Vec<CounterSample> {
        let g = self.values.read();
        g.iter().map(|(labels, v)| CounterSample {
            labels: labels.iter().map(|(k, val)| (k.to_string(), val.clone())).collect(),
            value: v.load(Ordering::Relaxed),
        }).collect()
    }
    /// Zero every gauge series. Used by operational `reset()`.
    pub fn reset(&self) {
        let g = self.values.read();
        for v in g.values() { v.store(0, Ordering::Relaxed); }
    }
}

// ---- Central Metrics struct ----

/// Default latency buckets in microseconds: 1ms, 2ms, 5ms, 10ms, 20ms, 50ms,
/// 100ms, 200ms, 500ms, 1s, 2s, 5s, 10s, 30s.
pub const DEFAULT_BUCKETS_US: &[u64] = &[
    1_000, 2_000, 5_000, 10_000, 20_000, 50_000,
    100_000, 200_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000, 30_000_000,
];

pub struct Metrics {
    // Counters
    pub requests_total: Counter,
    pub upstream_requests_total: Counter,
    pub success_total: Counter,
    pub error_total: Counter,
    pub input_tokens_total: Counter,
    pub output_tokens_total: Counter,
    pub cost_micros_total: Counter,
    pub cache_read_input_tokens_total: Counter,
    pub cache_write_input_tokens_total: Counter,

    // Histograms
    pub request_duration: Histogram,
    pub upstream_duration: Histogram,
    pub ttft: Histogram,
    pub inter_token: Histogram,

    // Labeled histograms (per upstream_id × model)
    pub request_duration_by_upstream: LabeledHistogram,
    pub upstream_duration_by_upstream: LabeledHistogram,

    // Gauges
    pub active_requests: Gauge,
    pub active_streams: Gauge,
    pub upstream_up: Gauge,

    // Sliding-window rates
    pub rates: parking_lot::RwLock<Rates>,

    // OTLP-shaped trace ring
    pub traces: TraceRing,

    // Live event bus for SSE dashboard tail
    pub events: EventBus,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        // Smaller buckets for inter-token (10ms..2s).
        let inter_token_buckets: &[u64] = &[
            10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000,
        ];
        Arc::new(Self {
            requests_total: Counter::new("requests_total", "Total incoming proxy requests."),
            upstream_requests_total: Counter::new("upstream_requests_total", "Requests forwarded to upstreams."),
            success_total: Counter::new("success_total", "Successful upstream responses (2xx)."),
            error_total: Counter::new("error_total", "Failed upstream responses, labeled by reason."),
            input_tokens_total: Counter::new("input_tokens_total", "Total prompt tokens consumed."),
            output_tokens_total: Counter::new("output_tokens_total", "Total completion tokens generated."),
            cost_micros_total: Counter::new("cost_micros_total", "Total cost in USD micros (1e-6)."),
            cache_read_input_tokens_total: Counter::new("cache_read_input_tokens_total", "Provider-side cache-read prompt tokens."),
            cache_write_input_tokens_total: Counter::new("cache_write_input_tokens_total", "Provider-side cache-write prompt tokens."),

            request_duration: Histogram::with_buckets("request_duration_seconds", "Full request latency (proxy receive → response end).", DEFAULT_BUCKETS_US),
            upstream_duration: Histogram::with_buckets("upstream_duration_seconds", "Upstream call latency (send → first byte).", DEFAULT_BUCKETS_US),
            ttft: Histogram::with_buckets("time_to_first_token_seconds", "Time from upstream send to first SSE byte (streaming only).", DEFAULT_BUCKETS_US),
            inter_token: Histogram::with_buckets("stream_inter_token_seconds", "Gap between consecutive SSE chunks during a stream.", inter_token_buckets),
            request_duration_by_upstream: LabeledHistogram::new("request_duration_by_upstream", "Request latency broken down by upstream_id and model.", DEFAULT_BUCKETS_US),
            upstream_duration_by_upstream: LabeledHistogram::new("upstream_duration_by_upstream", "Upstream latency broken down by upstream_id and model.", DEFAULT_BUCKETS_US),

            active_requests: Gauge::new("active_requests", "Currently in-flight proxy requests."),
            active_streams: Gauge::new("active_streams", "Currently open streaming responses."),
            upstream_up: Gauge::new("upstream_up", "1 if the last attempt to this upstream succeeded, 0 otherwise."),
            rates: parking_lot::RwLock::new(Rates::new()),
            traces: TraceRing::new(1000),
            events: EventBus::new(),
        })
    }

    /// Record one trace span.
    pub fn record_trace(&self, span: TraceSpan) { self.traces.push(span); }

    /// Record one finished request with its token counts + cost.
    /// Updates the sliding-window rate rings.
    pub fn record_request(&self, total_tokens: u64, cost_usd: f64) {
        let now = chrono::Utc::now().timestamp() as u64;
        let r = self.rates.read();
        r.rps_1m.add(1, now);
        r.rps_5m.add(1, now);
        r.rps_1h.add(1, now);
        r.tpm_1m.add(total_tokens, now);
        r.tpm_5m.add(total_tokens, now);
        r.tpm_1h.add(total_tokens, now);
        r.cost_1m.add((cost_usd * 1_000_000.0) as u64, now);
        r.cost_1h.add((cost_usd * 1_000_000.0) as u64, now);
    }

    /// Operational reset: zero every in-memory counter, histogram, gauge,
    /// rate ring, and the trace ring. **Does NOT** touch the SQLite
    /// `metric_samples` table — those are long-term cumulative totals that
    /// survive a reset by design. The next persist cycle will overwrite
    /// the SQLite snapshot with the (now-zero) in-memory values, which
    /// is the intended behaviour for an operational reset.
    pub fn reset(&self) {
        self.requests_total.reset();
        self.upstream_requests_total.reset();
        self.success_total.reset();
        self.error_total.reset();
        self.input_tokens_total.reset();
        self.output_tokens_total.reset();
        self.cost_micros_total.reset();
        self.cache_read_input_tokens_total.reset();
        self.cache_write_input_tokens_total.reset();
        self.request_duration.reset();
        self.upstream_duration.reset();
        self.ttft.reset();
        self.inter_token.reset();
        self.request_duration_by_upstream.reset();
        self.upstream_duration_by_upstream.reset();
        self.active_requests.reset();
        self.active_streams.reset();
        self.upstream_up.reset();
        let r = self.rates.read();
        r.rps_1m.reset();
        r.rps_5m.reset();
        r.rps_1h.reset();
        r.tpm_1m.reset();
        r.tpm_5m.reset();
        r.tpm_1h.reset();
        r.cost_1m.reset();
        r.cost_1h.reset();
        self.traces.clear();
    }

    /// Top-level JSON snapshot for the admin endpoint.
    pub fn snapshot_json(&self) -> serde_json::Value {
        let q = |h: &Histogram| {
            let qs = h.quantiles_us(&[0.5, 0.95, 0.99]);
            serde_json::json!({
                "p50_us": qs[0].1, "p95_us": qs[1].1, "p99_us": qs[2].1,
                "snapshot": h.snapshot(),
            })
        };
        serde_json::json!({
            "counters": {
                "requests_total": self.requests_total.snapshot(),
                "upstream_requests_total": self.upstream_requests_total.snapshot(),
                "success_total": self.success_total.snapshot(),
                "error_total": self.error_total.snapshot(),
                "input_tokens_total": self.input_tokens_total.snapshot(),
                "output_tokens_total": self.output_tokens_total.snapshot(),
                "cost_micros_total": self.cost_micros_total.snapshot(),
                "cache_read_input_tokens_total": self.cache_read_input_tokens_total.snapshot(),
                "cache_write_input_tokens_total": self.cache_write_input_tokens_total.snapshot(),
            },
            "histograms": {
                "request_duration_seconds": q(&self.request_duration),
                "upstream_duration_seconds": q(&self.upstream_duration),
                "time_to_first_token_seconds": q(&self.ttft),
                "stream_inter_token_seconds": q(&self.inter_token),
                "request_duration_by_upstream": self.request_duration_by_upstream.snapshot()
                    .into_iter().map(|(labels, snap)| {
                        let qs = Histogram::quantiles_from_sample(&snap, &[0.5, 0.95, 0.99]);
                        serde_json::json!({
                            "labels": labels,
                            "p50_us": qs[0].1, "p95_us": qs[1].1, "p99_us": qs[2].1,
                            "snapshot": snap,
                        })
                    }).collect::<Vec<_>>(),
                "upstream_duration_by_upstream": self.upstream_duration_by_upstream.snapshot()
                    .into_iter().map(|(labels, snap)| {
                        let qs = Histogram::quantiles_from_sample(&snap, &[0.5, 0.95, 0.99]);
                        serde_json::json!({
                            "labels": labels,
                            "p50_us": qs[0].1, "p95_us": qs[1].1, "p99_us": qs[2].1,
                            "snapshot": snap,
                        })
                    }).collect::<Vec<_>>(),
            },
            "gauges": {
                "active_requests": self.active_requests.snapshot(),
                "active_streams": self.active_streams.snapshot(),
                "upstream_up": self.upstream_up.snapshot(),
            },
        })
    }

    /// Prometheus text-format export (compatible with the `text/plain; version=0.0.4` content type).
    pub fn prometheus_text(&self) -> String {
        let mut out = String::with_capacity(4096);
        let write_metric = |out: &mut String, name: &str, help: &str, kind: &str, samples: &[(Vec<(String, String)>, u64)]| {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
            for (labels, value) in samples {
                if labels.is_empty() {
                    out.push_str(&format!("{name} {value}\n"));
                } else {
                    let lbl = labels.iter()
                        .map(|(k, v)| format!("{k}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
                        .collect::<Vec<_>>().join(",");
                    out.push_str(&format!("{name}{{{lbl}}} {value}\n"));
                }
            }
        };
        // Counters
        for c in [&self.requests_total, &self.upstream_requests_total, &self.success_total,
                  &self.error_total, &self.input_tokens_total, &self.output_tokens_total,
                  &self.cost_micros_total, &self.cache_read_input_tokens_total,
                  &self.cache_write_input_tokens_total] {
            let samples: Vec<_> = c.snapshot().into_iter()
                .map(|s| (s.labels, s.value)).collect();
            write_metric(&mut out, c.name, c.help, "counter", &samples);
        }
        // Gauges
        for g in [&self.active_requests, &self.active_streams, &self.upstream_up] {
            let samples: Vec<_> = g.snapshot().into_iter()
                .map(|s| (s.labels, s.value)).collect();
            write_metric(&mut out, g.name, g.help, "gauge", &samples);
        }
        // Histograms (Prometheus convention: emit `_bucket{le=...}`, `_sum`, `_count`)
        for h in [&self.request_duration, &self.upstream_duration, &self.ttft, &self.inter_token] {
            let snap = h.snapshot();
            out.push_str(&format!("# HELP {} {}\n# TYPE {} histogram\n", h.name, h.help, h.name));
            for b in &snap.buckets {
                out.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", h.name, fmt_le(b.le_us), b.count));
            }
            out.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", h.name, snap.count));
            out.push_str(&format!("{}_sum {}\n", h.name, snap.sum_us as f64 / 1_000_000.0));
            out.push_str(&format!("{}_count {}\n", h.name, snap.count));
        }
        // Labeled histograms: one HELP/TYPE per series, suffixed with the label values.
        for lh in [&self.request_duration_by_upstream, &self.upstream_duration_by_upstream] {
            for (labels, snap) in lh.snapshot() {
                if labels.is_empty() { continue; }
                let lbl_str = labels.iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v.replace('\\', "\\\\").replace('"', "\\\"")))
                    .collect::<Vec<_>>().join(",");
                let series_name = format!("{lh_name}{{{lbl_str}}}", lh_name = lh.name);
                out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n",
                    name = series_name, help = lh.help));
                for b in &snap.buckets {
                    out.push_str(&format!("{name}_bucket{{le=\"{le}\"}} {count}\n",
                        name = series_name, le = fmt_le(b.le_us), count = b.count));
                }
                out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {count}\n",
                    name = series_name, count = snap.count));
                out.push_str(&format!("{name}_sum {sum}\n",
                    name = series_name, sum = snap.sum_us as f64 / 1_000_000.0));
                out.push_str(&format!("{name}_count {count}\n", name = series_name, count = snap.count));
            }
        }
        out
    }
}

fn fmt_le(us: u64) -> String {
    if us >= 1_000_000 { format!("{:.2}s", us as f64 / 1_000_000.0) }
    else if us >= 1_000 { format!("{:.0}ms", us as f64 / 1_000.0) }
    else { format!("{us}us") }
}

impl Metrics {
    /// Walk every counter + histogram bucket and produce flat `MetricSampleRow`s
    /// ready to UPSERT into the `metric_samples` table. Histogram sum/count
    /// are stored as synthetic rows with name `_hist_sum` / `_hist_count` and
    /// the metric path in the labels so restore can route them back.
    pub fn snapshot_for_persist(&self) -> Vec<MetricSampleRow> {
        let mut out = Vec::new();
        let counter_pairs: Vec<(&str, &Counter)> = vec![
            ("requests_total", &self.requests_total),
            ("upstream_requests_total", &self.upstream_requests_total),
            ("success_total", &self.success_total),
            ("error_total", &self.error_total),
            ("input_tokens_total", &self.input_tokens_total),
            ("output_tokens_total", &self.output_tokens_total),
            ("cost_micros_total", &self.cost_micros_total),
            ("cache_read_input_tokens_total", &self.cache_read_input_tokens_total),
            ("cache_write_input_tokens_total", &self.cache_write_input_tokens_total),
        ];
        for (name, c) in counter_pairs {
            for s in c.snapshot() {
                let labels_json = serde_json::to_string(
                    &s.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<Vec<_>>()
                ).unwrap_or_else(|_| "[]".to_string());
                out.push(MetricSampleRow { name: name.to_string(), labels_json, value: s.value as i64 });
            }
        }
        // Histograms
        let hist_pairs: Vec<(&str, &Histogram)> = vec![
            ("request_duration_seconds", &self.request_duration),
            ("upstream_duration_seconds", &self.upstream_duration),
            ("time_to_first_token_seconds", &self.ttft),
            ("stream_inter_token_seconds", &self.inter_token),
        ];
        for (path, h) in hist_pairs {
            // Emit cumulative bucket counts (Prometheus convention). This means
            // bucket[i].value = total observations in buckets [0..=i], which is
            // what a scraper expects. Persist every bucket including zeros so
            // the restore path can rehydrate the in-memory per-slot counters.
            let mut running: u64 = 0;
            for (i, c) in h.counts.iter().enumerate() {
                let le = h.buckets_us.get(i).copied().unwrap_or(0);
                running = running.saturating_add(c.load(Ordering::Relaxed));
                let labels_json = serde_json::to_string(&vec![
                    ("le_us", le.to_string()),
                    ("path", path.to_string()),
                ]).unwrap_or_else(|_| "[]".to_string());
                out.push(MetricSampleRow {
                    name: format!("{path}_bucket"),
                    labels_json,
                    value: running as i64,
                });
            }
            // sum + count: use a single label "path" and store the actual value in `value`.
            let labels_json = serde_json::to_string(&vec![("path", path.to_string())]).unwrap_or_default();
            out.push(MetricSampleRow {
                name: format!("_hist_sum::{path}"),
                labels_json: labels_json.clone(),
                value: h.sum_us.load(Ordering::Relaxed) as i64,
            });
            out.push(MetricSampleRow {
                name: format!("_hist_count::{path}"),
                labels_json,
                value: h.count.load(Ordering::Relaxed) as i64,
            });
        }
        out
    }
}

impl Metrics {
    /// Like `snapshot_for_persist`, but excludes histogram bucket rows.
    /// Used on the first persist after startup to avoid overwriting
    /// restored cumulative bucket values with zero.
    pub fn snapshot_for_persist_counters_only(&self) -> Vec<MetricSampleRow> {
        let mut out = Vec::new();
        let counter_pairs: Vec<(&str, &Counter)> = vec![
            ("requests_total", &self.requests_total),
            ("upstream_requests_total", &self.upstream_requests_total),
            ("success_total", &self.success_total),
            ("error_total", &self.error_total),
            ("input_tokens_total", &self.input_tokens_total),
            ("output_tokens_total", &self.output_tokens_total),
            ("cost_micros_total", &self.cost_micros_total),
            ("cache_read_input_tokens_total", &self.cache_read_input_tokens_total),
            ("cache_write_input_tokens_total", &self.cache_write_input_tokens_total),
        ];
        for (name, c) in counter_pairs {
            for s in c.snapshot() {
                let labels_json = serde_json::to_string(
                    &s.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<Vec<_>>()
                ).unwrap_or_else(|_| "[]".to_string());
                out.push(MetricSampleRow { name: name.to_string(), labels_json, value: s.value as i64 });
            }
        }
        // Histogram sum + count only.
        let hist_pairs: Vec<(&str, &Histogram)> = vec![
            ("request_duration_seconds", &self.request_duration),
            ("upstream_duration_seconds", &self.upstream_duration),
            ("time_to_first_token_seconds", &self.ttft),
            ("stream_inter_token_seconds", &self.inter_token),
        ];
        for (path, h) in hist_pairs {
            let labels_json = serde_json::to_string(&vec![("path", path.to_string())]).unwrap_or_default();
            out.push(MetricSampleRow { name: format!("_hist_sum::{path}"), labels_json: labels_json.clone(), value: h.sum_us.load(Ordering::Relaxed) as i64 });
            out.push(MetricSampleRow { name: format!("_hist_count::{path}"), labels_json, value: h.count.load(Ordering::Relaxed) as i64 });
        }
        out
    }
}

// ---- Sliding-window rate gauges (RPS, RPM, TPM over 1m / 5m / 1h) ----

/// A ring of 1-second buckets, each holding a count. Rolling sum over
/// the last N seconds gives the rate gauge value. The ring is rotated
/// by `tick()` (called once per second by the persister task).
pub struct RateRing {
    /// 1-second buckets. The slot at `head` is the most recent one.
    buckets: parking_lot::Mutex<Vec<u64>>,
    /// Logical second at `head`.
    head: std::sync::atomic::AtomicU64,
    /// Window length in seconds.
    window_s: u32,
    /// What the ring is counting (request count, token count, etc).
    pub name: &'static str,
}

impl RateRing {
    pub fn new(name: &'static str, window_s: u32) -> Self {
        Self {
            buckets: parking_lot::Mutex::new(vec![0; window_s as usize]),
            head: std::sync::atomic::AtomicU64::new(0),
            window_s,
            name,
        }
    }
    /// Record `n` events at the current `now_s` second.
    pub fn add(&self, n: u64, now_s: u64) {
        let mut g = self.buckets.lock();
        // If `head` is far behind `now_s`, zero everything between.
        let head = self.head.load(std::sync::atomic::Ordering::Relaxed);
        if now_s > head {
            let shift = (now_s - head).min(self.window_s as u64);
            for i in 0..shift {
                let idx = ((head + 1 + i) % self.window_s as u64) as usize;
                g[idx] = 0;
            }
            self.head.store(now_s, std::sync::atomic::Ordering::Relaxed);
        } else if now_s < head {
            // Out-of-order or clock skew; ignore.
            return;
        }
        let idx = (now_s % self.window_s as u64) as usize;
        g[idx] = g[idx].saturating_add(n);
    }
    /// Total events in the last `window_s` seconds (from `now_s`).
    pub fn sum(&self, now_s: u64) -> u64 {
        let g = self.buckets.lock();
        let head = self.head.load(std::sync::atomic::Ordering::Relaxed);
        // We assume `add` has been called recently enough to keep the ring current.
        // If `now_s` is far ahead of `head`, treat missing buckets as 0.
        if now_s > head + self.window_s as u64 + 5 { return 0; }
        let mut total = 0u64;
        for i in 0..self.window_s as u64 {
            let ts = now_s.saturating_sub(i);
            let idx = (ts % self.window_s as u64) as usize;
            total = total.saturating_add(g[idx]);
        }
        total
    }
    /// Per-second average over the window.
    pub fn rate_per_second(&self, now_s: u64) -> f64 {
        self.sum(now_s) as f64 / self.window_s as f64
    }
    /// Zero every bucket in the ring and reset the head pointer.
    /// The next `add()` will rebuild the ring from the current second.
    pub fn reset(&self) {
        let mut g = self.buckets.lock();
        for v in g.iter_mut() { *v = 0; }
        self.head.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct Rates {
    pub rps_1m: RateRing,
    pub rps_5m: RateRing,
    pub rps_1h: RateRing,
    pub tpm_1m: RateRing, // tokens per second
    pub tpm_5m: RateRing,
    pub tpm_1h: RateRing,
    pub cost_1m: RateRing, // cost per second (USD × 1e6)
    pub cost_1h: RateRing,
}

impl Rates {
    pub fn new() -> Self {
        Self {
            rps_1m: RateRing::new("rps_1m", 60),
            rps_5m: RateRing::new("rps_5m", 300),
            rps_1h: RateRing::new("rps_1h", 3600),
            tpm_1m: RateRing::new("tpm_1m", 60),
            tpm_5m: RateRing::new("tpm_5m", 300),
            tpm_1h: RateRing::new("tpm_1h", 3600),
            cost_1m: RateRing::new("cost_1m", 60),
            cost_1h: RateRing::new("cost_1h", 3600),
        }
    }
    pub fn snapshot(&self) -> serde_json::Value {
        let now = chrono::Utc::now().timestamp() as u64;
        serde_json::json!({
            "rps": {
                "1m": self.rps_1m.rate_per_second(now),
                "5m": self.rps_5m.rate_per_second(now),
                "1h": self.rps_1h.rate_per_second(now),
            },
            "tps": {
                "1m": self.tpm_1m.rate_per_second(now),
                "5m": self.tpm_5m.rate_per_second(now),
                "1h": self.tpm_1h.rate_per_second(now),
            },
            "cost_per_sec": {
                "1m": self.cost_1m.sum(now) as f64 / 1_000_000.0 / 60.0,
                "1h": self.cost_1h.sum(now) as f64 / 1_000_000.0 / 3600.0,
            },
        })
    }
}

// ---- Trace span ring (OTLP-shaped, no external dep) ----

#[derive(Debug, Clone, serde::Serialize)]
pub struct TraceSpan {
    pub name: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub kind: String, // "client" | "internal"
    pub start_unix_nano: u128,
    pub end_unix_nano: u128,
    pub status_code: String, // "ok" | "error"
    pub attributes: Vec<(String, String)>,
}

pub struct TraceRing {
    inner: parking_lot::Mutex<Vec<TraceSpan>>,
    capacity: usize,
}

impl TraceRing {
    pub fn new(capacity: usize) -> Self {
        Self { inner: parking_lot::Mutex::new(Vec::with_capacity(capacity)), capacity }
    }
    pub fn push(&self, span: TraceSpan) {
        let mut g = self.inner.lock();
        if g.len() >= self.capacity { g.remove(0); }
        g.push(span);
    }
    pub fn snapshot(&self, limit: usize) -> Vec<TraceSpan> {
        let g = self.inner.lock();
        let start = g.len().saturating_sub(limit);
        g[start..].to_vec()
    }
    /// Drop every buffered span. Used by operational `reset()`.
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}

// ---- Live event broadcast (for SSE live-tail) ----

pub struct EventBus {
    subs: parking_lot::Mutex<Vec<tokio::sync::mpsc::UnboundedSender<serde_json::Value>>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self { subs: parking_lot::Mutex::new(Vec::new()) }
    }
    pub fn subscribe(&self) -> tokio::sync::mpsc::UnboundedReceiver<serde_json::Value> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.subs.lock().push(tx);
        rx
    }
    pub fn publish(&self, ev: serde_json::Value) {
        let mut g = self.subs.lock();
        g.retain(|tx| tx.send(ev.clone()).is_ok());
    }
    pub fn sub_count(&self) -> usize { self.subs.lock().len() }
}
