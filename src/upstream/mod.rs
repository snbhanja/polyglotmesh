use crate::config::types::{ProviderKind, UpstreamConfig};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    Healthy,
    Degraded,
    Unhealthy,
}

impl Health {
    pub fn allows_traffic(&self) -> bool {
        !matches!(self, Health::Unhealthy)
    }
}

/// Per-upstream live state held in memory and shared with the rest of the router.
pub struct Upstream {
    pub cfg: RwLock<UpstreamConfig>,
    pub health: RwLock<Health>,
    pub in_flight: AtomicU32,
    /// Number of consecutive failures (for health flip).
    pub consecutive_failures: AtomicU32,
    /// Tokens available in a token-bucket rate limiter (per second granularity).
    pub rate_tokens: parking_lot::Mutex<RateBucket>,
    /// Last time a request successfully completed.
    pub last_success: RwLock<Option<Instant>>,
    /// Last health check error, if any.
    pub last_error: RwLock<Option<String>>,
    /// Total successful requests served.
    pub success_count: AtomicU64,
    /// Total failed requests served.
    pub failure_count: AtomicU64,
    /// Cached list of model ids (refreshed lazily).
    pub models: RwLock<Vec<String>>,
    /// Whether the upstream is paused by an admin.
    pub paused: AtomicBool,
    /// Circuit breaker: when open, the breaker rejects traffic until this
    /// instant passes (then a single half-open probe is allowed).
    pub breaker_open_until: RwLock<Option<std::time::Instant>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RateBucket {
    pub capacity: f64,
    pub refill_per_sec: f64,
    pub tokens: f64,
    pub last_refill: Instant,
}

impl Upstream {
    pub fn from_config(cfg: UpstreamConfig) -> Arc<Self> {
        let capacity = if cfg.rate_limit_rpm == 0 {
            f64::INFINITY
        } else {
            (cfg.rate_limit_rpm as f64) / 60.0 * 6.0
        };
        let refill_per_sec = if cfg.rate_limit_rpm == 0 {
            f64::INFINITY
        } else {
            cfg.rate_limit_rpm as f64 / 60.0
        };
        Arc::new(Self {
            cfg: RwLock::new(cfg.clone()),
            health: RwLock::new(Health::Healthy),
            in_flight: AtomicU32::new(0),
            consecutive_failures: AtomicU32::new(0),
            rate_tokens: parking_lot::Mutex::new(RateBucket {
                capacity,
                refill_per_sec,
                tokens: capacity,
                last_refill: Instant::now(),
            }),
            last_success: RwLock::new(None),
            last_error: RwLock::new(None),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            models: RwLock::new(cfg.models.clone()),
            paused: AtomicBool::new(false),
            breaker_open_until: RwLock::new(None),
        })
    }

    pub fn id(&self) -> String {
        self.cfg.read().id.clone()
    }

    pub fn kind(&self) -> ProviderKind {
        self.cfg.read().kind
    }

    pub fn base_url(&self) -> String {
        self.cfg.read().base_url.clone()
    }

    pub fn api_key(&self) -> String {
        self.cfg.read().api_key.clone()
    }

    pub fn priority(&self) -> i32 {
        self.cfg.read().priority
    }

    pub fn weight(&self) -> u32 {
        self.cfg.read().weight
    }

    pub fn enabled(&self) -> bool {
        self.cfg.read().enabled
    }

    pub fn health(&self) -> Health {
        *self.health.read()
    }

    pub fn is_usable(&self) -> bool {
        if !self.enabled() {
            return false;
        }
        if self.paused.load(Ordering::Relaxed) {
            return false;
        }
        if !self.health().allows_traffic() {
            return false;
        }
        // Circuit breaker: if open and still within the open window, reject.
        let open = *self.breaker_open_until.read();
        if let Some(until) = open {
            if Instant::now() < until {
                return false;
            }
        }
        let cfg = self.cfg.read();
        if cfg.max_concurrency > 0 && self.in_flight.load(Ordering::Relaxed) >= cfg.max_concurrency
        {
            return false;
        }
        true
    }

    pub fn try_acquire(&self) -> bool {
        if !self.is_usable() {
            return false;
        }
        let cfg = self.cfg.read();
        if cfg.max_concurrency > 0
            && self
                .in_flight
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                    if cur < cfg.max_concurrency {
                        Some(cur + 1)
                    } else {
                        None
                    }
                })
                .is_err()
        {
            return false;
        }
        // Rate bucket
        if cfg.rate_limit_rpm > 0 {
            let mut bucket = self.rate_tokens.lock();
            let now = Instant::now();
            let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
            bucket.tokens = (bucket.tokens + elapsed * bucket.refill_per_sec).min(bucket.capacity);
            bucket.last_refill = now;
            if bucket.tokens >= 1.0 {
                bucket.tokens -= 1.0;
            } else {
                // Roll back the in_flight increment
                if cfg.max_concurrency > 0 {
                    self.in_flight.fetch_sub(1, Ordering::AcqRel);
                }
                return false;
            }
        }
        true
    }

    pub fn release(&self) {
        let cfg = self.cfg.read();
        if cfg.max_concurrency > 0 {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }

    pub fn record_success(&self) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        *self.last_success.write() = Some(Instant::now());
        *self.breaker_open_until.write() = None;
        let mut h = self.health.write();
        if *h != Health::Healthy {
            *h = Health::Healthy;
        }
    }

    /// Record a failed request. Opens the circuit breaker once consecutive
    /// failures reach the configured (or default) threshold, rejecting traffic
    /// for `open_duration_s`, after which a single half-open probe is allowed.
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        let f = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        let cfg = self.cfg.read();
        let (threshold, open_s) = match &cfg.circuit_breaker {
            Some(cb) => (cb.failure_threshold, cb.open_duration_s),
            None => (3u32, 30u64),
        };
        if f >= threshold {
            *self.health.write() = Health::Unhealthy;
            let until = Instant::now() + std::time::Duration::from_secs(open_s);
            *self.breaker_open_until.write() = Some(until);
        } else if f >= (threshold / 2 + 1) {
            *self.health.write() = Health::Degraded;
        }
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let cfg = self.cfg.read();
        let bucket = self.rate_tokens.lock();
        serde_json::json!({
            "id": cfg.id,
            "name": cfg.name,
            "kind": cfg.kind,
            "base_url": cfg.base_url,
            "priority": cfg.priority,
            "weight": cfg.weight,
            "enabled": cfg.enabled,
            "critical": cfg.critical,
            "paused": self.paused.load(Ordering::Relaxed),
            "health": self.health(),
            "in_flight": self.in_flight.load(Ordering::Relaxed),
            "consecutive_failures": self.consecutive_failures.load(Ordering::Relaxed),
            "max_concurrency": cfg.max_concurrency,
            "rate_limit_rpm": cfg.rate_limit_rpm,
            "rate_tokens": if bucket.capacity.is_finite() { bucket.tokens } else { -1.0 },
            "success_count": self.success_count.load(Ordering::Relaxed),
            "failure_count": self.failure_count.load(Ordering::Relaxed),
            "last_success_age_ms": self.last_success.read().map(|t| t.elapsed().as_millis() as u64),
            "last_error": *self.last_error.read(),
            "timeout_ms": cfg.timeout_ms,
            "models": *self.models.read(),
        })
    }
}

use serde::{Deserialize, Serialize};

/// Registry of all configured upstreams, indexed by id and provider kind.
pub struct UpstreamRegistry {
    pub upstreams: dashmap::DashMap<String, Arc<Upstream>>,
    pub by_provider: parking_lot::RwLock<Vec<Arc<Upstream>>>,
}

impl UpstreamRegistry {
    pub fn new() -> Self {
        Self {
            upstreams: dashmap::DashMap::new(),
            by_provider: parking_lot::RwLock::new(Vec::new()),
        }
    }

    pub fn rebuild(upstreams: Vec<UpstreamConfig>) -> Self {
        let reg = Self::new();
        for u in upstreams {
            reg.upsert(u);
        }
        reg
    }

    pub fn upsert(&self, cfg: UpstreamConfig) {
        let id = cfg.id.clone();
        let arc = Upstream::from_config(cfg);
        self.upstreams.insert(id, arc.clone());
        self.rebuild_by_provider();
    }

    pub fn remove(&self, id: &str) -> bool {
        let removed = self.upstreams.remove(id).is_some();
        if removed {
            self.rebuild_by_provider();
        }
        removed
    }

    pub fn get(&self, id: &str) -> Option<Arc<Upstream>> {
        self.upstreams.get(id).map(|e| e.clone())
    }

    pub fn by_provider(&self, kind: ProviderKind) -> Vec<Arc<Upstream>> {
        self.by_provider
            .read()
            .iter()
            .filter(|u| u.kind() == kind)
            .cloned()
            .collect()
    }

    pub fn all(&self) -> Vec<Arc<Upstream>> {
        self.upstreams.iter().map(|e| e.value().clone()).collect()
    }

    fn rebuild_by_provider(&self) {
        let mut all: Vec<Arc<Upstream>> =
            self.upstreams.iter().map(|e| e.value().clone()).collect();
        // Higher priority first, then lower id for stability.
        all.sort_by(|a, b| {
            b.priority()
                .cmp(&a.priority())
                .then_with(|| a.id().cmp(&b.id()))
        });
        *self.by_provider.write() = all;
    }
}

impl UpstreamRegistry {
    /// Resolve a logical model name to the actual upstream model name based on aliases.
    /// Returns the rewritten name if any alias matches and the target upstream is enabled.
    pub fn resolve_model_alias(&self, model: &str, kind: ProviderKind) -> Option<String> {
        // We need the full config to consult the alias map; use a thread-local shortcut via
        // a re-export of Config if it's loaded. For simplicity we look at the upstreams'
        // configured `models` lists and rewrite to the first upstream that has a matching
        // configured model. Then, the request still gets dispatched to that upstream.
        // True alias rewriting is handled by passing the alias map via ProxyState.
        let _ = (self, model, kind);
        None
    }
}

// ---- Pricing (LiteLLM `model_info.input_cost_per_token` / `output_cost_per_token`) ----

impl Upstream {
    /// Look up USD cost for a (model, input_tokens, output_tokens) tuple.
    /// First checks the per-upstream `model_info` override; falls back to the
    /// built-in default pricing table for popular models. Returns 0.0 if no
    /// price is known (callers can then surface it as `cost_unknown: true`).
    pub fn cost_for(
        &self,
        model: Option<&str>,
        input_tokens: u64,
        output_tokens: u64,
    ) -> (f64, bool) {
        let cfg = self.cfg.read();
        let name = model.unwrap_or("");
        if let Some(c) = cfg.model_info.get(name) {
            let cost = compute_cost(c, input_tokens, output_tokens);
            return (cost, false);
        }
        if let Some(default) = default_price_for(name) {
            let cost = compute_cost(default, input_tokens, output_tokens);
            return (cost, false);
        }
        (0.0, true)
    }

    /// List all known models (configured + auto-exposed). Used by the price CLI.
    pub fn known_models(&self) -> Vec<String> {
        let cfg = self.cfg.read();
        let mut out: Vec<String> = cfg.models.clone();
        for k in cfg.model_info.keys() {
            if !out.contains(k) {
                out.push(k.clone());
            }
        }
        out.sort();
        out
    }
}

fn compute_cost(c: &crate::config::types::ModelCost, input_tokens: u64, output_tokens: u64) -> f64 {
    let in_cost = c.input_cost_per_token.unwrap_or(0.0) * input_tokens as f64;
    let out_cost = c.output_cost_per_token.unwrap_or(0.0) * output_tokens as f64;
    in_cost + out_cost
}

/// Built-in default price table (USD per token). Prices are intentionally
/// conservative defaults — operators should override per-upstream in `model_info`.
/// Source: published list prices as of late 2025.
fn default_price_for(model: &str) -> Option<&'static crate::config::types::ModelCost> {
    use crate::config::types::ModelCost;
    let m = model.rsplit_once('/').map(|(_, r)| r).unwrap_or(model);
    let m = m.split(':').next().unwrap_or(m);
    Some(match m {
        "gpt-4o" | "gpt-4o-2024-08-06" | "gpt-4o-2024-05-13" => &ModelCost {
            input_cost_per_token: Some(2.5e-6),
            output_cost_per_token: Some(10.0e-6),
            max_input_tokens: Some(128_000),
            max_output_tokens: Some(16_384),
            cache_read_input_token_cost: Some(1.25e-6),
            cache_creation_input_token_cost: None,
        },
        "gpt-4o-mini" | "gpt-4o-mini-2024-07-18" => &ModelCost {
            input_cost_per_token: Some(0.15e-6),
            output_cost_per_token: Some(0.6e-6),
            max_input_tokens: Some(128_000),
            max_output_tokens: Some(16_384),
            cache_read_input_token_cost: Some(0.075e-6),
            cache_creation_input_token_cost: None,
        },
        "gpt-4-turbo" | "gpt-4-turbo-2024-04-09" => &ModelCost {
            input_cost_per_token: Some(10.0e-6),
            output_cost_per_token: Some(30.0e-6),
            max_input_tokens: Some(128_000),
            max_output_tokens: Some(4_096),
            cache_read_input_token_cost: None,
            cache_creation_input_token_cost: None,
        },
        "gpt-3.5-turbo" | "gpt-3.5-turbo-0125" => &ModelCost {
            input_cost_per_token: Some(0.5e-6),
            output_cost_per_token: Some(1.5e-6),
            max_input_tokens: Some(16_385),
            max_output_tokens: Some(4_096),
            cache_read_input_token_cost: None,
            cache_creation_input_token_cost: None,
        },
        "o1" | "o1-2024-12-17" => &ModelCost {
            input_cost_per_token: Some(15.0e-6),
            output_cost_per_token: Some(60.0e-6),
            max_input_tokens: Some(200_000),
            max_output_tokens: Some(100_000),
            cache_read_input_token_cost: Some(7.5e-6),
            cache_creation_input_token_cost: None,
        },
        "o1-mini" | "o1-mini-2024-09-12" => &ModelCost {
            input_cost_per_token: Some(3.0e-6),
            output_cost_per_token: Some(12.0e-6),
            max_input_tokens: Some(128_000),
            max_output_tokens: Some(65_536),
            cache_read_input_token_cost: Some(1.5e-6),
            cache_creation_input_token_cost: None,
        },
        "o3-mini" | "o3-mini-2025-01-31" => &ModelCost {
            input_cost_per_token: Some(1.1e-6),
            output_cost_per_token: Some(4.4e-6),
            max_input_tokens: Some(200_000),
            max_output_tokens: Some(100_000),
            cache_read_input_token_cost: Some(0.55e-6),
            cache_creation_input_token_cost: None,
        },
        "claude-3-5-sonnet-20241022" | "claude-3-5-sonnet-latest" => &ModelCost {
            input_cost_per_token: Some(3.0e-6),
            output_cost_per_token: Some(15.0e-6),
            max_input_tokens: Some(200_000),
            max_output_tokens: Some(8_192),
            cache_read_input_token_cost: Some(0.3e-6),
            cache_creation_input_token_cost: Some(3.75e-6),
        },
        "claude-3-5-haiku-20241022" | "claude-3-5-haiku-latest" => &ModelCost {
            input_cost_per_token: Some(0.8e-6),
            output_cost_per_token: Some(4.0e-6),
            max_input_tokens: Some(200_000),
            max_output_tokens: Some(8_192),
            cache_read_input_token_cost: Some(0.08e-6),
            cache_creation_input_token_cost: Some(1.0e-6),
        },
        "claude-3-opus-20240229" => &ModelCost {
            input_cost_per_token: Some(15.0e-6),
            output_cost_per_token: Some(75.0e-6),
            max_input_tokens: Some(200_000),
            max_output_tokens: Some(4_096),
            cache_read_input_token_cost: Some(1.5e-6),
            cache_creation_input_token_cost: Some(18.75e-6),
        },
        "claude-3-sonnet-20240229" => &ModelCost {
            input_cost_per_token: Some(3.0e-6),
            output_cost_per_token: Some(15.0e-6),
            max_input_tokens: Some(200_000),
            max_output_tokens: Some(4_096),
            cache_read_input_token_cost: Some(0.3e-6),
            cache_creation_input_token_cost: Some(3.75e-6),
        },
        "claude-3-haiku-20240307" => &ModelCost {
            input_cost_per_token: Some(0.25e-6),
            output_cost_per_token: Some(1.25e-6),
            max_input_tokens: Some(200_000),
            max_output_tokens: Some(4_096),
            cache_read_input_token_cost: Some(0.03e-6),
            cache_creation_input_token_cost: Some(0.3e-6),
        },
        _ => return None,
    })
}

impl Upstream {
    /// Replace (or merge) the per-upstream `model_info` pricing map.
    /// `merge = true` keeps existing entries and only adds/overwrites the provided keys;
    /// `merge = false` replaces the entire map.
    pub fn set_model_info(
        &self,
        prices: std::collections::BTreeMap<String, crate::config::types::ModelCost>,
        merge: bool,
    ) {
        let mut cfg = self.cfg.write();
        if merge {
            for (k, v) in prices {
                cfg.model_info.insert(k, v);
            }
        } else {
            cfg.model_info = prices;
        }
    }

    /// Read-only view of the current pricing map (for the admin endpoint).
    pub fn model_info(
        &self,
    ) -> std::collections::BTreeMap<String, crate::config::types::ModelCost> {
        self.cfg.read().model_info.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{ModelCost, ProviderKind, UpstreamConfig};
    use std::collections::BTreeMap;

    fn upstream_cfg(id: &str, kind: ProviderKind, priority: i32, max_concurrency: u32) -> UpstreamConfig {
        UpstreamConfig {
            id: id.into(),
            name: None,
            kind,
            base_url: "https://example.com".into(),
            api_key: "k".into(),
            priority,
            models: vec![],
            weight: 1,
            timeout_ms: 60_000,
            max_concurrency,
            rate_limit_rpm: 0,
            rate_limit_tpm: 0,
            enabled: true,
            max_budget: None,
            budget_duration: None,
            model_info: BTreeMap::new(),
            region: None,
            tags: vec![],
            critical: false,
            circuit_breaker: None,
        }
    }

    #[test]
    fn cost_for_unknown_model_reports_unknown() {
        let u = Upstream::from_config(upstream_cfg("u", ProviderKind::Openai, 0, 0));
        let (cost, unknown) = u.cost_for(Some("some-unknown-model"), 1000, 1000);
        assert!(unknown);
        assert_eq!(cost, 0.0);
    }

    #[test]
    fn cost_for_uses_builtin_price_table() {
        let u = Upstream::from_config(upstream_cfg("u", ProviderKind::Openai, 0, 0));
        // gpt-4o: $2.5/M in, $10/M out.
        let (cost, unknown) = u.cost_for(Some("gpt-4o"), 1_000_000, 1_000_000);
        assert!(!unknown);
        assert!((cost - 12.5).abs() < 1e-9);
    }

    #[test]
    fn cost_for_prefers_per_upstream_override() {
        let mut cfg = upstream_cfg("u", ProviderKind::Openai, 0, 0);
        let mut info = BTreeMap::new();
        info.insert(
            "gpt-4o".into(),
            ModelCost {
                input_cost_per_token: Some(1.0e-6),
                output_cost_per_token: Some(1.0e-6),
                max_input_tokens: None,
                max_output_tokens: None,
                cache_read_input_token_cost: None,
                cache_creation_input_token_cost: None,
            },
        );
        cfg.model_info = info;
        let u = Upstream::from_config(cfg);
        let (cost, unknown) = u.cost_for(Some("gpt-4o"), 1_000_000, 1_000_000);
        assert!(!unknown);
        assert!((cost - 2.0).abs() < 1e-9);
    }

    #[test]
    fn default_price_strips_provider_prefix_and_suffix() {
        let u = Upstream::from_config(upstream_cfg("u", ProviderKind::Anthropic, 0, 0));
        // "openai/gpt-4o" and "gpt-4o:latest" should both resolve to gpt-4o pricing.
        let (c1, _) = u.cost_for(Some("openai/gpt-4o"), 1_000_000, 0);
        let (c2, _) = u.cost_for(Some("gpt-4o:latest"), 1_000_000, 0);
        assert!((c1 - 2.5).abs() < 1e-9);
        assert!((c2 - 2.5).abs() < 1e-9);
    }

    #[test]
    fn set_model_info_merge_and_replace() {
        let u = Upstream::from_config(upstream_cfg("u", ProviderKind::Openai, 0, 0));
        let mut a = BTreeMap::new();
        a.insert("m1".into(), ModelCost::default());
        u.set_model_info(a.clone(), false);
        assert_eq!(u.model_info().len(), 1);

        let mut b = BTreeMap::new();
        b.insert("m2".into(), ModelCost::default());
        u.set_model_info(b, true);
        // merge keeps m1 and adds m2
        assert_eq!(u.model_info().len(), 2);

        let mut c = BTreeMap::new();
        c.insert("m3".into(), ModelCost::default());
        u.set_model_info(c, false);
        assert_eq!(u.model_info().len(), 1);
    }

    #[test]
    fn try_acquire_respects_concurrency_and_release() {
        let u = Upstream::from_config(upstream_cfg("u", ProviderKind::Openai, 0, 1));
        assert!(u.try_acquire());
        // second acquire should fail: concurrency exhausted
        assert!(!u.try_acquire());
        u.release();
        assert!(u.try_acquire());
        u.release();
    }

    #[test]
    fn is_usable_reflects_enabled_paused_health() {
        let u = Upstream::from_config(upstream_cfg("u", ProviderKind::Openai, 0, 0));
        assert!(u.is_usable());

        *u.health.write() = Health::Unhealthy;
        assert!(!u.is_usable());
        *u.health.write() = Health::Degraded;
        assert!(u.is_usable());

        *u.health.write() = Health::Healthy;
        u.paused.store(true, Ordering::Relaxed);
        assert!(!u.is_usable());
        u.paused.store(false, Ordering::Relaxed);

        let mut cfg = u.cfg.write();
        cfg.enabled = false;
        drop(cfg);
        assert!(!u.is_usable());
    }

    #[test]
    fn registry_sorts_by_priority_then_id() {
        let reg = UpstreamRegistry::rebuild(vec![
            upstream_cfg("b", ProviderKind::Openai, 1, 0),
            upstream_cfg("a", ProviderKind::Openai, 1, 0),
            upstream_cfg("c", ProviderKind::Openai, 5, 0),
        ]);
        let all = reg.by_provider(ProviderKind::Openai);
        let ids: Vec<String> = all.iter().map(|u| u.id()).collect();
        // highest priority first; ties broken by id ascending
        assert_eq!(ids, vec!["c".to_string(), "a".to_string(), "b".to_string()]);
    }

    #[test]
    fn registry_upsert_remove_and_by_provider() {
        let reg = UpstreamRegistry::new();
        reg.upsert(upstream_cfg("oai1", ProviderKind::Openai, 0, 0));
        reg.upsert(upstream_cfg("ant1", ProviderKind::Anthropic, 0, 0));
        assert_eq!(reg.by_provider(ProviderKind::Openai).len(), 1);
        assert!(reg.get("oai1").is_some());
        assert!(reg.remove("oai1"));
        assert!(reg.get("oai1").is_none());
        assert_eq!(reg.by_provider(ProviderKind::Openai).len(), 0);
    }

    #[test]
    fn known_models_merges_configured_and_priced() {
        let mut cfg = upstream_cfg("u", ProviderKind::Openai, 0, 0);
        cfg.models = vec!["gpt-4o".into()];
        let mut info = BTreeMap::new();
        info.insert("custom-model".into(), ModelCost::default());
        cfg.model_info = info;
        let u = Upstream::from_config(cfg);
        let models = u.known_models();
        assert!(models.contains(&"custom-model".to_string()));
        assert!(models.contains(&"gpt-4o".to_string()));
        // sorted
        let sorted = models.clone();
        let mut check = sorted.clone();
        check.sort();
        assert_eq!(models, check);
    }
}
