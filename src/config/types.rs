use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    Openai,
    Anthropic,
}

impl ProviderKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderKind::Openai => "openai",
            ProviderKind::Anthropic => "anthropic",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_matches_serde_rename() {
        assert_eq!(ProviderKind::Openai.as_str(), "openai");
        assert_eq!(ProviderKind::Anthropic.as_str(), "anthropic");

        let o = serde_json::to_string(&ProviderKind::Openai).unwrap();
        assert_eq!(o, "\"openai\"");
        let a: ProviderKind = serde_json::from_str("\"anthropic\"").unwrap();
        assert_eq!(a, ProviderKind::Anthropic);
    }

    #[test]
    fn config_defaults_are_sane() {
        let c = Config::default();
        assert_eq!(c.server.bind, "0.0.0.0:8080");
        assert!(c.upstreams.is_empty());
        assert_eq!(c.queue.queue_wait_timeout_ms, 30_000);
        assert_eq!(c.queue.healthcheck_failure_threshold, 3);
    }

    #[test]
    fn model_cost_round_trips() {
        let mc = ModelCost {
            input_cost_per_token: Some(1.0e-6),
            output_cost_per_token: Some(2.0e-6),
            max_input_tokens: Some(1000),
            max_output_tokens: Some(500),
            cache_read_input_token_cost: None,
            cache_creation_input_token_cost: None,
        };
        let json = serde_json::to_string(&mc).unwrap();
        let back: ModelCost = serde_json::from_str(&json).unwrap();
        assert_eq!(back, mc);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamConfig {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub kind: ProviderKind,
    pub base_url: String,
    pub api_key: String,
    /// Higher number = higher priority. Default 0.
    #[serde(default)]
    pub priority: i32,
    /// Per-upstream model list to expose. Empty = forward everything to upstream default.
    #[serde(default)]
    pub models: Vec<String>,
    /// Per-deployment weight (LiteLLM-style `weight`).
    #[serde(default)]
    pub weight: u32,
    /// Soft timeout (ms) for a single upstream request.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Concurrency limit for in-flight requests on this upstream. 0 = unlimited.
    /// (LiteLLM's `max_parallel_requests`.)
    #[serde(default)]
    pub max_concurrency: u32,
    /// Optional request rate limit (requests per minute). 0 = unlimited.
    #[serde(default)]
    pub rate_limit_rpm: u32,
    /// Token rate limit (tokens per minute). 0 = unlimited.
    /// (LiteLLM's `tpm`.)
    #[serde(default)]
    pub rate_limit_tpm: u32,
    /// Whether this upstream is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Optional deployment budget in USD (LiteLLM's `max_budget`).
    #[serde(default)]
    pub max_budget: Option<f64>,
    /// Reset period for `max_budget` (e.g. "30s", "1h", "1d", "30d", "1w", "1mo").
    #[serde(default)]
    pub budget_duration: Option<String>,
    /// Per-model spend / token override; LiteLLM `model_info` style.
    #[serde(default)]
    pub model_info: BTreeMap<String, ModelCost>,
    /// Region tag for the deployment (used with `allowed_model_region`).
    #[serde(default)]
    pub region: Option<String>,
    /// Custom tag-based routing tags (LiteLLM's `tags`).
    #[serde(default)]
    pub tags: Vec<String>,
    /// When true, the router is considered unhealthy (`/healthz?strict=1` → 503)
    /// if this upstream is not usable. Use for must-be-up providers.
    #[serde(default)]
    pub critical: bool,
    /// Optional circuit breaker. When consecutive failures hit `failure_threshold`,
    /// the upstream is marked `unhealthy` (rejects traffic) for `open_duration_s`,
    /// then probed once (half-open). Unset = use the global default threshold (3).
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

/// Per-upstream circuit breaker settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures before the breaker opens.
    #[serde(default = "default_fail_threshold")]
    pub failure_threshold: u32,
    /// Seconds to stay open (rejecting traffic) before a half-open probe.
    #[serde(default = "default_open_duration_s")]
    pub open_duration_s: u64,
}

fn default_fail_threshold() -> u32 {
    3
}

fn default_open_duration_s() -> u64 {
    30
}

fn default_timeout_ms() -> u64 {
    60_000
}
fn default_true() -> bool {
    true
}

impl Default for ModelCost {
    fn default() -> Self {
        Self {
            input_cost_per_token: None,
            output_cost_per_token: None,
            max_input_tokens: None,
            max_output_tokens: None,
            cache_read_input_token_cost: None,
            cache_creation_input_token_cost: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelCost {
    #[serde(default)]
    pub input_cost_per_token: Option<f64>,
    #[serde(default)]
    pub output_cost_per_token: Option<f64>,
    #[serde(default)]
    pub max_input_tokens: Option<u32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub cache_read_input_token_cost: Option<f64>,
    #[serde(default)]
    pub cache_creation_input_token_cost: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address to bind the proxy/admin HTTP server.
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Master / admin token. Set with `polyglotmesh key --role admin` or directly in config.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Optional global max-parallel-requests cap (across all keys).
    #[serde(default)]
    pub global_max_parallel_requests: Option<u32>,
    /// Maximum size in MB of a single request body.
    #[serde(default)]
    pub max_request_size_mb: Option<u32>,
    /// Retention for `usage_events` rows in days. 0 = keep forever.
    /// A background task prunes rows older than this once per day.
    #[serde(default)]
    pub usage_retention_days: u32,
    /// Hour of day (0-23, local) at which the retention prune runs, instead of
    /// the default fixed 24h cadence. `None` (default) keeps the simple daily loop.
    #[serde(default)]
    pub usage_retention_hour: Option<u8>,
    /// Graceful-shutdown drain timeout in seconds. On SIGTERM/SIGINT the server
    /// stops accepting new connections and waits this long for in-flight
    /// requests to finish before exiting. Default 30s.
    #[serde(default)]
    pub drain_timeout_s: Option<u64>,
    /// Trace sampling ratio (0.0–1.0). Fraction of requests that get a trace
    /// span recorded into the in-memory ring. 1.0 = record all (default),
    /// 0.1 = record ~10%. Useful for trading trace fidelity for retention
    /// under heavy load.
    #[serde(default)]
    pub trace_sample_ratio: Option<f64>,
}

fn default_bind() -> String {
    "0.0.0.0:8080".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelAliasEntry {
    pub upstream_id: String,
    pub upstream_model: String,
}

/// One entry in the `model_list` (LiteLLM-style top-level alias map).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelListEntry {
    /// Logical model name exposed to clients.
    pub model_name: String,
    /// ID of the upstream to dispatch to.
    pub upstream_id: String,
    /// Optional override of the model name sent upstream.
    #[serde(default)]
    pub upstream_model: Option<String>,
}

/// A self-issued key, with LiteLLM-style limit fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApiKeyConfig {
    /// The raw key (hashed internally at runtime). Either `key` or `key_alias` is required.
    #[serde(default)]
    pub key: Option<String>,
    /// Friendly name for the key. Auto-generated if missing.
    #[serde(default)]
    pub key_alias: Option<String>,
    /// Optional role: "api" (default) or "admin".
    #[serde(default = "default_role_api")]
    pub role: String,
    /// Allowed model names. Empty = any.
    #[serde(default)]
    pub models: Vec<String>,
    /// Allowed provider kinds. Empty = both.
    #[serde(default)]
    pub allowed_providers: Vec<ProviderKind>,
    /// Per-key RPM cap (requests per minute). 0 = unlimited.
    #[serde(default)]
    pub rpm_limit: u32,
    /// Per-key TPM cap (tokens per minute). 0 = unlimited.
    #[serde(default)]
    pub tpm_limit: u32,
    /// Per-key max parallel requests. 0 = unlimited.
    #[serde(default)]
    pub max_parallel_requests: u32,
    /// Per-key max budget in USD. None = unlimited.
    #[serde(default)]
    pub max_budget: Option<f64>,
    /// Reset period for `max_budget` ("30s"/"1h"/"1d"/"30d"/"1w"/"1mo").
    #[serde(default)]
    pub budget_duration: Option<String>,
    /// Optional expiry (ISO 8601 timestamp or relative like "7d").
    #[serde(default)]
    pub expires: Option<String>,
    /// Soft budget in USD; when crossed the key is throttled (not blocked).
    #[serde(default)]
    pub soft_budget: Option<f64>,
    /// Allowed region tag (LiteLLM `allowed_model_region`).
    #[serde(default)]
    pub allowed_model_region: Option<String>,
    /// Whether this key is blocked.
    #[serde(default)]
    pub blocked: bool,
    /// Optional IP allowlist, as CIDR strings (e.g. "10.0.0.0/8", "203.0.113.5/32").
    /// Empty = allow from any address. Enforced against the client IP in the
    /// auth middleware (X-Forwarded-For, then socket address).
    #[serde(default)]
    pub allowed_cidrs: Vec<String>,
}

fn default_role_api() -> String {
    "api".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    /// Self-issued API keys (LiteLLM-style rich entries).
    #[serde(default)]
    pub api_keys: Vec<ApiKeyConfig>,
    /// Legacy: bare list of API key strings (still supported, role=api, no limits).
    #[serde(default)]
    pub api_keys_legacy: Vec<String>,
    /// Upstream providers (OpenAI / Anthropic compatible).
    #[serde(default)]
    pub upstreams: Vec<UpstreamConfig>,
    /// Top-level model_name → upstream mapping (LiteLLM `model_list` style).
    #[serde(default)]
    pub model_list: Vec<ModelListEntry>,
    /// Per-model logical alias → list of upstream model overrides.
    #[serde(default)]
    pub model_aliases: BTreeMap<String, Vec<ModelAliasEntry>>,
    /// Global queue tuning.
    #[serde(default)]
    pub queue: QueueConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Max number of queued requests per provider. 0 = unbounded.
    #[serde(default)]
    pub max_queue_per_provider: usize,
    /// How long a request waits in queue before timing out (ms).
    #[serde(default = "default_queue_wait_ms")]
    pub queue_wait_timeout_ms: u64,
    /// Health check: how often to probe upstreams (ms). 0 = disable.
    #[serde(default = "default_healthcheck_ms")]
    pub healthcheck_interval_ms: u64,
    /// Health check timeout (ms).
    #[serde(default = "default_healthcheck_timeout_ms")]
    pub healthcheck_timeout_ms: u64,
    /// Number of consecutive failures before marking upstream unhealthy.
    #[serde(default = "default_healthcheck_threshold")]
    pub healthcheck_failure_threshold: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_queue_per_provider: 0,
            queue_wait_timeout_ms: default_queue_wait_ms(),
            healthcheck_interval_ms: default_healthcheck_ms(),
            healthcheck_timeout_ms: default_healthcheck_timeout_ms(),
            healthcheck_failure_threshold: default_healthcheck_threshold(),
        }
    }
}

fn default_queue_wait_ms() -> u64 {
    30_000
}
fn default_healthcheck_ms() -> u64 {
    15_000
}
fn default_healthcheck_timeout_ms() -> u64 {
    5_000
}
fn default_healthcheck_threshold() -> u32 {
    3
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind: default_bind(),
                admin_token: None,
                global_max_parallel_requests: None,
                max_request_size_mb: None,
                usage_retention_days: 0,
                usage_retention_hour: None,
                drain_timeout_s: None,
                trace_sample_ratio: None,
            },
            api_keys: Vec::new(),
            api_keys_legacy: Vec::new(),
            upstreams: Vec::new(),
            model_list: Vec::new(),
            model_aliases: BTreeMap::new(),
            queue: QueueConfig::default(),
        }
    }
}
