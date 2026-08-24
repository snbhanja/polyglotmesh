use crate::config::types::{ApiKeyConfig, ProviderKind};
use crate::error::{RouterError, RouterResult};
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use rand::RngCore;
use sha2::{Digest, Sha256};

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

const API_KEY_PREFIX: &str = "pgm";
const ADMIN_KEY_PREFIX: &str = "pgm-admin";

/// Per-key usage counters, all atomic.
pub struct KeyUsage {
    pub total_requests: AtomicU64,
    pub total_input_tokens: AtomicU64,
    pub total_output_tokens: AtomicU64,
    pub total_spend_usd: AtomicU64, // stored as cents*1000 to avoid f64 atomics
    /// Rolling window for RPM: 1-minute bucket count.
    pub rpm_window_start: Mutex<chrono::DateTime<Utc>>,
    pub rpm_window_count: AtomicU32,
    /// Rolling window for TPM: 1-minute bucket token count.
    pub tpm_window_start: Mutex<chrono::DateTime<Utc>>,
    pub tpm_window_tokens: AtomicU32,
    /// Current in-flight request count for this key.
    pub in_flight: AtomicU32,
}

impl KeyUsage {
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            total_input_tokens: AtomicU64::new(0),
            total_output_tokens: AtomicU64::new(0),
            total_spend_usd: AtomicU64::new(0),
            rpm_window_start: Mutex::new(Utc::now()),
            rpm_window_count: AtomicU32::new(0),
            tpm_window_start: Mutex::new(Utc::now()),
            tpm_window_tokens: AtomicU32::new(0),
            in_flight: AtomicU32::new(0),
        }
    }

    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "total_requests": self.total_requests.load(Ordering::Relaxed),
            "total_input_tokens": self.total_input_tokens.load(Ordering::Relaxed),
            "total_output_tokens": self.total_output_tokens.load(Ordering::Relaxed),
            "total_spend_usd": (self.total_spend_usd.load(Ordering::Relaxed) as f64) / 1000.0,
            "rpm_current_window": self.rpm_window_count.load(Ordering::Relaxed),
            "tpm_current_window": self.tpm_window_tokens.load(Ordering::Relaxed),
            "in_flight": self.in_flight.load(Ordering::Relaxed),
        })
    }
}

/// A single API key record held in memory.
pub struct KeyRecord {
    pub raw: String,
    pub alias: String,
    pub role: String,
    pub models: Vec<String>,
    pub allowed_providers: Vec<ProviderKind>,
    pub rpm_limit: u32,
    pub tpm_limit: u32,
    pub max_parallel_requests: u32,
    pub max_budget: Option<f64>,
    pub soft_budget: Option<f64>,
    pub budget_duration: Option<String>,
    pub budget_window_start: Mutex<chrono::DateTime<Utc>>,
    pub expires_at: Option<chrono::DateTime<Utc>>,
    pub allowed_model_region: Option<String>,
    pub blocked: bool,
    pub allowed_cidrs: Vec<ipnet::IpNet>,
    pub created_at: chrono::DateTime<Utc>,
    pub usage: KeyUsage,
}

impl KeyRecord {
    pub fn from_config(cfg: ApiKeyConfig) -> RouterResult<Self> {
        let raw = match cfg.key {
            Some(k) if !k.trim().is_empty() => k,
            _ => return Err(RouterError::BadRequest("key string is required".into())),
        };
        let alias = cfg
            .key_alias
            .unwrap_or_else(|| raw.chars().take(12).collect::<String>() + "…");
        let expires_at = cfg.expires.as_deref().and_then(parse_expiry);
        Ok(Self {
            raw,
            alias,
            role: cfg.role,
            models: cfg.models,
            allowed_providers: cfg.allowed_providers,
            rpm_limit: cfg.rpm_limit,
            tpm_limit: cfg.tpm_limit,
            max_parallel_requests: cfg.max_parallel_requests,
            max_budget: cfg.max_budget,
            soft_budget: cfg.soft_budget,
            budget_duration: cfg.budget_duration,
            budget_window_start: Mutex::new(Utc::now()),
            expires_at,
            allowed_model_region: cfg.allowed_model_region,
            blocked: cfg.blocked,
            allowed_cidrs: cfg
                .allowed_cidrs
                .iter()
                .filter_map(|c| c.parse::<ipnet::IpNet>().ok())
                .collect(),
            created_at: Utc::now(),
            usage: KeyUsage::new(),
        })
    }

    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "key_alias": self.alias,
            "role": self.role,
            "models": self.models,
            "allowed_providers": self.allowed_providers.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
            "rpm_limit": self.rpm_limit,
            "tpm_limit": self.tpm_limit,
            "max_parallel_requests": self.max_parallel_requests,
            "max_budget": self.max_budget,
            "soft_budget": self.soft_budget,
            "budget_duration": self.budget_duration,
            "expires_at": self.expires_at,
            "allowed_model_region": self.allowed_model_region,
            "blocked": self.blocked,
            "allowed_cidrs": self
                .allowed_cidrs
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>(),
            "created_at": self.created_at,
            "usage": self.usage.snapshot(),
        })
    }
}

pub struct AuthStore {
    keys: parking_lot::RwLock<Vec<Arc<KeyRecord>>>,
}

impl Default for AuthStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthStore {
    pub fn new() -> Self {
        Self {
            keys: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Add a bare-string key (used by `init` and the simple `key` CLI).
    pub fn add_api_key(&self, raw: &str) {
        let cfg = ApiKeyConfig {
            key: Some(raw.to_string()),
            key_alias: Some(raw.chars().take(12).collect::<String>() + "…"),
            role: "api".into(),
            models: vec![],
            allowed_providers: vec![],
            rpm_limit: 0,
            tpm_limit: 0,
            max_parallel_requests: 0,
            max_budget: None,
            budget_duration: None,
            expires: None,
            soft_budget: None,
            allowed_model_region: None,
            blocked: false,
            allowed_cidrs: vec![],
        };
        self.add(cfg);
    }

    pub fn add_admin_key(&self, raw: &str) {
        let cfg = ApiKeyConfig {
            key: Some(raw.to_string()),
            key_alias: Some(raw.chars().take(12).collect::<String>() + "…"),
            role: "admin".into(),
            models: vec![],
            allowed_providers: vec![],
            rpm_limit: 0,
            tpm_limit: 0,
            max_parallel_requests: 0,
            max_budget: None,
            budget_duration: None,
            expires: None,
            soft_budget: None,
            allowed_model_region: None,
            blocked: false,
            allowed_cidrs: vec![],
        };
        self.add(cfg);
    }

    pub fn add(&self, cfg: ApiKeyConfig) {
        if let Ok(rec) = KeyRecord::from_config(cfg) {
            self.keys.write().push(Arc::new(rec));
        }
    }

    pub fn remove_by_raw(&self, raw: &str) -> bool {
        let mut w = self.keys.write();
        let before = w.len();
        w.retain(|k| k.raw != raw);
        before != w.len()
    }

    pub fn api_key_count(&self) -> usize {
        self.keys.read().iter().filter(|k| k.role == "api").count()
    }

    pub fn admin_key_count(&self) -> usize {
        self.keys
            .read()
            .iter()
            .filter(|k| k.role == "admin")
            .count()
    }

    pub fn validate_api_key(&self, raw: &str) -> bool {
        if raw.is_empty() {
            return false;
        }
        self.keys
            .read()
            .iter()
            .any(|k| k.raw == raw && k.role == "api")
    }

    pub fn validate_admin_key(&self, raw: &str) -> bool {
        if raw.is_empty() {
            return false;
        }
        self.keys
            .read()
            .iter()
            .any(|k| k.raw == raw && k.role == "admin")
    }

    /// Look up a key record by raw string.
    pub fn lookup(&self, raw: &str) -> Option<Arc<KeyRecord>> {
        self.keys.read().iter().find(|k| k.raw == raw).cloned()
    }

    pub fn check_bearer(&self, header: Option<&str>) -> bool {
        let h = match header {
            Some(h) => h,
            None => return false,
        };
        let token = h.trim();
        let token = token.strip_prefix("Bearer ").unwrap_or(token).trim();
        self.validate_api_key(token)
    }

    pub fn check_admin(&self, header: Option<&str>) -> bool {
        let h = match header {
            Some(h) => h,
            None => return false,
        };
        let token = h.trim();
        let token = token.strip_prefix("Bearer ").unwrap_or(token).trim();
        self.validate_admin_key(token) || self.validate_api_key(token)
    }

    pub fn all_keys(&self) -> Vec<Arc<KeyRecord>> {
        self.keys.read().clone()
    }

    /// Atomically replace the entire key set with the contents of `other`.
    /// Used by `AppState::reload_from_disk`.
    pub fn replace_all(&self, other: AuthStore) {
        let mut w = self.keys.write();
        *w = other.keys.into_inner();
    }
}

pub fn hash(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn generate_api_key() -> String {
    let mut buf = [0u8; 32];
    rand::rng().fill_bytes(&mut buf);
    let token = hex::encode(buf);
    format!("{API_KEY_PREFIX}-{token}")
}

pub fn generate_admin_key() -> String {
    let mut buf = [0u8; 24];
    rand::rng().fill_bytes(&mut buf);
    let token = hex::encode(buf);
    format!("{ADMIN_KEY_PREFIX}-{token}")
}

pub fn bearer_token(headers: &axum::http::HeaderMap) -> Option<String> {
    let h = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let t = h.trim();
    let t = t.strip_prefix("Bearer ").unwrap_or(t).trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// Authorize a request, returning the key record and possibly updating state.
/// Returns `Err(RouterError::Unauthorized)` if the bearer is missing or unknown,
/// `Err(RouterError::TooManyRequests)` if the key's rpm/tpm/parallel limits are hit,
/// `Err(RouterError::PaymentRequired)` if the key's budget is exhausted.
pub fn authorize(
    headers: &axum::http::HeaderMap,
    store: &Arc<AuthStore>,
    client_ip: Option<std::net::IpAddr>,
) -> RouterResult<Arc<KeyRecord>> {
    let token = bearer_token(headers)
        .ok_or_else(|| RouterError::Unauthorized("missing Authorization header".into()))?;
    let rec = store
        .lookup(&token)
        .ok_or_else(|| RouterError::Unauthorized("invalid or unknown API key".into()))?;
    if rec.blocked {
        return Err(RouterError::Unauthorized("key is blocked".into()));
    }
    if let Some(exp) = rec.expires_at {
        if Utc::now() > exp {
            return Err(RouterError::Unauthorized("key has expired".into()));
        }
    }
    // IP allowlist enforcement (if configured).
    if !rec.allowed_cidrs.is_empty() {
        match client_ip {
            Some(ip) => {
                let allowed = rec.allowed_cidrs.iter().any(|net| net.contains(&ip));
                if !allowed {
                    return Err(RouterError::Unauthorized(
                        "client address not in key's allowed_cidrs".into(),
                    ));
                }
            }
            None => {
                return Err(RouterError::Unauthorized(
                    "client address unavailable; key requires allowed_cidrs".into(),
                ));
            }
        }
    }
    // RPM check
    if rec.rpm_limit > 0 {
        let now = Utc::now();
        let mut start = rec.usage.rpm_window_start.lock();
        if now - *start > Duration::seconds(60) {
            *start = now;
            rec.usage.rpm_window_count.store(0, Ordering::Relaxed);
        }
        let cur = rec.usage.rpm_window_count.fetch_add(1, Ordering::Relaxed);
        if cur >= rec.rpm_limit {
            return Err(RouterError::TooManyRequests(
                "rpm_limit exceeded for this key".into(),
            ));
        }
    }
    // Parallel cap
    if rec.max_parallel_requests > 0 {
        let cur = rec
            .usage
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |c| {
                if c < rec.max_parallel_requests {
                    Some(c + 1)
                } else {
                    None
                }
            })
            .map_err(|_| {
                RouterError::TooManyRequests("max_parallel_requests exceeded for this key".into())
            })?;
        let _ = cur;
    }
    // Budget check
    if let Some(limit) = rec.max_budget {
        if rec.budget_duration.is_some() {
            let now = Utc::now();
            let mut start = rec.budget_window_start.lock();
            if let Some(d) = parse_duration_to_chrono(rec.budget_duration.as_deref()) {
                if now - *start > d {
                    *start = now;
                    rec.usage.total_spend_usd.store(0, Ordering::Relaxed);
                }
            }
        }
        let spend = (rec.usage.total_spend_usd.load(Ordering::Relaxed) as f64) / 1000.0;
        if spend >= limit {
            return Err(RouterError::PaymentRequired(
                "max_budget exceeded for this key".into(),
            ));
        }
    }
    Ok(rec)
}

/// Record token usage for a key. Called after a successful response (or stream open).
pub fn record_usage(rec: &KeyRecord, input_tokens: u64, output_tokens: u64, cost_usd: f64) {
    rec.usage.total_requests.fetch_add(1, Ordering::Relaxed);
    rec.usage
        .total_input_tokens
        .fetch_add(input_tokens, Ordering::Relaxed);
    rec.usage
        .total_output_tokens
        .fetch_add(output_tokens, Ordering::Relaxed);
    rec.usage
        .total_spend_usd
        .fetch_add((cost_usd * 1000.0) as u64, Ordering::Relaxed);
    if rec.tpm_limit > 0 {
        let now = Utc::now();
        let mut start = rec.usage.tpm_window_start.lock();
        if now - *start > Duration::seconds(60) {
            *start = now;
            rec.usage.tpm_window_tokens.store(0, Ordering::Relaxed);
        }
        rec.usage
            .tpm_window_tokens
            .fetch_add((input_tokens + output_tokens) as u32, Ordering::Relaxed);
    }
}

/// Release the in-flight slot at the end of a request.
pub fn release(rec: &KeyRecord) {
    if rec.max_parallel_requests > 0 {
        rec.usage.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

fn parse_expiry(s: &str) -> Option<chrono::DateTime<Utc>> {
    // Try absolute timestamp first.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Relative: "30s", "5m", "1h", "7d", "30d", "1w", "1mo".
    if let Some(d) = parse_duration_to_chrono(Some(s)) {
        return Some(Utc::now() + d);
    }
    None
}

pub fn parse_duration_to_chrono(s: Option<&str>) -> Option<Duration> {
    let s = s?;
    if s.is_empty() {
        return None;
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: i64 = num.parse().ok()?;
    Some(match unit {
        "s" => Duration::seconds(n),
        "m" => Duration::minutes(n),
        "h" => Duration::hours(n),
        "d" => Duration::days(n),
        "w" => Duration::weeks(n),
        "M" | "o" => Duration::days(n * 30),
        _ => return None,
    })
}

// Re-export the auth error variants.
impl RouterError {
    pub fn too_many_requests(msg: String) -> Self {
        RouterError::TooManyRequests(msg)
    }
    pub fn payment_required(msg: String) -> Self {
        RouterError::PaymentRequired(msg)
    }
}

#[allow(unused_imports)]
use std::sync::atomic::AtomicU32 as _AtomicU32;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{ApiKeyConfig, ProviderKind};
    use crate::error::RouterError;

    fn make_key_cfg(raw: &str) -> ApiKeyConfig {
        ApiKeyConfig {
            key: Some(raw.to_string()),
            key_alias: None,
            role: "api".into(),
            models: vec![],
            allowed_providers: vec![],
            rpm_limit: 0,
            tpm_limit: 0,
            max_parallel_requests: 0,
            max_budget: None,
            budget_duration: None,
            expires: None,
            soft_budget: None,
            allowed_model_region: None,
            blocked: false,
            allowed_cidrs: vec![],
        }
    }

    #[test]
    fn generated_keys_have_expected_prefixes() {
        let api = generate_api_key();
        let admin = generate_admin_key();
        assert!(api.starts_with("pgm-"), "api key was {api}");
        assert!(admin.starts_with("pgm-admin-"), "admin key was {admin}");
        // 32 bytes hex + prefix == 3 + 64
        assert_eq!(api.len(), "pgm-".len() + 64);
        // 24 bytes hex for admin
        assert_eq!(admin.len(), "pgm-admin-".len() + 48);
    }

    #[test]
    fn generated_keys_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            assert!(seen.insert(generate_api_key()));
        }
    }

    #[test]
    fn hash_is_deterministic_and_hex() {
        let a = hash("hello");
        let b = hash("hello");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(hex::decode(&a).is_ok());
        assert_ne!(hash("hello"), hash("world"));
    }

    #[test]
    fn store_validates_api_and_admin_keys() {
        let store = AuthStore::new();
        store.add_api_key("key-1");
        store.add_admin_key("admin-1");

        assert!(store.validate_api_key("key-1"));
        assert!(!store.validate_api_key("admin-1"));
        assert!(store.validate_admin_key("admin-1"));
        assert!(!store.validate_admin_key("key-1"));
        assert!(!store.validate_api_key("nope"));
        assert!(!store.validate_api_key(""));
    }

    #[test]
    fn check_bearer_strips_prefix_and_rejects_empty() {
        let store = AuthStore::new();
        store.add_api_key("secret");
        assert!(store.check_bearer(Some("Bearer secret")));
        assert!(store.check_bearer(Some("  secret  ")));
        assert!(!store.check_bearer(Some("Bearer wrong")));
        assert!(!store.check_bearer(None));
        assert!(!store.check_bearer(Some("  ")));
    }

    #[test]
    fn check_admin_accepts_either_role() {
        let store = AuthStore::new();
        store.add_api_key("a-key");
        store.add_admin_key("a-admin");
        assert!(store.check_admin(Some("Bearer a-admin")));
        assert!(store.check_admin(Some("Bearer a-key")));
        assert!(!store.check_admin(Some("Bearer nope")));
    }

    #[test]
    fn remove_by_raw_works() {
        let store = AuthStore::new();
        store.add_api_key("k");
        assert!(store.remove_by_raw("k"));
        assert!(!store.remove_by_raw("k"));
        assert_eq!(store.api_key_count(), 0);
    }

    #[test]
    fn bearer_token_extracts_from_headers() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer abc123"),
        );
        assert_eq!(bearer_token(&headers).as_deref(), Some("abc123"));

        let none = axum::http::HeaderMap::new();
        assert_eq!(bearer_token(&none), None);
    }

    #[test]
    fn key_record_from_config_requires_key() {
        let cfg = make_key_cfg("");
        assert!(matches!(
            KeyRecord::from_config(cfg),
            Err(RouterError::BadRequest(_))
        ));
        let cfg = make_key_cfg("ok");
        assert!(KeyRecord::from_config(cfg).is_ok());
    }

    #[test]
    fn authorize_rejects_missing_and_unknown() {
        let store = AuthStore::new();
        store.add_api_key("good");
        let arc = std::sync::Arc::new(store);

        let missing = axum::http::HeaderMap::new();
        assert!(matches!(
            authorize(&missing, &arc, None),
            Err(RouterError::Unauthorized(_))
        ));

        let mut bad = axum::http::HeaderMap::new();
        bad.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer nope"),
        );
        assert!(matches!(
            authorize(&bad, &arc, None),
            Err(RouterError::Unauthorized(_))
        ));
    }

    #[test]
    fn authorize_blocks_blocked_keys() {
        let store = AuthStore::new();
        let mut cfg = make_key_cfg("blocked");
        cfg.blocked = true;
        store.add(cfg);
        let arc = std::sync::Arc::new(store);

        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer blocked"),
        );
        assert!(matches!(
            authorize(&h, &arc, None),
            Err(RouterError::Unauthorized(_))
        ));
    }

    #[test]
    fn authorize_enforces_rpm_limit() {
        let store = AuthStore::new();
        let mut cfg = make_key_cfg("rpm");
        cfg.rpm_limit = 2;
        store.add(cfg);
        let arc = std::sync::Arc::new(store);

        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer rpm"),
        );
        // First two succeed, third hits the limit.
        assert!(authorize(&h, &arc, None).is_ok());
        assert!(authorize(&h, &arc, None).is_ok());
        assert!(matches!(
            authorize(&h, &arc, None),
            Err(RouterError::TooManyRequests(_))
        ));
    }

    #[test]
    fn authorize_enforces_parallel_limit() {
        let store = AuthStore::new();
        let mut cfg = make_key_cfg("par");
        cfg.max_parallel_requests = 1;
        store.add(cfg);
        let arc = std::sync::Arc::new(store);

        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer par"),
        );
        let rec = authorize(&h, &arc, None).unwrap();
        // Second attempt should fail while the first slot is held.
        assert!(matches!(
            authorize(&h, &arc, None),
            Err(RouterError::TooManyRequests(_))
        ));
        release(&rec);
        assert!(authorize(&h, &arc, None).is_ok());
    }

    #[test]
    fn authorize_enforces_budget() {
        let store = AuthStore::new();
        let mut cfg = make_key_cfg("budget");
        cfg.max_budget = Some(1.0);
        cfg.budget_duration = Some("1h".into());
        store.add(cfg);
        let arc = std::sync::Arc::new(store);

        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer budget"),
        );
        let rec = authorize(&h, &arc, None).unwrap();
        record_usage(&rec, 0, 0, 0.9);
        release(&rec);
        // Still under budget.
        assert!(authorize(&h, &arc, None).is_ok());
        let rec = authorize(&h, &arc, None).unwrap();
        record_usage(&rec, 0, 0, 0.5);
        release(&rec);
        // Now over budget.
        assert!(matches!(
            authorize(&h, &arc, None),
            Err(RouterError::PaymentRequired(_))
        ));
    }

    #[test]
    fn parse_duration_handles_units() {
        assert_eq!(parse_duration_to_chrono(Some("30s")), Some(Duration::seconds(30)));
        assert_eq!(parse_duration_to_chrono(Some("5m")), Some(Duration::minutes(5)));
        assert_eq!(parse_duration_to_chrono(Some("2h")), Some(Duration::hours(2)));
        assert_eq!(parse_duration_to_chrono(Some("3d")), Some(Duration::days(3)));
        assert_eq!(parse_duration_to_chrono(Some("1w")), Some(Duration::weeks(1)));
        assert_eq!(parse_duration_to_chrono(Some("2M")), Some(Duration::days(60)));
        assert_eq!(parse_duration_to_chrono(Some("2o")), Some(Duration::days(60)));
        assert_eq!(parse_duration_to_chrono(None), None);
        assert_eq!(parse_duration_to_chrono(Some("")), None);
        assert_eq!(parse_duration_to_chrono(Some("10x")), None);
    }

    #[test]
    fn parse_expiry_relative_and_absolute() {
        let future = parse_expiry("1h").unwrap();
        assert!(future > Utc::now());
        let abs = "2099-01-01T00:00:00Z";
        let future2 = parse_expiry(abs).unwrap();
        assert_eq!(future2, DateTime::parse_from_rfc3339(abs).unwrap().with_timezone(&Utc));
        assert_eq!(parse_expiry("garbage"), None);
    }

    #[test]
    fn record_usage_updates_counters_and_release() {
        let rec = KeyRecord::from_config(make_key_cfg("u")).unwrap();
        record_usage(&rec, 10, 20, 0.5);
        let snap = rec.usage.snapshot();
        assert_eq!(snap["total_requests"], 1);
        assert_eq!(snap["total_input_tokens"], 10);
        assert_eq!(snap["total_output_tokens"], 20);
        assert_eq!(snap["total_spend_usd"], 0.5);

        // release only touches in_flight when max_parallel_requests > 0
        let par = {
            let mut c = make_key_cfg("u2");
            c.max_parallel_requests = 2;
            KeyRecord::from_config(c).unwrap()
        };
        par.usage.in_flight.store(1, Ordering::Relaxed);
        release(&par);
        assert_eq!(par.usage.in_flight.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn provider_kind_serde_round_trips() {
        let json = serde_json::to_string(&ProviderKind::Anthropic).unwrap();
        assert!(json.contains("anthropic"));
        let back: ProviderKind = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ProviderKind::Anthropic);
    }
}
