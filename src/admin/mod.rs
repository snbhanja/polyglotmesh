use crate::auth::{generate_admin_key, generate_api_key};
use crate::config::types::{
    ApiKeyConfig, ModelAliasEntry, ProviderKind, UpstreamConfig,
};
use crate::error::RouterError;

use crate::state::AppState;
use crate::storage::UsageBucket;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub type SharedState = Arc<AppState>;

pub fn admin_router(state: SharedState) -> Router<SharedState> {
    Router::new()
        .route("/v1/admin/status", get(admin_status))
        .route("/v1/admin/upstreams", get(list_upstreams).post(create_upstream))
        .route(
            "/v1/admin/upstreams/:id",
            get(get_upstream)
                .put(update_upstream)
                .delete(delete_upstream)
                .post(control_upstream),
        )
        .route("/v1/admin/upstreams/:id/pause", post(pause_upstream))
        .route("/v1/admin/upstreams/:id/resume", post(resume_upstream))
        .route("/v1/admin/upstreams/:id/prices", get(get_upstream_prices).post(set_upstream_prices))
        .route("/v1/admin/keys", get(list_keys).post(create_key))
        .route("/v1/admin/keys/:alias", delete(revoke_key_by_alias))
        .route("/v1/admin/reload", post(reload_config))
        .route("/v1/admin/usage", get(admin_usage))
        .route("/v1/admin/usage/recent", get(admin_usage_recent))
        .route("/v1/admin/usage/retention", post(set_retention))
        .route("/v1/admin/metrics", get(admin_metrics_json))
        .route("/v1/admin/metrics/prom", get(admin_metrics_prom))
        .route("/v1/admin/rates", get(admin_rates))
        .route("/v1/admin/traces/recent", get(admin_traces_recent))
        .route("/v1/admin/events/stream", get(admin_events_stream))
        .route("/v1/admin/metrics/reset", post(admin_metrics_reset))
        .route("/v1/admin/audit", get(admin_audit))
        .route("/v1/admin/aliases", get(list_aliases).put(set_aliases))
        .route("/v1/admin/aliases/:name", delete(delete_alias))
        .route("/v1/admin/model_list", get(list_model_list).put(put_model_list))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            admin_auth_middleware,
        ))
}

pub async fn admin_auth_middleware(
    State(state): State<SharedState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    match crate::auth::authorize(req.headers(), &state.auth) {
        Ok(rec) if rec.role == "admin" => next.run(req).await,
        _ => crate::error::RouterError::Unauthorized("admin token required".into()).into_response(),
    }
}

/// Best-effort "who triggered this" string for the audit log.
/// Returns the admin token's alias, or "unknown" if the request had none.
fn actor_from_headers(headers: &axum::http::HeaderMap) -> String {
    if let Some(h) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(s) = h.to_str() {
            // Strip "Bearer " prefix and keep the alias portion (e.g. "pgm-admin-abcd1234").
            let tok = s.trim_start_matches("Bearer ").trim_start_matches("bearer ");
            return tok.split('.').next().unwrap_or(tok).to_string();
        }
    }
    "unknown".to_string()
}

async fn admin_status(State(state): State<SharedState>) -> Response {
    let cfg = state.config.read().clone();
    let upstreams = state.registry.all();
    let body = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "bind": cfg.server.bind,
        "api_keys": state.auth.api_key_count(),
        "admin_keys": state.auth.admin_key_count(),
        "upstreams": upstreams.len(),
        "upstreams_by_kind": {
            "openai": state.registry.by_provider(ProviderKind::Openai).len(),
            "anthropic": state.registry.by_provider(ProviderKind::Anthropic).len(),
        },
        "queue": state.queue.stats.snapshot(),
        "pending": state.queue.pending_snapshot(),
        "config_file": crate::config::RouterPaths::discover().config_file,
    });
    (axum::http::StatusCode::OK, Json(body)).into_response()
}

async fn list_upstreams(State(state): State<SharedState>) -> Response {
    let upstreams = state.registry.all();
    let body: Vec<serde_json::Value> = upstreams.iter().map(|u| u.snapshot()).collect();
    (axum::http::StatusCode::OK, Json(body)).into_response()
}

async fn get_upstream(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    match state.registry.get(&id) {
        Some(u) => (axum::http::StatusCode::OK, Json(u.snapshot())).into_response(),
        None => RouterError::UpstreamNotFound.into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct UpstreamInput {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub kind: ProviderKind,
    pub base_url: String,
    pub api_key: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub weight: u32,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub max_concurrency: u32,
    #[serde(default)]
    pub rate_limit_rpm: u32,
    #[serde(default)]
    pub rate_limit_tpm: u32,
    #[serde(default)]
    pub max_budget: Option<f64>,
    #[serde(default)]
    pub budget_duration: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_timeout() -> u64 {
    60_000
}
fn default_enabled() -> bool {
    true
}

impl From<UpstreamInput> for UpstreamConfig {
    fn from(v: UpstreamInput) -> Self {
        UpstreamConfig {
            id: v.id,
            name: v.name,
            kind: v.kind,
            base_url: v.base_url,
            api_key: v.api_key,
            priority: v.priority,
            models: v.models,
            weight: v.weight,
            timeout_ms: v.timeout_ms,
            max_concurrency: v.max_concurrency,
            rate_limit_rpm: v.rate_limit_rpm,
            rate_limit_tpm: v.rate_limit_tpm,
            max_budget: v.max_budget,
            budget_duration: v.budget_duration,
            model_info: std::collections::BTreeMap::new(),
            region: v.region,
            tags: v.tags,
            enabled: v.enabled,
        }
    }
}

async fn create_upstream(
    State(state): State<SharedState>,
    Json(input): Json<UpstreamInput>,
) -> Response {
    let cfg: UpstreamConfig = input.into();
    if cfg.id.trim().is_empty() {
        return RouterError::BadRequest("upstream id is required".into()).into_response();
    }
    if cfg.base_url.trim().is_empty() {
        return RouterError::BadRequest("base_url is required".into()).into_response();
    }
    if cfg.api_key.trim().is_empty() {
        return RouterError::BadRequest("api_key is required".into()).into_response();
    }
    {
        let mut w = state.config.write();
        w.upstreams.retain(|u| u.id != cfg.id);
        w.upstreams.push(cfg.clone());
    }
    state.registry.upsert(cfg.clone());
    if let Err(e) = state.save_to_disk() {
        tracing::warn!("failed to persist config: {e}");
    }
    (axum::http::StatusCode::CREATED, Json(cfg)).into_response()
}

async fn update_upstream(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(input): Json<UpstreamInput>,
) -> Response {
    if input.id != id {
        return RouterError::BadRequest("path id and body id must match".into()).into_response();
    }
    let cfg: UpstreamConfig = input.into();
    {
        let mut w = state.config.write();
        w.upstreams.retain(|u| u.id != id);
        w.upstreams.push(cfg.clone());
    }
    state.registry.remove(&id);
    state.registry.upsert(cfg.clone());
    if let Err(e) = state.save_to_disk() {
        tracing::warn!("failed to persist config: {e}");
    }
    (axum::http::StatusCode::OK, Json(cfg)).into_response()
}

async fn delete_upstream(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    {
        let mut w = state.config.write();
        w.upstreams.retain(|u| u.id != id);
    }
    let removed = state.registry.remove(&id);
    if let Err(e) = state.save_to_disk() {
        tracing::warn!("failed to persist config: {e}");
    }
    if removed {
        (axum::http::StatusCode::NO_CONTENT, "").into_response()
    } else {
        RouterError::UpstreamNotFound.into_response()
    }
}

#[derive(Debug, Deserialize)]
pub struct ControlBody {
    pub action: String,
}

async fn control_upstream(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(body): Json<ControlBody>,
) -> Response {
    let Some(u) = state.registry.get(&id) else {
        return RouterError::UpstreamNotFound.into_response();
    };
    match body.action.as_str() {
        "enable" => { u.cfg.write().enabled = true; }
        "disable" => { u.cfg.write().enabled = false; }
        "reset" => {
            u.consecutive_failures.store(0, std::sync::atomic::Ordering::Relaxed);
            *u.health.write() = crate::upstream::Health::Healthy;
        }
        other => {
            return RouterError::BadRequest(format!("unknown action '{other}'")).into_response();
        }
    }
    let enabled = u.cfg.read().enabled;
    {
        let mut w = state.config.write();
        if let Some(cfg) = w.upstreams.iter_mut().find(|c| c.id == id) {
            cfg.enabled = enabled;
        }
    }
    let _ = state.save_to_disk();
    (axum::http::StatusCode::OK, Json(u.snapshot())).into_response()
}

async fn pause_upstream(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    let Some(u) = state.registry.get(&id) else {
        return RouterError::UpstreamNotFound.into_response();
    };
    u.paused.store(true, std::sync::atomic::Ordering::Relaxed);
    state.queue.notify_change();
    (axum::http::StatusCode::OK, Json(u.snapshot())).into_response()
}

async fn resume_upstream(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    let Some(u) = state.registry.get(&id) else {
        return RouterError::UpstreamNotFound.into_response();
    };
    u.paused.store(false, std::sync::atomic::Ordering::Relaxed);
    state.queue.notify_change();
    (axum::http::StatusCode::OK, Json(u.snapshot())).into_response()
}

// ---- Keys ----

#[derive(Debug, Deserialize, Default)]
pub struct CreateKeyBody {
    /// If true, generate a fresh raw key. Otherwise, the request must include `key`.
    #[serde(default)]
    pub generate: bool,
    /// Optional role override. Defaults to "api".
    #[serde(default)]
    pub role: Option<String>,
    /// All other fields are forwarded from the JSON body into the ApiKeyConfig.
    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct CreatedKey {
    pub key: String,
    pub key_alias: String,
    pub role: String,
    pub usage: serde_json::Value,
}

async fn create_key(
    State(state): State<SharedState>,
    headers: axum::http::HeaderMap,
    body: Option<Json<CreateKeyBody>>,
) -> Response {
    let body = body.map(|b| b.0).unwrap_or_default();
    let mut key_cfg: ApiKeyConfig = match serde_json::from_value(serde_json::Value::Object(body.fields.clone())) {
        Ok(c) => c,
        Err(e) => {
            return RouterError::BadRequest(format!("invalid key fields: {e}")).into_response();
        }
    };
    if let Some(r) = &body.role { key_cfg.role = r.clone(); }
    if body.generate || key_cfg.key.is_none() {
        let new_key = match key_cfg.role.as_str() {
            "admin" => generate_admin_key(),
            _ => generate_api_key(),
        };
        key_cfg.key = Some(new_key);
    }
    if key_cfg.key_alias.is_none() {
        let k = key_cfg.key.clone().unwrap_or_default();
        key_cfg.key_alias = Some(format!("{}…", k.chars().take(12).collect::<String>()));
    }
    let alias = key_cfg.key_alias.clone().unwrap();
    let raw = key_cfg.key.clone().unwrap();
    // Save to AuthStore
    state.auth.add(key_cfg.clone());
    // Save to config
    {
        let mut w = state.config.write();
        w.api_keys.retain(|k| k.key.as_deref() != Some(&raw));
        w.api_keys.push(key_cfg.clone());
        if key_cfg.role == "admin" {
            w.server.admin_token = Some(raw.clone());
        }
    }
    let _ = state.save_to_disk();
    let rec = state.auth.lookup(&raw);
    let usage = rec.as_ref().map(|r| r.usage.snapshot()).unwrap_or(serde_json::json!({}));
    let actor = actor_from_headers(&headers);
    let detail = format!(r#"{{"alias":"{}","role":"{}"}}"#, alias, key_cfg.role);
    let _ = state.storage.append_audit("key.create", &actor, &detail);
    (axum::http::StatusCode::CREATED, Json(CreatedKey {
        key: raw,
        key_alias: alias,
        role: key_cfg.role.clone(),
        usage,
    })).into_response()
}

async fn list_keys(State(state): State<SharedState>) -> Response {
    let keys: Vec<serde_json::Value> = state.auth.all_keys().iter().map(|k| k.snapshot()).collect();
    let body = serde_json::json!({
        "count": keys.len(),
        "keys": keys,
    });
    (axum::http::StatusCode::OK, Json(body)).into_response()
}

async fn revoke_key_by_alias(
    State(state): State<SharedState>,
    headers: axum::http::HeaderMap,
    Path(alias): Path<String>,
) -> Response {
    // Find by alias first, then by raw.
    let target_raw = state
        .auth
        .all_keys()
        .into_iter()
        .find(|k| k.alias == alias)
        .map(|k| k.raw.clone());
    let Some(raw) = target_raw else {
        return (axum::http::StatusCode::NOT_FOUND, "key not found").into_response();
    };
    let removed = state.auth.remove_by_raw(&raw);
    let actor = actor_from_headers(&headers);
    if removed {
        let detail = format!(r#"{{"alias":"{}"}}"#, alias);
        let _ = state.storage.append_audit("key.revoke", &actor, &detail);
    }
    {
        let mut w = state.config.write();
        w.api_keys.retain(|k| k.key.as_deref() != Some(&raw));
        if w.server.admin_token.as_deref() == Some(&raw) {
            w.server.admin_token = None;
        }
    }
    let _ = state.save_to_disk();
    if removed {
        (axum::http::StatusCode::NO_CONTENT, "").into_response()
    } else {
        (axum::http::StatusCode::NOT_FOUND, "key not found").into_response()
    }
}


// ---- Per-upstream pricing (override the built-in default price table) ----

async fn get_upstream_prices(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Response {
    match state.registry.get(&id) {
        Some(u) => {
            let prices = u.model_info();
            let known = u.known_models();
            (axum::http::StatusCode::OK, Json(serde_json::json!({
                "id": id,
                "model_info": prices,
                "known_models": known,
            }))).into_response()
        }
        None => RouterError::UpstreamNotFound.into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct PricesBody {
    /// If true (default), merge into the existing map. If false, replace entirely.
    #[serde(default = "default_true_bool")]
    pub merge: bool,
    pub prices: std::collections::BTreeMap<String, crate::config::types::ModelCost>,
}

fn default_true_bool() -> bool { true }

async fn set_upstream_prices(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    Json(body): Json<PricesBody>,
) -> Response {
    let upstream = match state.registry.get(&id) {
        Some(u) => u,
        None => return RouterError::UpstreamNotFound.into_response(),
    };
    upstream.set_model_info(body.prices.clone(), body.merge);

    {
        let mut w = state.config.write();
        if let Some(cfg) = w.upstreams.iter_mut().find(|u| u.id == id) {
            if body.merge {
                for (k, v) in &body.prices { cfg.model_info.insert(k.clone(), v.clone()); }
            } else {
                cfg.model_info = body.prices.clone();
            }
        } else {
            return RouterError::UpstreamNotFound.into_response();
        }
    }
    let _ = state.save_to_disk();

    let cfg = state.config.read().upstreams.iter().find(|u| u.id == id).cloned();
    if let Some(c) = cfg {
        let _ = state.storage.upsert_upstream(&c);
    }

    (axum::http::StatusCode::OK, Json(serde_json::json!({
        "status": "ok",
        "id": id,
        "merge": body.merge,
        "applied": body.prices.len(),
        "total_prices": upstream.model_info().len(),
    }))).into_response()
}

// ---- Aliases / model_list ----

async fn list_aliases(State(state): State<SharedState>) -> Response {
    let cfg = state.config.read();
    (axum::http::StatusCode::OK, Json(&cfg.model_aliases)).into_response()
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AliasesBody {
    pub name: String,
    pub entries: Vec<ModelAliasEntry>,
}

async fn set_aliases(
    State(state): State<SharedState>,
    Json(body): Json<AliasesBody>,
) -> Response {
    {
        let mut w = state.config.write();
        if body.entries.is_empty() {
            w.model_aliases.remove(&body.name);
        } else {
            w.model_aliases.insert(body.name.clone(), body.entries.clone());
        }
    }
    let _ = state.save_to_disk();
    (axum::http::StatusCode::OK, Json(body)).into_response()
}

async fn delete_alias(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Response {
    let removed = {
        let mut w = state.config.write();
        w.model_aliases.remove(&name).is_some()
    };
    let _ = state.save_to_disk();
    if removed {
        (axum::http::StatusCode::NO_CONTENT, "").into_response()
    } else {
        RouterError::NotFound(format!("alias {name}")).into_response()
    }
}

async fn list_model_list(State(state): State<SharedState>) -> Response {
    let cfg = state.config.read();
    (axum::http::StatusCode::OK, Json(&cfg.model_list)).into_response()
}

async fn put_model_list(
    State(state): State<SharedState>,
    Json(body): Json<Vec<crate::config::types::ModelListEntry>>,
) -> Response {
    {
        let mut w = state.config.write();
        w.model_list = body.clone();
    }
    let _ = state.save_to_disk();
    (axum::http::StatusCode::OK, Json(body)).into_response()
}


async fn reload_config(
    State(state): State<SharedState>,
    headers: axum::http::HeaderMap,
) -> Response {
    let actor = actor_from_headers(&headers);
    match state.reload_from_disk() {
        Ok(summary) => {
            let detail = format!(r#"{{"keys":{},"upstreams":{},"status":"{}"}}"#,
                summary.get("keys").and_then(|v| v.as_u64()).unwrap_or(0),
                summary.get("upstreams").and_then(|v| v.as_u64()).unwrap_or(0),
                summary.get("status").and_then(|v| v.as_str()).unwrap_or("reloaded"),
            );
            let _ = state.storage.append_audit("config.reload", &actor, &detail);
            (axum::http::StatusCode::OK, Json(summary)).into_response()
        }
        Err(e) => {
            let detail = format!(r#"{{"error":"{}"}}"#, e.to_string().replace('"', "'"));
            let _ = state.storage.append_audit("config.reload", &actor, &detail);
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                   Json(serde_json::json!({"status": "error", "error": format!("{e}")}))).into_response()
        }
    }
}


/// GET `/v1/admin/usage?group_by=alias|upstream|model|all&since=<unix>&until=<unix>`
async fn admin_usage(
    State(state): State<SharedState>,
    axum::extract::Query(q): axum::extract::Query<UsageQuery>,
) -> Response {
    let group_by = q.group_by.as_deref().unwrap_or("all");
    let since = q.since;
    let until = q.until;
    let limit = q.limit.unwrap_or(50).min(500);
    let buckets = match state.storage.usage_summary(group_by, since, until) {
        Ok(b) => b,
        Err(e) => return (axum::http::StatusCode::BAD_REQUEST, format!("{e}")).into_response(),
    };
    let totals = UsageBucket {
        key: "_total".to_string(),
        requests: buckets.iter().map(|b| b.requests).sum(),
        input_tokens: buckets.iter().map(|b| b.input_tokens).sum(),
        output_tokens: buckets.iter().map(|b| b.output_tokens).sum(),
        cost_usd: (buckets.iter().map(|b| b.cost_usd).sum::<f64>() * 1_000_000.0).round() / 1_000_000.0,
    };
    let top: Vec<UsageBucket> = buckets.into_iter().take(limit as usize).collect();
    (axum::http::StatusCode::OK, Json(serde_json::json!({
        "group_by": group_by,
        "since": since,
        "until": until,
        "totals": totals,
        "buckets": top,
    }))).into_response()
}

#[derive(serde::Deserialize)]
pub struct UsageQuery {
    #[serde(default)]
    pub group_by: Option<String>,
    #[serde(default)]
    pub since: Option<i64>,
    #[serde(default)]
    pub until: Option<i64>,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// GET /v1/admin/usage/recent?limit=50
async fn admin_usage_recent(
    State(state): State<SharedState>,
    axum::extract::Query(q): axum::extract::Query<RecentQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).min(1000);
    let events = match state.storage.last_events(limit) {
        Ok(e) => e,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    };
    let count = events.len();
    (axum::http::StatusCode::OK, Json(serde_json::json!({
        "count": count,
        "events": events,
    }))).into_response()
}

#[derive(serde::Deserialize)]
pub struct RecentQuery {
    #[serde(default)]
    pub limit: Option<u32>,
}

/// Background health checker.
pub fn spawn_health_checker(state: SharedState) {
    let interval = state.config.read().queue.healthcheck_interval_ms;
    if interval == 0 {
        return;
    }
    let timeout = Duration::from_millis(state.config.read().queue.healthcheck_timeout_ms);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(interval));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            for u in state.registry.all() {
                if !u.enabled() {
                    continue;
                }
                let cfg = u.cfg.read().clone();
                let url = match cfg.kind {
                    ProviderKind::Openai => format!("{}/models", cfg.base_url.trim_end_matches('/')),
                    ProviderKind::Anthropic => format!("{}/v1/messages", cfg.base_url.trim_end_matches('/')),
                };
                let mut req = state.http.request(reqwest::Method::GET, &url).timeout(timeout);
                req = match cfg.kind {
                    ProviderKind::Openai => req.header("Authorization", format!("Bearer {}", cfg.api_key)),
                    ProviderKind::Anthropic => req.header("x-api-key", cfg.api_key.clone()),
                };
                let resp = req.send().await;
                match resp {
                    Ok(r) if r.status().is_success() || r.status().as_u16() == 405 || r.status().as_u16() == 404 => {
                        u.record_success();
                    }
                    Ok(r) => {
                        let _ = r.bytes().await;
                        u.record_failure();
                    }
                    Err(_) => {
                        u.record_failure();
                    }
                }
            }
            state.queue.notify_change();
        }
    });
}


/// POST /v1/admin/usage/retention  body: { "days": 30 }   (days=0 disables retention)
async fn set_retention(
    State(state): State<SharedState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<RetentionBody>,
) -> Response {
    {
        let mut w = state.config.write();
        w.server.usage_retention_days = body.days;
    }
    let _ = state.save_to_disk();
    let actor = actor_from_headers(&headers);
    let detail = format!(r#"{{"days":{}}}"#, body.days);
    let _ = state.storage.append_audit("retention.set", &actor, &detail);
    (axum::http::StatusCode::OK, Json(serde_json::json!({
        "status": "ok",
        "usage_retention_days": body.days,
    }))).into_response()
}

#[derive(serde::Deserialize)]
pub struct RetentionBody {
    pub days: u32,
}



/// GET /v1/admin/metrics  — JSON snapshot of counters, histograms, gauges.
async fn admin_metrics_json(State(state): State<SharedState>) -> Response {
    let snap = state.metrics.snapshot_json();
    (axum::http::StatusCode::OK, Json(snap)).into_response()
}

/// GET /v1/admin/metrics/prom  — Prometheus text format, scrape-compatible.
async fn admin_metrics_prom(State(state): State<SharedState>) -> Response {
    let text = state.metrics.prometheus_text();
    (axum::http::StatusCode::OK, [("content-type", "text/plain; version=0.0.4")], text).into_response()
}


/// GET /v1/admin/rates — sliding-window RPS, TPS, cost/sec over 1m/5m/1h.
async fn admin_rates(State(state): State<SharedState>) -> Response {
    (axum::http::StatusCode::OK, Json(state.metrics.rates.read().snapshot())).into_response()
}


/// GET /v1/admin/traces/recent?limit=N — OTLP-shaped JSON of the most recent spans.
async fn admin_traces_recent(
    State(state): State<SharedState>,
    axum::extract::Query(q): axum::extract::Query<TracesQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).min(1000);
    let spans = state.metrics.traces.snapshot(limit as usize);
    (axum::http::StatusCode::OK, Json(serde_json::json!({
        "resourceSpans": [{
            "resource": { "attributes": [{"key": "service.name", "value": "polyglotmesh"}] },
            "scopeSpans": [{
                "scope": { "name": "polyglotmesh.proxy" },
                "spans": spans,
            }],
        }],
    }))).into_response()
}

#[derive(serde::Deserialize)]
pub struct TracesQuery { #[serde(default)] pub limit: Option<u32> }


/// GET /v1/admin/events/stream  — Server-Sent Events stream of every completed
/// request. The dashboard uses this for the live-tail view.
async fn admin_events_stream(State(state): State<SharedState>) -> Response {
    use axum::body::Body;

    let mut rx = state.metrics.events.subscribe();
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Some(ev) => {
                    let line = format!("data: {}\n\n", ev);
                    yield Ok::<_, std::io::Error>(line.into_bytes());
                }
                None => break,
            }
        }
    };
    let body = Body::from_stream(stream);
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(body)
        .unwrap()
}

/// Background task: every 10s, snapshot all counters + histogram buckets and
/// persist them to `metric_samples` so they survive a restart. Gauges are
/// intentionally NOT persisted (transient by definition).
pub fn spawn_metrics_persister(state: SharedState) {
    tokio::spawn(async move {
        let initial = std::time::Duration::from_secs(5);
        let interval = std::time::Duration::from_secs(10);
        tokio::time::sleep(initial).await;
        // The first snapshot after startup intentionally skips histogram
        // bucket rows. Reason: the restore path wrote cumulative bucket
        // values into SQLite, but the in-memory per-slot counters start at 0
        // at boot. Without this guard, the first persist would UPSERT 0
        // into the bucket rows, wiping the restored values. Counters +
        // sum + count are unaffected.
        let mut first = true;
        loop {
            let samples = if first {
                state.metrics.snapshot_for_persist_counters_only()
            } else {
                state.metrics.snapshot_for_persist()
            };
            if let Err(e) = state.storage.save_metric_samples(&samples) {
                tracing::debug!("metrics persist failed: {e}");
            }
            first = false;
            tokio::time::sleep(interval).await;
        }
    });
}

/// Background task: every 60s, check all keys' `budget_duration`. If the current
/// window has expired, reset `total_spend_usd` to 0 in memory AND in SQLite.
///
/// This complements the in-request check in `auth::authorize` (which only fires
/// on the next request after expiry) — a key with zero traffic still gets its
/// budget cleared at the right time.
pub fn spawn_budget_reset_task(state: SharedState) {
    tokio::spawn(async move {
        let initial = std::time::Duration::from_secs(15);
        let interval = std::time::Duration::from_secs(60);
        tokio::time::sleep(initial).await;
        loop {
            let now = chrono::Utc::now();
            // Snapshot the keys to avoid holding the auth lock across awaits.
            let keys: Vec<Arc<crate::auth::KeyRecord>> = state.auth.all_keys();
            for rec in keys {
                if rec.max_budget.is_none() { continue; }
                let dur = match crate::auth::parse_duration_to_chrono(rec.budget_duration.as_deref()) {
                    Some(d) => d,
                    None => continue,
                };
                let reset = {
                    let mut start = rec.budget_window_start.lock();
                    if now - *start > dur {
                        *start = now;
                        true
                    } else {
                        false
                    }
                };
                if reset {
                    use std::sync::atomic::Ordering;
                    rec.usage.total_spend_usd.store(0, Ordering::Relaxed);
                    if let Err(e) = state.storage.reset_spend(&rec.alias) {
                        tracing::debug!("budget reset persist failed for {}: {e}", rec.alias);
                    } else {
                        tracing::info!(alias = %rec.alias, "budget window expired; spend reset to 0");
                    }
                }
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// Background task: once per day, delete `usage_events` rows older than
/// `server.usage_retention_days`. No-op when retention = 0.
pub fn spawn_retention_task(state: SharedState) {
    tokio::spawn(async move {
        let initial = std::time::Duration::from_secs(30);
        let interval = std::time::Duration::from_secs(24 * 60 * 60);
        tokio::time::sleep(initial).await;
        loop {
            let days = state.config.read().server.usage_retention_days;
            if days > 0 {
                let cutoff = chrono::Utc::now().timestamp() - (days as i64) * 86_400;
                match state.storage.delete_events_older_than(cutoff) {
                    Ok(n) if n > 0 => tracing::info!(rows = n, days, "pruned usage_events older than retention window"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!("retention prune failed: {e}"),
                }
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// Background task: watch `$POLYGLOTMESH_HOME/config.toml` for changes using
/// the OS-level inotify/FSEvents API (via the `notify` crate) and trigger
/// `AppState::reload_from_disk()`. Falls back to a 2s stat() poll if the
/// platform watcher fails to install (e.g. inside a restrictive sandbox).
pub fn spawn_config_watcher(state: SharedState) {
    let paths = crate::config::RouterPaths::discover();
    let path = paths.config_file.clone();
    tokio::spawn(async move {
        // Best-effort: use inotify via notify. If creation fails, fall back to polling.
        let watcher_result = (|| -> Result<notify::RecommendedWatcher, notify::Error> {
            use notify::Watcher;
            let parent = path.parent().unwrap_or(std::path::Path::new("."));
            // We only watch the parent directory; any event there is treated as
            // a potential config change and re-checks mtime before reloading.
            let mut w = notify::recommended_watcher(|res: notify::Result<notify::Event>| {
                if let Ok(ev) = res {
                    if matches!(ev.kind, notify::EventKind::Modify(_) | notify::EventKind::Create(_) | notify::EventKind::Any) {
                        if let Some(tx) = WATCHER_TX.get() {
                            let _ = tx.send(());
                        }
                    }
                }
            })?;
            w.watch(parent, notify::RecursiveMode::NonRecursive)?;
            Ok(w)
        })();

        match watcher_result {
            Ok(_w) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
                WATCHER_TX.set(tx).ok();
                // Keep the watcher alive for the lifetime of the task.
                let _keep_alive = _w;
                tracing::info!(file = %path.display(), "config watcher active (inotify/FSEvents)");
                // Debounce: many editors emit several events per save.
                loop {
                    if rx.recv().await.is_none() { return; }
                    // Drain duplicates.
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    while rx.try_recv().is_ok() {}
                    tracing::info!(file = %path.display(), "config.toml changed; auto-reloading");
                    match state.reload_from_disk() {
                        Ok(summary) => tracing::info!(?summary, "auto-reload complete"),
                        Err(e) => tracing::warn!("auto-reload failed: {e}"),
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "inotify watcher unavailable; falling back to 2s stat() poll");
                let mut last_mtime: Option<std::time::SystemTime> = None;
                loop {
                    let cur = std::fs::metadata(&path).ok().and_then(|m| m.modified().ok());
                    if cur.is_some() && cur != last_mtime {
                        if last_mtime.is_some() {
                            tracing::info!(file = %path.display(), "config.toml changed; auto-reloading");
                            match state.reload_from_disk() {
                                Ok(summary) => tracing::info!(?summary, "auto-reload complete"),
                                Err(e) => tracing::warn!("auto-reload failed: {e}"),
                            }
                        }
                        last_mtime = cur;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    });
}

use std::sync::OnceLock;
static WATCHER_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<()>> = OnceLock::new();




/// POST /v1/admin/metrics/reset
/// Zero every in-memory counter, histogram, gauge, rate ring, and the trace ring.
/// The SQLite `metric_samples` table (long-term totals) is intentionally NOT
/// touched here — the next persist cycle will overwrite it with the new
/// (zero) in-memory values, completing the reset.
async fn admin_metrics_reset(
    State(state): State<SharedState>,
    headers: axum::http::HeaderMap,
) -> Response {
    let actor = actor_from_headers(&headers);
    state.metrics.reset();
    // Best-effort audit log; don't fail the request if the DB is read-only.
    let _ = state.storage.append_audit("metrics.reset", &actor, "");
    (axum::http::StatusCode::OK, Json(serde_json::json!({
        "ok": true,
        "reset_at": chrono::Utc::now().timestamp(),
        "actor": actor,
    }))).into_response()
}


#[derive(serde::Deserialize)]
pub struct AuditQuery { #[serde(default)] pub limit: Option<u32> }

/// GET /v1/admin/audit?limit=N
/// Newest-first list of audit events (key create/revoke, config reload,
/// retention change, metrics reset, etc). `limit` defaults to 100, max 1000.
async fn admin_audit(
    State(state): State<SharedState>,
    axum::extract::Query(q): axum::extract::Query<AuditQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(100);
    match state.storage.recent_audit(limit) {
        Ok(events) => (axum::http::StatusCode::OK, Json(serde_json::json!({
            "events": events,
            "count": events.len(),
        }))).into_response(),
        Err(e) => crate::error::RouterError::Internal(e.to_string()).into_response(),
    }
}
