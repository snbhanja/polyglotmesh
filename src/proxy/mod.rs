use crate::auth::KeyRecord;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

pub type SharedState = Arc<AppState>;
type StreamAccum = Arc<parking_lot::Mutex<Option<(u64, u64)>>>;


/// Per-request timing record. Captures key timestamps so the dispatch path
/// can emit one set of metric observations at the end.
pub struct RequestTimer {
    pub start: std::time::Instant,
    pub upstream_start: Option<std::time::Instant>,
    pub first_byte: Option<std::time::Instant>,
    pub method: &'static str, // "openai" | "anthropic"
    pub model: Option<String>,
    pub stream: bool,
}

impl RequestTimer {
    pub fn new(method: &'static str, model: Option<String>, stream: bool) -> Self {
        Self { start: std::time::Instant::now(), upstream_start: None, first_byte: None, method, model, stream }
    }
    pub fn observe_first_byte(&mut self) {
        if self.first_byte.is_none() {
            self.first_byte = Some(std::time::Instant::now());
        }
    }
    /// Emit final metrics for this request. Safe to call from a Drop-style
    /// context (no awaits). `ok` true → success, false → error. For streams
    /// the cost/token totals come from the watcher, so this only emits
    /// counters + latency histograms.
    pub fn finalize(
        self,
        state: &SharedState,
        upstream_id: Option<&str>,
        ok: bool,
        status_label: &str,
        in_t: u64,
        out_t: u64,
        cost_usd: f64,
    ) {
        let total_us = self.start.elapsed().as_micros() as u64;
        state.metrics.request_duration.observe_us(total_us);
        if let Some(t) = self.upstream_start {
            state.metrics.upstream_duration.observe_us(t.elapsed().as_micros() as u64);
        }
        if self.stream {
            if let Some(fb) = self.first_byte {
                let ttft_us = fb.duration_since(self.start).as_micros() as u64;
                state.metrics.ttft.observe_us(ttft_us);
            }
        }
        let kind = self.method;
        let model_str = self.model.as_deref().unwrap_or("");
        // Sliding-window rates (RPS/TPS/cost over 1m, 5m, 1h)
        if ok {
            state.metrics.record_request(in_t + out_t, cost_usd);
        }
        // Publish live event for the SSE dashboard tail.
        state.metrics.events.publish(serde_json::json!({
            "at": chrono::Utc::now().timestamp(),
            "model": model_str,
            "method": kind,
            "upstream_id": upstream_id.unwrap_or(""),
            "input_tokens": in_t,
            "output_tokens": out_t,
            "cost_usd": cost_usd,
            "duration_us": total_us,
            "ok": ok,
            "status_label": status_label,
            "stream": self.stream,
        }));

        // OTLP-shaped trace span
        let end_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u128;
        let start_ns = end_ns.saturating_sub((total_us as u128) * 1000);
        let trace_id: String = (0..16).map(|j| format!("{:02x}", ((end_ns as u64).wrapping_shr((j % 8) * 8)) & 0xff)).collect();
        let span_id: String = (0..8).map(|j| format!("{:02x}", ((end_ns as u64 ^ (j as u64 * 0xdeadbeef)) & 0xff))).collect();
        let stream_flag = self.stream;
        state.metrics.record_trace(crate::metrics::TraceSpan {
            name: format!("{} {kind} {model_str}", if stream_flag { "stream" } else { "request" }),
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            parent_span_id: None,
            kind: "client".to_string(),
            start_unix_nano: start_ns,
            end_unix_nano: end_ns,
            status_code: if ok { "ok".to_string() } else { "error".to_string() },
            attributes: vec![
                ("http.method".to_string(), "POST".to_string()),
                ("http.route".to_string(), if kind == "anthropic" { "/v1/messages".to_string() } else { "/v1/chat/completions".to_string() }),
                ("model".to_string(), model_str.to_string()),
                ("upstream_id".to_string(), upstream_id.unwrap_or("").to_string()),
                ("request.duration_us".to_string(), total_us.to_string()),
                ("input.tokens".to_string(), in_t.to_string()),
                ("output.tokens".to_string(), out_t.to_string()),
                ("cost.usd".to_string(), format!("{cost_usd:.9}")),
                ("stream".to_string(), stream_flag.to_string()),
                ("status_label".to_string(), status_label.to_string()),
            ],
        });
        // Also record into the per-upstream/per-model labeled histograms.
        if let Some(uid) = upstream_id {
            let label_vec = vec![
                ("upstream_id", uid.to_string()),
                ("model", model_str.to_string()),
            ];
            state.metrics.request_duration_by_upstream.observe(label_vec.clone(), total_us);
            if let Some(t) = self.upstream_start {
                state.metrics.upstream_duration_by_upstream.observe(label_vec, t.elapsed().as_micros() as u64);
            }
        }
        // Labeled counters
        state.metrics.requests_total.inc(
            vec![("method", kind.to_string()), ("model", model_str.to_string())], 1);
        if upstream_id.is_some() {
            state.metrics.upstream_requests_total.inc(
                vec![("upstream_id", upstream_id.unwrap_or("").to_string()),
                     ("model", model_str.to_string())], 1);
        }
        if ok {
            state.metrics.success_total.inc(
                vec![("upstream_id", upstream_id.unwrap_or("").to_string()),
                     ("model", model_str.to_string())], 1);
            state.metrics.upstream_up.set(
                vec![("upstream_id", upstream_id.unwrap_or("").to_string())], 1);
            if in_t > 0 || out_t > 0 {
                state.metrics.input_tokens_total.inc(
                    vec![("upstream_id", upstream_id.unwrap_or("").to_string()),
                         ("model", model_str.to_string())], in_t);
                state.metrics.output_tokens_total.inc(
                    vec![("upstream_id", upstream_id.unwrap_or("").to_string()),
                         ("model", model_str.to_string())], out_t);
                state.metrics.cost_micros_total.inc(
                    vec![("upstream_id", upstream_id.unwrap_or("").to_string()),
                         ("model", model_str.to_string())], (cost_usd * 1_000_000.0) as u64);
            }
        } else {
            state.metrics.error_total.inc(
                vec![("upstream_id", upstream_id.unwrap_or("").to_string()),
                     ("model", model_str.to_string()),
                     ("reason", status_label.to_string())], 1);
            state.metrics.upstream_up.set(
                vec![("upstream_id", upstream_id.unwrap_or("").to_string())], 0);
        }
    }
}

type StreamUsage = (Response, Option<(u64, u64, f64)>, Option<StreamAccum>);


/// Auth middleware: validates the key and enforces per-key rpm/tpm/parallel limits.
/// The full `KeyRecord` is stashed in a request extension for downstream handlers.
pub async fn auth_middleware(
    State(state): State<SharedState>,
    mut req: Request,
    next: axum::middleware::Next,
) -> Response {
    let rec = match state.authorize(req.headers()) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    req.extensions_mut().insert(rec);
    next.run(req).await
}

fn get_key(req: &Request) -> Option<Arc<KeyRecord>> {
    req.extensions().get::<Arc<KeyRecord>>().cloned()
}

fn get_key_from_parts(parts: &axum::http::request::Parts) -> Option<Arc<KeyRecord>> {
    parts.extensions.get::<Arc<KeyRecord>>().cloned()
}

fn check_key_authorization(rec: &KeyRecord, model: Option<&str>, kind: crate::config::types::ProviderKind) -> bool {
    if rec.blocked {
        return false;
    }
    if !rec.models.is_empty() {
        if let Some(m) = model {
            if !rec.models.iter().any(|p| p == m) {
                return false;
            }
        }
    }
    if !rec.allowed_providers.is_empty() && !rec.allowed_providers.contains(&kind) {
        return false;
    }
    true
}

fn read_stream_requested(headers: &HeaderMap, body: &[u8]) -> bool {
    if let Some(accept) = headers.get("accept").and_then(|v| v.to_str().ok()) {
        if accept.contains("text/event-stream") {
            return true;
        }
    }
    if let Ok(v) = serde_json::from_slice::<Value>(body) {
        if let Some(b) = v.get("stream").and_then(|x| x.as_bool()) {
            return b;
        }
    }
    false
}

fn read_model(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(|s| s.to_string()))
}

fn rewrite_model(body: &mut Vec<u8>, model: &str) {
    if let Ok(mut v) = serde_json::from_slice::<Value>(body.as_slice()) {
        if let Some(obj) = v.as_object_mut() {
            obj.insert("model".to_string(), Value::String(model.to_string()));
        }
        if let Ok(new) = serde_json::to_vec(&v) {
            *body = new;
        }
    }
}

/// Extract input/output token counts and USD cost from an upstream JSON response body.
fn extract_usage(body: &[u8], upstream: &Arc<crate::upstream::Upstream>, model: Option<&str>) -> (u64, u64, f64) {
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return (0, 0, 0.0),
    };
    let usage = v.get("usage").cloned().unwrap_or(Value::Null);
    // OpenAI uses prompt_tokens/completion_tokens; Anthropic uses input_tokens/output_tokens.
    let input = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let output = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    // Honor an upstream-supplied cost if present, otherwise compute from the price table.
    let cost = v
        .get("_cost_usd")
        .and_then(|x| x.as_f64())
        .or_else(|| usage.get("cost").and_then(|x| x.as_f64()))
        .unwrap_or_else(|| upstream.cost_for(model, input, output).0);
    (input, output, cost)
}

fn copy_header(src: &reqwest::header::HeaderMap, dst: &mut HeaderMap, name: &'static str) {
    if let Some(v) = src.get(name) {
        if let Ok(s) = v.to_str() {
            if let Ok(hv) = axum::http::HeaderValue::from_str(s) {
                dst.insert(name, hv);
            }
        }
    }
}

fn build_response(status: StatusCode, headers: HeaderMap, body: Body) -> Response {
    let mut builder = Response::builder().status(status);
    for (k, v) in headers.iter() {
        builder = builder.header(k, v);
    }
    builder.body(body).unwrap()
}

/// Wraps an SSE byte stream so the caller can read the final usage tokens after the stream finishes.
/// `accum` starts as None and is filled in once the first `usage` JSON object is seen in a chunk.

/// For stream responses, the usage comes in late SSE chunks. We spawn a watcher task
/// that polls the accumulator (filled in by `upstream_stream_body` as bytes flow through)
/// and once it sees a usage object, computes cost and records into the storage layer.
/// On the off chance the upstream never sends usage, the watcher times out and exits silently.
pub fn spawn_stream_usage_watcher(
    state: SharedState,
    key: Arc<crate::auth::KeyRecord>,
    accum: Arc<parking_lot::Mutex<Option<(u64, u64)>>>,
    upstream: Arc<crate::upstream::Upstream>,
    model: Option<String>,
) {
    tokio::spawn(async move {
        let upstream_id = upstream.id();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(600);
        loop {
            {
                let g = accum.lock();
                if g.is_some() { break; }
            }
            if tokio::time::Instant::now() > deadline { return; }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
        let (in_t, out_t) = accum.lock().unwrap_or((0, 0));
        if in_t == 0 && out_t == 0 { return; }
        let (cost, _unknown) = upstream.cost_for(model.as_deref(), in_t, out_t);
        state.record_key_usage(&key, in_t, out_t, cost, Some(&upstream_id), model.as_deref(), 200);
    });
}

fn upstream_stream_body(
    resp: reqwest::Response,
    accum: Arc<parking_lot::Mutex<Option<(u64, u64)>>>,
    model: Option<String>,
    metrics: Arc<crate::metrics::Metrics>,
    upstream_id: String,
    stream_start: std::time::Instant,
) -> Body {
    let s = resp.bytes_stream();
    Body::from_stream(async_stream::stream! {
        tokio::pin!(s);
        let mut buf: Vec<u8> = Vec::new();
        let mut first_chunk_at: Option<std::time::Instant> = None;
        let mut last_chunk_at: Option<std::time::Instant> = None;
        while let Some(chunk_res) = s.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(e) => {
                    yield Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                    return;
                }
            };
            let now = std::time::Instant::now();
            if first_chunk_at.is_none() {
                first_chunk_at = Some(now);
                let ttft_us = now.duration_since(stream_start).as_micros() as u64;
                metrics.ttft.observe_us(ttft_us);
            } else if let Some(prev) = last_chunk_at {
                let inter_us = now.duration_since(prev).as_micros() as u64;
                metrics.inter_token.observe_us(inter_us);
            }
            last_chunk_at = Some(now);
            // Tee: try to extract usage from this chunk.
            extract_usage_from_sse(&chunk, &accum, model.as_deref());
            buf.extend_from_slice(&chunk);
            yield Ok(chunk);
        }
        // End-of-stream: try to parse the *full* accumulated body in case usage was
            // spread across multiple chunks (e.g. Anthropic puts input_tokens in
            // message_start and output_tokens in message_delta).
        extract_usage_from_sse(&buf, &accum, model.as_deref());
        let _ = upstream_id; // currently used only in dispatcher finalize()
    })
}

/// Best-effort SSE usage parser. Handles both OpenAI (`prompt_tokens`/`completion_tokens`)
/// and Anthropic (`input_tokens`/`output_tokens` from `message_delta` or a final usage object).
fn extract_usage_from_sse(
    chunk: &[u8],
    accum: &Arc<parking_lot::Mutex<Option<(u64, u64)>>>,
    model: Option<&str>,
) {
    let text = match std::str::from_utf8(chunk) {
        Ok(s) => s,
        Err(_) => return,
    };
    // Split on SSE event boundaries: "data: " lines, blank-line separated.
    let mut cur = accum.lock();
    let (mut in_t, mut out_t) = cur.unwrap_or((0, 0));
    let mut found = cur.is_some();
    for line in text.split('\n') {
        let line = line.trim();
        if line.is_empty() { continue; }
        let payload = if let Some(p) = line.strip_prefix("data: ") { p } else { continue; };
        if payload == "[DONE]" { continue; }
        let v: Value = match serde_json::from_str(payload) { Ok(v) => v, Err(_) => continue };
        // OpenAI chat.completion.chunk: { usage: { prompt_tokens, completion_tokens } } (when include_usage)
        if let Some(u) = v.get("usage") {
            if let Some(i) = u.get("prompt_tokens").and_then(|x| x.as_u64()) { in_t = in_t.max(i); found = true; }
            if let Some(o) = u.get("completion_tokens").and_then(|x| x.as_u64()) { out_t = out_t.max(o); found = true; }
        }
        // Anthropic message_delta: { usage: { output_tokens } }
        if let Some(u) = v.get("usage") {
            if let Some(i) = u.get("input_tokens").and_then(|x| x.as_u64()) { in_t = in_t.max(i); found = true; }
            if let Some(o) = u.get("output_tokens").and_then(|x| x.as_u64()) { out_t = out_t.max(o); found = true; }
        }
        // Anthropic message_start: { message: { usage: { input_tokens } } }
        if let Some(msg) = v.get("message") {
            if let Some(u) = msg.get("usage") {
                if let Some(i) = u.get("input_tokens").and_then(|x| x.as_u64()) { in_t = in_t.max(i); found = true; }
                if let Some(o) = u.get("output_tokens").and_then(|x| x.as_u64()) { out_t = out_t.max(o); found = true; }
            }
        }
    }
    if found {
        *cur = Some((in_t, out_t));
    }
    let _ = model;
}

// ---- OpenAI ----

pub async fn openai_chat_completions(
    State(state): State<SharedState>,
    req: Request,
) -> Response {
    let (parts, body) = req.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => return crate::error::RouterError::BadRequest(format!("read body: {e}")).into_response(),
    };
    let key = get_key_from_parts(&parts);
    if let Some(k) = &key {
        if !check_key_authorization(k, None, crate::config::types::ProviderKind::Openai) {
            return crate::error::RouterError::Unauthorized("key not allowed for this provider/model".into()).into_response();
        }
    }
    let requested_model = read_model(&body_bytes);
    let stream = read_stream_requested(&parts.headers, &body_bytes);
    let state2 = state.clone();
    let key2 = key.clone();
    let parts2 = parts;
    let body2 = body_bytes;
    let resp = dispatch_openai(&state2, &parts2.headers, &body2, requested_model.as_deref(), stream, key2.as_ref()).await;
    if let Some(k) = &key2 {
        if let Ok((_r, usage)) = &resp {
            if let Some((in_t, out_t, cost)) = usage {
                state.record_key_usage(
                    k,
                    *in_t,
                    *out_t,
                    *cost,
                    None,
                    requested_model.as_deref(),
                    200,
                );
            }
        }
        state.release_key(k);
    }
    match resp {
        Ok((r, _usage)) => r,
        Err(e) => e.into_response(),
    }
}

pub async fn openai_list_models(State(state): State<SharedState>) -> Response {
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for u in state.registry.by_provider(crate::config::types::ProviderKind::Openai) {
        let cfg = u.cfg.read();
        for m in &cfg.models {
            set.insert(m.clone());
        }
    }
    {
        let cfg = state.config.read();
        for k in cfg.model_list.iter().map(|e| &e.model_name) {
            set.insert(k.clone());
        }
        for k in cfg.model_aliases.keys() {
            set.insert(k.clone());
        }
    }
    let data: Vec<Value> = set
        .into_iter()
        .map(|id| {
            serde_json::json!({
                "id": id,
                "object": "model",
                "created": 0,
                "owned_by": "polyglotmesh",
            })
        })
        .collect();
    (StatusCode::OK, axum::Json(serde_json::json!({ "object": "list", "data": data }))).into_response()
}

pub async fn openai_get_model(
    State(state): State<SharedState>,
    Path(model): Path<String>,
) -> Response {
    let upstreams = state.registry.by_provider(crate::config::types::ProviderKind::Openai);
    let found = upstreams
        .iter()
        .any(|u| u.cfg.read().models.iter().any(|m| m == &model));
    if !found {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": {"message": "model not found", "type": "router_error"}})),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "id": model,
            "object": "model",
            "created": 0,
            "owned_by": "polyglotmesh",
        })),
    )
        .into_response()
}

async fn dispatch_openai(
    state: &SharedState,
    headers: &HeaderMap,
    body: &[u8],
    requested_model: Option<&str>,
    stream: bool,
    key: Option<&Arc<crate::auth::KeyRecord>>,
) -> Result<(Response, Option<(u64, u64, f64)>), crate::error::RouterError> {
    let mut body_owned = body.to_vec();
    if let Some(model) = requested_model {
        if let Some(rewritten) = state.resolve_alias(model, crate::config::types::ProviderKind::Openai) {
            rewrite_model(&mut body_owned, &rewritten);
        }
    }
    let upstreams = state.registry.by_provider(crate::config::types::ProviderKind::Openai);
    let upstreams = filter_upstreams_for_model(&upstreams, requested_model);
    if upstreams.is_empty() {
        return Err(crate::error::RouterError::NoHealthyUpstream("openai".into()));
    }
    let mut timer = RequestTimer::new("openai", requested_model.map(|s| s.to_string()), stream);
    state.metrics.active_requests.inc(vec![("method", "openai".to_string())]);
    if stream { state.metrics.active_streams.inc(vec![("method", "openai".to_string())]); }
    let upstream = state
        .queue
        .acquire(upstreams, crate::config::types::ProviderKind::Openai)
        .await?;
    timer.upstream_start = Some(std::time::Instant::now());
    let result = forward_openai(state, &upstream, headers, &body_owned, stream, requested_model).await;
    upstream.release();
    let success = result.is_ok();
    if success {
        upstream.record_success();
        if stream {
            if let (Ok((_, _, Some(accum))), Some(k)) = (&result, key) {
                spawn_stream_usage_watcher(state.clone(), k.clone(), accum.clone(), upstream.clone(), requested_model.map(|s| s.to_string()));
            }
        }
    } else {
        upstream.record_failure();
    }
    state.metrics.active_requests.dec(vec![("method", "openai".to_string())]);
    if stream { state.metrics.active_streams.dec(vec![("method", "openai".to_string())]); }
    // Pull token/cost info if available, and finalize metrics.
    let usage = result.as_ref().ok().and_then(|(_, u, _)| u.as_ref()).copied().unwrap_or((0, 0, 0.0));
    let in_t = usage.0;
    let out_t = usage.1;
    let cost = usage.2;
    let reason = if success { "ok" } else { "upstream_error" };
    timer.finalize(state, Some(&upstream.id()), success, reason, in_t, out_t, cost);
    // Collapse the 3-tuple back to 2-tuple for the handler.
    result.map(|(r, u, _)| (r, u))
}

async fn forward_openai(
    state: &SharedState,
    upstream: &Arc<crate::upstream::Upstream>,
    headers: &HeaderMap,
    body: &[u8],
    stream: bool,
    requested_model: Option<&str>,
) -> Result<StreamUsage, crate::error::RouterError> {
    let cfg = upstream.cfg.read().clone();
    let url = format!("{}/chat/completions", cfg.base_url.trim_end_matches('/'));
    let timeout = Duration::from_millis(cfg.timeout_ms);
    let mut req = state
        .http
        .request(reqwest::Method::POST, &url)
        .header("Authorization", format!("Bearer {}", cfg.api_key))
        .header("Content-Type", "application/json")
        .timeout(timeout)
        .body(body.to_vec());
    for h in ["x-request-id", "x-correlation-id", "user-agent"] {
        if let Some(v) = headers.get(h) {
            if let Ok(v) = v.to_str() {
                req = req.header(h, v);
            }
        }
    }
    let resp = req.send().await?;
    let status = resp.status();
    let mut out_headers = HeaderMap::new();
    copy_header(resp.headers(), &mut out_headers, "content-type");
    copy_header(resp.headers(), &mut out_headers, "x-request-id");
    if !status.is_success() {
        let bytes = resp.bytes().await.unwrap_or_default();
        return Err(crate::error::RouterError::UpstreamHttp {
            status: status.as_u16(),
            body: String::from_utf8_lossy(&bytes).to_string(),
        });
    }
    if stream {
        let accum: StreamAccum = Arc::new(parking_lot::Mutex::new(None));
        let body = upstream_stream_body(
            resp,
            accum.clone(),
            requested_model.map(|s| s.to_string()),
            state.metrics.clone(),
            upstream.id(),
            std::time::Instant::now(),
        );
        return Ok((build_response(status, out_headers, body), Some((0, 0, 0.0)), Some(accum)));
    }
    let bytes = resp.bytes().await?;
    let usage = extract_usage(&bytes, upstream, requested_model);
    Ok((build_response(status, out_headers, Body::from(bytes)), Some(usage), None))
}

// ---- Anthropic ----

pub async fn anthropic_messages(
    State(state): State<SharedState>,
    req: Request,
) -> Response {
    let (parts, body) = req.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => return crate::error::RouterError::BadRequest(format!("read body: {e}")).into_response(),
    };
    let key = get_key_from_parts(&parts);
    if let Some(k) = &key {
        if !check_key_authorization(k, None, crate::config::types::ProviderKind::Anthropic) {
            return crate::error::RouterError::Unauthorized("key not allowed for this provider/model".into()).into_response();
        }
    }
    let requested_model = read_model(&body_bytes);
    let stream = read_stream_requested(&parts.headers, &body_bytes);
    let resp = dispatch_anthropic(&state, &parts.headers, &body_bytes, requested_model.as_deref(), stream, key.as_ref()).await;
    if let Some(k) = &key {
        if let Ok((_r, usage)) = &resp {
            if let Some((in_t, out_t, cost)) = usage {
                state.record_key_usage(
                    k,
                    *in_t,
                    *out_t,
                    *cost,
                    None,
                    requested_model.as_deref(),
                    200,
                );
            }
        }
        state.release_key(k);
    }
    match resp {
        Ok((r, _usage)) => r,
        Err(e) => e.into_response(),
    }
}

async fn dispatch_anthropic(
    state: &SharedState,
    headers: &HeaderMap,
    body: &[u8],
    requested_model: Option<&str>,
    stream: bool,
    key: Option<&Arc<crate::auth::KeyRecord>>,
) -> Result<(Response, Option<(u64, u64, f64)>), crate::error::RouterError> {
    let mut body_owned = body.to_vec();
    if let Some(model) = requested_model {
        if let Some(rewritten) = state.resolve_alias(model, crate::config::types::ProviderKind::Anthropic) {
            rewrite_model(&mut body_owned, &rewritten);
        }
    }
    let upstreams = state.registry.by_provider(crate::config::types::ProviderKind::Anthropic);
    let upstreams = filter_upstreams_for_model(&upstreams, requested_model);
    if upstreams.is_empty() {
        return Err(crate::error::RouterError::NoHealthyUpstream("anthropic".into()));
    }
    let mut timer = RequestTimer::new("anthropic", requested_model.map(|s| s.to_string()), stream);
    state.metrics.active_requests.inc(vec![("method", "anthropic".to_string())]);
    if stream { state.metrics.active_streams.inc(vec![("method", "anthropic".to_string())]); }
    let upstream = state
        .queue
        .acquire(upstreams, crate::config::types::ProviderKind::Anthropic)
        .await?;
    timer.upstream_start = Some(std::time::Instant::now());
    let result = forward_anthropic(state, &upstream, headers, &body_owned, stream, requested_model).await;
    upstream.release();
    let success = result.is_ok();
    if success {
        upstream.record_success();
        if stream {
            if let (Ok((_, _, Some(accum))), Some(k)) = (&result, key) {
                spawn_stream_usage_watcher(state.clone(), k.clone(), accum.clone(), upstream.clone(), requested_model.map(|s| s.to_string()));
            }
        }
    } else {
        upstream.record_failure();
    }
    state.metrics.active_requests.dec(vec![("method", "anthropic".to_string())]);
    if stream { state.metrics.active_streams.dec(vec![("method", "anthropic".to_string())]); }
    let usage = result.as_ref().ok().and_then(|(_, u, _)| u.as_ref()).copied().unwrap_or((0, 0, 0.0));
    let in_t = usage.0;
    let out_t = usage.1;
    let cost = usage.2;
    let reason = if success { "ok" } else { "upstream_error" };
    timer.finalize(state, Some(&upstream.id()), success, reason, in_t, out_t, cost);
    result.map(|(r, u, _)| (r, u))
}

async fn forward_anthropic(
    state: &SharedState,
    upstream: &Arc<crate::upstream::Upstream>,
    headers: &HeaderMap,
    body: &[u8],
    stream: bool,
    requested_model: Option<&str>,
) -> Result<StreamUsage, crate::error::RouterError> {
    let cfg = upstream.cfg.read().clone();
    let url = format!("{}/v1/messages", cfg.base_url.trim_end_matches('/'));
    let timeout = Duration::from_millis(cfg.timeout_ms);
    let mut req = state
        .http
        .request(reqwest::Method::POST, &url)
        .header("x-api-key", cfg.api_key.clone())
        .header("anthropic-version", "2023-06-01")
        .header("Content-Type", "application/json")
        .timeout(timeout)
        .body(body.to_vec());
    for h in ["x-request-id", "anthropic-beta", "user-agent"] {
        if let Some(v) = headers.get(h) {
            if let Ok(v) = v.to_str() {
                req = req.header(h, v);
            }
        }
    }
    let resp = req.send().await?;
    let status = resp.status();
    let mut out_headers = HeaderMap::new();
    copy_header(resp.headers(), &mut out_headers, "content-type");
    copy_header(resp.headers(), &mut out_headers, "x-request-id");
    copy_header(resp.headers(), &mut out_headers, "request-id");
    if !status.is_success() {
        let bytes = resp.bytes().await.unwrap_or_default();
        return Err(crate::error::RouterError::UpstreamHttp {
            status: status.as_u16(),
            body: String::from_utf8_lossy(&bytes).to_string(),
        });
    }
    if stream {
        let accum: StreamAccum = Arc::new(parking_lot::Mutex::new(None));
        let body = upstream_stream_body(
            resp,
            accum.clone(),
            requested_model.map(|s| s.to_string()),
            state.metrics.clone(),
            upstream.id(),
            std::time::Instant::now(),
        );
        return Ok((build_response(status, out_headers, body), Some((0, 0, 0.0)), Some(accum)));
    }
    let bytes = resp.bytes().await?;
    let usage = extract_usage(&bytes, upstream, requested_model);
    Ok((build_response(status, out_headers, Body::from(bytes)), Some(usage), None))
}

pub async fn health(State(state): State<SharedState>) -> Response {
    let ups: Vec<Arc<crate::upstream::Upstream>> = state.registry.all();
    let keys: Vec<serde_json::Value> = state.auth.all_keys().iter().map(|k| k.snapshot()).collect();
    let body = serde_json::json!({
        "status": "ok",
        "upstreams": ups.iter().map(|u| u.snapshot()).collect::<Vec<_>>(),
        "queue": state.queue.stats.snapshot(),
        "pending": state.queue.pending_snapshot(),
        "keys": keys,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

fn filter_upstreams_for_model(
    upstreams: &[Arc<crate::upstream::Upstream>],
    model: Option<&str>,
) -> Vec<Arc<crate::upstream::Upstream>> {
    let Some(model) = model else {
        return upstreams.to_vec();
    };
    let mut matched = Vec::new();
    let mut any_filter = false;
    for u in upstreams {
        let models = &u.cfg.read().models;
        if !models.is_empty() {
            any_filter = true;
            if models.iter().any(|m| m == model) {
                matched.push(u.clone());
            }
        }
    }
    if any_filter {
        matched
    } else {
        upstreams.to_vec()
    }
}
