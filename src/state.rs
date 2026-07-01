use crate::auth::{AuthStore, KeyRecord};
use crate::config::types::{Config, ModelAliasEntry, ProviderKind};
use crate::error::RouterResult;
use crate::metrics::Metrics;
use crate::queue::QueueManager;
use crate::storage::Storage;
use crate::upstream::UpstreamRegistry;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<parking_lot::RwLock<Config>>,
    pub registry: Arc<UpstreamRegistry>,
    pub queue: Arc<QueueManager>,
    pub auth: Arc<AuthStore>,
    pub storage: Arc<Storage>,
    pub http: reqwest::Client,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    pub fn from_config(cfg: Config) -> Self {
        let paths = crate::config::RouterPaths::discover();
        let storage = Arc::new(
            Storage::open(paths.state_file.with_extension("db")).unwrap_or_else(|e| {
                tracing::warn!("storage open failed: {e}; falling back to in-memory");
                Storage::open_in_memory().expect("in-memory storage")
            }),
        );

        let queue = Arc::new(QueueManager::new(cfg.queue.clone()));
        let registry = Arc::new(UpstreamRegistry::rebuild(cfg.upstreams.clone()));
        let auth = Arc::new(AuthStore::new());

        for k in &cfg.api_keys {
            if let Some(raw) = &k.key {
                let _ = storage.upsert_api_key(raw, k);
            }
            auth.add(k.clone());
        }
        for k in &cfg.api_keys_legacy {
            let _ = storage.upsert_api_key_legacy(k, "api");
            auth.add_api_key(k);
        }
        if let Some(t) = &cfg.server.admin_token {
            let _ = storage.upsert_api_key_legacy(t, "admin");
            auth.add_admin_key(t);
        }

        // Restore counters from SQLite.
        for rec in auth.all_keys() {
            if let Ok(row) = storage.get_key_usage(&rec.alias) {
                restore_usage(&rec, &row);
            }
        }

        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(32)
            .build()
            .expect("build reqwest client");

        let metrics = Metrics::new();
        // Rehydrate persisted counter + histogram values from SQLite.
        // Block briefly on the storage lock to read persisted samples.
        // Acceptable at startup: it is the only time we serialize on the DB.
        let rows: Vec<crate::storage::MetricSampleRow> = storage
            .with_conn(|c| crate::storage::load_metric_samples(c))
            .unwrap_or_default();
        let _ = metrics_restore(&metrics, &rows);

        Self {
            config: Arc::new(parking_lot::RwLock::new(cfg)),
            registry,
            queue,
            auth,
            storage,
            http,
            metrics,
        }
    }

    pub fn save_to_disk(&self) -> RouterResult<()> {
        let cfg = self.config.read().clone();
        let paths = crate::config::RouterPaths::discover();
        crate::config::save_to_path(&paths.config_file, &cfg)
    }

    pub fn resolve_alias(&self, model: &str, kind: ProviderKind) -> Option<String> {
        let cfg = self.config.read();
        for entry in &cfg.model_list {
            if entry.model_name == model {
                if let Some(u) = self.registry.get(&entry.upstream_id) {
                    if u.kind() == kind && u.is_usable() {
                        return Some(entry.upstream_model.clone().unwrap_or(model.to_string()));
                    }
                }
            }
        }
        if let Some(entries) = cfg.model_aliases.get(model) {
            for entry in entries {
                if let Some(u) = self.registry.get(&entry.upstream_id) {
                    if u.kind() == kind && u.is_usable() {
                        return Some(entry.upstream_model.clone());
                    }
                }
            }
        }
        let _ = ModelAliasEntry {
            upstream_id: String::new(),
            upstream_model: String::new(),
        };
        None
    }

    /// Reload config from disk: rebuild registry, refresh auth store, restore counters.
    pub fn reload_from_disk(&self) -> RouterResult<serde_json::Value> {
        let paths = crate::config::RouterPaths::discover();
        let cfg = crate::config::load_from_path(&paths.config_file)?;

        // Rebuild registry in place.
        let existing_ids: Vec<String> = self.registry.all().iter().map(|u| u.id()).collect();
        for id in existing_ids {
            self.registry.remove(&id);
        }
        for u in cfg.upstreams.clone() {
            self.registry.upsert(u);
        }

        // Build a new auth store from the new config and atomically replace.
        let new_auth = AuthStore::new();
        for k in &cfg.api_keys {
            new_auth.add(k.clone());
        }
        for k in &cfg.api_keys_legacy {
            new_auth.add_api_key(k);
        }
        if let Some(t) = &cfg.server.admin_token {
            new_auth.add_admin_key(t);
        }
        self.auth.replace_all(new_auth);

        // Restore counters from SQLite for every (now re-loaded) key.
        for rec in self.auth.all_keys() {
            if let Ok(row) = self.storage.get_key_usage(&rec.alias) {
                restore_usage(&rec, &row);
            }
        }

        *self.config.write() = cfg.clone();

        Ok(serde_json::json!({
            "status": "reloaded",
            "upstreams": self.registry.all().len(),
            "keys": self.auth.all_keys().len(),
        }))
    }
}

fn restore_usage(rec: &Arc<KeyRecord>, row: &crate::storage::KeyUsageRow) {
    use std::sync::atomic::Ordering;
    rec.usage
        .total_requests
        .store(row.total_requests, Ordering::Relaxed);
    rec.usage
        .total_input_tokens
        .store(row.total_input_tokens, Ordering::Relaxed);
    rec.usage
        .total_output_tokens
        .store(row.total_output_tokens, Ordering::Relaxed);
    let micros = (row.total_spend_usd * 1_000_000.0) as u64;
    rec.usage.total_spend_usd.store(micros, Ordering::Relaxed);
    rec.usage.in_flight.store(row.in_flight, Ordering::Relaxed);
    rec.usage
        .rpm_window_count
        .store(row.rpm_window_count, Ordering::Relaxed);
    rec.usage
        .tpm_window_tokens
        .store(row.tpm_window_tokens, Ordering::Relaxed);
    {
        let mut s = rec.usage.rpm_window_start.lock();
        let ts = chrono::DateTime::<chrono::Utc>::from_timestamp(row.rpm_window_start, 0)
            .unwrap_or_else(chrono::Utc::now);
        *s = ts;
    }
    {
        let mut s = rec.usage.tpm_window_start.lock();
        let ts = chrono::DateTime::<chrono::Utc>::from_timestamp(row.tpm_window_start, 0)
            .unwrap_or_else(chrono::Utc::now);
        *s = ts;
    }
}

// ---- Convenience pass-throughs used by the proxy layer ----

impl AppState {
    /// Validate the bearer token, check per-key RPM/TPM/parallel/budget limits.
    /// On RPM success, also writes the rolling window to SQLite.
    pub fn authorize(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Arc<crate::auth::KeyRecord>, crate::error::RouterError> {
        let rec = crate::auth::authorize(headers, &self.auth)?;
        // Persist the rolling RPM window so it survives restarts.
        if rec.rpm_limit > 0 {
            if let Err(e) = self.storage.update_rpm_window(&rec.alias, 60) {
                tracing::debug!("rpm window persist failed for {}: {e}", rec.alias);
            }
        }
        Ok(rec)
    }

    /// Record token usage for a key on a successful request.
    /// Updates in-memory counters AND writes to SQLite.
    pub fn record_key_usage(
        &self,
        rec: &crate::auth::KeyRecord,
        input_tokens: u64,
        output_tokens: u64,
        cost_usd: f64,
        upstream_id: Option<&str>,
        model: Option<&str>,
        status: i32,
    ) {
        crate::auth::record_usage(rec, input_tokens, output_tokens, cost_usd);
        let event = crate::storage::UsageEvent {
            alias: rec.alias.clone(),
            upstream_id: upstream_id.map(|s| s.to_string()),
            model: model.map(|s| s.to_string()),
            input_tokens,
            output_tokens,
            cost_usd,
            status: status as u16,
        };
        if let Err(e) = self.storage.apply_counter_delta(
            &rec.alias,
            input_tokens as i64,
            output_tokens as i64,
            cost_usd,
            1,
            Some(&event),
        ) {
            tracing::debug!("counter delta persist failed for {}: {e}", rec.alias);
        }
        // Persist updated TPM window (rolling minute-bucket of total tokens).
        let total_tokens = (input_tokens + output_tokens) as u32;
        if rec.tpm_limit > 0 {
            if let Err(e) = self.storage.update_tpm_window(&rec.alias, total_tokens, 60) {
                tracing::debug!("tpm window persist failed for {}: {e}", rec.alias);
            }
        }
    }

    /// Release the in-flight slot and persist the new count.
    pub fn release_key(&self, rec: &crate::auth::KeyRecord) {
        crate::auth::release(rec);
        let cur = rec
            .usage
            .in_flight
            .load(std::sync::atomic::Ordering::Relaxed);
        if let Err(e) = self.storage.set_in_flight(&rec.alias, cur as i64) {
            tracing::debug!("in_flight persist failed for {}: {e}", rec.alias);
        }
    }
}

/// Replay persisted `metric_samples` rows back into the in-memory `Metrics`.
/// Counters restore their absolute value (overwriting); histogram bucket
/// counts are summed into the in-memory buckets.
fn metrics_restore(m: &Arc<Metrics>, rows: &[crate::storage::MetricSampleRow]) -> RouterResult<()> {
    for r in rows {
        let labels: Vec<(String, String)> = if r.labels_json.is_empty() || r.labels_json == "[]" {
            Vec::new()
        } else {
            serde_json::from_str(&r.labels_json).unwrap_or_default()
        };
        let labels_static: Vec<(&'static str, String)> = labels
            .into_iter()
            .map(|(k, v)| (Box::leak(k.into_boxed_str()) as &'static str, v))
            .collect();
        let v = r.value as u64;
        match r.name.as_str() {
            "requests_total" => m
                .requests_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "upstream_requests_total" => m
                .upstream_requests_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "success_total" => m
                .success_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "error_total" => m
                .error_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "input_tokens_total" => m
                .input_tokens_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "output_tokens_total" => m
                .output_tokens_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "cost_micros_total" => m
                .cost_micros_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "cache_read_input_tokens_total" => m
                .cache_read_input_tokens_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            "cache_write_input_tokens_total" => m
                .cache_write_input_tokens_total
                .values
                .write()
                .entry(labels_static)
                .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
                .store(v, std::sync::atomic::Ordering::Relaxed),
            // Histogram bucket counts: label encodes the le_us boundary.
            "request_duration_seconds_bucket" => {
                restore_histogram(&m.request_duration, &labels_static, v)
            }
            "upstream_duration_seconds_bucket" => {
                restore_histogram(&m.upstream_duration, &labels_static, v)
            }
            "time_to_first_token_seconds_bucket" => restore_histogram(&m.ttft, &labels_static, v),
            "stream_inter_token_seconds_bucket" => {
                restore_histogram(&m.inter_token, &labels_static, v)
            }
            n if n.starts_with("_hist_sum::") || n.starts_with("_hist_count::") => {
                let v = r.value as u64;
                let is_sum = r.name.starts_with("_hist_sum::");
                let path = if is_sum {
                    &n["_hist_sum::".len()..]
                } else {
                    &n["_hist_count::".len()..]
                };
                let ord = std::sync::atomic::Ordering::Relaxed;
                match (is_sum, path) {
                    (true, "request_duration_seconds") => m.request_duration.sum_us.store(v, ord),
                    (false, "request_duration_seconds") => m.request_duration.count.store(v, ord),
                    (true, "upstream_duration_seconds") => m.upstream_duration.sum_us.store(v, ord),
                    (false, "upstream_duration_seconds") => m.upstream_duration.count.store(v, ord),
                    (true, "time_to_first_token_seconds") => m.ttft.sum_us.store(v, ord),
                    (false, "time_to_first_token_seconds") => m.ttft.count.store(v, ord),
                    (true, "stream_inter_token_seconds") => m.inter_token.sum_us.store(v, ord),
                    (false, "stream_inter_token_seconds") => m.inter_token.count.store(v, ord),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn restore_histogram(
    h: &crate::metrics::Histogram,
    labels: &[(&'static str, String)],
    cumulative_v: u64,
) {
    // Persisted values are cumulative. Convert to per-bucket delta and store.
    if let Some((_, le_str)) = labels.first() {
        if let Ok(le) = le_str.parse::<usize>() {
            if le < h.counts.len() {
                let already: u64 = (0..le)
                    .map(|i| h.counts[i].load(std::sync::atomic::Ordering::Relaxed))
                    .sum();
                let delta = cumulative_v.saturating_sub(already);
                h.counts[le].store(delta, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}
