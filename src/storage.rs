//! SQLite-backed persistent state for `ai-llm_router`.
//!
//! The TOML config file is the human-edited source of truth for *what the
//! user wants*. SQLite is the runtime source of truth for *what has
//! happened*: per-key counters (requests, tokens, spend, in-flight, rpm/tpm
//! windows) and an append-only event log.
//!
//! Schema is created lazily on first open. All calls go through
//! `spawn_blocking` so the async runtime isn't blocked by synchronous
//! SQLite I/O. A single `Arc<Mutex<Connection>>` is shared across handlers.

use crate::config::types::{ApiKeyConfig, UpstreamConfig};
use crate::error::{RouterError, RouterResult};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS api_keys (
    alias        TEXT PRIMARY KEY,
    raw_hash     TEXT NOT NULL,
    role         TEXT NOT NULL,
    config_json  TEXT NOT NULL,
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upstreams (
    id           TEXT PRIMARY KEY,
    config_json  TEXT NOT NULL,
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS key_usage (
    alias                      TEXT PRIMARY KEY REFERENCES api_keys(alias) ON DELETE CASCADE,
    total_requests             INTEGER NOT NULL DEFAULT 0,
    total_input_tokens         INTEGER NOT NULL DEFAULT 0,
    total_output_tokens        INTEGER NOT NULL DEFAULT 0,
    total_spend_micros         INTEGER NOT NULL DEFAULT 0,
    in_flight                  INTEGER NOT NULL DEFAULT 0,
    rpm_window_start           INTEGER NOT NULL DEFAULT 0,
    rpm_window_count           INTEGER NOT NULL DEFAULT 0,
    tpm_window_start           INTEGER NOT NULL DEFAULT 0,
    tpm_window_tokens          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS usage_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    alias         TEXT NOT NULL,
    upstream_id   TEXT,
    model         TEXT,
    input_tokens  INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cost_usd      REAL    NOT NULL DEFAULT 0,
    status        INTEGER NOT NULL,
    at            INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_events_alias_at ON usage_events(alias, at);
CREATE INDEX IF NOT EXISTS idx_usage_events_at       ON usage_events(at);

-- Persisted metric samples (counters + histogram bucket counts + sum + count).
-- Gauges are intentionally NOT persisted — they are transient by definition.
-- PRIMARY KEY on (name, labels_json) so re-snapshotting an existing series UPSERTs.
CREATE TABLE IF NOT EXISTS metric_samples (
    name         TEXT NOT NULL,
    labels_json  TEXT NOT NULL,
    value        INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (name, labels_json)
);
CREATE INDEX IF NOT EXISTS idx_metric_samples_name ON metric_samples(name);

CREATE TABLE IF NOT EXISTS audit_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    action        TEXT NOT NULL,
    actor         TEXT NOT NULL,
    detail        TEXT NOT NULL,
    at            INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_events_at ON audit_events(at);
"#;

/// A single in-memory snapshot of a key's counters.
#[derive(Debug, Clone, Default)]
pub struct KeyUsageRow {
    pub total_requests: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    /// Spend in USD (f64 reconstructed from the integer micro-cents storage).
    pub total_spend_usd: f64,
    pub in_flight: u32,
    pub rpm_window_start: i64,
    pub rpm_window_count: u32,
    pub tpm_window_start: i64,
    pub tpm_window_tokens: u32,
}

/// Append-only usage event row.
#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub alias: String,
    pub upstream_id: Option<String>,
    pub model: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: f64,
    pub status: u16,
}

pub struct Storage {
    conn: Arc<Mutex<Connection>>,
    pub path: PathBuf,
}

impl Storage {
    /// Open the database at `path`, creating the schema if needed.
    pub fn open(path: impl AsRef<Path>) -> RouterResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&path).map_err(sqlite_err)?;
        // Apply schema (idempotent CREATE IF NOT EXISTS).
        conn.execute_batch(SCHEMA).map_err(sqlite_err)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)), path })
    }

    /// In-memory only (for tests).
    pub fn open_in_memory() -> RouterResult<Self> {
        let conn = Connection::open_in_memory().map_err(sqlite_err)?;
        conn.execute_batch(SCHEMA).map_err(sqlite_err)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)), path: PathBuf::from(":memory:") })
    }

    /// Upsert an API key (raw token, role, full config JSON).
    /// Insert a bare-string key (legacy `api_keys_legacy` form) as a rich row.
    pub fn upsert_api_key_legacy(&self, raw: &str, role: &str) -> RouterResult<()> {
        let cfg = ApiKeyConfig {
            key: Some(raw.to_string()),
            key_alias: Some(format!("{}…", raw.chars().take(12).collect::<String>())),
            role: role.to_string(),
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
        };
        self.upsert_api_key(raw, &cfg)
    }

    pub fn upsert_api_key(&self, raw: &str, cfg: &ApiKeyConfig) -> RouterResult<()> {
        let alias = cfg.key_alias.clone().unwrap_or_else(|| raw.chars().take(12).collect::<String>() + "…");
        let role = cfg.role.clone();
        let config_json = serde_json::to_string(cfg)?;
        let raw_hash = crate::auth::hash(raw);
        let now = now_unix();
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO api_keys (alias, raw_hash, role, config_json, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(alias) DO UPDATE SET \
               raw_hash=excluded.raw_hash, \
               role=excluded.role, \
               config_json=excluded.config_json",
            params![alias, raw_hash, role, config_json, now],
        )
        .map_err(sqlite_err)?;
        // Make sure a key_usage row exists.
        conn.execute(
            "INSERT OR IGNORE INTO key_usage (alias) VALUES (?1)",
            params![alias],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    pub fn delete_api_key_by_alias(&self, alias: &str) -> RouterResult<bool> {
        let conn = self.conn.lock();
        let n = conn
            .execute("DELETE FROM api_keys WHERE alias = ?1", params![alias])
            .map_err(sqlite_err)?;
        Ok(n > 0)
    }

    pub fn delete_api_key_by_raw(&self, raw: &str) -> RouterResult<bool> {
        let h = crate::auth::hash(raw);
        let conn = self.conn.lock();
        let n = conn
            .execute("DELETE FROM api_keys WHERE raw_hash = ?1", params![h])
            .map_err(sqlite_err)?;
        Ok(n > 0)
    }

    pub fn all_api_keys(&self) -> RouterResult<Vec<(String, String, ApiKeyConfig)>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT raw_hash, role, config_json FROM api_keys")
            .map_err(sqlite_err)?;
        let mut out = Vec::new();
        let rows = stmt
            .query_map([], |row| {
                let raw_hash: String = row.get(0)?;
                let role: String = row.get(1)?;
                let cfg_json: String = row.get(2)?;
                Ok((raw_hash, role, cfg_json))
            })
            .map_err(sqlite_err)?;
        for r in rows {
            let (raw_hash, role, cfg_json) = r.map_err(sqlite_err)?;
            let cfg: ApiKeyConfig = serde_json::from_str(&cfg_json).unwrap_or(ApiKeyConfig {
                key: None,
                key_alias: None,
                role: role.clone(),
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
            });
            out.push((raw_hash, role, cfg));
        }
        Ok(out)
    }

    pub fn upsert_upstream(&self, cfg: &UpstreamConfig) -> RouterResult<()> {
        let config_json = serde_json::to_string(cfg)?;
        let now = now_unix();
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO upstreams (id, config_json, created_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(id) DO UPDATE SET config_json=excluded.config_json",
            params![cfg.id, config_json, now],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    pub fn delete_upstream(&self, id: &str) -> RouterResult<bool> {
        let conn = self.conn.lock();
        let n = conn
            .execute("DELETE FROM upstreams WHERE id = ?1", params![id])
            .map_err(sqlite_err)?;
        Ok(n > 0)
    }

    pub fn all_upstreams(&self) -> RouterResult<Vec<UpstreamConfig>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT config_json FROM upstreams")
            .map_err(sqlite_err)?;
        let mut out = Vec::new();
        let rows = stmt
            .query_map([], |row| {
                let cfg_json: String = row.get(0)?;
                Ok(cfg_json)
            })
            .map_err(sqlite_err)?;
        for r in rows {
            let cfg_json = r.map_err(sqlite_err)?;
            if let Ok(cfg) = serde_json::from_str::<UpstreamConfig>(&cfg_json) {
                out.push(cfg);
            }
        }
        Ok(out)
    }

    pub fn get_key_usage(&self, alias: &str) -> RouterResult<KeyUsageRow> {
        let conn = self.conn.lock();
        let row = conn
            .query_row(
                "SELECT total_requests, total_input_tokens, total_output_tokens, \
                        total_spend_micros, in_flight, rpm_window_start, rpm_window_count, \
                        tpm_window_start, tpm_window_tokens \
                 FROM key_usage WHERE alias = ?1",
                params![alias],
                |r| {
                    let total_spend_micros: i64 = r.get(3)?;
                    Ok(KeyUsageRow {
                        total_requests: r.get::<_, i64>(0)? as u64,
                        total_input_tokens: r.get::<_, i64>(1)? as u64,
                        total_output_tokens: r.get::<_, i64>(2)? as u64,
                        total_spend_usd: (total_spend_micros as f64) / 1_000_000.0,
                        in_flight: r.get::<_, i64>(4)? as u32,
                        rpm_window_start: r.get(5)?,
                        rpm_window_count: r.get::<_, i64>(6)? as u32,
                        tpm_window_start: r.get(7)?,
                        tpm_window_tokens: r.get::<_, i64>(8)? as u32,
                    })
                },
            )
            .optional()
            .map_err(sqlite_err)?;
        Ok(row.unwrap_or_default())
    }

    /// Atomically apply a counter delta to a key. Also inserts an event log row.
    pub fn apply_counter_delta(
        &self,
        alias: &str,
        delta_in: i64,
        delta_out: i64,
        delta_spend_usd: f64,
        delta_requests: i64,
        event: Option<&UsageEvent>,
    ) -> RouterResult<()> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        let micros = (delta_spend_usd * 1_000_000.0) as i64;
        tx.execute(
            "INSERT INTO key_usage (alias) VALUES (?1) \
             ON CONFLICT(alias) DO NOTHING",
            params![alias],
        )
        .map_err(sqlite_err)?;
        tx.execute(
            "UPDATE key_usage SET \
                total_requests     = total_requests     + ?2, \
                total_input_tokens = total_input_tokens + ?3, \
                total_output_tokens= total_output_tokens+ ?4, \
                total_spend_micros = total_spend_micros + ?5 \
             WHERE alias = ?1",
            params![alias, delta_requests, delta_in, delta_out, micros],
        )
        .map_err(sqlite_err)?;
        if let Some(e) = event {
            tx.execute(
                "INSERT INTO usage_events (alias, upstream_id, model, input_tokens, output_tokens, cost_usd, status, at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    e.alias,
                    e.upstream_id,
                    e.model,
                    e.input_tokens as i64,
                    e.output_tokens as i64,
                    e.cost_usd,
                    e.status as i64,
                    now_unix()
                ],
            )
            .map_err(sqlite_err)?;
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(())
    }

    /// Update rolling rpm window for a key. Returns the post-update window
    /// start and the current count.
    pub fn update_rpm_window(&self, alias: &str, window_seconds: i64) -> RouterResult<(i64, u32)> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        tx.execute("INSERT OR IGNORE INTO key_usage (alias) VALUES (?1)", params![alias])
            .map_err(sqlite_err)?;
        let now = now_unix();
        // Read current
        let (start, count): (i64, i64) = tx
            .query_row(
                "SELECT rpm_window_start, rpm_window_count FROM key_usage WHERE alias = ?1",
                params![alias],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sqlite_err)?;
        let (new_start, new_count) = if now - start >= window_seconds {
            (now, 1i64)
        } else {
            (start, count + 1)
        };
        tx.execute(
            "UPDATE key_usage SET rpm_window_start = ?2, rpm_window_count = ?3 WHERE alias = ?1",
            params![alias, new_start, new_count],
        )
        .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok((new_start, new_count as u32))
    }

    pub fn update_tpm_window(&self, alias: &str, delta_tokens: u32, window_seconds: i64) -> RouterResult<(i64, u32)> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        tx.execute("INSERT OR IGNORE INTO key_usage (alias) VALUES (?1)", params![alias])
            .map_err(sqlite_err)?;
        let now = now_unix();
        let (start, count): (i64, i64) = tx
            .query_row(
                "SELECT tpm_window_start, tpm_window_tokens FROM key_usage WHERE alias = ?1",
                params![alias],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sqlite_err)?;
        let (new_start, new_count) = if now - start >= window_seconds {
            (now, delta_tokens as i64)
        } else {
            (start, count + delta_tokens as i64)
        };
        tx.execute(
            "UPDATE key_usage SET tpm_window_start = ?2, tpm_window_tokens = ?3 WHERE alias = ?1",
            params![alias, new_start, new_count],
        )
        .map_err(sqlite_err)?;
        tx.commit().map_err(sqlite_err)?;
        Ok((new_start, new_count as u32))
    }

    pub fn set_in_flight(&self, alias: &str, in_flight: i64) -> RouterResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO key_usage (alias, in_flight) VALUES (?1, ?2) \
             ON CONFLICT(alias) DO UPDATE SET in_flight = excluded.in_flight",
            params![alias, in_flight],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    pub fn get_budget_window(&self, alias: &str) -> RouterResult<Option<i64>> {
        let conn = self.conn.lock();
        // We use the first key row that matches the alias and read spend directly.
        let spend: Option<i64> = conn
            .query_row(
                "SELECT total_spend_micros FROM key_usage WHERE alias = ?1",
                params![alias],
                |r| r.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        Ok(spend)
    }

    /// Delete `usage_events` rows older than `cutoff_unix`. Returns the number of rows deleted.
    pub fn delete_events_older_than(&self, cutoff_unix: i64) -> RouterResult<u64> {
        let conn = self.conn.lock();
        let n = conn
            .execute("DELETE FROM usage_events WHERE at < ?1", params![cutoff_unix])
            .map_err(sqlite_err)?;
        // VACUUM-lite: leave the freed pages to the next checkpoint, but run a manual
        // WAL checkpoint so disk usage doesn't grow without bound.
        let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
        Ok(n as u64)
    }

    /// Count rows in `usage_events` (for diagnostics / admin endpoint).
    pub fn count_events(&self) -> RouterResult<u64> {
        let conn = self.conn.lock();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_events", [], |r| r.get(0))
            .map_err(sqlite_err)?;
        Ok(n as u64)
    }

    pub fn reset_spend(&self, alias: &str) -> RouterResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE key_usage SET total_spend_micros = 0 WHERE alias = ?1",
            params![alias],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    pub fn last_events(&self, limit: u32) -> RouterResult<Vec<UsageEventWithTime>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT alias, upstream_id, model, input_tokens, output_tokens, cost_usd, status, at \
                 FROM usage_events ORDER BY id DESC LIMIT ?1",
            )
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map(params![limit as i64], |r| {
                Ok(UsageEventWithTime {
                    alias: r.get(0)?,
                    upstream_id: r.get(1)?,
                    model: r.get(2)?,
                    input_tokens: r.get::<_, i64>(3)? as u64,
                    output_tokens: r.get::<_, i64>(4)? as u64,
                    cost_usd: r.get(5)?,
                    status: r.get::<_, i64>(6)? as u16,
                    at: r.get(7)?,
                })
            })
            .map_err(sqlite_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(sqlite_err)?);
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UsageEventWithTime {
    pub alias: String,
    pub upstream_id: Option<String>,
    pub model: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: f64,
    pub status: u16,
    pub at: i64,
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn sqlite_err(e: rusqlite::Error) -> RouterError {
    RouterError::Internal(format!("sqlite: {e}"))
}

// ---- Usage aggregation (read-only queries) ----

/// A single bucket of aggregated usage. `key` is the value of the chosen grouping
/// dimension (alias, upstream_id, model, or "all").
#[derive(Debug, Clone, serde::Serialize)]
pub struct UsageBucket {
    pub key: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: f64,
}

/// Group `usage_events` rows by the chosen dimension, optionally limited to a time window.
pub fn usage_aggregation(
    conn: &rusqlite::Connection,
    group_by: &str,           // "alias" | "upstream" | "model" | "all"
    since_unix: Option<i64>,  // inclusive lower bound
    until_unix: Option<i64>,  // exclusive upper bound
) -> RouterResult<Vec<UsageBucket>> {
    let col = match group_by {
        "alias" => "alias",
        "upstream" => "upstream_id",
        "model" => "model",
        "all" => "NULL",
        other => return Err(crate::error::RouterError::BadRequest(format!("invalid group_by '{other}'"))),
    };
    let mut sql = format!(
        "SELECT {col} AS k, COUNT(*) AS requests, COALESCE(SUM(input_tokens),0) AS in_t, COALESCE(SUM(output_tokens),0) AS out_t, COALESCE(SUM(cost_usd),0) AS cost \
         FROM usage_events WHERE 1=1"
    );
    let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(s) = since_unix {
        sql.push_str(" AND at >= ?");
        args.push(Box::new(s));
    }
    if let Some(u) = until_unix {
        sql.push_str(" AND at < ?");
        args.push(Box::new(u));
    }
    sql.push_str(" GROUP BY k ORDER BY SUM(cost_usd) DESC, requests DESC");
    let params_vec: Vec<&dyn rusqlite::ToSql> = args.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql).map_err(sqlite_err)?;
    let rows = stmt
        .query_map(params_vec.as_slice(), |r| {
            Ok(UsageBucket {
                key: r.get::<_, Option<String>>(0)?.unwrap_or_else(|| "(all)".to_string()),
                requests: r.get::<_, i64>(1)? as u64,
                input_tokens: r.get::<_, i64>(2)? as u64,
                output_tokens: r.get::<_, i64>(3)? as u64,
                cost_usd: r.get(4)?,
            })
        })
        .map_err(sqlite_err)?;
    let mut out = Vec::new();
    for r in rows { out.push(r.map_err(sqlite_err)?); }
    Ok(out)
}

impl Storage {
    /// Public API for `usage_aggregation` — acquires the connection lock and runs the query.
    pub fn usage_summary(
        &self,
        group_by: &str,
        since_unix: Option<i64>,
        until_unix: Option<i64>,
    ) -> RouterResult<Vec<UsageBucket>> {
        let conn = self.conn.lock();
        usage_aggregation(&conn, group_by, since_unix, until_unix)
    }
}

// ---- Persisted metric samples (counters + histogram bucket counts) ----

/// One persisted sample row. `value` is the raw counter or bucket count.
#[derive(Debug, Clone)]
pub struct MetricSampleRow {
    pub name: String,
    pub labels_json: String,
    pub value: i64,
}

/// Load every row of `metric_samples`. Used at startup to rehydrate counters
/// and histogram bucket counts into the in-memory `Metrics` registry.
pub fn load_metric_samples(conn: &rusqlite::Connection) -> RouterResult<Vec<MetricSampleRow>> {
    let mut stmt = conn
        .prepare("SELECT name, labels_json, value FROM metric_samples")
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map([], |r| {
            Ok(MetricSampleRow {
                name: r.get(0)?,
                labels_json: r.get(1)?,
                value: r.get(2)?,
            })
        })
        .map_err(sqlite_err)?;
    let mut out = Vec::new();
    for r in rows { out.push(r.map_err(sqlite_err)?); }
    Ok(out)
}

impl Storage {
    /// Replace the entire `metric_samples` table with the provided snapshot.
    /// Done in a single transaction so a partial persist is impossible.
    /// Persist metric samples using UPSERT (INSERT OR REPLACE). Counters are
    /// monotonic, so any new value overwrites the old one and a "0" that
    /// arrives after a non-zero value will also overwrite — callers should
    /// therefore never emit 0-valued rows from a live counter (the persister
    /// already filters these out).
    pub fn save_metric_samples(&self, samples: &[MetricSampleRow]) -> RouterResult<()> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction().map_err(sqlite_err)?;
        {
            let mut stmt = tx
                .prepare("INSERT OR REPLACE INTO metric_samples (name, labels_json, value, updated_at) VALUES (?1, ?2, ?3, ?4)")
                .map_err(sqlite_err)?;
            let now = now_unix();
            for s in samples {
                stmt.execute(params![s.name, s.labels_json, s.value, now])
                    .map_err(sqlite_err)?;
            }
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(())
    }
}

impl Storage {
    /// Convenience: take the lock, run a closure with the connection, return its result.
    pub fn with_conn<R, F>(&self, f: F) -> RouterResult<R>
    where F: FnOnce(&rusqlite::Connection) -> RouterResult<R> {
        let conn = self.conn.lock();
        f(&conn)
    }
}


// ---- Audit log ----

#[derive(Debug, Clone, serde::Serialize)]
pub struct AuditEvent {
    pub id: i64,
    pub action: String,
    pub actor: String,
    pub detail: String,
    pub at: i64,
}

impl Storage {
    /// Append one audit event. `action` is e.g. "key.create", "key.revoke",
    /// "config.reload", "metrics.reset", "retention.set". `actor` is the
    /// admin-token alias that triggered it. `detail` is a free-form JSON
    /// string (or empty) for context.
    pub fn append_audit(&self, action: &str, actor: &str, detail: &str) -> RouterResult<i64> {
        let conn = self.conn.lock();
        let now = now_unix();
        conn.execute(
            "INSERT INTO audit_events (action, actor, detail, at) VALUES (?1, ?2, ?3, ?4)",
            params![action, actor, detail, now],
        ).map_err(sqlite_err)?;
        Ok(conn.last_insert_rowid())
    }

    /// Most-recent audit events, newest first. `limit` is clamped to 1000.
    pub fn recent_audit(&self, limit: u32) -> RouterResult<Vec<AuditEvent>> {
        let lim = limit.min(1000) as i64;
        self.with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT id, action, actor, detail, at FROM audit_events ORDER BY id DESC LIMIT ?1"
            ).map_err(sqlite_err)?;
            let rows = stmt.query_map(params![lim], |r| {
                Ok(AuditEvent {
                    id: r.get(0)?,
                    action: r.get(1)?,
                    actor: r.get(2)?,
                    detail: r.get(3)?,
                    at: r.get(4)?,
                })
            }).map_err(sqlite_err)?;
            let mut out = Vec::new();
            for row in rows { out.push(row.map_err(sqlite_err)?); }
            Ok(out)
        })
    }
}
