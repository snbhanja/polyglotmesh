use crate::config::types::{ProviderKind, QueueConfig};
use crate::error::{RouterError, RouterResult};
use crate::upstream::Upstream;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Tracks waiting requests for stats/observability.
#[derive(Default)]
pub struct QueueStats {
    pub total_waited: AtomicU64,
    pub total_rejected: AtomicU64,
    pub total_no_upstream: AtomicU64,
    pub total_dispatched: AtomicU64,
}

impl QueueStats {
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "total_waited": self.total_waited.load(Ordering::Relaxed),
            "total_rejected": self.total_rejected.load(Ordering::Relaxed),
            "total_no_upstream": self.total_no_upstream.load(Ordering::Relaxed),
            "total_dispatched": self.total_dispatched.load(Ordering::Relaxed),
        })
    }
}

pub struct QueueManager {
    pub cfg: QueueConfig,
    pub stats: QueueStats,
    notify: Notify,
    pending_waits: Mutex<HashMap<ProviderKind, u64>>,
}

impl QueueManager {
    pub fn new(cfg: QueueConfig) -> Self {
        Self {
            cfg,
            stats: QueueStats::default(),
            notify: Notify::new(),
            pending_waits: Mutex::new(HashMap::new()),
        }
    }

    pub fn notify_change(&self) {
        self.notify.notify_waiters();
    }

    pub fn pending_for(&self, kind: ProviderKind) -> u64 {
        *self.pending_waits.lock().get(&kind).unwrap_or(&0)
    }

    /// Greedy: iterate upstreams in priority order, return the first one that admits us.
    pub fn try_acquire(&self, upstreams: &[Arc<Upstream>]) -> Option<Arc<Upstream>> {
        for u in upstreams {
            if u.try_acquire() {
                return Some(u.clone());
            }
        }
        None
    }

    pub fn pending_snapshot(&self) -> serde_json::Value {
        let map = self.pending_waits.lock();
        let mut out = serde_json::Map::new();
        for (k, v) in map.iter() {
            out.insert(k.as_str().to_string(), serde_json::json!(*v));
        }
        serde_json::Value::Object(out)
    }

    /// Acquire a usable upstream, queueing if necessary.
    pub async fn acquire(
        &self,
        upstreams: Vec<Arc<Upstream>>,
        kind: ProviderKind,
    ) -> RouterResult<Arc<Upstream>> {
        if upstreams.is_empty() {
            self.stats.total_no_upstream.fetch_add(1, Ordering::Relaxed);
            return Err(RouterError::NoHealthyUpstream(kind.as_str().to_string()));
        }

        if let Some(u) = self.try_acquire(&upstreams) {
            self.stats.total_dispatched.fetch_add(1, Ordering::Relaxed);
            return Ok(u);
        }

        // Reserve a wait slot, checking capacity.
        {
            let mut map = self.pending_waits.lock();
            let cur = *map.get(&kind).unwrap_or(&0);
            if self.cfg.max_queue_per_provider > 0 && cur >= self.cfg.max_queue_per_provider as u64
            {
                self.stats.total_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(RouterError::NoHealthyUpstream(format!(
                    "{} (queue full: {} waiting)",
                    kind.as_str(),
                    cur
                )));
            }
            map.insert(kind, cur + 1);
        }
        self.stats.total_waited.fetch_add(1, Ordering::Relaxed);

        let deadline = Instant::now() + Duration::from_millis(self.cfg.queue_wait_timeout_ms);
        let result = self.wait_loop(&upstreams, kind, deadline).await;

        // Always decrement pending count.
        let mut map = self.pending_waits.lock();
        let cur = map.get(&kind).copied().unwrap_or(0).saturating_sub(1);
        map.insert(kind, cur);
        result
    }

    async fn wait_loop(
        &self,
        upstreams: &[Arc<Upstream>],
        kind: ProviderKind,
        deadline: Instant,
    ) -> RouterResult<Arc<Upstream>> {
        let mut backoff = Duration::from_millis(5);
        let max_backoff = Duration::from_millis(200);
        loop {
            if let Some(u) = self.try_acquire(upstreams) {
                self.stats.total_dispatched.fetch_add(1, Ordering::Relaxed);
                return Ok(u);
            }
            if Instant::now() >= deadline {
                self.stats.total_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(RouterError::NoHealthyUpstream(format!(
                    "{} (queue wait timeout)",
                    kind.as_str()
                )));
            }
            // Wait briefly for either a wake or the backoff timer.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(max_backoff);
            let _ = kind;
        }
    }
}
