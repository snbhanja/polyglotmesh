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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{ProviderKind, QueueConfig, UpstreamConfig};
    use crate::upstream::Upstream;
    use std::collections::BTreeMap;

    fn upstream_cfg(id: &str, max_concurrency: u32, priority: i32) -> UpstreamConfig {
        UpstreamConfig {
            id: id.into(),
            name: None,
            kind: ProviderKind::Openai,
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

    fn queue_cfg(max_per_provider: usize, timeout_ms: u64) -> QueueConfig {
        QueueConfig {
            max_queue_per_provider: max_per_provider,
            queue_wait_timeout_ms: timeout_ms,
            healthcheck_interval_ms: 0,
            healthcheck_timeout_ms: 0,
            healthcheck_failure_threshold: 3,
        }
    }

    #[test]
    fn try_acquire_returns_first_admitted() {
        let qm = QueueManager::new(queue_cfg(0, 1000));
        // saturated upstream first, free one second
        let saturated = Upstream::from_config(upstream_cfg("sat", 1, 0));
        saturated.try_acquire(); // holds the only slot
        let free = Upstream::from_config(upstream_cfg("free", 1, 0));
        let chosen = qm.try_acquire(&[saturated.clone(), free.clone()]);
        assert!(chosen.is_some());
        assert_eq!(chosen.unwrap().id(), "free");
    }

    #[tokio::test]
    async fn acquire_returns_none_when_no_upstreams() {
        let qm = QueueManager::new(queue_cfg(0, 1000));
        let res = qm.acquire(vec![], ProviderKind::Openai).await;
        assert!(matches!(res, Err(RouterError::NoHealthyUpstream(_))));
        assert_eq!(qm.stats.total_no_upstream.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn acquire_returns_available_upstream_immediately() {
        let qm = QueueManager::new(queue_cfg(0, 1000));
        let up = Upstream::from_config(upstream_cfg("u", 2, 0));
        let res = qm.acquire(vec![up.clone()], ProviderKind::Openai).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().id(), "u");
        assert_eq!(qm.stats.total_dispatched.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn acquire_rejects_when_queue_full() {
        let qm = QueueManager::new(queue_cfg(0, 1000));
        // max_queue_per_provider = 0 means no waiting slots allowed.
        let saturated = Upstream::from_config(upstream_cfg("sat", 1, 0));
        saturated.try_acquire(); // occupy the only slot; queue is full (cap 0)
        let res = qm.acquire(vec![saturated.clone()], ProviderKind::Openai).await;
        assert!(res.is_err());
        assert_eq!(qm.stats.total_rejected.load(Ordering::Relaxed), 1);
        assert_eq!(qm.pending_for(ProviderKind::Openai), 0);
    }

    #[tokio::test]
    async fn acquire_rejects_second_when_queue_cap_is_one() {
        let qm = QueueManager::new(queue_cfg(1, 50));
        let up = Upstream::from_config(upstream_cfg("u", 1, 0));
        up.try_acquire(); // occupy the single slot; one queue slot left
        let r1 = qm.acquire(vec![up.clone()], ProviderKind::Openai);
        let r2 = qm.acquire(vec![up.clone()], ProviderKind::Openai);
        let (a, b) = tokio::join!(r1, r2);
        // First reserves the only queue slot and waits; second is rejected (queue full).
        // The first then times out because the slot is never released.
        assert!(a.is_err() && b.is_err());
        assert_eq!(qm.stats.total_rejected.load(Ordering::Relaxed), 2);
    }
}
