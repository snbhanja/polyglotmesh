pub mod types;

use crate::error::{RouterError, RouterResult};
use serde::{Deserialize, Serialize};
use std::path::Path;
use types::Config;

pub fn load_from_path<P: AsRef<Path>>(path: P) -> RouterResult<Config> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Config::default());
    }
    let raw = std::fs::read_to_string(path)?;
    if path.extension().and_then(|e| e.to_str()) == Some("json") {
        let cfg: Config = serde_json::from_str(&raw)?;
        Ok(cfg)
    } else {
        // Default: TOML
        let cfg: Config =
            toml::from_str(&raw).map_err(|e| RouterError::Internal(format!("toml parse: {e}")))?;
        Ok(cfg)
    }
}

pub fn save_to_path<P: AsRef<Path>>(path: P, cfg: &Config) -> RouterResult<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let raw = if path.extension().and_then(|e| e.to_str()) == Some("json") {
        serde_json::to_string_pretty(cfg)?
    } else {
        toml::to_string_pretty(cfg)
            .map_err(|e| RouterError::Internal(format!("toml serialize: {e}")))?
    };
    std::fs::write(path, raw)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterPaths {
    pub config_dir: std::path::PathBuf,
    pub config_file: std::path::PathBuf,
    pub state_file: std::path::PathBuf,
}

impl RouterPaths {
    pub fn discover() -> Self {
        if let Ok(p) = std::env::var("POLYGLOTMESH_HOME") {
            let base = std::path::PathBuf::from(p);
            return Self {
                config_dir: base.clone(),
                config_file: base.join("config.toml"),
                state_file: base.join("state.json"),
            };
        }
        let home = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let base = home.join(".polyglotmesh");
        Self {
            config_dir: base.clone(),
            config_file: base.join("config.toml"),
            state_file: base.join("state.json"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{Config, ProviderKind, UpstreamConfig};

    #[test]
    fn missing_file_loads_default() {
        let cfg = load_from_path("/nonexistent/path/config.toml").unwrap();
        assert_eq!(cfg.server.bind, "0.0.0.0:8080");
        assert!(cfg.upstreams.is_empty());
    }

    #[test]
    fn toml_round_trip_preserves_upstreams() {
        let dir = std::env::temp_dir().join(format!("pgm_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");

        let mut cfg = Config::default();
        cfg.api_keys_legacy.push("pgm-abc".into());
        cfg.upstreams.push(UpstreamConfig {
            id: "oai".into(),
            name: None,
            kind: ProviderKind::Openai,
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk".into(),
            priority: 5,
            models: vec!["gpt-4o".into()],
            weight: 1,
            timeout_ms: 60_000,
            max_concurrency: 0,
            rate_limit_rpm: 0,
            rate_limit_tpm: 0,
            enabled: true,
            max_budget: None,
            budget_duration: None,
            model_info: Default::default(),
            region: None,
            tags: vec![],
            critical: false,
            circuit_breaker: None,
        });

        save_to_path(&path, &cfg).unwrap();
        let loaded = load_from_path(&path).unwrap();
        assert_eq!(loaded.api_keys_legacy, vec!["pgm-abc".to_string()]);
        assert_eq!(loaded.upstreams.len(), 1);
        assert_eq!(loaded.upstreams[0].id, "oai");
        assert_eq!(loaded.upstreams[0].kind, ProviderKind::Openai);
        assert_eq!(loaded.upstreams[0].priority, 5);
        assert_eq!(loaded.upstreams[0].models, vec!["gpt-4o".to_string()]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn json_round_trip_preserves_bind() {
        let dir = std::env::temp_dir().join(format!("pgm_test_json_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");

        let mut cfg = Config::default();
        cfg.server.bind = "127.0.0.1:9000".into();
        save_to_path(&path, &cfg).unwrap();
        let loaded = load_from_path(&path).unwrap();
        assert_eq!(loaded.server.bind, "127.0.0.1:9000");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn router_paths_honor_env_and_default() {
        std::env::set_var("POLYGLOTMESH_HOME", "/tmp/pgmhome");
        let p = RouterPaths::discover();
        assert_eq!(p.config_file, std::path::PathBuf::from("/tmp/pgmhome/config.toml"));
        assert_eq!(p.state_file, std::path::PathBuf::from("/tmp/pgmhome/state.json"));
        std::env::remove_var("POLYGLOTMESH_HOME");

        let default = RouterPaths::discover();
        assert!(default.config_file.ends_with(".polyglotmesh/config.toml"));
    }
}
