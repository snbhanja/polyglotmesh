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
        let cfg: Config = toml::from_str(&raw).map_err(|e| RouterError::Internal(format!("toml parse: {e}")))?;
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
        toml::to_string_pretty(cfg).map_err(|e| RouterError::Internal(format!("toml serialize: {e}")))?
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
