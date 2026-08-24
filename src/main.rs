use anyhow::Context;
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

mod admin;
mod auth;
mod config;
mod error;
mod metrics;
mod proxy;
mod queue;
mod state;
mod storage;
mod upstream;

use crate::config::types::ProviderKind;
use crate::config::{load_from_path, RouterPaths};
use crate::error::RouterResult;
use crate::state::AppState;

#[derive(Parser, Debug)]
#[command(
    name = "polyglotmesh",
    version,
    about = "Fast Rust LLM router for OpenAI/Anthropic-compatible APIs"
)]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Initialize the config directory and print a fresh API key + admin token.
    Init {
        #[arg(long, default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(long)]
        no_key: bool,
    },
    /// Generate a new self-issued API key (or admin token).
    Key {
        /// "api" (default) or "admin"
        #[arg(long, default_value = "api")]
        role: String,
    },
    /// Add or update an upstream provider.
    UpstreamAdd {
        #[arg(long)]
        id: String,
        #[arg(long, value_parser = ["openai", "anthropic"])]
        kind: String,
        #[arg(long)]
        base_url: String,
        #[arg(long)]
        api_key: String,
        #[arg(long, value_delimiter = ',')]
        models: Vec<String>,
        #[arg(long, default_value_t = 0)]
        priority: i32,
        #[arg(long, default_value_t = 0)]
        weight: u32,
        #[arg(long, default_value_t = 60_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = 0)]
        max_concurrency: u32,
        #[arg(long, default_value_t = 0)]
        rate_limit_rpm: u32,
        #[arg(long, default_value_t = 0)]
        rate_limit_tpm: u32,
        #[arg(long)]
        max_budget: Option<f64>,
        #[arg(long)]
        budget_duration: Option<String>,
        #[arg(long)]
        region: Option<String>,
        #[arg(long, value_delimiter = ',')]
        tags: Vec<String>,
        #[arg(long, default_value_t = false)]
        critical: bool,
        #[arg(long, default_value_t = 0)]
        circuit_breaker_threshold: u32,
        #[arg(long, default_value_t = 0)]
        circuit_breaker_open_s: u64,
    },
    /// Remove an upstream by id.
    UpstreamRemove {
        #[arg(long)]
        id: String,
    },
    /// List configured upstreams.
    UpstreamList,
    /// Show the router configuration.
    Show,
    /// Print the path to the active config file and exit.
    Where,
    /// Run the HTTP server.
    Serve {
        /// Override bind address.
        #[arg(long)]
        bind: Option<String>,
    },
    /// Diagnostic check: parse config, probe each upstream with a /v1/models
    /// GET (short timeout), and print a per-upstream health report.
    Doctor {
        /// Skip the live network probe of upstreams.
        #[arg(long, default_value_t = false)]
        no_probe: bool,
        /// Probe timeout in milliseconds.
        #[arg(long, default_value_t = 3000)]
        timeout_ms: u64,
    },
}

fn load_config(cli: &Cli) -> RouterResult<(PathBuf, crate::config::types::Config)> {
    let paths = RouterPaths::discover();
    let path = cli.config.clone().unwrap_or(paths.config_file.clone());
    let cfg = load_from_path(&path)?;
    Ok((path, cfg))
}

fn save_config(path: &PathBuf, cfg: &crate::config::types::Config) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    crate::config::save_to_path(path, cfg)?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    init_tracing();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let cli = Cli::parse();
    runtime.block_on(run(cli))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.cmd {
        Cmd::Init { ref bind, no_key } => cmd_init(&cli, &bind, !no_key),
        Cmd::Key { ref role } => cmd_key(&cli, &role),
        Cmd::UpstreamAdd {
            ref id,
            ref kind,
            ref base_url,
            ref api_key,
            ref models,
            ref priority,
            ref weight,
            ref timeout_ms,
            ref max_concurrency,
            ref rate_limit_rpm,
            ref rate_limit_tpm,
            ref max_budget,
            ref budget_duration,
            ref region,
            ref tags,
            ref critical,
            ref circuit_breaker_threshold,
            ref circuit_breaker_open_s,
        } => cmd_upstream_add(
            &cli,
            id.clone(),
            kind.clone(),
            base_url.clone(),
            api_key.clone(),
            models.clone(),
            *priority,
            *weight,
            *timeout_ms,
            *max_concurrency,
            *rate_limit_rpm,
            *rate_limit_tpm,
            *max_budget,
            budget_duration.clone(),
            region.clone(),
            tags.clone(),
            *critical,
            *circuit_breaker_threshold,
            *circuit_breaker_open_s,
        ),
        Cmd::UpstreamRemove { ref id } => cmd_upstream_remove(&cli, &id),
        Cmd::UpstreamList => cmd_upstream_list(&cli),
        Cmd::Show => cmd_show(&cli),
        Cmd::Where => {
            let paths = RouterPaths::discover();
            println!("config: {}", paths.config_file.display());
            Ok(())
        }
        Cmd::Serve { ref bind } => cmd_serve(&cli, bind.clone()).await,
        Cmd::Doctor {
            no_probe,
            timeout_ms,
        } => cmd_doctor(&cli, no_probe, timeout_ms).await,
    }
}

fn init_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}

fn cmd_init(cli: &Cli, bind: &str, gen_key: bool) -> anyhow::Result<()> {
    let (path, mut cfg) = load_config(cli).context("load config")?;
    if cfg.server.bind != bind {
        cfg.server.bind = bind.to_string();
    }
    let generated_key = if gen_key && cfg.api_keys.is_empty() && cfg.api_keys_legacy.is_empty() {
        let k = auth::generate_api_key();
        cfg.api_keys_legacy.push(k.clone());
        Some(k)
    } else {
        None
    };
    save_config(&path, &cfg)?;
    println!("Config written to: {}", path.display());
    println!("Bind: {}", cfg.server.bind);
    if let Some(k) = generated_key {
        println!();
        println!(
            "OpenAI-compatible base URL:    http://{}/v1",
            cfg.server.bind
        );
        println!(
            "Anthropic-compatible base URL: http://{}/v1",
            cfg.server.bind
        );
        println!();
        println!("Your self-issued API key (Bearer token): {k}");
        println!();
        println!("Start the router with: polyglotmesh serve");
        println!("To add upstreams:    polyglotmesh upstream add --help");
        println!("To edit limits, run: polyglotmesh show    (config file path above)");
    } else if gen_key {
        println!("API key already present; not generating a new one.");
    }
    Ok(())
}

fn cmd_key(cli: &Cli, role: &str) -> anyhow::Result<()> {
    let (path, mut cfg) = load_config(cli).context("load config")?;
    match role {
        "admin" => {
            let k = auth::generate_admin_key();
            cfg.server.admin_token = Some(k.clone());
            save_config(&path, &cfg)?;
            println!("Admin token: {k}");
        }
        _ => {
            let k = auth::generate_api_key();
            cfg.api_keys_legacy.push(k.clone());
            save_config(&path, &cfg)?;
            println!("API key: {k}");
        }
    }
    Ok(())
}

fn cmd_upstream_add(
    cli: &Cli,
    id: String,
    kind: String,
    base_url: String,
    api_key: String,
    models: Vec<String>,
    priority: i32,
    weight: u32,
    timeout_ms: u64,
    max_concurrency: u32,
    rate_limit_rpm: u32,
    rate_limit_tpm: u32,
    max_budget: Option<f64>,
    budget_duration: Option<String>,
    region: Option<String>,
    tags: Vec<String>,
    critical: bool,
    circuit_breaker_threshold: u32,
    circuit_breaker_open_s: u64,
) -> anyhow::Result<()> {
    let (path, mut cfg) = load_config(cli).context("load config")?;
    cfg.upstreams.retain(|u| u.id != id);
    let kind = match kind.as_str() {
        "openai" => ProviderKind::Openai,
        "anthropic" => ProviderKind::Anthropic,
        other => anyhow::bail!("unknown provider kind '{other}'"),
    };
    cfg.upstreams.push(crate::config::types::UpstreamConfig {
        id: id.clone(),
        name: None,
        kind,
        base_url,
        api_key,
        priority,
        models,
        weight,
        timeout_ms,
        max_concurrency,
        rate_limit_rpm,
        rate_limit_tpm,
        max_budget,
        budget_duration,
        model_info: std::collections::BTreeMap::new(),
        region,
        tags,
        enabled: true,
        critical,
        circuit_breaker: if circuit_breaker_threshold > 0 || circuit_breaker_open_s > 0 {
            Some(crate::config::types::CircuitBreakerConfig {
                failure_threshold: if circuit_breaker_threshold > 0 {
                    circuit_breaker_threshold
                } else {
                    3
                },
                open_duration_s: if circuit_breaker_open_s > 0 {
                    circuit_breaker_open_s
                } else {
                    30
                },
            })
        } else {
            None
        },
    });
    save_config(&path, &cfg)?;
    println!("Upstream '{id}' saved to {}", path.display());
    Ok(())
}

fn cmd_upstream_remove(cli: &Cli, id: &str) -> anyhow::Result<()> {
    let (path, mut cfg) = load_config(cli).context("load config")?;
    let before = cfg.upstreams.len();
    cfg.upstreams.retain(|u| u.id != id);
    if cfg.upstreams.len() == before {
        anyhow::bail!("upstream '{id}' not found");
    }
    save_config(&path, &cfg)?;
    println!("Removed upstream '{id}'.");
    Ok(())
}

fn cmd_upstream_list(cli: &Cli) -> anyhow::Result<()> {
    let (_path, cfg) = load_config(cli).context("load config")?;
    if cfg.upstreams.is_empty() {
        println!("(no upstreams configured)");
        return Ok(());
    }
    println!(
        "{:<22} {:<10} {:<10} {:<6} {:<7} {:<6} MODELS",
        "ID", "KIND", "BASE_URL", "PRIO", "RPM", "TPM"
    );
    for u in &cfg.upstreams {
        println!(
            "{:<22} {:<10} {:<10} {:<6} {:<7} {:<6} {}",
            u.id,
            u.kind.as_str(),
            truncate(&u.base_url, 36),
            u.priority,
            u.rate_limit_rpm,
            u.rate_limit_tpm,
            if u.models.is_empty() {
                "*".to_string()
            } else {
                u.models.join(",")
            }
        );
    }
    Ok(())
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n.saturating_sub(1)])
    }
}

fn cmd_show(cli: &Cli) -> anyhow::Result<()> {
    let (path, cfg) = load_config(cli).context("load config")?;
    println!("# {}", path.display());
    println!(
        "{}",
        toml::to_string_pretty(&cfg).unwrap_or_else(|_| "(unparseable)".to_string())
    );
    Ok(())
}

async fn cmd_serve(cli: &Cli, bind_override: Option<String>) -> anyhow::Result<()> {
    let (config_path, mut cfg) = load_config(cli).context("load config")?;
    if let Some(b) = bind_override {
        cfg.server.bind = b;
    }
    if cfg.api_keys.is_empty() && cfg.api_keys_legacy.is_empty() {
        eprintln!("warning: no self-issued API keys configured. Run `polyglotmesh init` first.");
    }
    if cfg.upstreams.is_empty() {
        eprintln!(
            "warning: no upstreams configured. Add some with `polyglotmesh upstream add ...`"
        );
    }
    // Back up the current config as last-known-good before serving, so a bad
    // live edit can be reverted via `POST /v1/admin/reload/rollback`.
    let paths = crate::config::RouterPaths::discover();
    if paths.config_file.exists() {
        let _ = std::fs::copy(&paths.config_file, paths.config_file.with_extension("toml.bak"));
    }

    let state = Arc::new(AppState::from_config(cfg.clone()));

    let app = build_router(state.clone());
    admin::spawn_health_checker(state.clone());
    admin::spawn_config_watcher(state.clone());
    admin::spawn_retention_task(state.clone());
    admin::spawn_budget_reset_task(state.clone());
    admin::spawn_metrics_persister(state.clone());
    admin::spawn_warmup_task(state.clone());
    admin::spawn_rollup_task(state.clone());

    let addr: SocketAddr = cfg
        .server
        .bind
        .parse()
        .with_context(|| format!("invalid bind address '{}'", cfg.server.bind))?;
    tracing::info!(%addr, ?config_path, "starting polyglotmesh");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    let state_clone = state.clone();
    let _ = state;

    // Provide client socket addresses to handlers/middleware (for per-key
    // allowed_cidrs enforcement) via ConnectInfo.
    let app = app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    // Graceful shutdown: on SIGTERM/SIGINT, stop accepting new connections and
    // let in-flight requests drain for up to `drain_timeout_s` (default 30s),
    // then flush metrics to disk before exiting.
    let drain = std::time::Duration::from_secs(cfg.server.drain_timeout_s.unwrap_or(30));
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let ctrl_c = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            #[cfg(unix)]
            let terminate = async {
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(mut sig) => {
                        sig.recv().await;
                    }
                    Err(_) => std::future::pending::<()>().await,
                }
            };
            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();
            tokio::select! {
                _ = ctrl_c => {},
                _ = terminate => {},
            }
            tracing::info!("shutdown signal received; draining in-flight requests");
            tokio::time::sleep(drain).await;
            let _ = state_clone.save_to_disk();
        })
        .await
        .context("axum::serve")?;
    Ok(())
}

async fn cmd_doctor(cli: &Cli, no_probe: bool, timeout_ms: u64) -> anyhow::Result<()> {
    let (path, cfg) = load_config(cli).context("load config")?;
    println!("config: {}", path.display());
    let mut errors = 0usize;
    let mut warnings = 0usize;

    if cfg.api_keys.is_empty() && cfg.api_keys_legacy.is_empty() {
        println!("[WARN] no API keys configured (clients cannot authenticate)");
        warnings += 1;
    }
    if cfg.upstreams.is_empty() {
        println!("[WARN] no upstreams configured (router will reject all requests)");
        warnings += 1;
    }
    if cfg.server.admin_token.is_none() {
        println!("[WARN] no admin_token set (admin endpoints unusable)");
        warnings += 1;
    }

    println!();
    println!("upstreams: {}", cfg.upstreams.len());
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()
        .context("build probe client")?;

    for u in &cfg.upstreams {
        let url = format!("{}/models", u.base_url.trim_end_matches('/'));
        let mut line = format!(
            "  - {} [{:?}] {} critical={} enabled={}",
            u.id, u.kind, u.base_url, u.critical, u.enabled
        );
        if u.models.is_empty() {
            line.push_str(" (no models declared)");
            warnings += 1;
        }
        if u.enabled {
            if no_probe {
                line.push_str(" [probe skipped]");
            } else {
                let start = std::time::Instant::now();
                let res = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {}", u.api_key))
                    .send()
                    .await;
                let elapsed = start.elapsed();
                match res {
                    Ok(r) if r.status().is_success() => {
                        line.push_str(&format!(" OK {}ms", elapsed.as_millis()));
                    }
                    Ok(r) => {
                        line.push_str(&format!(
                            " HTTP {} {}ms",
                            r.status().as_u16(),
                            elapsed.as_millis()
                        ));
                        errors += 1;
                    }
                    Err(e) => {
                        line.push_str(&format!(" ERR {e}"));
                        errors += 1;
                    }
                }
            }
        } else {
            line.push_str(" [disabled]");
        }
        println!("{line}");
    }

    println!();
    if errors > 0 {
        println!("RESULT: {} error(s), {} warning(s)", errors, warnings);
        anyhow::bail!("doctor found {} error(s)", errors);
    } else {
        println!("RESULT: OK ({} warning(s))", warnings);
    }
    Ok(())
}

pub fn build_router(state: Arc<AppState>) -> axum::Router {
    use axum::routing::{get, post};
    let shared: proxy::SharedState = state.clone();
    let public = axum::Router::new()
        .route("/healthz", get(proxy::health))
        .route("/dashboard", get(dashboard))
        .route("/dashboard/", get(dashboard))
        .with_state(shared.clone());
    let api: axum::Router<proxy::SharedState> = axum::Router::new()
        .route("/v1/chat/completions", post(proxy::openai_chat_completions))
        .route("/v1/models", get(proxy::openai_list_models))
        .route("/v1/models/:model", get(proxy::openai_get_model))
        .route("/v1/messages", post(proxy::anthropic_messages))
        .route_layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            proxy::auth_middleware,
        ));
    let admin = admin::admin_router(shared.clone());
    public.merge(api).merge(admin).with_state(shared)
}

/// Serve the built-in HTML dashboard. Single static file, no JS framework.
use axum::http::header;
use axum::response::IntoResponse;
async fn dashboard() -> impl IntoResponse {
    const HTML: &str = include_str!("../static/dashboard.html");
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], HTML)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_short_string_is_unchanged() {
        assert_eq!(truncate("short", 36), "short");
        assert_eq!(truncate("", 36), "");
    }

    #[test]
    fn truncate_long_string_gets_ellipsis() {
        let s = "https://example.com/a/very/long/base/url/path/that/should/be/cut";
        let out = truncate(s, 36);
        assert!(out.ends_with('…'));
        assert_eq!(out.chars().count(), 36);
        assert!(s.starts_with(&out[..out.char_indices().nth(35).unwrap().0]));
    }

    #[test]
    fn truncate_at_zero_is_safe() {
        let out = truncate("abc", 0);
        assert_eq!(out, "…");
    }
}
