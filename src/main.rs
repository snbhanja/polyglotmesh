use anyhow::Context;
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

mod admin;
mod auth;
mod config;
mod error;
mod proxy;
mod queue;
mod state;
mod storage;
mod upstream;
mod metrics;

use crate::config::types::ProviderKind;
use crate::config::{load_from_path, RouterPaths};
use crate::error::RouterResult;
use crate::state::AppState;

#[derive(Parser, Debug)]
#[command(name = "polyglotmesh", version, about = "Fast Rust LLM router for OpenAI/Anthropic-compatible APIs")]
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
}

fn load_config(cli: &Cli) -> RouterResult<(PathBuf, crate::config::types::Config)> {
    let paths = RouterPaths::discover();
    let path = cli.config.clone().unwrap_or(paths.config_file.clone());
    let cfg = load_from_path(&path)?;
    Ok((path, cfg))
}

fn save_config(path: &PathBuf, cfg: &crate::config::types::Config) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
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
        println!("OpenAI-compatible base URL:    http://{}/v1", cfg.server.bind);
        println!("Anthropic-compatible base URL: http://{}/v1", cfg.server.bind);
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
        "api" | _ => {
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
    println!("{:<22} {:<10} {:<10} {:<6} {:<7} {:<6} MODELS", "ID", "KIND", "BASE_URL", "PRIO", "RPM", "TPM");
    for u in &cfg.upstreams {
        println!(
            "{:<22} {:<10} {:<10} {:<6} {:<7} {:<6} {}",
            u.id,
            u.kind.as_str(),
            truncate(&u.base_url, 36),
            u.priority,
            u.rate_limit_rpm,
            u.rate_limit_tpm,
            if u.models.is_empty() { "*".to_string() } else { u.models.join(",") }
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
    println!("{}", toml::to_string_pretty(&cfg).unwrap_or_else(|_| "(unparseable)".to_string()));
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
        eprintln!("warning: no upstreams configured. Add some with `polyglotmesh upstream add ...`");
    }
    let state = Arc::new(AppState::from_config(cfg.clone()));

    let app = build_router(state.clone());
    admin::spawn_health_checker(state.clone());
    admin::spawn_config_watcher(state.clone());
    admin::spawn_retention_task(state.clone());
    admin::spawn_budget_reset_task(state.clone());
    admin::spawn_metrics_persister(state.clone());

    let addr: SocketAddr = cfg
        .server
        .bind
        .parse()
        .with_context(|| format!("invalid bind address '{}'", cfg.server.bind))?;
    tracing::info!(%addr, ?config_path, "starting polyglotmesh");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    let _ = state;
    axum::serve(listener, app).await.context("axum::serve")?;
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
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        HTML,
    )
}
