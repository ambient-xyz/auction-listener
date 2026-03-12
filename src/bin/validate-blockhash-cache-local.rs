mod harness_support;

use anyhow::{bail, Context, Result};
use clap::Parser;
use harness_support::{
    build_client, default_run_dir, preflight_linux, run_cache_validation, FallbackWarningCounter,
    FallbackWarningLayer, RepoPaths, ValidationConfig, ValidationSummary, ValidatorStack,
};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing_subscriber::prelude::*;

const DEFAULT_RPC_URL: &str = "http://127.0.0.1:8899";
const DEFAULT_YELLOWSTONE_URL: &str = "http://127.0.0.1:10000";

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    auction_listener_root: Option<PathBuf>,
    #[arg(long)]
    validator_root: Option<PathBuf>,
    #[arg(long)]
    agave_root: Option<PathBuf>,
    #[arg(long)]
    run_dir: Option<PathBuf>,
    #[arg(long, default_value = DEFAULT_RPC_URL)]
    rpc_url: String,
    #[arg(long, default_value = DEFAULT_YELLOWSTONE_URL)]
    yellowstone_url: String,
    #[arg(long, default_value_t = 50)]
    request_count: usize,
    #[arg(long, default_value_t = 5)]
    concurrency: usize,
    #[arg(long, default_value_t = 8)]
    additional_bundles: u64,
    #[arg(long, default_value_t = 100)]
    max_price: u64,
    #[arg(long, default_value_t = 100)]
    max_price_per_output_token: u64,
    #[arg(long, default_value_t = 100)]
    max_output_tokens: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    if args.auction_listener_root.is_none() {
        args.auction_listener_root = std::env::var_os("AUCTION_LISTENER_ROOT").map(PathBuf::from);
    }
    if args.validator_root.is_none() {
        args.validator_root = std::env::var_os("VALIDATOR_ROOT").map(PathBuf::from);
    }
    if args.agave_root.is_none() {
        args.agave_root = std::env::var_os("AGAVE_ROOT").map(PathBuf::from);
    }
    if args.run_dir.is_none() {
        args.run_dir = std::env::var_os("RUN_DIR").map(PathBuf::from);
    }

    let repo_paths = RepoPaths::discover(
        args.auction_listener_root.clone(),
        args.validator_root.clone(),
        args.agave_root.clone(),
    )?;
    preflight_linux(&repo_paths)?;

    let run_dir = args.run_dir.unwrap_or_else(|| {
        default_run_dir(&repo_paths.auction_listener_root, "local-blockhash-cache")
    });
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create {}", run_dir.display()))?;

    let fallback_counter = FallbackWarningCounter::default();
    install_tracing(fallback_counter.clone())?;

    let mut validator =
        ValidatorStack::start(&repo_paths, &run_dir, &args.rpc_url, &args.yellowstone_url).await?;
    let client = build_client(&validator).await?;
    let config = ValidationConfig {
        request_count: args.request_count,
        concurrency: args.concurrency,
        additional_bundles: Some(args.additional_bundles),
        max_price: args.max_price,
        max_price_per_output_token: args.max_price_per_output_token,
        max_output_tokens: args.max_output_tokens,
        context_length_tier: ambient_auction_api::RequestTier::Standard,
        expiry_duration_tier: ambient_auction_api::RequestTier::Standard,
        prompt: ValidationConfig::default_prompt(),
    };

    let run_result = run_cache_validation(client, config, fallback_counter).await;
    let stop_result = validator.stop();
    stop_result?;

    let summary = run_result?;
    let summary_json_path = run_dir.join("summary.json");
    let summary_md_path = run_dir.join("summary.md");
    write_summary_json(&summary_json_path, &summary)?;
    write_summary_markdown(&summary_md_path, &summary, &validator)?;

    if summary.block_not_available_yet_count != 0 {
        bail!(
            "validation saw {} `block is not available yet` errors; see {}",
            summary.block_not_available_yet_count,
            summary_md_path.display()
        );
    }
    if summary.other_error_count != 0 || summary.requests_failed != 0 {
        bail!(
            "validation had request failures; see {}",
            summary_md_path.display()
        );
    }
    if summary.fallback_warning_count != 0 {
        bail!(
            "validation observed {} RPC fallback warnings; see {}",
            summary.fallback_warning_count,
            summary_md_path.display()
        );
    }
    if !summary.keep_cache_warm_alive {
        bail!(
            "keep_cache_warm exited before the workload completed; see {}",
            summary_md_path.display()
        );
    }

    println!("validation artifacts: {}", run_dir.display());
    Ok(())
}

fn install_tracing(fallback_counter: FallbackWarningCounter) -> Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(FallbackWarningLayer::new(fallback_counter));
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|error| anyhow::anyhow!("failed to install tracing subscriber: {error}"))
}

fn write_summary_json(path: &Path, summary: &ValidationSummary) -> Result<()> {
    let file = File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    serde_json::to_writer_pretty(file, summary)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn write_summary_markdown(
    path: &Path,
    summary: &ValidationSummary,
    validator: &ValidatorStack,
) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    writeln!(file, "# Local Blockhash Cache Validation")?;
    writeln!(file)?;
    writeln!(file, "| Field | Value |")?;
    writeln!(file, "| --- | --- |")?;
    writeln!(file, "| Requests attempted | {} |", summary.requests_attempted)?;
    writeln!(file, "| Requests succeeded | {} |", summary.requests_succeeded)?;
    writeln!(file, "| Requests failed | {} |", summary.requests_failed)?;
    writeln!(
        file,
        "| `block is not available yet` count | {} |",
        summary.block_not_available_yet_count
    )?;
    writeln!(file, "| Other error count | {} |", summary.other_error_count)?;
    writeln!(
        file,
        "| Fallback warning count | {} |",
        summary.fallback_warning_count
    )?;
    writeln!(
        file,
        "| `keep_cache_warm()` alive | {} |",
        summary.keep_cache_warm_alive
    )?;
    writeln!(
        file,
        "| First blocking error | {} |",
        summary
            .first_blocking_error
            .clone()
            .unwrap_or_else(|| "none".to_string())
    )?;
    writeln!(file)?;
    write_latency_section(
        &mut file,
        "Blockhash Acquisition Latency (micros)",
        &summary.blockhash_acquisition_latency_micros,
    )?;
    write_latency_section(&mut file, "Request Latency (micros)", &summary.request_latency_micros)?;
    writeln!(file, "## Logs")?;
    writeln!(file)?;
    writeln!(file, "- Validator log: `{}`", validator.validator_log.display())?;
    writeln!(
        file,
        "- init-bundles log: `{}`",
        validator.init_bundles_log.display()
    )?;
    Ok(())
}

fn write_latency_section(
    file: &mut File,
    title: &str,
    summary: &harness_support::LatencySummary,
) -> Result<()> {
    writeln!(file, "## {title}")?;
    writeln!(file)?;
    writeln!(file, "| Sample Count | P50 | P95 | P99 |")?;
    writeln!(file, "| ---: | ---: | ---: | ---: |")?;
    writeln!(
        file,
        "| {} | {} | {} | {} |",
        summary.sample_count,
        display_optional(summary.p50_micros),
        display_optional(summary.p95_micros),
        display_optional(summary.p99_micros),
    )?;
    writeln!(file)?;
    Ok(())
}

fn display_optional(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}
