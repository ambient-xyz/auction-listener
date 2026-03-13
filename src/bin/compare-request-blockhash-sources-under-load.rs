mod harness_support;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use harness_support::{
    build_client, default_run_dir, preflight_linux, run_unary_hammer_load,
    run_validation_with_blockhash_source, BlockhashSource, FallbackWarningCounter,
    FallbackWarningLayer, LatencySummary, RepoPaths, UnaryHammerConfig, UnaryHammerSummary,
    ValidationConfig, ValidationSummary, ValidatorStack,
};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    #[arg(long, default_value_t = 5)]
    rounds: usize,
    #[arg(long, default_value_t = 1000)]
    request_count: usize,
    #[arg(long, default_value_t = 50)]
    concurrency: usize,
    #[arg(long, default_value_t = 20)]
    job_request_timeout_secs: u64,
    #[arg(long, default_value_t = 20)]
    auction_timeout_secs: u64,
    #[arg(long, default_value_t = 8)]
    additional_bundles: u64,
    #[arg(long, default_value_t = 100)]
    max_price: u64,
    #[arg(long, default_value_t = 100)]
    max_price_per_output_token: u64,
    #[arg(long, default_value_t = 100)]
    max_output_tokens: u64,
    #[arg(long, default_value_t = 64)]
    hammer_unary_concurrency: usize,
    #[arg(long, default_value_t = 8)]
    hammer_slot_spam_concurrency: usize,
    #[arg(long, default_value_t = 1)]
    hammer_transfer_lamports: u64,
    #[arg(long, default_value_t = 300)]
    stress_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
struct RoundResult {
    mode: String,
    round: usize,
    round_dir: PathBuf,
    validator_log: PathBuf,
    init_bundles_log: PathBuf,
    stress_log: PathBuf,
    request_summary_path: PathBuf,
    request_summary: ValidationSummary,
    background_hammer_summary_path: PathBuf,
    background_hammer_summary: UnaryHammerSummary,
}

#[derive(Debug, Clone, Serialize)]
struct ModeAggregate {
    mode: String,
    rounds: usize,
    requests_attempted: usize,
    requests_succeeded: usize,
    requests_failed: usize,
    block_not_available_yet_count: usize,
    other_error_count: usize,
    fallback_warning_count: usize,
    blockhash_acquisition_latency_micros: LatencySummary,
    request_latency_micros: LatencySummary,
    first_blocking_error: Option<String>,
    result: String,
}

#[derive(Debug, Clone, Serialize)]
struct ModeSummary {
    aggregate: ModeAggregate,
    rounds: Vec<RoundResult>,
    background_hammer_block_not_available_yet_count: u64,
    background_hammer_other_error_count: u64,
}

#[derive(Debug, Serialize)]
struct ComparisonSummary {
    run_dir: PathBuf,
    rounds_per_mode: usize,
    requests_per_round: usize,
    concurrency: usize,
    job_request_timeout_secs: u64,
    auction_timeout_secs: u64,
    hammer_unary_concurrency: usize,
    hammer_slot_spam_concurrency: usize,
    hammer_transfer_lamports: u64,
    stress_timeout_secs: u64,
    unary: ModeSummary,
    cache: ModeSummary,
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
    ensure_command("stress-ng")?;

    let run_dir = args.run_dir.unwrap_or_else(|| {
        default_run_dir(
            &repo_paths.auction_listener_root,
            "request-blockhash-source-ab",
        )
    });
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create {}", run_dir.display()))?;

    let fallback_counter = FallbackWarningCounter::default();
    install_tracing(fallback_counter.clone())?;

    let validation_config = ValidationConfig {
        request_count: args.request_count,
        concurrency: args.concurrency,
        job_request_timeout_secs: args.job_request_timeout_secs,
        auction_timeout_secs: args.auction_timeout_secs,
        additional_bundles: Some(args.additional_bundles),
        max_price: args.max_price,
        max_price_per_output_token: args.max_price_per_output_token,
        max_output_tokens: args.max_output_tokens,
        context_length_tier: ambient_auction_api::RequestTier::Standard,
        expiry_duration_tier: ambient_auction_api::RequestTier::Standard,
        prompt: ValidationConfig::default_prompt(),
    };
    let hammer_config = UnaryHammerConfig {
        unary_concurrency: args.hammer_unary_concurrency,
        slot_spam_concurrency: args.hammer_slot_spam_concurrency,
        transfer_lamports: args.hammer_transfer_lamports,
    };

    let unary = run_mode(
        &repo_paths,
        &run_dir,
        &args.rpc_url,
        &args.yellowstone_url,
        BlockhashSource::YellowstoneUnary,
        args.rounds,
        &validation_config,
        &hammer_config,
        args.stress_timeout_secs,
        fallback_counter.clone(),
    )
    .await?;
    let cache = run_mode(
        &repo_paths,
        &run_dir,
        &args.rpc_url,
        &args.yellowstone_url,
        BlockhashSource::Cache,
        args.rounds,
        &validation_config,
        &hammer_config,
        args.stress_timeout_secs,
        fallback_counter,
    )
    .await?;

    let summary = ComparisonSummary {
        run_dir: run_dir.clone(),
        rounds_per_mode: args.rounds,
        requests_per_round: args.request_count,
        concurrency: args.concurrency,
        job_request_timeout_secs: args.job_request_timeout_secs,
        auction_timeout_secs: args.auction_timeout_secs,
        hammer_unary_concurrency: args.hammer_unary_concurrency,
        hammer_slot_spam_concurrency: args.hammer_slot_spam_concurrency,
        hammer_transfer_lamports: args.hammer_transfer_lamports,
        stress_timeout_secs: args.stress_timeout_secs,
        unary,
        cache,
    };

    let summary_json_path = run_dir.join("comparison.json");
    let summary_md_path = run_dir.join("comparison.md");
    serde_json::to_writer_pretty(
        File::create(&summary_json_path)
            .with_context(|| format!("failed to create {}", summary_json_path.display()))?,
        &summary,
    )
    .with_context(|| format!("failed to write {}", summary_json_path.display()))?;
    write_summary_markdown(&summary_md_path, &summary)?;

    if summary.unary.aggregate.result == "inconclusive" {
        tracing::warn!("unary request-path comparison was inconclusive");
    }
    if summary.cache.aggregate.result != "cache clean" {
        bail!(
            "cache comparison run was not clean; see {}",
            summary_md_path.display()
        );
    }

    println!("comparison artifacts: {}", run_dir.display());
    Ok(())
}

async fn run_mode(
    repo_paths: &RepoPaths,
    run_dir: &Path,
    rpc_url: &str,
    yellowstone_url: &str,
    blockhash_source: BlockhashSource,
    rounds: usize,
    validation_config: &ValidationConfig,
    hammer_config: &UnaryHammerConfig,
    stress_timeout_secs: u64,
    fallback_counter: FallbackWarningCounter,
) -> Result<ModeSummary> {
    let mode_dir = run_dir.join(blockhash_source.as_str());
    fs::create_dir_all(&mode_dir)
        .with_context(|| format!("failed to create {}", mode_dir.display()))?;

    let mut round_results = Vec::with_capacity(rounds);
    for round in 1..=rounds {
        let round_dir = mode_dir.join(format!("round-{round:02}"));
        fs::create_dir_all(&round_dir)
            .with_context(|| format!("failed to create {}", round_dir.display()))?;
        round_results.push(
            run_round(
                repo_paths,
                &round_dir,
                rpc_url,
                yellowstone_url,
                blockhash_source,
                round,
                validation_config,
                hammer_config,
                stress_timeout_secs,
                fallback_counter.clone(),
            )
            .await?,
        );
    }

    let aggregate = aggregate_mode(blockhash_source, &round_results);
    let background_hammer_block_not_available_yet_count = round_results
        .iter()
        .map(|round| {
            round
                .background_hammer_summary
                .block_not_available_yet_count
        })
        .sum();
    let background_hammer_other_error_count = round_results
        .iter()
        .map(|round| {
            round.background_hammer_summary.other_error_count
                + round.background_hammer_summary.parse_error_count
        })
        .sum();

    Ok(ModeSummary {
        aggregate,
        rounds: round_results,
        background_hammer_block_not_available_yet_count,
        background_hammer_other_error_count,
    })
}

async fn run_round(
    repo_paths: &RepoPaths,
    round_dir: &Path,
    rpc_url: &str,
    yellowstone_url: &str,
    blockhash_source: BlockhashSource,
    round: usize,
    validation_config: &ValidationConfig,
    hammer_config: &UnaryHammerConfig,
    stress_timeout_secs: u64,
    fallback_counter: FallbackWarningCounter,
) -> Result<RoundResult> {
    let mut validator =
        ValidatorStack::start(repo_paths, round_dir, rpc_url, yellowstone_url).await?;
    let validator_log = validator.validator_log.clone();
    let init_bundles_log = validator.init_bundles_log.clone();
    let stress_log = round_dir.join("stress.log");
    let request_summary_path = round_dir.join("request-summary.json");
    let background_hammer_summary_path = round_dir.join("background-hammer-summary.json");

    let mut stress = start_stress_ng(&stress_log, stress_timeout_secs)?;
    let client = build_client(&validator).await?;
    let hammer_running = Arc::new(AtomicBool::new(true));
    let hammer_task = tokio::spawn(run_unary_hammer_load(
        client.clone(),
        hammer_config.clone(),
        hammer_running.clone(),
    ));

    let validation_result = run_validation_with_blockhash_source(
        client,
        validation_config.clone(),
        fallback_counter,
        blockhash_source,
    )
    .await;

    hammer_running.store(false, Ordering::Relaxed);
    let background_hammer_summary = hammer_task
        .await
        .map_err(|error| anyhow!("background hammer task failed: {error}"))?;
    let stop_stress_result = stop_child(&mut stress);
    let stop_validator_result = validator.stop();

    stop_stress_result?;
    stop_validator_result?;

    let request_summary = validation_result?;
    serde_json::to_writer_pretty(
        File::create(&request_summary_path)
            .with_context(|| format!("failed to create {}", request_summary_path.display()))?,
        &request_summary,
    )
    .with_context(|| format!("failed to write {}", request_summary_path.display()))?;
    serde_json::to_writer_pretty(
        File::create(&background_hammer_summary_path).with_context(|| {
            format!(
                "failed to create {}",
                background_hammer_summary_path.display()
            )
        })?,
        &background_hammer_summary,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            background_hammer_summary_path.display()
        )
    })?;

    Ok(RoundResult {
        mode: blockhash_source.as_str().to_string(),
        round,
        round_dir: round_dir.to_path_buf(),
        validator_log,
        init_bundles_log,
        stress_log,
        request_summary_path,
        request_summary,
        background_hammer_summary_path,
        background_hammer_summary,
    })
}

fn aggregate_mode(blockhash_source: BlockhashSource, rounds: &[RoundResult]) -> ModeAggregate {
    let mut blockhash_latencies = Vec::new();
    let mut request_latencies = Vec::new();
    let mut requests_attempted = 0usize;
    let mut requests_succeeded = 0usize;
    let mut requests_failed = 0usize;
    let mut block_not_available_yet_count = 0usize;
    let mut other_error_count = 0usize;
    let mut fallback_warning_count = 0usize;
    let mut first_blocking_error = None;

    for round in rounds {
        requests_attempted += round.request_summary.requests_attempted;
        requests_succeeded += round.request_summary.requests_succeeded;
        requests_failed += round.request_summary.requests_failed;
        block_not_available_yet_count += round.request_summary.block_not_available_yet_count;
        other_error_count += round.request_summary.other_error_count;
        fallback_warning_count += round.request_summary.fallback_warning_count;
        blockhash_latencies
            .extend_from_slice(&round.request_summary.blockhash_latency_samples_micros);
        request_latencies.extend_from_slice(&round.request_summary.request_latency_samples_micros);
        if first_blocking_error.is_none() {
            first_blocking_error = round.request_summary.first_blocking_error.clone();
        }
    }

    let result = match blockhash_source {
        BlockhashSource::YellowstoneUnary => {
            if block_not_available_yet_count > 0 {
                "reproduced"
            } else {
                "inconclusive"
            }
        }
        BlockhashSource::Cache => {
            if block_not_available_yet_count == 0
                && other_error_count == 0
                && requests_failed == 0
                && fallback_warning_count == 0
            {
                "cache clean"
            } else {
                "inconclusive"
            }
        }
    }
    .to_string();

    ModeAggregate {
        mode: blockhash_source.as_str().to_string(),
        rounds: rounds.len(),
        requests_attempted,
        requests_succeeded,
        requests_failed,
        block_not_available_yet_count,
        other_error_count,
        fallback_warning_count,
        blockhash_acquisition_latency_micros: summarize_latencies(&blockhash_latencies),
        request_latency_micros: summarize_latencies(&request_latencies),
        first_blocking_error,
        result,
    }
}

fn summarize_latencies(values: &[u64]) -> LatencySummary {
    if values.is_empty() {
        return LatencySummary {
            sample_count: 0,
            p50_micros: None,
            p95_micros: None,
            p99_micros: None,
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let percentile = |pct: f64| -> u64 {
        let last = sorted.len().saturating_sub(1);
        let index = ((last as f64) * pct).round() as usize;
        sorted[index.min(last)]
    };

    LatencySummary {
        sample_count: sorted.len(),
        p50_micros: Some(percentile(0.50)),
        p95_micros: Some(percentile(0.95)),
        p99_micros: Some(percentile(0.99)),
    }
}

fn install_tracing(fallback_counter: FallbackWarningCounter) -> Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(FallbackWarningLayer::new(fallback_counter));
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|error| anyhow!("failed to install tracing subscriber: {error}"))
}

fn start_stress_ng(log_path: &Path, timeout_secs: u64) -> Result<Child> {
    let stdout = File::create(log_path)
        .with_context(|| format!("failed to create {}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", log_path.display()))?;
    Command::new("stress-ng")
        .arg("--cpu")
        .arg("2")
        .arg("--io")
        .arg("1")
        .arg("--vm")
        .arg("1")
        .arg("--timeout")
        .arg(format!("{timeout_secs}s"))
        .arg("--metrics-brief")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .context("failed to start stress-ng")
}

fn stop_child(child: &mut Child) -> Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

fn ensure_command(name: &str) -> Result<()> {
    let status = Command::new("which")
        .arg(name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("failed to look up `{name}`"))?;
    if !status.success() {
        bail!("required command `{name}` is not available on PATH");
    }
    Ok(())
}

fn write_summary_markdown(path: &Path, summary: &ComparisonSummary) -> Result<()> {
    let mut file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;

    writeln!(file, "# Request Blockhash Source Comparison")?;
    writeln!(file)?;
    writeln!(file, "## Configuration")?;
    writeln!(file)?;
    writeln!(file, "- Rounds per mode: `{}`", summary.rounds_per_mode)?;
    writeln!(
        file,
        "- Requests per round: `{}`",
        summary.requests_per_round
    )?;
    writeln!(file, "- Concurrency: `{}`", summary.concurrency)?;
    writeln!(
        file,
        "- JobRequest timeout: `{}`s",
        summary.job_request_timeout_secs
    )?;
    writeln!(
        file,
        "- Auction timeout: `{}`s",
        summary.auction_timeout_secs
    )?;
    writeln!(
        file,
        "- Background hammer: unary `{}`, slot-spam `{}`, transfer lamports `{}`",
        summary.hammer_unary_concurrency,
        summary.hammer_slot_spam_concurrency,
        summary.hammer_transfer_lamports
    )?;
    writeln!(
        file,
        "- stress-ng timeout: `{}`s",
        summary.stress_timeout_secs
    )?;
    writeln!(file)?;

    writeln!(file, "## Aggregate Comparison")?;
    writeln!(file)?;
    writeln!(file, "| Mode | Rounds | Attempted | Succeeded | Failed | `block is not available yet` | Other Errors | Fallback Warnings | Blockhash p50/p95/p99 (µs) | Request p50/p95/p99 (µs) | First Blocking Error | Result |")?;
    writeln!(
        file,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |"
    )?;
    write_aggregate_row(&mut file, &summary.unary.aggregate)?;
    write_aggregate_row(&mut file, &summary.cache.aggregate)?;
    writeln!(file)?;

    writeln!(file, "## Round Results")?;
    writeln!(file)?;
    writeln!(file, "| Mode | Round | Attempted | Succeeded | Failed | `block is not available yet` | Other Errors | Fallback Warnings | Request Summary | Background Hammer |")?;
    writeln!(
        file,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |"
    )?;
    for round in summary
        .unary
        .rounds
        .iter()
        .chain(summary.cache.rounds.iter())
    {
        writeln!(
            file,
            "| {} | {} | {} | {} | {} | {} | {} | {} | `{}` | `{}` |",
            round.mode,
            round.round,
            round.request_summary.requests_attempted,
            round.request_summary.requests_succeeded,
            round.request_summary.requests_failed,
            round.request_summary.block_not_available_yet_count,
            round.request_summary.other_error_count,
            round.request_summary.fallback_warning_count,
            round.request_summary_path.display(),
            round.background_hammer_summary_path.display(),
        )?;
    }
    writeln!(file)?;

    writeln!(file, "## Background Hammer")?;
    writeln!(file)?;
    writeln!(
        file,
        "- Unary-mode rounds background `block is not available yet`: `{}`",
        summary
            .unary
            .background_hammer_block_not_available_yet_count
    )?;
    writeln!(
        file,
        "- Unary-mode rounds background other errors: `{}`",
        summary.unary.background_hammer_other_error_count
    )?;
    writeln!(
        file,
        "- Cache-mode rounds background `block is not available yet`: `{}`",
        summary
            .cache
            .background_hammer_block_not_available_yet_count
    )?;
    writeln!(
        file,
        "- Cache-mode rounds background other errors: `{}`",
        summary.cache.background_hammer_other_error_count
    )?;
    Ok(())
}

fn write_aggregate_row(file: &mut File, aggregate: &ModeAggregate) -> Result<()> {
    writeln!(
        file,
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        aggregate.mode,
        aggregate.rounds,
        aggregate.requests_attempted,
        aggregate.requests_succeeded,
        aggregate.requests_failed,
        aggregate.block_not_available_yet_count,
        aggregate.other_error_count,
        aggregate.fallback_warning_count,
        format_latency_triplet(&aggregate.blockhash_acquisition_latency_micros),
        format_latency_triplet(&aggregate.request_latency_micros),
        aggregate
            .first_blocking_error
            .clone()
            .unwrap_or_else(|| "none".to_string()),
        aggregate.result,
    )?;
    Ok(())
}

fn format_latency_triplet(summary: &LatencySummary) -> String {
    format!(
        "{}/{}/{}",
        display_optional(summary.p50_micros),
        display_optional(summary.p95_micros),
        display_optional(summary.p99_micros),
    )
}

fn display_optional(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}
