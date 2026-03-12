mod harness_support;

use anyhow::{bail, Context, Result};
use clap::Parser;
use harness_support::{
    default_run_dir, preflight_linux, PreparedCargoContext, RepoPaths, ValidatorStack,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

const DEFAULT_RPC_URL: &str = "http://127.0.0.1:8899";
const DEFAULT_YELLOWSTONE_URL: &str = "http://127.0.0.1:10000";
const TEST_NAME: &str = "request_submission_uses_cache_backed_blockhash_under_load_local";

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
    additional_bundles: usize,
    #[arg(long, default_value_t = 100)]
    max_price: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LocalHarnessLatencySummary {
    sample_count: usize,
    p50_micros: Option<u64>,
    p95_micros: Option<u64>,
    p99_micros: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LocalHarnessRunSummary {
    requests_attempted: usize,
    requests_succeeded: usize,
    requests_failed: usize,
    concurrency: usize,
    block_not_available_yet_count: usize,
    other_error_count: usize,
    first_blocking_error: Option<String>,
    keep_cache_warm_alive: bool,
    teardown_cancelled: bool,
    blockhash_source_counts: BTreeMap<String, usize>,
    blockhash_acquisition_latency_micros: LocalHarnessLatencySummary,
    request_latency_micros: LocalHarnessLatencySummary,
    blockhash_acquisition_latency_samples_micros: Vec<u64>,
    request_latency_samples_micros: Vec<u64>,
}

fn main() -> Result<()> {
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

    let cargo_context = PreparedCargoContext::prepare(&repo_paths, &run_dir)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    let mut validator = rt.block_on(ValidatorStack::start(
        &repo_paths,
        &cargo_context,
        &run_dir,
        &args.rpc_url,
        &args.yellowstone_url,
    ))?;

    let results_json_path = run_dir.join("summary.json");
    let test_log_path = run_dir.join("validation-test.log");
    let summary_md_path = run_dir.join("summary.md");

    let test_stdout = File::create(&test_log_path)
        .with_context(|| format!("failed to create {}", test_log_path.display()))?;
    let test_stderr = test_stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", test_log_path.display()))?;

    let status = Command::new("cargo")
        .arg("--config")
        .arg(&cargo_context.config_path)
        .arg("test")
        .arg("--manifest-path")
        .arg(repo_paths.auction_listener_root.join("Cargo.toml"))
        .arg("--lib")
        .arg(TEST_NAME)
        .arg("--")
        .arg("--ignored")
        .arg("--nocapture")
        .env("CARGO_TARGET_DIR", run_dir.join("validation-target"))
        .env("AMBIENT_LOCAL_RPC_URL", &validator.rpc_url)
        .env("AMBIENT_LOCAL_YELLOWSTONE_URL", &validator.yellowstone_url)
        .env("AMBIENT_LOCAL_PAYER_KEYPAIR", &validator.payer_keypair)
        .env("AMBIENT_LOCAL_REQUESTS", args.request_count.to_string())
        .env("AMBIENT_LOCAL_CONCURRENCY", args.concurrency.to_string())
        .env(
            "AMBIENT_LOCAL_ADDITIONAL_BUNDLES",
            args.additional_bundles.to_string(),
        )
        .env("AMBIENT_LOCAL_MAX_PRICE", args.max_price.to_string())
        .env("AMBIENT_LOCAL_RESULTS_JSON", &results_json_path)
        .stdout(Stdio::from(test_stdout))
        .stderr(Stdio::from(test_stderr))
        .status()
        .context("failed to run fixed-path local validation test")?;

    let stop_result = validator.stop();
    let summary = if results_json_path.is_file() {
        let raw = fs::read_to_string(&results_json_path)
            .with_context(|| format!("failed to read {}", results_json_path.display()))?;
        Some(
            serde_json::from_str::<LocalHarnessRunSummary>(&raw)
                .with_context(|| format!("failed to parse {}", results_json_path.display()))?,
        )
    } else {
        None
    };

    write_summary_markdown(
        &summary_md_path,
        &run_dir,
        &validator,
        &test_log_path,
        &results_json_path,
        summary.as_ref(),
        status.success(),
    )?;

    stop_result?;

    if !status.success() {
        bail!(
            "local blockhash cache validation failed; see {}",
            test_log_path.display()
        );
    }

    let summary = summary.context("validation test did not write summary.json")?;
    if summary.block_not_available_yet_count != 0 {
        bail!(
            "validation saw {} `block is not available yet` errors; see {}",
            summary.block_not_available_yet_count,
            summary_md_path.display()
        );
    }
    if summary.requests_failed != 0 {
        bail!(
            "validation had {} request failures; see {}",
            summary.requests_failed,
            summary_md_path.display()
        );
    }
    if summary
        .blockhash_source_counts
        .get("rpc-fallback")
        .copied()
        .unwrap_or(0)
        != 0
    {
        bail!(
            "validation used RPC fallback blockhashes; see {}",
            summary_md_path.display()
        );
    }
    if summary
        .blockhash_source_counts
        .get("cache")
        .copied()
        .unwrap_or(0)
        == 0
    {
        bail!(
            "validation did not record any cache-backed blockhash reads; see {}",
            summary_md_path.display()
        );
    }

    println!("validation artifacts: {}", run_dir.display());
    Ok(())
}

fn write_summary_markdown(
    summary_md_path: &PathBuf,
    run_dir: &PathBuf,
    validator: &ValidatorStack,
    test_log_path: &PathBuf,
    results_json_path: &PathBuf,
    summary: Option<&LocalHarnessRunSummary>,
    test_succeeded: bool,
) -> Result<()> {
    let mut file = File::create(summary_md_path)
        .with_context(|| format!("failed to create {}", summary_md_path.display()))?;

    writeln!(file, "# Local Blockhash Cache Validation")?;
    writeln!(file)?;
    writeln!(file, "- Run dir: `{}`", run_dir.display())?;
    writeln!(
        file,
        "- Test result: {}",
        if test_succeeded { "pass" } else { "fail" }
    )?;
    writeln!(
        file,
        "- Validator log: `{}`",
        validator.validator_log.display()
    )?;
    writeln!(
        file,
        "- init-bundles log: `{}`",
        validator.init_bundles_log.display()
    )?;
    writeln!(file, "- Test log: `{}`", test_log_path.display())?;
    writeln!(file, "- Summary JSON: `{}`", results_json_path.display())?;
    writeln!(file)?;

    if let Some(summary) = summary {
        writeln!(file, "## Summary")?;
        writeln!(file)?;
        writeln!(file, "| Field | Value |")?;
        writeln!(file, "| --- | --- |")?;
        writeln!(
            file,
            "| Requests attempted | {} |",
            summary.requests_attempted
        )?;
        writeln!(
            file,
            "| Requests succeeded | {} |",
            summary.requests_succeeded
        )?;
        writeln!(file, "| Requests failed | {} |", summary.requests_failed)?;
        writeln!(
            file,
            "| `block is not available yet` count | {} |",
            summary.block_not_available_yet_count
        )?;
        writeln!(
            file,
            "| Other error count | {} |",
            summary.other_error_count
        )?;
        writeln!(
            file,
            "| `keep_cache_warm()` alive | {} |",
            summary.keep_cache_warm_alive
        )?;
        writeln!(
            file,
            "| Teardown cancelled cleanly | {} |",
            summary.teardown_cancelled
        )?;
        writeln!(
            file,
            "| Cache-backed reads | {} |",
            summary
                .blockhash_source_counts
                .get("cache")
                .copied()
                .unwrap_or(0)
        )?;
        writeln!(
            file,
            "| RPC fallbacks | {} |",
            summary
                .blockhash_source_counts
                .get("rpc-fallback")
                .copied()
                .unwrap_or(0)
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
        write_latency_section(
            &mut file,
            "Request Latency (micros)",
            &summary.request_latency_micros,
        )?;
    }

    Ok(())
}

fn write_latency_section(
    file: &mut File,
    title: &str,
    summary: &LocalHarnessLatencySummary,
) -> Result<()> {
    writeln!(file, "## {title}")?;
    writeln!(file)?;
    writeln!(file, "| Sample Count | P50 | P95 | P99 |")?;
    writeln!(file, "| ---: | ---: | ---: | ---: |")?;
    writeln!(
        file,
        "| {} | {} | {} | {} |",
        summary.sample_count,
        display_optional_latency(summary.p50_micros),
        display_optional_latency(summary.p95_micros),
        display_optional_latency(summary.p99_micros),
    )?;
    writeln!(file)?;
    Ok(())
}

fn display_optional_latency(value: Option<u64>) -> String {
    value.map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}
