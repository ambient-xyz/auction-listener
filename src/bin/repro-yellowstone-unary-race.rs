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
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

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
    #[arg(long, default_value_t = 60)]
    hammer_duration_secs: u64,
    #[arg(long, default_value_t = 64)]
    unary_concurrency: usize,
    #[arg(long, default_value_t = 8)]
    slot_spam_concurrency: usize,
    #[arg(long, default_value_t = 1)]
    transfer_lamports: u64,
    #[arg(long, default_value_t = 75)]
    stress_timeout_secs: u64,
    #[arg(long, default_value_t = 50)]
    validation_request_count: usize,
    #[arg(long, default_value_t = 5)]
    validation_concurrency: usize,
    #[arg(long, default_value_t = 8)]
    validation_additional_bundles: usize,
    #[arg(long, default_value_t = 100)]
    validation_max_price: u64,
    #[arg(long)]
    skip_fixed_path_validation: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct UnaryHammerSummary {
    rpc_url: String,
    yellowstone_url: String,
    duration_secs: u64,
    unary_concurrency: usize,
    slot_spam_concurrency: usize,
    transfer_lamports: u64,
    requests_attempted: u64,
    success_count: u64,
    block_not_available_yet_count: u64,
    other_error_count: u64,
    parse_error_count: u64,
    slot_spam_success_count: u64,
    slot_spam_error_count: u64,
    first_block_not_available_sample: Option<String>,
    first_other_error_sample: Option<String>,
    first_parse_error_sample: Option<String>,
}

impl UnaryHammerSummary {
    fn result(&self) -> &'static str {
        if self.block_not_available_yet_count > 0 {
            "reproduced"
        } else if self.other_error_count > 0 || self.parse_error_count > 0 {
            "other-errors"
        } else {
            "inconclusive"
        }
    }

    fn first_blocking_error(&self) -> Option<&str> {
        self.first_block_not_available_sample
            .as_deref()
            .or(self.first_other_error_sample.as_deref())
            .or(self.first_parse_error_sample.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LocalHarnessLatencySummary {
    count: usize,
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
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

#[derive(Debug, Clone, Serialize)]
struct UnaryRoundResult {
    round: usize,
    result: String,
    summary_path: PathBuf,
    log_path: PathBuf,
    validator_log: PathBuf,
    init_bundles_log: PathBuf,
    stress_log: PathBuf,
    summary: UnaryHammerSummary,
}

#[derive(Debug, Clone, Serialize)]
struct FixedPathValidationResult {
    status: String,
    summary_path: Option<PathBuf>,
    markdown_path: Option<PathBuf>,
    log_path: PathBuf,
    summary: Option<LocalHarnessRunSummary>,
}

#[derive(Debug, Serialize)]
struct ReproSummary {
    run_dir: PathBuf,
    unary_result: String,
    unary_rounds_completed: usize,
    unary_rounds: Vec<UnaryRoundResult>,
    fixed_path_validation: Option<FixedPathValidationResult>,
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
    require_command("stress-ng")?;

    let run_dir = args.run_dir.clone().unwrap_or_else(|| {
        default_run_dir(&repo_paths.auction_listener_root, "yellowstone-unary-race")
    });
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create {}", run_dir.display()))?;

    let cargo_context = PreparedCargoContext::prepare(&repo_paths, &run_dir)?;
    let cargo_target_dir = run_dir.join("cargo-target");
    fs::create_dir_all(&cargo_target_dir)
        .with_context(|| format!("failed to create {}", cargo_target_dir.display()))?;
    build_binaries(&repo_paths, &cargo_context, &cargo_target_dir)?;

    let hammer_bin = cargo_target_dir.join("debug/hammer-yellowstone-unary-blockhash");
    let validate_bin = cargo_target_dir.join("debug/validate-blockhash-cache-local");
    let unary_root = run_dir.join("unary-repro");
    fs::create_dir_all(&unary_root)
        .with_context(|| format!("failed to create {}", unary_root.display()))?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    let mut unary_rounds = Vec::new();
    let mut unary_result = "inconclusive".to_string();
    for round in 1..=args.rounds {
        let round_dir = unary_root.join(format!("round-{round:02}"));
        fs::create_dir_all(&round_dir)
            .with_context(|| format!("failed to create {}", round_dir.display()))?;
        let round_result = rt.block_on(run_unary_round(
            &args,
            &repo_paths,
            &cargo_context,
            &hammer_bin,
            &round_dir,
        ))?;
        if round_result.summary.block_not_available_yet_count > 0 {
            unary_result = "reproduced".to_string();
        }
        unary_rounds.push(round_result);
        if unary_result == "reproduced" {
            break;
        }
    }

    let fixed_path_validation = if args.skip_fixed_path_validation {
        None
    } else {
        Some(run_fixed_path_validation(
            &args,
            &repo_paths,
            &validate_bin,
            &run_dir.join("fixed-path-validation"),
        )?)
    };

    let summary = ReproSummary {
        run_dir: run_dir.clone(),
        unary_result,
        unary_rounds_completed: unary_rounds.len(),
        unary_rounds,
        fixed_path_validation,
    };

    let summary_json_path = run_dir.join("summary.json");
    let summary_md_path = run_dir.join("summary.md");
    serde_json::to_writer_pretty(
        File::create(&summary_json_path)
            .with_context(|| format!("failed to create {}", summary_json_path.display()))?,
        &summary,
    )
    .with_context(|| format!("failed to write {}", summary_json_path.display()))?;
    write_summary_markdown(&summary_md_path, &summary)?;

    println!("repro artifacts: {}", run_dir.display());
    Ok(())
}

fn build_binaries(
    repo_paths: &RepoPaths,
    cargo_context: &PreparedCargoContext,
    cargo_target_dir: &Path,
) -> Result<()> {
    let status = Command::new("cargo")
        .arg("--config")
        .arg(&cargo_context.config_path)
        .arg("build")
        .arg("--manifest-path")
        .arg(repo_paths.auction_listener_root.join("Cargo.toml"))
        .arg("--bin")
        .arg("hammer-yellowstone-unary-blockhash")
        .arg("--bin")
        .arg("validate-blockhash-cache-local")
        .env("CARGO_TARGET_DIR", cargo_target_dir)
        .status()
        .context("failed to build repro binaries")?;
    if !status.success() {
        bail!("failed to build repro binaries");
    }
    Ok(())
}

async fn run_unary_round(
    args: &Args,
    repo_paths: &RepoPaths,
    cargo_context: &PreparedCargoContext,
    hammer_bin: &Path,
    round_dir: &Path,
) -> Result<UnaryRoundResult> {
    let mut validator = ValidatorStack::start(
        repo_paths,
        cargo_context,
        round_dir,
        &args.rpc_url,
        &args.yellowstone_url,
    )
    .await?;
    let validator_log = validator.validator_log.clone();
    let init_bundles_log = validator.init_bundles_log.clone();
    let stress_log = round_dir.join("stress.log");
    let hammer_log = round_dir.join("hammer.log");
    let summary_path = round_dir.join("hammer.json");

    let mut stress = start_stress_ng(&stress_log, args.stress_timeout_secs)?;
    let hammer_status = run_hammer_binary(hammer_bin, &validator, args, &summary_path, &hammer_log);
    let stress_stop = stop_child(&mut stress);
    let validator_stop = validator.stop();

    stress_stop?;
    validator_stop?;
    hammer_status?;

    let raw = fs::read_to_string(&summary_path)
        .with_context(|| format!("failed to read {}", summary_path.display()))?;
    let summary: UnaryHammerSummary = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", summary_path.display()))?;

    Ok(UnaryRoundResult {
        round: parse_round_index(round_dir)?,
        result: summary.result().to_string(),
        summary_path,
        log_path: hammer_log,
        validator_log,
        init_bundles_log,
        stress_log,
        summary,
    })
}

fn run_hammer_binary(
    hammer_bin: &Path,
    validator: &ValidatorStack,
    args: &Args,
    summary_path: &Path,
    log_path: &Path,
) -> Result<()> {
    let stdout = File::create(log_path)
        .with_context(|| format!("failed to create {}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", log_path.display()))?;
    let status = Command::new(hammer_bin)
        .arg("--rpc-url")
        .arg(&validator.rpc_url)
        .arg("--yellowstone-url")
        .arg(&validator.yellowstone_url)
        .arg("--payer-keypair")
        .arg(&validator.payer_keypair)
        .arg("--duration-secs")
        .arg(args.hammer_duration_secs.to_string())
        .arg("--unary-concurrency")
        .arg(args.unary_concurrency.to_string())
        .arg("--slot-spam-concurrency")
        .arg(args.slot_spam_concurrency.to_string())
        .arg("--transfer-lamports")
        .arg(args.transfer_lamports.to_string())
        .arg("--output-json")
        .arg(summary_path)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .status()
        .with_context(|| format!("failed to run {}", hammer_bin.display()))?;
    if !status.success() {
        bail!("hammer binary failed; see {}", log_path.display());
    }
    Ok(())
}

fn run_fixed_path_validation(
    args: &Args,
    repo_paths: &RepoPaths,
    validate_bin: &Path,
    validation_dir: &Path,
) -> Result<FixedPathValidationResult> {
    fs::create_dir_all(validation_dir)
        .with_context(|| format!("failed to create {}", validation_dir.display()))?;
    let log_path = validation_dir.join("validate.log");
    let summary_path = validation_dir.join("summary.json");
    let markdown_path = validation_dir.join("summary.md");
    let stress_log = validation_dir.join("stress.log");

    let stdout = File::create(&log_path)
        .with_context(|| format!("failed to create {}", log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", log_path.display()))?;
    let mut stress = start_stress_ng(&stress_log, args.stress_timeout_secs)?;
    let status = Command::new(validate_bin)
        .arg("--validator-root")
        .arg(&repo_paths.validator_root)
        .arg("--agave-root")
        .arg(&repo_paths.agave_root)
        .arg("--run-dir")
        .arg(validation_dir)
        .arg("--rpc-url")
        .arg(&args.rpc_url)
        .arg("--yellowstone-url")
        .arg(&args.yellowstone_url)
        .arg("--request-count")
        .arg(args.validation_request_count.to_string())
        .arg("--concurrency")
        .arg(args.validation_concurrency.to_string())
        .arg("--additional-bundles")
        .arg(args.validation_additional_bundles.to_string())
        .arg("--max-price")
        .arg(args.validation_max_price.to_string())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .status()
        .with_context(|| format!("failed to run {}", validate_bin.display()))?;
    stop_child(&mut stress)?;

    let summary = if summary_path.is_file() {
        let raw = fs::read_to_string(&summary_path)
            .with_context(|| format!("failed to read {}", summary_path.display()))?;
        Some(
            serde_json::from_str::<LocalHarnessRunSummary>(&raw)
                .with_context(|| format!("failed to parse {}", summary_path.display()))?,
        )
    } else {
        None
    };

    Ok(FixedPathValidationResult {
        status: if status.success() {
            "pass".to_string()
        } else {
            "fail".to_string()
        },
        summary_path: summary_path.is_file().then_some(summary_path),
        markdown_path: markdown_path.is_file().then_some(markdown_path),
        log_path,
        summary,
    })
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

fn require_command(name: &str) -> Result<()> {
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

fn parse_round_index(round_dir: &Path) -> Result<usize> {
    let name = round_dir
        .file_name()
        .and_then(|value| value.to_str())
        .context("round directory missing filename")?;
    let index = name
        .strip_prefix("round-")
        .context("round directory did not start with `round-`")?;
    index
        .parse::<usize>()
        .with_context(|| format!("failed to parse round index from `{name}`"))
}

fn write_summary_markdown(path: &Path, summary: &ReproSummary) -> Result<()> {
    let mut file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;

    writeln!(file, "# Yellowstone Unary Repro")?;
    writeln!(file)?;
    writeln!(file, "## Unary Repro")?;
    writeln!(file)?;
    writeln!(file, "- Result: `{}`", summary.unary_result)?;
    writeln!(
        file,
        "- Rounds completed: `{}`",
        summary.unary_rounds_completed
    )?;
    writeln!(file)?;
    writeln!(file, "| Round | Result | Attempts | `block is not available yet` | Other Errors | First Blocking Error |")?;
    writeln!(file, "| ---: | --- | ---: | ---: | ---: | --- |")?;
    for round in &summary.unary_rounds {
        writeln!(
            file,
            "| {} | {} | {} | {} | {} | {} |",
            round.round,
            round.result,
            round.summary.requests_attempted,
            round.summary.block_not_available_yet_count,
            round.summary.other_error_count + round.summary.parse_error_count,
            round.summary.first_blocking_error().unwrap_or("none"),
        )?;
    }
    writeln!(file)?;
    for round in &summary.unary_rounds {
        writeln!(file, "### Unary Round {}", round.round)?;
        writeln!(file)?;
        writeln!(file, "- Summary JSON: `{}`", round.summary_path.display())?;
        writeln!(file, "- Hammer log: `{}`", round.log_path.display())?;
        writeln!(file, "- Validator log: `{}`", round.validator_log.display())?;
        writeln!(
            file,
            "- init-bundles log: `{}`",
            round.init_bundles_log.display()
        )?;
        writeln!(file, "- stress-ng log: `{}`", round.stress_log.display())?;
        writeln!(file)?;
    }

    writeln!(file, "## Fixed-Path Validation")?;
    writeln!(file)?;
    match &summary.fixed_path_validation {
        Some(result) => {
            writeln!(file, "- Result: `{}`", result.status)?;
            writeln!(file, "- Log: `{}`", result.log_path.display())?;
            if let Some(path) = &result.markdown_path {
                writeln!(file, "- Summary markdown: `{}`", path.display())?;
            }
            if let Some(path) = &result.summary_path {
                writeln!(file, "- Summary JSON: `{}`", path.display())?;
            }
            if let Some(summary) = &result.summary {
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
                    "| Cache-backed reads | {} |",
                    summary
                        .blockhash_source_counts
                        .get("cache")
                        .copied()
                        .unwrap_or(0)
                )?;
                writeln!(
                    file,
                    "| RPC fallback count | {} |",
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
            }
        }
        None => {
            writeln!(file, "- Skipped")?;
        }
    }

    Ok(())
}
