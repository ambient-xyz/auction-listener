use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use serde::Serialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::signature::{read_keypair_file, Keypair};
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_system_interface::instruction::transfer;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use ambient_auction_listener::yellowstone_grpc::CloneableGeyserGrpcClient;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,
    #[arg(long, default_value = "http://127.0.0.1:10000")]
    yellowstone_url: String,
    #[arg(long)]
    payer_keypair: PathBuf,
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    #[arg(long, default_value_t = 64)]
    unary_concurrency: usize,
    #[arg(long, default_value_t = 8)]
    slot_spam_concurrency: usize,
    #[arg(long, default_value_t = 1)]
    transfer_lamports: u64,
    #[arg(long)]
    output_json: Option<PathBuf>,
}

#[derive(Default)]
struct SharedStats {
    requests_attempted: AtomicU64,
    success_count: AtomicU64,
    block_not_available_yet_count: AtomicU64,
    other_error_count: AtomicU64,
    parse_error_count: AtomicU64,
    slot_spam_success_count: AtomicU64,
    slot_spam_error_count: AtomicU64,
    first_block_not_available_sample: Mutex<Option<String>>,
    first_other_error_sample: Mutex<Option<String>>,
    first_parse_error_sample: Mutex<Option<String>>,
}

#[derive(Debug, Serialize)]
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

fn record_first_sample(slot: &Mutex<Option<String>>, sample: String) {
    let mut guard = slot.lock().expect("stats mutex poisoned");
    if guard.is_none() {
        *guard = Some(sample);
    }
}

fn is_block_not_available_error(message: &str) -> bool {
    message.contains("block is not available yet")
}

async fn run_unary_worker(
    client: CloneableGeyserGrpcClient,
    deadline: Instant,
    stats: Arc<SharedStats>,
) {
    let mut client = client.0;
    while Instant::now() < deadline {
        stats.requests_attempted.fetch_add(1, Ordering::Relaxed);
        match client
            .get_latest_blockhash(Some(CommitmentLevel::Processed))
            .await
        {
            Ok(response) => match Hash::from_str(&response.blockhash) {
                Ok(_) => {
                    stats.success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    stats.parse_error_count.fetch_add(1, Ordering::Relaxed);
                    record_first_sample(
                        &stats.first_parse_error_sample,
                        format!(
                            "failed to parse blockhash '{}': {error}",
                            response.blockhash
                        ),
                    );
                }
            },
            Err(error) => {
                let message = error.to_string();
                if is_block_not_available_error(&message) {
                    stats
                        .block_not_available_yet_count
                        .fetch_add(1, Ordering::Relaxed);
                    record_first_sample(&stats.first_block_not_available_sample, message);
                } else {
                    stats.other_error_count.fetch_add(1, Ordering::Relaxed);
                    record_first_sample(&stats.first_other_error_sample, message);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }
}

async fn run_slot_spam_worker(
    worker_id: usize,
    rpc_client: Arc<RpcClient>,
    payer: Arc<Keypair>,
    transfer_lamports: u64,
    deadline: Instant,
    stats: Arc<SharedStats>,
) {
    let mut sequence = 0u64;

    while Instant::now() < deadline {
        let recent_blockhash = match rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .await
        {
            Ok((blockhash, _)) => blockhash,
            Err(_) => {
                stats.slot_spam_error_count.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
        };

        let unique_cu_price = ((worker_id as u64) << 32) | sequence;
        let instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_price(unique_cu_price),
            transfer(&payer.pubkey(), &payer.pubkey(), transfer_lamports),
        ];
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&payer.pubkey()),
            &[payer.as_ref()],
            recent_blockhash,
        );

        match rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(
                        solana_sdk::commitment_config::CommitmentLevel::Processed,
                    ),
                    encoding: None,
                    max_retries: Some(0),
                    min_context_slot: None,
                },
            )
            .await
        {
            Ok(_) => {
                stats
                    .slot_spam_success_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                stats.slot_spam_error_count.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        sequence = sequence.wrapping_add(1);
    }
}

fn build_summary(args: &Args, stats: &SharedStats) -> UnaryHammerSummary {
    UnaryHammerSummary {
        rpc_url: args.rpc_url.clone(),
        yellowstone_url: args.yellowstone_url.clone(),
        duration_secs: args.duration_secs,
        unary_concurrency: args.unary_concurrency,
        slot_spam_concurrency: args.slot_spam_concurrency,
        transfer_lamports: args.transfer_lamports,
        requests_attempted: stats.requests_attempted.load(Ordering::Relaxed),
        success_count: stats.success_count.load(Ordering::Relaxed),
        block_not_available_yet_count: stats.block_not_available_yet_count.load(Ordering::Relaxed),
        other_error_count: stats.other_error_count.load(Ordering::Relaxed),
        parse_error_count: stats.parse_error_count.load(Ordering::Relaxed),
        slot_spam_success_count: stats.slot_spam_success_count.load(Ordering::Relaxed),
        slot_spam_error_count: stats.slot_spam_error_count.load(Ordering::Relaxed),
        first_block_not_available_sample: stats
            .first_block_not_available_sample
            .lock()
            .expect("stats mutex poisoned")
            .clone(),
        first_other_error_sample: stats
            .first_other_error_sample
            .lock()
            .expect("stats mutex poisoned")
            .clone(),
        first_parse_error_sample: stats
            .first_parse_error_sample
            .lock()
            .expect("stats mutex poisoned")
            .clone(),
    }
}

fn maybe_write_summary(path: &Option<PathBuf>, summary: &UnaryHammerSummary) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory for {}", path.display()))?;
    }

    let file = File::create(path)
        .with_context(|| format!("failed to create summary file {}", path.display()))?;
    serde_json::to_writer_pretty(file, summary)
        .with_context(|| format!("failed to write summary file {}", path.display()))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let payer = Arc::new(read_keypair_file(&args.payer_keypair).map_err(|error| {
        anyhow::anyhow!("failed to read {}: {error}", args.payer_keypair.display())
    })?);
    let rpc_client = Arc::new(RpcClient::new_with_timeout(
        args.rpc_url.clone(),
        Duration::from_secs(30),
    ));
    let geyser_client = CloneableGeyserGrpcClient::new(args.yellowstone_url.clone())
        .await
        .context("failed to connect to Yellowstone gRPC")?;

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let stats = Arc::new(SharedStats::default());
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..args.unary_concurrency {
        tasks.push(tokio::spawn(run_unary_worker(
            geyser_client.clone(),
            deadline,
            stats.clone(),
        )));
    }

    for worker_id in 0..args.slot_spam_concurrency {
        tasks.push(tokio::spawn(run_slot_spam_worker(
            worker_id,
            rpc_client.clone(),
            payer.clone(),
            args.transfer_lamports,
            deadline,
            stats.clone(),
        )));
    }

    for task in tasks {
        task.await.context("hammer task panicked")?;
    }

    let summary = build_summary(&args, &stats);
    maybe_write_summary(&args.output_json, &summary)?;

    println!(
        "Unary repro | attempted={} success={} block_not_available_yet={} other_errors={} parse_errors={} slot_spam_success={} slot_spam_errors={} result={} first_blocking_error={}",
        summary.requests_attempted,
        summary.success_count,
        summary.block_not_available_yet_count,
        summary.other_error_count,
        summary.parse_error_count,
        summary.slot_spam_success_count,
        summary.slot_spam_error_count,
        summary.result(),
        summary.first_blocking_error().unwrap_or("none"),
    );

    Ok(())
}
