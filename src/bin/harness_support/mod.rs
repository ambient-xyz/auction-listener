use anyhow::{anyhow, bail, Context, Result};
use ambient_auction_api::bundle::RequestBundle;
use ambient_auction_api::{Auction, BundleRegistry, JobRequest, RequestTier, BUNDLE_REGISTRY_SEED};
use ambient_auction_client::sdk::request_job;
use ambient_auction_client::ID as AUCTION_PROGRAM;
use ambient_auction_listener::error::Error as ListenerError;
use ambient_auction_listener::listener::AuctionClient;
use ambient_auction_listener::ID as LISTENER_PROGRAM_ID;
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::message::v0::Message;
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use solana_system_interface::instruction::transfer;
use std::fs::File;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::registry::LookupSpan;

const FALLBACK_WARNING: &str =
    "Recent blockhash cache was empty after warmup wait; falling back to RPC get_latest_blockhash.";

#[derive(Debug, Clone)]
pub struct RepoPaths {
    pub auction_listener_root: PathBuf,
    pub validator_root: PathBuf,
    pub agave_root: PathBuf,
}

impl RepoPaths {
    pub fn discover(
        auction_listener_root: Option<PathBuf>,
        validator_root: Option<PathBuf>,
        agave_root: Option<PathBuf>,
    ) -> Result<Self> {
        let auction_listener_root = canonicalize_existing(
            &auction_listener_root.unwrap_or_else(default_auction_listener_root),
            "AUCTION_LISTENER_ROOT",
        )?;
        let validator_root = canonicalize_existing(
            &validator_root.unwrap_or_else(|| auction_listener_root.join("../ambient")),
            "VALIDATOR_ROOT",
        )?;
        let agave_root = canonicalize_existing(
            &agave_root.unwrap_or_else(|| validator_root.join("../agave")),
            "AGAVE_ROOT",
        )?;
        Ok(Self {
            auction_listener_root,
            validator_root,
            agave_root,
        })
    }

    pub fn validator_script(&self) -> PathBuf {
        self.validator_root.join("auction/run-validator.sh")
    }

    pub fn yellowstone_plugin(&self) -> PathBuf {
        self.validator_root.join(
            "auction/yellowstone-config/yellowstone-grpc-geyser-release/lib/libyellowstone_grpc_geyser.so",
        )
    }

    pub fn payer_keypair(&self) -> PathBuf {
        self.validator_root
            .join("auction/program/target/test-ledger/faucet-keypair.json")
    }
}

#[derive(Debug)]
pub struct ValidatorStack {
    child: Child,
    pub rpc_url: String,
    pub yellowstone_url: String,
    pub payer_keypair: PathBuf,
    pub validator_log: PathBuf,
    pub init_bundles_log: PathBuf,
}

impl ValidatorStack {
    pub async fn start(
        repo_paths: &RepoPaths,
        run_dir: &Path,
        rpc_url: &str,
        yellowstone_url: &str,
    ) -> Result<Self> {
        let validator_log = run_dir.join("validator.log");
        let init_bundles_log = run_dir.join("init-bundles.log");
        let payer_keypair = repo_paths.payer_keypair();

        let validator_stdout = File::create(&validator_log)
            .with_context(|| format!("failed to create {}", validator_log.display()))?;
        let validator_stderr = validator_stdout
            .try_clone()
            .with_context(|| format!("failed to clone {}", validator_log.display()))?;

        let child = Command::new(repo_paths.validator_script())
            .current_dir(&repo_paths.validator_root)
            .stdout(Stdio::from(validator_stdout))
            .stderr(Stdio::from(validator_stderr))
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start validator helper {}",
                    repo_paths.validator_script().display()
                )
            })?;

        wait_for_rpc(rpc_url, Duration::from_secs(120))
            .await
            .with_context(|| format!("timed out waiting for RPC at {rpc_url}"))?;
        wait_for_tcp(yellowstone_url, Duration::from_secs(120))
            .await
            .with_context(|| format!("timed out waiting for Yellowstone at {yellowstone_url}"))?;

        let init_bundles_stdout = File::create(&init_bundles_log)
            .with_context(|| format!("failed to create {}", init_bundles_log.display()))?;
        let init_bundles_stderr = init_bundles_stdout
            .try_clone()
            .with_context(|| format!("failed to clone {}", init_bundles_log.display()))?;

        let init_status = Command::new("cargo")
            .arg("run")
            .arg("--manifest-path")
            .arg(repo_paths.auction_listener_root.join("Cargo.toml"))
            .arg("--bin")
            .arg("init-bundles")
            .arg("--")
            .arg("-r")
            .arg(rpc_url)
            .arg(&payer_keypair)
            .env("CARGO_TARGET_DIR", run_dir.join("init-bundles-target"))
            .stdout(Stdio::from(init_bundles_stdout))
            .stderr(Stdio::from(init_bundles_stderr))
            .status()
            .context("failed to run init-bundles")?;

        if !init_status.success() {
            let mut child = child;
            let _ = stop_child(&mut child);
            bail!("init-bundles failed; see {}", init_bundles_log.display());
        }

        Ok(Self {
            child,
            rpc_url: rpc_url.to_string(),
            yellowstone_url: yellowstone_url.to_string(),
            payer_keypair,
            validator_log,
            init_bundles_log,
        })
    }

    pub fn stop(&mut self) -> Result<()> {
        stop_child(&mut self.child)
    }
}

impl Drop for ValidatorStack {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySummary {
    pub sample_count: usize,
    pub p50_micros: Option<u64>,
    pub p95_micros: Option<u64>,
    pub p99_micros: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub requests_attempted: usize,
    pub requests_succeeded: usize,
    pub requests_failed: usize,
    pub concurrency: usize,
    pub block_not_available_yet_count: usize,
    pub other_error_count: usize,
    pub first_blocking_error: Option<String>,
    pub keep_cache_warm_alive: bool,
    pub fallback_warning_count: usize,
    pub blockhash_acquisition_latency_micros: LatencySummary,
    pub request_latency_micros: LatencySummary,
}

#[derive(Debug, Clone)]
pub struct ValidationConfig {
    pub request_count: usize,
    pub concurrency: usize,
    pub additional_bundles: Option<u64>,
    pub max_price: u64,
    pub max_price_per_output_token: u64,
    pub max_output_tokens: u64,
    pub context_length_tier: RequestTier,
    pub expiry_duration_tier: RequestTier,
    pub prompt: String,
}

impl ValidationConfig {
    pub fn default_prompt() -> String {
        "Reply with the single word ambient.".to_string()
    }
}

#[derive(Clone, Default)]
pub struct FallbackWarningCounter {
    count: Arc<AtomicU64>,
}

impl FallbackWarningCounter {
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Default)]
pub struct FallbackWarningLayer {
    counter: FallbackWarningCounter,
}

impl FallbackWarningLayer {
    pub fn new(counter: FallbackWarningCounter) -> Self {
        Self { counter }
    }
}

impl<S> Layer<S> for FallbackWarningLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        if *event.metadata().level() != Level::WARN {
            return;
        }

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let Some(message) = visitor.message else {
            return;
        };

        if message.contains(FALLBACK_WARNING) {
            self.counter.count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }
}

#[derive(Clone)]
struct RequestWorkloadContext {
    input_hash: [u8; 32],
    prompt_len: u64,
    auction_lamports: u64,
    bundle_lamports: u64,
}

#[derive(Debug)]
struct RequestOutcome {
    blockhash_latency_micros: u64,
    request_latency_micros: u64,
}

pub fn default_auction_listener_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn default_run_dir(root: &Path, prefix: &str) -> PathBuf {
    root.join("target").join(prefix).join(timestamp_string())
}

pub fn preflight_linux(repo_paths: &RepoPaths) -> Result<()> {
    if std::env::consts::OS != "linux" {
        bail!("the local validator harness is supported only on Linux");
    }

    require_command("cargo")?;
    require_command("git")?;
    require_command("git-lfs")?;
    require_command("solana")?;

    let solana_status = Command::new("solana")
        .arg("address")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to run `solana address`")?;
    if !solana_status.success() {
        bail!("unable to resolve a default Solana signer via `solana address`");
    }

    if !repo_paths
        .agave_root
        .join("sdk/program/Cargo.toml")
        .is_file()
    {
        bail!(
            "expected Agave source tree at {}",
            repo_paths
                .agave_root
                .join("sdk/program/Cargo.toml")
                .display()
        );
    }

    if !repo_paths.validator_script().is_file() {
        bail!(
            "missing validator helper at {}",
            repo_paths.validator_script().display()
        );
    }

    if !repo_paths.yellowstone_plugin().is_file() {
        bail!(
            "missing Yellowstone plugin at {}",
            repo_paths.yellowstone_plugin().display()
        );
    }

    if !repo_paths.payer_keypair().is_file() {
        bail!(
            "missing payer keypair at {}",
            repo_paths.payer_keypair().display()
        );
    }

    Ok(())
}

pub async fn build_client(stack: &ValidatorStack) -> Result<Arc<AuctionClient>> {
    let client = AuctionClient::new(
        LISTENER_PROGRAM_ID,
        stack.rpc_url.clone(),
        Some(stack.payer_keypair.clone()),
        Pubkey::default(),
        None,
        stack.yellowstone_url.clone(),
    )
    .await
    .context("failed to create AuctionClient")?;
    Ok(Arc::new(client))
}

pub async fn warm_recent_blockhash_cache(
    client: &Arc<AuctionClient>,
    fallback_counter: &FallbackWarningCounter,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut sequence = 0u64;

    while Instant::now() < deadline {
        send_self_transfer(client, sequence).await?;
        let before = fallback_counter.count();
        client
            .get_recent_blockhash_cached()
            .await
            .context("failed to fetch recent blockhash during warmup")?;
        if fallback_counter.count() == before {
            return Ok(());
        }
        sequence = sequence.wrapping_add(1);
        sleep(Duration::from_millis(100)).await;
    }

    bail!("recent blockhash cache did not warm without RPC fallback")
}

pub async fn run_cache_validation(
    client: Arc<AuctionClient>,
    config: ValidationConfig,
    fallback_counter: FallbackWarningCounter,
) -> Result<ValidationSummary> {
    let keep_cache_warm_task = tokio::spawn({
        let client = client.clone();
        async move { client.keep_cache_warm().await }
    });

    warm_recent_blockhash_cache(&client, &fallback_counter).await?;
    let fallback_baseline = fallback_counter.count();
    let workload_context = prepare_request_context(&client, &config).await?;

    let outcomes = stream::iter(0..config.request_count)
        .map(|index| {
            let client = client.clone();
            let config = config.clone();
            let workload_context = workload_context.clone();
            async move { run_single_cache_request(index, client, &config, &workload_context).await }
        })
        .buffer_unordered(config.concurrency)
        .collect::<Vec<_>>()
        .await;

    let keep_cache_warm_alive = !keep_cache_warm_task.is_finished();
    let fallback_warning_count =
        fallback_counter.count().saturating_sub(fallback_baseline) as usize;

    if keep_cache_warm_alive {
        keep_cache_warm_task.abort();
    }

    summarize_validation(
        &config,
        outcomes,
        keep_cache_warm_alive,
        fallback_warning_count,
    )
}

fn summarize_validation(
    config: &ValidationConfig,
    outcomes: Vec<Result<RequestOutcome>>,
    keep_cache_warm_alive: bool,
    fallback_warning_count: usize,
) -> Result<ValidationSummary> {
    let mut requests_succeeded = 0usize;
    let mut block_not_available_yet_count = 0usize;
    let mut other_error_count = 0usize;
    let mut first_blocking_error = None;
    let mut request_latencies = Vec::with_capacity(outcomes.len());
    let mut blockhash_latencies = Vec::with_capacity(outcomes.len());

    for outcome in outcomes {
        match outcome {
            Ok(outcome) => {
                requests_succeeded += 1;
                request_latencies.push(outcome.request_latency_micros);
                blockhash_latencies.push(outcome.blockhash_latency_micros);
            }
            Err(error) => {
                let error_text = format!("{error:#}");
                if error_text.contains("block is not available yet") {
                    block_not_available_yet_count += 1;
                } else {
                    other_error_count += 1;
                }
                if first_blocking_error.is_none() {
                    first_blocking_error = Some(error_text);
                }
            }
        }
    }

    if !keep_cache_warm_alive && first_blocking_error.is_none() {
        first_blocking_error = Some("keep_cache_warm exited before the workload completed".into());
    }

    Ok(ValidationSummary {
        requests_attempted: config.request_count,
        requests_succeeded,
        requests_failed: config.request_count.saturating_sub(requests_succeeded),
        concurrency: config.concurrency,
        block_not_available_yet_count,
        other_error_count,
        first_blocking_error,
        keep_cache_warm_alive,
        fallback_warning_count,
        blockhash_acquisition_latency_micros: summarize_latencies(&blockhash_latencies),
        request_latency_micros: summarize_latencies(&request_latencies),
    })
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

async fn prepare_request_context(
    client: &Arc<AuctionClient>,
    config: &ValidationConfig,
) -> Result<RequestWorkloadContext> {
    let balance = client
        .rpc_client
        .get_balance_with_commitment(&client.keypair.pubkey(), CommitmentConfig::confirmed())
        .await
        .context("failed to fetch payer balance")?
        .value;
    if balance < config.max_price {
        bail!(
            "payer balance {balance} is below configured max price {}",
            config.max_price
        );
    }

    let input_hash = hash_prompt(&config.prompt);
    let prompt_len = config.prompt.len() as u64;
    let auction_lamports = client
        .rpc_client
        .get_minimum_balance_for_rent_exemption(Auction::LEN)
        .await
        .context("failed to fetch auction rent exemption")?;
    let bundle_lamports = client
        .rpc_client
        .get_minimum_balance_for_rent_exemption(RequestBundle::LEN)
        .await
        .context("failed to fetch bundle rent exemption")?;

    Ok(RequestWorkloadContext {
        input_hash,
        prompt_len,
        auction_lamports,
        bundle_lamports,
    })
}

fn hash_prompt(prompt: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut output = [0u8; 32];
    output.copy_from_slice(&Sha256::digest(prompt.as_bytes()));
    output
}

async fn run_single_cache_request(
    request_index: usize,
    client: Arc<AuctionClient>,
    config: &ValidationConfig,
    workload: &RequestWorkloadContext,
) -> Result<RequestOutcome> {
    let request_started = Instant::now();
    let bundle = latest_bundle_for_tiers(
        &client,
        config.context_length_tier,
        config.expiry_duration_tier,
    )
    .await?;

    let (ready_tx, ready_rx) = oneshot::channel();
    let auction_waiter = tokio::spawn({
        let client = client.clone();
        async move { client.wait_for_bundle_to_be_auctioned(bundle, ready_tx).await }
    });
    ready_rx
        .await
        .map_err(|_| anyhow!("failed to subscribe to bundle auction updates"))?;

    let ix = build_request_job_instruction(request_index, &client, config, workload, bundle);
    let job_request_key = ix.accounts[1].pubkey;
    let payer_key = client.keypair.pubkey();

    let blockhash_started = Instant::now();
    let recent_blockhash = client
        .get_recent_blockhash_cached()
        .await
        .context("failed to fetch cached recent blockhash")?;
    let blockhash_latency_micros = blockhash_started.elapsed().as_micros() as u64;

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            Message::try_compile(&payer_key, &[ix], &[], recent_blockhash)
                .context("failed to compile request transaction")?,
        ),
        &[&client.keypair],
    )
    .context("failed to sign request transaction")?;

    if let Err(error) = client
        .rpc_client
        .send_transaction_with_config(
            &tx,
            RpcSendTransactionConfig {
                skip_preflight: true,
                max_retries: Some(0),
                ..Default::default()
            },
        )
        .await
    {
        auction_waiter.abort();
        return Err(anyhow!("failed to send request transaction: {error}"));
    }

    wait_for_job_request_account(&client, job_request_key, Duration::from_secs(20)).await?;
    wait_for_auction(auction_waiter).await?;

    Ok(RequestOutcome {
        blockhash_latency_micros,
        request_latency_micros: request_started.elapsed().as_micros() as u64,
    })
}

fn build_request_job_instruction(
    request_index: usize,
    client: &Arc<AuctionClient>,
    config: &ValidationConfig,
    workload: &RequestWorkloadContext,
    bundle: Pubkey,
) -> solana_sdk::instruction::Instruction {
    let seed = Keypair::new().pubkey();
    let _ = request_index;
    request_job(
        client.keypair.pubkey(),
        workload.input_hash,
        None,
        seed.to_bytes(),
        workload.prompt_len,
        config.max_output_tokens,
        workload.bundle_lamports,
        workload.auction_lamports,
        bundle,
        config.max_price_per_output_token,
        config.context_length_tier,
        config.expiry_duration_tier,
        None,
        config.additional_bundles,
    )
}

async fn latest_bundle_for_tiers(
    client: &Arc<AuctionClient>,
    context_length_tier: RequestTier,
    expiry_duration_tier: RequestTier,
) -> Result<Pubkey> {
    let (registry_pubkey, _) = Pubkey::find_program_address(
        &[
            BUNDLE_REGISTRY_SEED,
            (context_length_tier as u64).to_le_bytes().as_ref(),
            (expiry_duration_tier as u64).to_le_bytes().as_ref(),
        ],
        &AUCTION_PROGRAM,
    );

    let registry: BundleRegistry = client
        .get_bundle_registry_cached(&registry_pubkey)
        .await
        .with_context(|| format!("failed to fetch bundle registry {registry_pubkey}"))?;
    let latest_bundle = Pubkey::new_from_array(registry.latest_bundle.inner());
    if latest_bundle == Pubkey::default() {
        bail!("bundle registry {registry_pubkey} did not contain a latest bundle");
    }
    Ok(latest_bundle)
}

async fn wait_for_job_request_account(
    client: &Arc<AuctionClient>,
    job_request_key: Pubkey,
    timeout_duration: Duration,
) -> Result<JobRequest> {
    let deadline = Instant::now() + timeout_duration;

    while Instant::now() < deadline {
        match client.get_account::<JobRequest>(&job_request_key).await {
            Ok(job_request) => return Ok(job_request),
            Err(ListenerError::AccountNotExist(_, _)) => sleep(Duration::from_millis(50)).await,
            Err(error) => return Err(anyhow!("failed to fetch JobRequest {job_request_key}: {error}")),
        }
    }

    bail!("timed out waiting for JobRequest account {job_request_key}")
}

async fn wait_for_auction(auction_waiter: JoinHandle<Result<Pubkey, ambient_auction_listener::run::Error>>) -> Result<Pubkey> {
    tokio::time::timeout(Duration::from_secs(20), async move {
        auction_waiter
            .await
            .map_err(|error| anyhow!("bundle auction waiter panicked: {error}"))?
            .map_err(|error| anyhow!("bundle auction waiter failed: {error}"))
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for bundle auction"))?
}

async fn send_self_transfer(client: &Arc<AuctionClient>, sequence: u64) -> Result<()> {
    let recent_blockhash = client
        .rpc_client
        .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
        .await
        .context("failed to fetch processed blockhash for warmup transfer")?
        .0;

    let instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_price(sequence),
        transfer(&client.keypair.pubkey(), &client.keypair.pubkey(), 1),
    ];
    let tx = Transaction::new_signed_with_payer(
        &instructions,
        Some(&client.keypair.pubkey()),
        &[&client.keypair],
        recent_blockhash,
    );

    client
        .rpc_client
        .send_transaction_with_config(
            &tx,
            RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Processed),
                max_retries: Some(0),
                ..Default::default()
            },
        )
        .await
        .context("failed to submit warmup transfer")?;

    Ok(())
}

fn canonicalize_existing(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.exists() {
        bail!("{label} does not exist at {}", path.display());
    }
    path.canonicalize()
        .with_context(|| format!("failed to canonicalize {}", path.display()))
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

fn stop_child(child: &mut Child) -> Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

fn timestamp_string() -> String {
    if let Ok(output) = Command::new("date").arg("+%Y%m%dT%H%M%S").output() {
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
        .to_string()
}

async fn wait_for_rpc(rpc_url: &str, timeout_duration: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + timeout_duration;

    while Instant::now() < deadline {
        let response = client
            .post(rpc_url)
            .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"getSlot"}))
            .send()
            .await;
        if matches!(response, Ok(ref resp) if resp.status().is_success()) {
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }

    bail!("RPC endpoint did not become ready in time")
}

async fn wait_for_tcp(endpoint: &str, timeout_duration: Duration) -> Result<()> {
    let url = reqwest::Url::parse(endpoint).with_context(|| format!("invalid URL `{endpoint}`"))?;
    let host = url.host_str().context("Yellowstone URL missing host")?;
    let port = url.port_or_known_default().context("Yellowstone URL missing port")?;
    let socket_addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .with_context(|| format!("failed to parse {host}:{port}"))?;
    let deadline = Instant::now() + timeout_duration;

    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&socket_addr, Duration::from_millis(250)).is_ok() {
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }

    bail!("Yellowstone gRPC endpoint did not become ready in time")
}
