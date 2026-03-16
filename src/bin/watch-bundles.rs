use ambient_auction_api::{
    BundleRegistry, BundleStatus, JobRequest, RequestBundle, RequestTier, BUNDLE_REGISTRY_SEED,
};
use ambient_auction_listener::yellowstone_grpc::{decode_account_info, reply_to_ping, CloneableGeyserGrpcClient};
use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use futures_util::{future::join_all, StreamExt};
use libc::{ioctl, tcgetattr, tcsetattr, termios, winsize, ECHO, ICANON, STDIN_FILENO, STDOUT_FILENO, TCSANOW, TIOCGWINSZ, VMIN, VTIME};
use serde::Serialize;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::{
    account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey,
};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{BufWriter, IsTerminal, Write},
    mem::size_of,
    path::PathBuf,
    time::{Duration, Instant},
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime, UtcOffset};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter::Filter,
    subscribe_request_filter_accounts_filter_memcmp::Data,
    subscribe_update::UpdateOneof,
    CommitmentLevel as GrpcCommitmentLevel,
    SubscribeRequest,
    SubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp,
    SubscribeRequestFilterSlots,
};

const DEFAULT_RPC_URL: &str = "http://127.0.0.1:8899";
const RATE_LIMIT_REFRESH_MS: u64 = 3_000;
const LAG_THRESHOLD: Duration = Duration::from_secs(2);
const OFFLINE_FAILURE_THRESHOLD: u32 = 2;
const DEFAULT_RPC_TIMEOUT_MS: u64 = 60_000;
const REQUEST_BRICK_TIMEOUT_MS: u64 = 4_000;
const LIVE_REQUEST_BRICK_TIMEOUT_MS: u64 = 1_500;
const REQUEST_BRICK_REFRESH_LIMIT: usize = 3;
const REQUEST_BRICK_CACHE_TTL: Duration = Duration::from_secs(30);
const BRICKS_PER_TILE: usize = 30;
const BRICK_COLUMNS: usize = 6;
const BRICK_ROWS: usize = BRICKS_PER_TILE / BRICK_COLUMNS;
const BRICK_CELL_WIDTH: usize = 4;
const BRICK_LINE_WIDTH: usize = BRICK_COLUMNS * BRICK_CELL_WIDTH + (BRICK_COLUMNS - 1);
const TILE_WIDTH: usize = 29;
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "\x1b[1m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_BLUE: &str = "\x1b[34m";
const ANSI_MAGENTA: &str = "\x1b[35m";
const ANSI_CYAN: &str = "\x1b[36m";
const ANSI_RED: &str = "\x1b[31m";
const ANSI_REDRAW_FRAME: &str = "\x1b[3J\x1b[H\x1b[J";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum TierArg {
    Eco,
    Standard,
    Pro,
}

impl TierArg {
    fn label(self) -> &'static str {
        match self {
            Self::Eco => "eco",
            Self::Standard => "standard",
            Self::Pro => "pro",
        }
    }
}

impl From<TierArg> for RequestTier {
    fn from(value: TierArg) -> Self {
        match value {
            TierArg::Eco => RequestTier::Eco,
            TierArg::Standard => RequestTier::Standard,
            TierArg::Pro => RequestTier::Pro,
        }
    }
}

impl From<RequestTier> for TierArg {
    fn from(value: RequestTier) -> Self {
        match value {
            RequestTier::Eco => TierArg::Eco,
            RequestTier::Standard => TierArg::Standard,
            RequestTier::Pro => TierArg::Pro,
        }
    }
}

#[derive(Clone, Debug)]
struct ExpectedMaxContextArg {
    tier: TierArg,
    value: u64,
}

#[derive(Clone, Debug)]
struct ExpectedBundleCapacityArg {
    tier: TierArg,
    value: u64,
}

fn parse_expected_max_context(value: &str) -> std::result::Result<ExpectedMaxContextArg, String> {
    let (tier, raw_value) = value
        .split_once('=')
        .ok_or_else(|| "expected format <tier>=<u64>".to_string())?;
    let tier = match tier.to_ascii_lowercase().as_str() {
        "eco" => TierArg::Eco,
        "standard" => TierArg::Standard,
        "pro" => TierArg::Pro,
        _ => return Err(format!("unknown tier '{tier}'")),
    };
    let value = raw_value
        .parse::<u64>()
        .map_err(|error| format!("invalid max context '{raw_value}': {error}"))?;
    Ok(ExpectedMaxContextArg { tier, value })
}

fn parse_expected_bundle_capacity(
    value: &str,
) -> std::result::Result<ExpectedBundleCapacityArg, String> {
    let (tier, raw_value) = value
        .split_once('=')
        .ok_or_else(|| "expected format <tier>=<u64>".to_string())?;
    let tier = match tier.to_ascii_lowercase().as_str() {
        "eco" => TierArg::Eco,
        "standard" => TierArg::Standard,
        "pro" => TierArg::Pro,
        _ => return Err(format!("unknown tier '{tier}'")),
    };
    let value = raw_value
        .parse::<u64>()
        .map_err(|error| format!("invalid bundle capacity '{raw_value}': {error}"))?;
    Ok(ExpectedBundleCapacityArg { tier, value })
}

#[derive(Parser, Debug)]
#[command(about = "Watch bundle mix and latest-bundle shifts in real time")]
struct Args {
    #[arg(short = 'r', long, default_value = DEFAULT_RPC_URL)]
    cluster_rpc: String,
    #[arg(short = 'y', long)]
    yellowstone_url: Option<String>,
    #[arg(long)]
    yellowstone_x_token: Option<String>,
    #[arg(long, default_value_t = DEFAULT_RPC_TIMEOUT_MS)]
    rpc_timeout_ms: u64,
    #[arg(long, default_value_t = 1_000)]
    refresh_ms: u64,
    #[arg(long, value_enum)]
    context_tier: Vec<TierArg>,
    #[arg(long, value_enum)]
    duration_tier: Vec<TierArg>,
    #[arg(long, default_value_t = 20)]
    events: usize,
    #[arg(long)]
    once: bool,
    #[arg(long, requires = "once")]
    json: bool,
    #[arg(long, value_parser = parse_expected_max_context)]
    expected_max_context: Vec<ExpectedMaxContextArg>,
    #[arg(long, value_parser = parse_expected_bundle_capacity)]
    expected_bundle_capacity: Vec<ExpectedBundleCapacityArg>,
    #[arg(long)]
    snapshot_log: Option<PathBuf>,
    #[arg(long)]
    events_log: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LaneKey {
    context_tier: TierArg,
    duration_tier: TierArg,
}

impl LaneKey {
    fn label(self) -> String {
        format!(
            "{}/{}",
            self.context_tier.label(),
            self.duration_tier.label()
        )
    }
}

fn short_tier_label(label: &str) -> &str {
    match label {
        "standard" => "std",
        _ => label,
    }
}

fn short_lane_label(context_tier: &str, duration_tier: &str) -> String {
    format!(
        "{}/{}",
        short_tier_label(context_tier),
        short_tier_label(duration_tier)
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum BundleShape {
    Target,
    LegacyCustom,
    Missing,
}

impl BundleShape {
    fn label(self) -> &'static str {
        match self {
            Self::Target => "target",
            Self::LegacyCustom => "legacy/custom",
            Self::Missing => "missing",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RpcStatus {
    Healthy,
    Lagging,
    RateLimited,
    Offline,
}

impl RpcStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Lagging => "lagging",
            Self::RateLimited => "rate_limited",
            Self::Offline => "offline",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RequestBrickStatus {
    Live,
    Stale,
    Aggregate,
}

impl RequestBrickStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Stale => "stale",
            Self::Aggregate => "agg",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct LaneSnapshot {
    lane: String,
    context_tier: &'static str,
    duration_tier: &'static str,
    registry: String,
    latest_bundle: Option<String>,
    bundle_status: Option<&'static str>,
    classification: &'static str,
    max_context_length: Option<u64>,
    bundle_capacity: Option<u64>,
    requests_len: Option<u64>,
    total_input_tokens: Option<u64>,
    maximum_output_tokens: Option<u64>,
    child_bundle: Option<String>,
    request_input_tokens: Vec<u64>,
    request_brick_status: RequestBrickStatus,
}

#[derive(Clone, Debug, Default, Serialize)]
struct SnapshotSummary {
    target_lanes: usize,
    legacy_custom_lanes: usize,
    missing_lanes: usize,
    max_context_distribution: BTreeMap<String, usize>,
    target_requests_len: u64,
    legacy_custom_requests_len: u64,
    target_total_input_tokens: u64,
    legacy_custom_total_input_tokens: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ClusterSnapshot {
    observed_at: String,
    slot: Option<u64>,
    rpc_status: RpcStatus,
    refresh_ms: u64,
    lanes: Vec<LaneSnapshot>,
    summary: SnapshotSummary,
}

impl ClusterSnapshot {
    fn with_status(&self, rpc_status: RpcStatus, refresh_ms: u64) -> Self {
        Self {
            observed_at: now_string(),
            slot: self.slot,
            rpc_status,
            refresh_ms,
            lanes: self.lanes.clone(),
            summary: self.summary.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct WatchEvent {
    observed_at: String,
    lane: Option<String>,
    event_type: &'static str,
    message: String,
    previous: Option<String>,
    current: Option<String>,
}

#[derive(Clone, Debug)]
struct CachedRequestTokens {
    tokens: Vec<u64>,
    requests_len: u64,
    total_input_tokens: u64,
    fetched_at: Instant,
}

#[derive(Debug, Default)]
struct RequestTokenCache {
    entries: BTreeMap<Pubkey, CachedRequestTokens>,
}

impl RequestTokenCache {
    fn classify_entry(
        &self,
        bundle_pubkey: Pubkey,
        requests_len: u64,
        total_input_tokens: u64,
    ) -> (Vec<u64>, RequestBrickStatus, bool) {
        let Some(entry) = self.entries.get(&bundle_pubkey) else {
            return (Vec::new(), RequestBrickStatus::Aggregate, true);
        };

        let is_stale = entry.requests_len != requests_len
            || entry.total_input_tokens != total_input_tokens
            || entry.fetched_at.elapsed() > REQUEST_BRICK_CACHE_TTL;
        (
            entry.tokens.clone(),
            if is_stale {
                RequestBrickStatus::Stale
            } else {
                RequestBrickStatus::Live
            },
            is_stale,
        )
    }

    fn insert(
        &mut self,
        bundle_pubkey: Pubkey,
        tokens: Vec<u64>,
        requests_len: u64,
        total_input_tokens: u64,
    ) {
        self.entries.insert(
            bundle_pubkey,
            CachedRequestTokens {
                tokens,
                requests_len,
                total_input_tokens,
                fetched_at: Instant::now(),
            },
        );
    }
}

#[derive(Debug, Default)]
struct YellowstoneState {
    registries: BTreeMap<Pubkey, BundleRegistry>,
    bundles: BTreeMap<Pubkey, RequestBundle>,
    requests_by_bundle: BTreeMap<Pubkey, BTreeMap<Pubkey, u64>>,
    current_request_bundles: BTreeSet<Pubkey>,
    slot: Option<u64>,
}

#[derive(Debug)]
enum YellowstoneEvent {
    Slot(u64),
    Registry(Pubkey, BundleRegistry),
    Bundle(Pubkey, RequestBundle),
    JobRequest(Pubkey, JobRequest),
    StreamError {
        source: &'static str,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PollFailureKind {
    RateLimited,
    Offline,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LaneChange {
    latest_bundle_changed: bool,
    requests_len_changed: bool,
    bundle_status_changed: bool,
    max_context_length_changed: bool,
}

impl LaneChange {
    fn is_changed(self) -> bool {
        self.latest_bundle_changed
            || self.requests_len_changed
            || self.bundle_status_changed
            || self.max_context_length_changed
    }
}

#[derive(Clone, Copy, Debug)]
struct HealthTracker {
    base_refresh: Duration,
    current_refresh: Duration,
    last_progress_slot: Option<u64>,
    last_progress_at: Option<Duration>,
    consecutive_failures: u32,
    status: RpcStatus,
}

impl HealthTracker {
    fn new(base_refresh_ms: u64) -> Self {
        let base_refresh = Duration::from_millis(base_refresh_ms);
        Self {
            base_refresh,
            current_refresh: base_refresh,
            last_progress_slot: None,
            last_progress_at: None,
            consecutive_failures: 0,
            status: RpcStatus::Healthy,
        }
    }

    fn current_refresh_ms(self) -> u64 {
        self.current_refresh.as_millis() as u64
    }

    fn record_success(&mut self, slot: u64, now: Duration) -> RpcStatus {
        self.consecutive_failures = 0;
        self.current_refresh = self.base_refresh;

        let progressed = self.last_progress_slot.is_none_or(|previous| slot > previous);
        if progressed {
            self.last_progress_slot = Some(slot);
            self.last_progress_at = Some(now);
            self.status = RpcStatus::Healthy;
            return self.status;
        }

        let last_progress_at = self.last_progress_at.unwrap_or(now);
        if now.saturating_sub(last_progress_at) > LAG_THRESHOLD {
            self.status = RpcStatus::Lagging;
        } else {
            self.status = RpcStatus::Healthy;
        }
        self.status
    }

    fn record_failure(&mut self, failure: PollFailureKind) -> RpcStatus {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        match failure {
            PollFailureKind::RateLimited => {
                self.current_refresh = Duration::from_millis(RATE_LIMIT_REFRESH_MS);
                self.status = RpcStatus::RateLimited;
            }
            PollFailureKind::Offline => {
                if self.last_progress_slot.is_none()
                    || self.consecutive_failures >= OFFLINE_FAILURE_THRESHOLD
                {
                    self.status = RpcStatus::Offline;
                }
            }
        }
        self.status
    }

    fn current_status(self, now: Duration) -> RpcStatus {
        if matches!(self.status, RpcStatus::RateLimited | RpcStatus::Offline) {
            return self.status;
        }

        if let Some(last_progress_at) = self.last_progress_at {
            if now.saturating_sub(last_progress_at) > LAG_THRESHOLD {
                return RpcStatus::Lagging;
            }
        }

        RpcStatus::Healthy
    }
}

#[derive(Clone, Copy, Debug)]
struct ExpectedTargets {
    eco: u64,
    standard: u64,
    pro: u64,
}

impl Default for ExpectedTargets {
    fn default() -> Self {
        Self {
            eco: RequestTier::Eco.get_max_context_length_tokens(),
            standard: RequestTier::Standard.get_max_context_length_tokens(),
            pro: RequestTier::Pro.get_max_context_length_tokens(),
        }
    }
}

impl ExpectedTargets {
    fn apply_override(&mut self, override_arg: &ExpectedMaxContextArg) {
        match override_arg.tier {
            TierArg::Eco => self.eco = override_arg.value,
            TierArg::Standard => self.standard = override_arg.value,
            TierArg::Pro => self.pro = override_arg.value,
        }
    }

    fn for_tier(self, tier: TierArg) -> u64 {
        match tier {
            TierArg::Eco => self.eco,
            TierArg::Standard => self.standard,
            TierArg::Pro => self.pro,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ExpectedBundleCapacities {
    eco: u64,
    standard: u64,
    pro: u64,
}

impl Default for ExpectedBundleCapacities {
    fn default() -> Self {
        Self {
            eco: RequestTier::Eco.get_request_per_bundle(),
            standard: RequestTier::Standard.get_request_per_bundle(),
            pro: RequestTier::Pro.get_request_per_bundle(),
        }
    }
}

impl ExpectedBundleCapacities {
    fn apply_override(&mut self, override_arg: &ExpectedBundleCapacityArg) {
        match override_arg.tier {
            TierArg::Eco => self.eco = override_arg.value,
            TierArg::Standard => self.standard = override_arg.value,
            TierArg::Pro => self.pro = override_arg.value,
        }
    }

    fn for_tier(self, tier: TierArg) -> u64 {
        match tier {
            TierArg::Eco => self.eco,
            TierArg::Standard => self.standard,
            TierArg::Pro => self.pro,
        }
    }
}

#[derive(Debug)]
struct LogWriters {
    snapshot_log: Option<BufWriter<File>>,
    event_log: Option<BufWriter<File>>,
}

impl LogWriters {
    fn new(snapshot_log: Option<&PathBuf>, event_log: Option<&PathBuf>) -> Result<Self> {
        Ok(Self {
            snapshot_log: snapshot_log.map(open_log_file).transpose()?,
            event_log: event_log.map(open_log_file).transpose()?,
        })
    }

    fn write_snapshot(&mut self, snapshot: &ClusterSnapshot) -> Result<()> {
        if let Some(writer) = self.snapshot_log.as_mut() {
            serde_json::to_writer(&mut *writer, snapshot)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
        Ok(())
    }

    fn write_events(&mut self, events: &[WatchEvent]) -> Result<()> {
        if let Some(writer) = self.event_log.as_mut() {
            for event in events {
                serde_json::to_writer(&mut *writer, event)?;
                writer.write_all(b"\n")?;
            }
            writer.flush()?;
        }
        Ok(())
    }
}

fn open_log_file(path: &PathBuf) -> Result<BufWriter<File>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(BufWriter::new(file))
}

fn now_string() -> String {
    let now = OffsetDateTime::now_utc();
    let local = UtcOffset::current_local_offset()
        .ok()
        .map(|offset| now.to_offset(offset))
        .unwrap_or(now);
    local
        .format(&Rfc3339)
        .unwrap_or_else(|_| local.unix_timestamp().to_string())
}

fn bundle_status_label(status: BundleStatus) -> &'static str {
    match status {
        BundleStatus::Active => "active",
        BundleStatus::Full => "full",
        BundleStatus::PendingVerification => "pending_verification",
        BundleStatus::Verified => "verified",
        BundleStatus::BadJobOutput => "bad_job_output",
        BundleStatus::Canceled => "canceled",
    }
}

fn registry_pubkey(context_tier: TierArg, duration_tier: TierArg) -> Pubkey {
    let context_bytes = (RequestTier::from(context_tier) as u64).to_le_bytes();
    let duration_bytes = (RequestTier::from(duration_tier) as u64).to_le_bytes();
    Pubkey::find_program_address(
        &[BUNDLE_REGISTRY_SEED, &context_bytes, &duration_bytes],
        &ambient_auction_client::ID,
    )
    .0
}

fn selected_lanes(context_filters: &[TierArg], duration_filters: &[TierArg]) -> Vec<LaneKey> {
    let all = [TierArg::Eco, TierArg::Standard, TierArg::Pro];
    let contexts: BTreeSet<_> = if context_filters.is_empty() {
        all.into_iter().collect()
    } else {
        context_filters.iter().copied().collect()
    };
    let durations: BTreeSet<_> = if duration_filters.is_empty() {
        all.into_iter().collect()
    } else {
        duration_filters.iter().copied().collect()
    };

    let mut lanes = Vec::new();
    for context_tier in contexts {
        for duration_tier in &durations {
            lanes.push(LaneKey {
                context_tier,
                duration_tier: *duration_tier,
            });
        }
    }
    lanes
}

fn short_pubkey(value: &str) -> String {
    if value.len() <= 10 {
        return value.to_string();
    }
    format!("{}..{}", &value[..4], &value[value.len() - 4..])
}

fn styled(value: impl AsRef<str>, style: &str, use_color: bool) -> String {
    let value = value.as_ref();
    if use_color {
        format!("{style}{value}{ANSI_RESET}")
    } else {
        value.to_string()
    }
}

fn classification_style(classification: &str) -> &'static str {
    match classification {
        "target" => ANSI_GREEN,
        "legacy/custom" => ANSI_YELLOW,
        _ => ANSI_RED,
    }
}

fn event_style(event_type: &str) -> &'static str {
    match event_type {
        "latest_bundle_shift" => "\x1b[1;35m",
        "requests_len_changed" => "\x1b[1;33m",
        "bundle_status_changed" => ANSI_CYAN,
        "max_context_length_changed" => "\x1b[1;34m",
        "rpc_status_changed" => "\x1b[1;34m",
        "rpc_poll_error" => "\x1b[1;31m",
        _ => ANSI_BOLD,
    }
}

fn rpc_status_style(status: RpcStatus) -> &'static str {
    match status {
        RpcStatus::Healthy => ANSI_GREEN,
        RpcStatus::Lagging => ANSI_YELLOW,
        RpcStatus::RateLimited => ANSI_MAGENTA,
        RpcStatus::Offline => ANSI_RED,
    }
}

fn change_marker(change: LaneChange, use_color: bool) -> String {
    let bundle = if change.latest_bundle_changed {
        styled("B", ANSI_MAGENTA, use_color)
    } else {
        ".".to_string()
    };
    let requests = if change.requests_len_changed {
        styled("R", ANSI_YELLOW, use_color)
    } else {
        ".".to_string()
    };
    let status = if change.bundle_status_changed {
        styled("S", ANSI_CYAN, use_color)
    } else {
        ".".to_string()
    };
    let max_context = if change.max_context_length_changed {
        styled("M", ANSI_GREEN, use_color)
    } else {
        ".".to_string()
    };
    format!("{bundle}{requests}{status}{max_context}")
}

fn render_bar(filled: u64, total: u64, width: usize) -> String {
    if total == 0 || width == 0 {
        return "-".to_string();
    }
    let clamped = filled.min(total);
    let filled_width = (((clamped as f64) / (total as f64)) * width as f64).round() as usize;
    let empty_width = width.saturating_sub(filled_width.min(width));
    format!(
        "[{}{}]",
        "#".repeat(filled_width.min(width)),
        "-".repeat(empty_width)
    )
}

fn summarize_lanes(lanes: &[LaneSnapshot]) -> SnapshotSummary {
    let mut summary = SnapshotSummary::default();
    for lane in lanes {
        match lane.classification {
            "target" => {
                summary.target_lanes += 1;
                summary.target_requests_len = summary
                    .target_requests_len
                    .saturating_add(lane.requests_len.unwrap_or_default());
                summary.target_total_input_tokens = summary
                    .target_total_input_tokens
                    .saturating_add(lane.total_input_tokens.unwrap_or_default());
            }
            "legacy/custom" => {
                summary.legacy_custom_lanes += 1;
                summary.legacy_custom_requests_len = summary
                    .legacy_custom_requests_len
                    .saturating_add(lane.requests_len.unwrap_or_default());
                summary.legacy_custom_total_input_tokens = summary
                    .legacy_custom_total_input_tokens
                    .saturating_add(lane.total_input_tokens.unwrap_or_default());
            }
            _ => {
                summary.missing_lanes += 1;
            }
        }

        if let Some(max_context_length) = lane.max_context_length {
            *summary
                .max_context_distribution
                .entry(max_context_length.to_string())
                .or_insert(0) += 1;
        }
    }
    summary
}

fn classify_bundle(bundle: &RequestBundle, expected_targets: ExpectedTargets) -> BundleShape {
    let expected = expected_targets.for_tier(TierArg::from(bundle.context_length_tier));
    if bundle.max_context_length == expected {
        BundleShape::Target
    } else {
        BundleShape::LegacyCustom
    }
}

fn decode_bundle(account: &Account) -> Option<&RequestBundle> {
    bytemuck::try_from_bytes::<RequestBundle>(&account.data).ok()
}

fn decode_registry(account: &Account) -> Option<&BundleRegistry> {
    BundleRegistry::from_bytes(&account.data)
}

fn decode_job_request(account: &Account) -> Option<JobRequest> {
    bytemuck::try_pod_read_unaligned::<JobRequest>(&account.data).ok()
}

async fn fetch_bundle_request_tokens(
    rpc: &RpcClient,
    bundle_pubkey: Pubkey,
) -> std::result::Result<Vec<u64>, ClientError> {
    let program_config = RpcProgramAccountsConfig {
        filters: Some(vec![
            RpcFilterType::DataSize(size_of::<JobRequest>() as u64),
            RpcFilterType::Memcmp(Memcmp::new(
                std::mem::offset_of!(JobRequest, bundle),
                MemcmpEncodedBytes::Bytes(bundle_pubkey.to_bytes().to_vec()),
            )),
        ]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: None,
            commitment: Some(CommitmentConfig::confirmed()),
            min_context_slot: None,
        },
        with_context: None,
        sort_results: None,
    };

    let mut requests: Vec<_> = rpc
        .get_program_accounts_with_config(&ambient_auction_listener::ID, program_config)
        .await?
        .into_iter()
        .filter_map(|(pubkey, account)| {
            decode_job_request(&account).map(|request| (pubkey, request.input_token_count))
        })
        .collect();
    requests.sort_by_key(|(pubkey, tokens)| (Reverse(*tokens), pubkey.to_string()));
    Ok(requests.into_iter().map(|(_, tokens)| tokens).collect())
}

async fn fetch_registry_and_bundle_state(
    rpc: &RpcClient,
    lanes: &[LaneKey],
) -> std::result::Result<
    (
        Option<u64>,
        BTreeMap<Pubkey, BundleRegistry>,
        BTreeMap<Pubkey, RequestBundle>,
    ),
    ClientError,
> {
    let registry_pubkeys: Vec<_> = lanes
        .iter()
        .map(|lane| registry_pubkey(lane.context_tier, lane.duration_tier))
        .collect();

    let registry_response = rpc
        .get_multiple_accounts_with_commitment(&registry_pubkeys, CommitmentConfig::confirmed())
        .await?;
    let mut slot = Some(registry_response.context.slot);
    let mut registries = BTreeMap::new();
    let mut latest_bundle_pubkeys = BTreeSet::new();

    for (registry_pubkey, account) in registry_pubkeys.iter().zip(registry_response.value.iter()) {
        let Some(registry) = account.as_ref().and_then(decode_registry).copied() else {
            continue;
        };
        latest_bundle_pubkeys.insert(Pubkey::new_from_array(registry.latest_bundle.inner()));
        registries.insert(*registry_pubkey, registry);
    }

    let mut bundles = BTreeMap::new();
    let latest_bundle_pubkeys: Vec<_> = latest_bundle_pubkeys.into_iter().collect();
    if !latest_bundle_pubkeys.is_empty() {
        let bundle_response = rpc
            .get_multiple_accounts_with_commitment(
                &latest_bundle_pubkeys,
                CommitmentConfig::confirmed(),
            )
            .await?;
        slot = slot.map(|value| value.max(bundle_response.context.slot));
        for (pubkey, account) in latest_bundle_pubkeys
            .iter()
            .copied()
            .zip(bundle_response.value.into_iter())
        {
            let Some(bundle) = account.as_ref().and_then(decode_bundle).copied() else {
                continue;
            };
            bundles.insert(pubkey, bundle);
        }
    }

    Ok((slot, registries, bundles))
}

async fn seed_yellowstone_state_from_rpc(
    rpc: &RpcClient,
    lanes: &[LaneKey],
    state: &mut YellowstoneState,
    request_token_cache: &mut RequestTokenCache,
) -> std::result::Result<(), ClientError> {
    let (slot, registries, bundles) = fetch_registry_and_bundle_state(rpc, lanes).await?;
    state.registries.extend(registries);
    state.bundles.extend(bundles);
    if let Some(slot) = slot {
        state.slot = Some(state.slot.map_or(slot, |previous| previous.max(slot)));
    }

    let latest_bundles = latest_bundles_for_lanes(state, lanes);
    state.current_request_bundles = latest_bundles.clone();
    if !latest_bundles.is_empty() {
        let bundle_pubkeys: Vec<_> = latest_bundles.into_iter().collect();
        refresh_request_token_cache(
            rpc,
            &bundle_pubkeys,
            request_token_cache,
            LIVE_REQUEST_BRICK_TIMEOUT_MS,
            bundle_pubkeys.len(),
        )
        .await;
    }

    Ok(())
}

async fn hydrate_latest_bundles_from_rpc(
    rpc: &RpcClient,
    latest_bundle_pubkeys: &BTreeSet<Pubkey>,
    state: &mut YellowstoneState,
    request_token_cache: &mut RequestTokenCache,
) -> std::result::Result<(), ClientError> {
    let missing_bundle_pubkeys: Vec<_> = latest_bundle_pubkeys
        .iter()
        .copied()
        .filter(|pubkey| !state.bundles.contains_key(pubkey))
        .collect();

    if missing_bundle_pubkeys.is_empty() {
        return Ok(());
    }

    let bundle_response = rpc
        .get_multiple_accounts_with_commitment(
            &missing_bundle_pubkeys,
            CommitmentConfig::confirmed(),
        )
        .await?;
    state.slot = Some(
        state
            .slot
            .map_or(bundle_response.context.slot, |previous| previous.max(bundle_response.context.slot)),
    );

    let mut hydrated_bundle_pubkeys = Vec::new();
    for (pubkey, account) in missing_bundle_pubkeys
        .iter()
        .copied()
        .zip(bundle_response.value.into_iter())
    {
        let Some(bundle) = account.as_ref().and_then(decode_bundle).copied() else {
            continue;
        };
        state.bundles.insert(pubkey, bundle);
        hydrated_bundle_pubkeys.push(pubkey);
    }

    if !hydrated_bundle_pubkeys.is_empty() {
        refresh_request_token_cache(
            rpc,
            &hydrated_bundle_pubkeys,
            request_token_cache,
            LIVE_REQUEST_BRICK_TIMEOUT_MS,
            hydrated_bundle_pubkeys.len(),
        )
        .await;
    }

    Ok(())
}

async fn fetch_snapshot(
    rpc: &RpcClient,
    lanes: &[LaneKey],
    expected_targets: ExpectedTargets,
    expected_bundle_capacities: ExpectedBundleCapacities,
    refresh_ms: u64,
    rpc_status: RpcStatus,
    request_token_cache: &RequestTokenCache,
) -> std::result::Result<(ClusterSnapshot, Vec<Pubkey>), ClientError> {
    let registry_pubkeys: Vec<_> = lanes
        .iter()
        .map(|lane| registry_pubkey(lane.context_tier, lane.duration_tier))
        .collect();

    let registry_response = rpc
        .get_multiple_accounts_with_commitment(&registry_pubkeys, CommitmentConfig::confirmed())
        .await?;
    let registry_slot = registry_response.context.slot;

    let mut latest_bundle_pubkeys = BTreeSet::new();
    let mut registry_entries = Vec::with_capacity(lanes.len());

    for ((lane, registry_pubkey), account) in lanes
        .iter()
        .zip(registry_pubkeys.iter())
        .zip(registry_response.value.iter())
    {
        let latest_bundle = account
            .as_ref()
            .and_then(decode_registry)
            .map(|registry| Pubkey::new_from_array(registry.latest_bundle.inner()));
        if let Some(bundle_pubkey) = latest_bundle {
            latest_bundle_pubkeys.insert(bundle_pubkey);
        }
        registry_entries.push((*lane, *registry_pubkey, latest_bundle));
    }

    let latest_bundle_pubkeys: Vec<_> = latest_bundle_pubkeys.into_iter().collect();
    let bundle_response = rpc
        .get_multiple_accounts_with_commitment(&latest_bundle_pubkeys, CommitmentConfig::confirmed())
        .await?;
    let bundle_slot = bundle_response.context.slot;
    let mut bundles = BTreeMap::new();
    for (pubkey, account) in latest_bundle_pubkeys
        .iter()
        .copied()
        .zip(bundle_response.value.into_iter())
    {
        bundles.insert(pubkey, account);
    }
    let mut lane_snapshots = Vec::with_capacity(lanes.len());
    let mut bundles_needing_request_refresh = BTreeSet::new();
    for (lane, registry, latest_bundle) in registry_entries {
        let Some(latest_bundle_pubkey) = latest_bundle else {
            lane_snapshots.push(LaneSnapshot {
                lane: lane.label(),
                context_tier: lane.context_tier.label(),
                duration_tier: lane.duration_tier.label(),
                registry: registry.to_string(),
                latest_bundle: None,
                bundle_status: None,
                classification: BundleShape::Missing.label(),
                max_context_length: None,
                bundle_capacity: None,
                requests_len: None,
                total_input_tokens: None,
                maximum_output_tokens: None,
                child_bundle: None,
                request_input_tokens: Vec::new(),
                request_brick_status: RequestBrickStatus::Aggregate,
            });
            continue;
        };

        let bundle_account = bundles.get(&latest_bundle_pubkey).and_then(|account| account.as_ref());
        let bundle = bundle_account.and_then(decode_bundle);
        let (bundle_status, classification, max_context_length, requests_len, total_input_tokens, maximum_output_tokens, child_bundle, bundle_capacity, request_input_tokens, request_brick_status) =
            if let Some(bundle) = bundle {
                let (request_input_tokens, request_brick_status, needs_refresh) =
                    request_token_cache.classify_entry(
                        latest_bundle_pubkey,
                        bundle.requests_len,
                        bundle.total_input_tokens,
                    );
                if needs_refresh {
                    bundles_needing_request_refresh.insert(latest_bundle_pubkey);
                }
                (
                    Some(bundle_status_label(bundle.status)),
                    classify_bundle(bundle, expected_targets).label(),
                    Some(bundle.max_context_length),
                    Some(bundle.requests_len),
                    Some(bundle.total_input_tokens),
                    Some(bundle.maximum_output_tokens),
                    bundle
                        .child_bundle_key
                        .get()
                        .map(|pubkey| Pubkey::new_from_array(pubkey.inner()).to_string()),
                    Some(expected_bundle_capacities.for_tier(TierArg::from(
                        bundle.context_length_tier,
                    ))),
                    request_input_tokens,
                    request_brick_status,
                )
            } else {
                (
                    None,
                    BundleShape::Missing.label(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Vec::new(),
                    RequestBrickStatus::Aggregate,
                )
            };

        lane_snapshots.push(LaneSnapshot {
            lane: lane.label(),
            context_tier: lane.context_tier.label(),
            duration_tier: lane.duration_tier.label(),
            registry: registry.to_string(),
            latest_bundle: Some(latest_bundle_pubkey.to_string()),
            bundle_status,
            classification,
            max_context_length,
            bundle_capacity,
            requests_len,
            total_input_tokens,
            maximum_output_tokens,
            child_bundle,
            request_input_tokens,
            request_brick_status,
        });
    }

    Ok((
        ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(registry_slot.max(bundle_slot)),
            rpc_status,
            refresh_ms,
            summary: summarize_lanes(&lane_snapshots),
            lanes: lane_snapshots,
        },
        bundles_needing_request_refresh.into_iter().collect(),
    ))
}

async fn refresh_request_token_cache(
    rpc: &RpcClient,
    bundles_to_refresh: &[Pubkey],
    request_token_cache: &mut RequestTokenCache,
    timeout_ms: u64,
    max_bundles: usize,
) {
    if bundles_to_refresh.is_empty() || max_bundles == 0 {
        return;
    }

    let refresh_targets: Vec<_> = bundles_to_refresh
        .iter()
        .copied()
        .take(max_bundles)
        .collect();
    let refresh_results = join_all(refresh_targets.iter().copied().map(|pubkey| async move {
        let result = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            fetch_bundle_request_tokens(rpc, pubkey),
        )
        .await;
        (pubkey, result)
    }))
    .await;

    let bundle_accounts = match rpc
        .get_multiple_accounts_with_commitment(&refresh_targets, CommitmentConfig::confirmed())
        .await
    {
        Ok(response) => response.value,
        Err(_) => return,
    };
    let bundle_metadata: BTreeMap<_, _> = refresh_targets
        .into_iter()
        .zip(bundle_accounts.into_iter())
        .filter_map(|(pubkey, account)| {
            let bundle = account.as_ref().and_then(decode_bundle)?;
            Some((pubkey, (bundle.requests_len, bundle.total_input_tokens)))
        })
        .collect();

    for (pubkey, result) in refresh_results {
        let Ok(Ok(tokens)) = result else {
            continue;
        };
        let Some((requests_len, total_input_tokens)) = bundle_metadata.get(&pubkey).copied() else {
            continue;
        };
        request_token_cache.insert(pubkey, tokens, requests_len, total_input_tokens);
    }
}

fn latest_bundles_for_lanes(state: &YellowstoneState, lanes: &[LaneKey]) -> BTreeSet<Pubkey> {
    lanes.iter()
        .filter_map(|lane| {
            let registry = registry_pubkey(lane.context_tier, lane.duration_tier);
            state
                .registries
                .get(&registry)
                .map(|registry| Pubkey::new_from_array(registry.latest_bundle.inner()))
        })
        .collect()
}

fn request_tokens_from_state(
    state: &YellowstoneState,
    bundle_pubkey: Pubkey,
    requests_len: u64,
) -> (Vec<u64>, RequestBrickStatus) {
    let Some(requests) = state.requests_by_bundle.get(&bundle_pubkey) else {
        return (
            Vec::new(),
            if requests_len == 0 {
                RequestBrickStatus::Live
            } else {
                RequestBrickStatus::Aggregate
            },
        );
    };

    let mut tokens: Vec<_> = requests.iter().map(|(pubkey, tokens)| (*pubkey, *tokens)).collect();
    tokens.sort_by_key(|(pubkey, tokens)| (Reverse(*tokens), pubkey.to_string()));
    let request_brick_status = if state.current_request_bundles.contains(&bundle_pubkey)
        && (requests_len == 0 || tokens.len() >= requests_len as usize)
    {
        RequestBrickStatus::Live
    } else if tokens.is_empty() {
        RequestBrickStatus::Aggregate
    } else {
        RequestBrickStatus::Stale
    };

    (
        tokens.into_iter().map(|(_, tokens)| tokens).collect(),
        request_brick_status,
    )
}

fn request_tokens_for_bundle(
    state: &YellowstoneState,
    request_token_cache: &RequestTokenCache,
    bundle_pubkey: Pubkey,
    requests_len: u64,
    total_input_tokens: u64,
) -> (Vec<u64>, RequestBrickStatus) {
    let (state_tokens, state_status) = request_tokens_from_state(state, bundle_pubkey, requests_len);
    let (cached_tokens, cached_status, _) =
        request_token_cache.classify_entry(bundle_pubkey, requests_len, total_input_tokens);

    if matches!(state_status, RequestBrickStatus::Live) {
        return (state_tokens, state_status);
    }

    if cached_tokens.len() > state_tokens.len() {
        return (cached_tokens, cached_status);
    }

    if !state_tokens.is_empty() {
        return (state_tokens, state_status);
    }

    (cached_tokens, cached_status)
}

fn snapshot_from_yellowstone_state(
    state: &YellowstoneState,
    request_token_cache: &RequestTokenCache,
    lanes: &[LaneKey],
    expected_targets: ExpectedTargets,
    expected_bundle_capacities: ExpectedBundleCapacities,
    refresh_ms: u64,
    rpc_status: RpcStatus,
) -> ClusterSnapshot {
    let mut lane_snapshots = Vec::with_capacity(lanes.len());

    for lane in lanes {
        let registry = registry_pubkey(lane.context_tier, lane.duration_tier);
        let latest_bundle = state
            .registries
            .get(&registry)
            .map(|registry| Pubkey::new_from_array(registry.latest_bundle.inner()));

        let Some(latest_bundle_pubkey) = latest_bundle else {
            lane_snapshots.push(LaneSnapshot {
                lane: lane.label(),
                context_tier: lane.context_tier.label(),
                duration_tier: lane.duration_tier.label(),
                registry: registry.to_string(),
                latest_bundle: None,
                bundle_status: None,
                classification: BundleShape::Missing.label(),
                max_context_length: None,
                bundle_capacity: None,
                requests_len: None,
                total_input_tokens: None,
                maximum_output_tokens: None,
                child_bundle: None,
                request_input_tokens: Vec::new(),
                request_brick_status: RequestBrickStatus::Aggregate,
            });
            continue;
        };

        let bundle = state.bundles.get(&latest_bundle_pubkey);
        let (
            bundle_status,
            classification,
            max_context_length,
            requests_len,
            total_input_tokens,
            maximum_output_tokens,
            child_bundle,
            bundle_capacity,
            request_input_tokens,
            request_brick_status,
        ) = if let Some(bundle) = bundle {
            let (request_input_tokens, request_brick_status) = request_tokens_for_bundle(
                state,
                request_token_cache,
                latest_bundle_pubkey,
                bundle.requests_len,
                bundle.total_input_tokens,
            );
            (
                Some(bundle_status_label(bundle.status)),
                classify_bundle(bundle, expected_targets).label(),
                Some(bundle.max_context_length),
                Some(bundle.requests_len),
                Some(bundle.total_input_tokens),
                Some(bundle.maximum_output_tokens),
                bundle
                    .child_bundle_key
                    .get()
                    .map(|pubkey| Pubkey::new_from_array(pubkey.inner()).to_string()),
                Some(expected_bundle_capacities.for_tier(TierArg::from(
                    bundle.context_length_tier,
                ))),
                request_input_tokens,
                request_brick_status,
            )
        } else {
            (
                None,
                BundleShape::Missing.label(),
                None,
                None,
                None,
                None,
                None,
                None,
                Vec::new(),
                RequestBrickStatus::Aggregate,
            )
        };

        lane_snapshots.push(LaneSnapshot {
            lane: lane.label(),
            context_tier: lane.context_tier.label(),
            duration_tier: lane.duration_tier.label(),
            registry: registry.to_string(),
            latest_bundle: Some(latest_bundle_pubkey.to_string()),
            bundle_status,
            classification,
            max_context_length,
            bundle_capacity,
            requests_len,
            total_input_tokens,
            maximum_output_tokens,
            child_bundle,
            request_input_tokens,
            request_brick_status,
        });
    }

    ClusterSnapshot {
        observed_at: now_string(),
        slot: state.slot,
        rpc_status,
        refresh_ms,
        summary: summarize_lanes(&lane_snapshots),
        lanes: lane_snapshots,
    }
}

fn slot_subscription_request() -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            "watch-bundles-slot".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(GrpcCommitmentLevel::Processed.into()),
        ..Default::default()
    }
}

fn registry_subscription_request(registry_pubkeys: &[Pubkey]) -> SubscribeRequest {
    SubscribeRequest {
        accounts: HashMap::from([(
            "watch-bundles-registries".to_string(),
            SubscribeRequestFilterAccounts {
                account: registry_pubkeys.iter().map(ToString::to_string).collect(),
                ..Default::default()
            },
        )]),
        commitment: Some(GrpcCommitmentLevel::Processed.into()),
        ..Default::default()
    }
}

fn bundle_subscription_request(bundle_pubkeys: &[Pubkey]) -> Option<SubscribeRequest> {
    if bundle_pubkeys.is_empty() {
        return None;
    }
    Some(SubscribeRequest {
        accounts: HashMap::from([(
            "watch-bundles-bundles".to_string(),
            SubscribeRequestFilterAccounts {
                account: bundle_pubkeys.iter().map(ToString::to_string).collect(),
                ..Default::default()
            },
        )]),
        commitment: Some(GrpcCommitmentLevel::Processed.into()),
        ..Default::default()
    })
}

fn job_request_subscription_request(bundle_pubkeys: &[Pubkey]) -> Option<SubscribeRequest> {
    if bundle_pubkeys.is_empty() {
        return None;
    }

    let account_filters = bundle_pubkeys
        .iter()
        .enumerate()
        .map(|(idx, bundle_pubkey)| {
            (
                format!("watch-bundles-requests-{idx}"),
                SubscribeRequestFilterAccounts {
                    owner: vec![ambient_auction_listener::ID.to_string()],
                    filters: vec![
                        SubscribeRequestFilterAccountsFilter {
                            filter: Some(Filter::Datasize(JobRequest::LEN as u64)),
                        },
                        SubscribeRequestFilterAccountsFilter {
                            filter: Some(Filter::Memcmp(
                                SubscribeRequestFilterAccountsFilterMemcmp {
                                    offset: std::mem::offset_of!(JobRequest, bundle) as u64,
                                    data: Some(Data::Bytes(bundle_pubkey.to_bytes().to_vec())),
                                },
                            )),
                        },
                    ],
                    ..Default::default()
                },
            )
        })
        .collect();

    Some(SubscribeRequest {
        accounts: account_filters,
        commitment: Some(GrpcCommitmentLevel::Processed.into()),
        ..Default::default()
    })
}

fn spawn_slot_subscription(
    geyser: CloneableGeyserGrpcClient,
    sender: UnboundedSender<YellowstoneEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut geyser = geyser.into_0();
        let (mut sink, mut stream) =
            match geyser.subscribe_with_request(Some(slot_subscription_request())).await {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-slot",
                        message: error.to_string(),
                    });
                    return;
                }
            };

        while let Some(update) = stream.next().await {
            let update = match update {
                Ok(update) => update,
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-slot",
                        message: error.to_string(),
                    });
                    return;
                }
            };
            if reply_to_ping(&mut sink, &update).await.unwrap_or(false) {
                continue;
            }
            if let Some(UpdateOneof::Slot(slot)) = update.update_oneof {
                let _ = sender.send(YellowstoneEvent::Slot(slot.slot));
            }
        }
    })
}

fn spawn_registry_subscription(
    geyser: CloneableGeyserGrpcClient,
    registry_pubkeys: Vec<Pubkey>,
    sender: UnboundedSender<YellowstoneEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut geyser = geyser.into_0();
        let (mut sink, mut stream) =
            match geyser
                .subscribe_with_request(Some(registry_subscription_request(&registry_pubkeys)))
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-registries",
                        message: error.to_string(),
                    });
                    return;
                }
            };

        while let Some(update) = stream.next().await {
            let update = match update {
                Ok(update) => update,
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-registries",
                        message: error.to_string(),
                    });
                    return;
                }
            };
            if reply_to_ping(&mut sink, &update).await.unwrap_or(false) {
                continue;
            }
            let Some(UpdateOneof::Account(account)) = update.update_oneof else {
                continue;
            };
            let Some(account_info) = account.account else {
                continue;
            };
            match decode_account_info::<BundleRegistry>(account_info) {
                Ok((pubkey, registry)) => {
                    let _ = sender.send(YellowstoneEvent::Registry(pubkey, registry));
                }
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-registries",
                        message: error.to_string(),
                    });
                    return;
                }
            }
        }
    })
}

fn spawn_bundle_subscription(
    geyser: CloneableGeyserGrpcClient,
    bundle_pubkeys: Vec<Pubkey>,
    sender: UnboundedSender<YellowstoneEvent>,
) -> Option<JoinHandle<()>> {
    let request = bundle_subscription_request(&bundle_pubkeys)?;
    Some(tokio::spawn(async move {
        let mut geyser = geyser.into_0();
        let (mut sink, mut stream) = match geyser.subscribe_with_request(Some(request)).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = sender.send(YellowstoneEvent::StreamError {
                    source: "yellowstone-bundles",
                    message: error.to_string(),
                });
                return;
            }
        };

        while let Some(update) = stream.next().await {
            let update = match update {
                Ok(update) => update,
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-bundles",
                        message: error.to_string(),
                    });
                    return;
                }
            };
            if reply_to_ping(&mut sink, &update).await.unwrap_or(false) {
                continue;
            }
            let Some(UpdateOneof::Account(account)) = update.update_oneof else {
                continue;
            };
            let Some(account_info) = account.account else {
                continue;
            };
            match decode_account_info::<RequestBundle>(account_info) {
                Ok((pubkey, bundle)) => {
                    let _ = sender.send(YellowstoneEvent::Bundle(pubkey, bundle));
                }
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-bundles",
                        message: error.to_string(),
                    });
                    return;
                }
            }
        }
    }))
}

fn spawn_job_request_subscription(
    geyser: CloneableGeyserGrpcClient,
    bundle_pubkeys: Vec<Pubkey>,
    sender: UnboundedSender<YellowstoneEvent>,
) -> Option<JoinHandle<()>> {
    let request = job_request_subscription_request(&bundle_pubkeys)?;
    Some(tokio::spawn(async move {
        let mut geyser = geyser.into_0();
        let (mut sink, mut stream) = match geyser.subscribe_with_request(Some(request)).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = sender.send(YellowstoneEvent::StreamError {
                    source: "yellowstone-requests",
                    message: error.to_string(),
                });
                return;
            }
        };

        while let Some(update) = stream.next().await {
            let update = match update {
                Ok(update) => update,
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-requests",
                        message: error.to_string(),
                    });
                    return;
                }
            };
            if reply_to_ping(&mut sink, &update).await.unwrap_or(false) {
                continue;
            }
            let Some(UpdateOneof::Account(account)) = update.update_oneof else {
                continue;
            };
            let Some(account_info) = account.account else {
                continue;
            };
            match decode_account_info::<JobRequest>(account_info) {
                Ok((pubkey, job_request)) => {
                    let _ = sender.send(YellowstoneEvent::JobRequest(pubkey, job_request));
                }
                Err(error) => {
                    let _ = sender.send(YellowstoneEvent::StreamError {
                        source: "yellowstone-requests",
                        message: error.to_string(),
                    });
                    return;
                }
            }
        }
    }))
}

fn event_for_change(
    lane: &str,
    event_type: &'static str,
    message: String,
    previous: Option<String>,
    current: Option<String>,
) -> WatchEvent {
    WatchEvent {
        observed_at: now_string(),
        lane: Some(lane.to_string()),
        event_type,
        message,
        previous,
        current,
    }
}

fn diff_snapshots(previous: Option<&ClusterSnapshot>, current: &ClusterSnapshot) -> Vec<WatchEvent> {
    let Some(previous) = previous else {
        return Vec::new();
    };

    let previous_by_lane: BTreeMap<_, _> = previous
        .lanes
        .iter()
        .map(|lane| (lane.lane.as_str(), lane))
        .collect();
    let mut events = Vec::new();

    for lane in &current.lanes {
        let Some(old_lane) = previous_by_lane.get(lane.lane.as_str()) else {
            continue;
        };

        if old_lane.latest_bundle != lane.latest_bundle {
            events.push(event_for_change(
                &lane.lane,
                "latest_bundle_shift",
                format!(
                    "{} latest bundle moved {} -> {}",
                    lane.lane,
                    old_lane
                        .latest_bundle
                        .as_deref()
                        .map(short_pubkey)
                        .unwrap_or_else(|| "-".to_string()),
                    lane.latest_bundle
                        .as_deref()
                        .map(short_pubkey)
                        .unwrap_or_else(|| "-".to_string())
                ),
                old_lane.latest_bundle.clone(),
                lane.latest_bundle.clone(),
            ));
        }

        if old_lane.requests_len != lane.requests_len {
            events.push(event_for_change(
                &lane.lane,
                "requests_len_changed",
                format!(
                    "{} requests_len {} -> {}",
                    lane.lane,
                    old_lane
                        .requests_len
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    lane.requests_len
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string())
                ),
                old_lane.requests_len.map(|value| value.to_string()),
                lane.requests_len.map(|value| value.to_string()),
            ));
        }

        if old_lane.bundle_status != lane.bundle_status {
            events.push(event_for_change(
                &lane.lane,
                "bundle_status_changed",
                format!(
                    "{} status {} -> {}",
                    lane.lane,
                    old_lane.bundle_status.unwrap_or("-"),
                    lane.bundle_status.unwrap_or("-")
                ),
                old_lane.bundle_status.map(str::to_string),
                lane.bundle_status.map(str::to_string),
            ));
        }

        if old_lane.max_context_length != lane.max_context_length {
            events.push(event_for_change(
                &lane.lane,
                "max_context_length_changed",
                format!(
                    "{} max_context_length {} -> {}",
                    lane.lane,
                    old_lane
                        .max_context_length
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    lane.max_context_length
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string())
                ),
                old_lane.max_context_length.map(|value| value.to_string()),
                lane.max_context_length.map(|value| value.to_string()),
            ));
        }
    }

    events
}

fn collect_lane_changes(
    previous: Option<&ClusterSnapshot>,
    current: &ClusterSnapshot,
) -> BTreeMap<String, LaneChange> {
    let Some(previous) = previous else {
        return BTreeMap::new();
    };

    let previous_by_lane: BTreeMap<_, _> = previous
        .lanes
        .iter()
        .map(|lane| (lane.lane.as_str(), lane))
        .collect();
    let mut changes = BTreeMap::new();

    for lane in &current.lanes {
        let Some(old_lane) = previous_by_lane.get(lane.lane.as_str()) else {
            continue;
        };
        let change = LaneChange {
            latest_bundle_changed: old_lane.latest_bundle != lane.latest_bundle,
            requests_len_changed: old_lane.requests_len != lane.requests_len,
            bundle_status_changed: old_lane.bundle_status != lane.bundle_status,
            max_context_length_changed: old_lane.max_context_length != lane.max_context_length,
        };
        if change.is_changed() {
            changes.insert(lane.lane.clone(), change);
        }
    }

    changes
}

fn push_event(
    events: &mut VecDeque<WatchEvent>,
    event: WatchEvent,
    capacity: usize,
) {
    if capacity == 0 {
        return;
    }
    while events.len() >= capacity {
        events.pop_front();
    }
    events.push_back(event);
}

fn push_events(
    events: &mut VecDeque<WatchEvent>,
    new_events: Vec<WatchEvent>,
    capacity: usize,
) -> Vec<WatchEvent> {
    for event in &new_events {
        push_event(events, event.clone(), capacity);
    }
    new_events
}

fn render_distribution(distribution: &BTreeMap<String, usize>) -> String {
    if distribution.is_empty() {
        return "-".to_string();
    }
    distribution
        .iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Clone, Copy)]
enum Align {
    Left,
    Right,
}

fn pad_text(value: impl AsRef<str>, width: usize, align: Align) -> String {
    let value = value.as_ref();
    if value.len() >= width {
        return value[..width].to_string();
    }
    let padding = " ".repeat(width - value.len());
    match align {
        Align::Left => format!("{value}{padding}"),
        Align::Right => format!("{padding}{value}"),
    }
}

fn style_padded(
    value: impl AsRef<str>,
    width: usize,
    align: Align,
    style: Option<&str>,
    use_color: bool,
) -> String {
    let padded = pad_text(value, width, align);
    if let Some(style) = style {
        styled(padded, style, use_color)
    } else {
        padded
    }
}

fn compact_token_label(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        let rounded_tenths = ((tokens + 50_000) / 100_000) as u64;
        if rounded_tenths >= 100 {
            format!("{}m", rounded_tenths / 10)
        } else {
            format!("{}.{}m", rounded_tenths / 10, rounded_tenths % 10)
        }
    } else if tokens >= 10_000 {
        format!("{}k", (tokens + 500) / 1_000)
    } else if tokens >= 1_000 {
        let rounded_tenths = ((tokens + 50) / 100) as u64;
        if rounded_tenths >= 100 {
            format!("{}k", rounded_tenths / 10)
        } else {
            format!("{}.{}k", rounded_tenths / 10, rounded_tenths % 10)
        }
    } else {
        tokens.to_string()
    }
}

fn format_total_tokens(tokens: Option<u64>) -> String {
    let Some(tokens) = tokens else {
        return "-".to_string();
    };
    if tokens >= 1_000_000 {
        format!("{:.1}m", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn summarize_request_brick_statuses(snapshot: &ClusterSnapshot) -> String {
    let mut live = 0usize;
    let mut stale = 0usize;
    let mut aggregate = 0usize;
    for lane in &snapshot.lanes {
        match lane.request_brick_status {
            RequestBrickStatus::Live => live += 1,
            RequestBrickStatus::Stale => stale += 1,
            RequestBrickStatus::Aggregate => aggregate += 1,
        }
    }
    format!("req_detail live={live} stale={stale} agg={aggregate}")
}

fn render_brick_cell(
    tokens: Option<u64>,
    unknown: bool,
    fill_style: &str,
    use_color: bool,
) -> String {
    let raw = match tokens {
        Some(tokens) => pad_text(compact_token_label(tokens), BRICK_CELL_WIDTH, Align::Right),
        None if unknown => pad_text("??", BRICK_CELL_WIDTH, Align::Right),
        None => pad_text(".", BRICK_CELL_WIDTH, Align::Right),
    };
    match (tokens, unknown) {
        (Some(_), _) => styled(raw, fill_style, use_color),
        (None, true) => styled(raw, ANSI_DIM, use_color),
        (None, false) => styled(raw, ANSI_DIM, use_color),
    }
}

fn lane_tile_lines(lane: &LaneSnapshot, change: LaneChange, use_color: bool) -> Vec<String> {
    let shape_style = classification_style(lane.classification);
    let shape_code = match lane.classification {
        "target" => "T",
        "legacy/custom" => "L",
        _ => "?",
    };
    let short_lane = short_lane_label(lane.context_tier, lane.duration_tier);
    let header = format!(
        "{} [{}] {:>2}/{} {}",
        short_lane,
        shape_code,
        lane.requests_len.unwrap_or_default(),
        lane.bundle_capacity.unwrap_or_default(),
        change_marker(change, false)
    );
    let header = style_padded(
        header,
        TILE_WIDTH,
        Align::Left,
        if change.is_changed() { Some(ANSI_BOLD) } else { None },
        use_color,
    );
    let detail = format!(
        "in:{} max:{} {}",
        format_total_tokens(lane.total_input_tokens),
        lane.max_context_length
            .map(compact_token_label)
            .unwrap_or_else(|| "-".to_string()),
        format!("req:{}", lane.request_brick_status.label())
    );
    let detail = style_padded(detail, TILE_WIDTH, Align::Left, Some(shape_style), use_color);

    let fill_style = if change.requests_len_changed {
        ANSI_BOLD
    } else {
        shape_style
    };
    let mut lines = vec![header, detail];
    for row in 0..BRICK_ROWS {
        let mut brick_line = String::new();
        for column in 0..BRICK_COLUMNS {
            let idx = row * BRICK_COLUMNS + column;
            if column > 0 {
                brick_line.push(' ');
            }
            let has_request = lane
                .requests_len
                .is_some_and(|requests_len| idx < requests_len as usize);
            let exact_tokens = lane.request_input_tokens.get(idx).copied();
            brick_line.push_str(&render_brick_cell(
                exact_tokens,
                has_request && exact_tokens.is_none(),
                fill_style,
                use_color,
            ));
        }
        let mut padded_line = brick_line;
        if TILE_WIDTH > BRICK_LINE_WIDTH {
            padded_line.push_str(&" ".repeat(TILE_WIDTH - BRICK_LINE_WIDTH));
        }
        lines.push(padded_line);
    }
    lines
}

fn render_lane_matrix(
    snapshot: &ClusterSnapshot,
    lane_changes: &BTreeMap<String, LaneChange>,
    use_color: bool,
) -> String {
    let mut lanes_by_key = BTreeMap::new();
    let ordered_contexts = ["eco", "standard", "pro"];
    let ordered_durations = ["eco", "standard", "pro"];

    for lane in &snapshot.lanes {
        lanes_by_key.insert((lane.context_tier, lane.duration_tier), lane);
    }

    let mut rows = Vec::new();
    rows.push(
        "bundle matrix (size/speed lanes; brick labels are request input tokens with k/m suffixes)"
            .to_string(),
    );
    for context in ordered_contexts {
        let mut tiles = Vec::new();
        for duration in ordered_durations {
            if let Some(lane) = lanes_by_key.get(&(context, duration)) {
                tiles.push(lane_tile_lines(
                    lane,
                    lane_changes.get(&lane.lane).copied().unwrap_or_default(),
                    use_color,
                ));
            }
        }
        if tiles.is_empty() {
            continue;
        }
        for line_index in 0..(BRICK_ROWS + 2) {
            rows.push(
                tiles
                    .iter()
                    .map(|tile| tile[line_index].clone())
                    .collect::<Vec<_>>()
                    .join("   "),
            );
        }
        rows.push(String::new());
    }
    rows.join("\n")
}

fn render_dashboard(
    snapshot: &ClusterSnapshot,
    events: &VecDeque<WatchEvent>,
    lane_changes: &BTreeMap<String, LaneChange>,
    new_event_count: usize,
    poll_count: u64,
    max_rows: Option<usize>,
    clear_screen: bool,
    use_color: bool,
) -> String {
    let mut output = String::new();
    if clear_screen {
        output.push_str(ANSI_REDRAW_FRAME);
    }

    output.push_str(&styled("watch-bundles", ANSI_BOLD, use_color));
    output.push('\n');
    output.push_str(&format!(
        "time: {} | poll: {} | slot: {} | feed: {} | refresh: {}ms\n",
        snapshot.observed_at,
        poll_count,
        snapshot
            .slot
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        styled(snapshot.rpc_status.label(), rpc_status_style(snapshot.rpc_status), use_color),
        snapshot.refresh_ms
    ));
    output.push_str(&format!(
        "shape: target={} {} legacy/custom={} {} missing={}\n",
        styled(
            snapshot.summary.target_lanes.to_string(),
            ANSI_GREEN,
            use_color
        ),
        render_bar(
            snapshot.summary.target_lanes as u64,
            snapshot.lanes.len() as u64,
            12
        ),
        styled(
            snapshot.summary.legacy_custom_lanes.to_string(),
            ANSI_YELLOW,
            use_color
        ),
        render_bar(
            snapshot.summary.legacy_custom_lanes as u64,
            snapshot.lanes.len() as u64,
            12
        ),
        styled(snapshot.summary.missing_lanes.to_string(), ANSI_RED, use_color)
    ));
    output.push_str(&format!(
        "max_context: {} | requests(target/legacy)={}/{} | input_tokens(target/legacy)={}/{} | {}\n\n",
        render_distribution(&snapshot.summary.max_context_distribution),
        snapshot.summary.target_requests_len,
        snapshot.summary.legacy_custom_requests_len,
        snapshot.summary.target_total_input_tokens,
        snapshot.summary.legacy_custom_total_input_tokens,
        summarize_request_brick_statuses(snapshot),
    ));
    output.push_str(
        "key: size/speed uses eco/std/pro shorthand; first tier=size second=speed\n",
    );
    output.push_str(
        "key: chg B=latest_bundle moved R=requests_len changed S=status changed M=max_context changed\n",
    );
    output.push_str(
        "key: [T]=target shape [L]=legacy/custom shape [?]=missing or not yet hydrated\n",
    );
    output.push_str(
        "key: req live=exact request detail stale=cached exact detail agg=bundle-only detail | requests show used/expected-cap | bricks: number=input tokens ??=unknown .=empty\n\n",
    );

    output.push_str(&render_lane_matrix(snapshot, lane_changes, use_color));
    output.push_str("\n\n");

    output.push_str(
        "chg  size/speed        latest bundle  status               shape          max_ctx   requests             input_tokens  max_output  child\n",
    );
    output.push_str(
        "---- ----------------- ------------- -------------------- -------------- --------- -------------------- ------------- ----------- -----------\n",
    );
    for lane in &snapshot.lanes {
        let change = lane_changes.get(&lane.lane).copied().unwrap_or_default();
        let requests_raw = match (lane.requests_len, lane.bundle_capacity) {
            (Some(requests_len), Some(bundle_capacity)) => format!(
                "{:>2}/{:<2} {}",
                requests_len,
                bundle_capacity,
                render_bar(requests_len, bundle_capacity, 10)
            ),
            _ => "-".to_string(),
        };
        let latest_bundle = lane
            .latest_bundle
            .as_deref()
            .map(short_pubkey)
            .unwrap_or_else(|| "-".to_string());
        let status = lane.bundle_status.unwrap_or("-").to_string();
        let classification = lane.classification.to_string();
        let max_context_raw = lane
            .max_context_length
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let input_tokens_raw = lane
            .total_input_tokens
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());

        let max_output = lane
            .maximum_output_tokens
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let child = lane
            .child_bundle
            .as_deref()
            .map(short_pubkey)
            .unwrap_or_else(|| "-".to_string());

        output.push_str(&style_padded(change_marker(change, false), 4, Align::Left, None, use_color));
        output.push(' ');
        output.push_str(&style_padded(
            short_lane_label(lane.context_tier, lane.duration_tier),
            17,
            Align::Left,
            None,
            use_color,
        ));
        output.push(' ');
        output.push_str(&style_padded(
            latest_bundle,
            13,
            Align::Left,
            if change.latest_bundle_changed { Some(ANSI_MAGENTA) } else { None },
            use_color,
        ));
        output.push(' ');
        output.push_str(&style_padded(
            status,
            20,
            Align::Left,
            if change.bundle_status_changed { Some(ANSI_CYAN) } else { None },
            use_color,
        ));
        output.push(' ');
        output.push_str(&style_padded(
            classification,
            14,
            Align::Left,
            Some(classification_style(lane.classification)),
            use_color,
        ));
        output.push(' ');
        output.push_str(&style_padded(
            max_context_raw,
            9,
            Align::Right,
            if change.max_context_length_changed { Some(ANSI_BLUE) } else { None },
            use_color,
        ));
        output.push(' ');
        output.push_str(&style_padded(
            requests_raw,
            20,
            Align::Left,
            if change.requests_len_changed { Some(ANSI_YELLOW) } else { None },
            use_color,
        ));
        output.push(' ');
        output.push_str(&style_padded(
            input_tokens_raw,
            13,
            Align::Right,
            if change.requests_len_changed { Some(ANSI_YELLOW) } else { None },
            use_color,
        ));
        output.push(' ');
        output.push_str(&style_padded(max_output, 11, Align::Right, None, use_color));
        output.push(' ');
        output.push_str(&style_padded(child, 11, Align::Left, None, use_color));
        output.push('\n');
    }

    output.push_str("\nrecent events\n");
    output.push_str("-------------\n");
    if events.is_empty() {
        output.push_str("(none)\n");
    } else {
        let total_events = events.len();
        for (index, event) in events.iter().rev().enumerate() {
            let lane = event.lane.as_deref().unwrap_or("system");
            let is_new = index < new_event_count.min(total_events);
            let style = if is_new {
                event_style(event.event_type)
            } else {
                ANSI_DIM
            };
            output.push_str(&styled(
                format!("{} [{lane}] {}", if is_new { ">" } else { "." }, event.message),
                style,
                use_color,
            ));
            output.push('\n');
        }
    }

    if let Some(max_rows) = max_rows {
        let mut lines: Vec<_> = output.lines().map(str::to_string).collect();
        if lines.len() > max_rows && max_rows > 0 {
            lines.truncate(max_rows.saturating_sub(1));
            lines.push(styled(
                "[truncated to fit terminal; widen the window or use --once for full view]",
                ANSI_DIM,
                use_color,
            ));
            return lines.join("\n");
        }
    }

    output
}

fn classify_client_error(error: &ClientError) -> PollFailureKind {
    if is_rate_limited(error) {
        PollFailureKind::RateLimited
    } else {
        PollFailureKind::Offline
    }
}

fn is_rate_limited(error: &ClientError) -> bool {
    if let ClientErrorKind::Reqwest(reqwest_error) = error.kind() {
        if reqwest_error
            .status()
            .is_some_and(|status| status.as_u16() == 429)
        {
            return true;
        }
    }

    let rendered = format!("{error:?}").to_ascii_lowercase();
    rendered.contains("429") || rendered.contains("too many requests")
}

fn system_event(event_type: &'static str, message: String) -> WatchEvent {
    WatchEvent {
        observed_at: now_string(),
        lane: None,
        event_type,
        message,
        previous: None,
        current: None,
    }
}

fn empty_snapshot(lanes: &[LaneKey], refresh_ms: u64, rpc_status: RpcStatus) -> ClusterSnapshot {
    let lane_snapshots: Vec<_> = lanes
        .iter()
        .map(|lane| {
            let registry = registry_pubkey(lane.context_tier, lane.duration_tier);
            LaneSnapshot {
                lane: lane.label(),
                context_tier: lane.context_tier.label(),
                duration_tier: lane.duration_tier.label(),
                registry: registry.to_string(),
                latest_bundle: None,
                bundle_status: None,
                classification: BundleShape::Missing.label(),
                max_context_length: None,
                bundle_capacity: None,
                requests_len: None,
                total_input_tokens: None,
                maximum_output_tokens: None,
                child_bundle: None,
                request_input_tokens: Vec::new(),
                request_brick_status: RequestBrickStatus::Aggregate,
            }
        })
        .collect();

    ClusterSnapshot {
        observed_at: now_string(),
        slot: None,
        rpc_status,
        refresh_ms,
        summary: summarize_lanes(&lane_snapshots),
        lanes: lane_snapshots,
    }
}

struct TerminalSession {
    active: bool,
    original_termios: Option<termios>,
}

impl TerminalSession {
    fn enter() -> Result<Self> {
        let mut original = None;
        unsafe {
            let mut term: termios = std::mem::zeroed();
            if tcgetattr(STDIN_FILENO, &mut term) == 0 {
                original = Some(term);
                let mut rawish = term;
                rawish.c_lflag &= !(ECHO | ICANON);
                rawish.c_cc[VMIN] = 0;
                rawish.c_cc[VTIME] = 0;
                if tcsetattr(STDIN_FILENO, TCSANOW, &rawish) != 0 {
                    return Err(anyhow!(std::io::Error::last_os_error()));
                }
            }
        }
        print!("\x1b[?1049h\x1b[?7l\x1b[?25l{ANSI_REDRAW_FRAME}");
        std::io::stdout().flush()?;
        Ok(Self {
            active: true,
            original_termios: original,
        })
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        if self.active {
            if let Some(term) = self.original_termios.take() {
                unsafe {
                    let _ = tcsetattr(STDIN_FILENO, TCSANOW, &term);
                }
            }
            let _ = write!(std::io::stdout(), "\x1b[?25h\x1b[?7h\x1b[?1049l");
            let _ = std::io::stdout().flush();
        }
    }
}

fn terminal_rows() -> Option<usize> {
    unsafe {
        let mut ws: winsize = std::mem::zeroed();
        if ioctl(STDOUT_FILENO, TIOCGWINSZ, &mut ws) == 0 && ws.ws_row > 0 {
            Some(ws.ws_row as usize)
        } else {
            None
        }
    }
}

fn new_rpc_client(args: &Args) -> RpcClient {
    RpcClient::new_with_timeout_and_commitment(
        args.cluster_rpc.clone(),
        Duration::from_millis(args.rpc_timeout_ms),
        CommitmentConfig::confirmed(),
    )
}

async fn watch_with_rpc(
    args: &Args,
    lanes: &[LaneKey],
    expected_targets: ExpectedTargets,
    expected_bundle_capacities: ExpectedBundleCapacities,
) -> Result<()> {
    let rpc = new_rpc_client(args);
    let mut tracker = HealthTracker::new(args.refresh_ms);
    let mut events = VecDeque::with_capacity(args.events);
    let mut last_snapshot: Option<ClusterSnapshot> = None;
    let mut request_token_cache = RequestTokenCache::default();
    let mut logs = LogWriters::new(args.snapshot_log.as_ref(), args.events_log.as_ref())?;
    let started_at = Instant::now();
    let mut poll_count = 0u64;
    let interactive = !args.once && std::io::stdout().is_terminal();
    let use_color = std::io::stdout().is_terminal() && !args.json;
    let _terminal = if interactive {
        Some(TerminalSession::enter()?)
    } else {
        None
    };
    if interactive {
        print!(
            "\x1b[2J\x1b[Hwatch-bundles\nloading first snapshot from {} ...\n",
            args.cluster_rpc
        );
        std::io::stdout().flush()?;
    }

    loop {
        let fetch_result = if interactive {
            tokio::select! {
                result = fetch_snapshot(
                    &rpc,
                    &lanes,
                    expected_targets,
                    expected_bundle_capacities,
                    tracker.current_refresh_ms(),
                    tracker.status,
                    &request_token_cache,
                ) => result,
                _ = tokio::signal::ctrl_c() => {
                    return Ok(());
                }
            }
        } else {
            fetch_snapshot(
                &rpc,
                &lanes,
                expected_targets,
                expected_bundle_capacities,
                tracker.current_refresh_ms(),
                tracker.status,
                &request_token_cache,
            )
            .await
        };

        match fetch_result {
            Ok((mut snapshot, bundles_needing_request_refresh)) => {
                poll_count = poll_count.saturating_add(1);
                let max_rows = if interactive { terminal_rows() } else { None };
                let previous_status = last_snapshot.as_ref().map(|value| value.rpc_status);
                let slot = snapshot.slot.unwrap_or_default();
                snapshot.rpc_status = tracker.record_success(slot, started_at.elapsed());
                snapshot.refresh_ms = tracker.current_refresh_ms();

                if args.once && !bundles_needing_request_refresh.is_empty() {
                    refresh_request_token_cache(
                        &rpc,
                        &bundles_needing_request_refresh,
                        &mut request_token_cache,
                        REQUEST_BRICK_TIMEOUT_MS,
                        bundles_needing_request_refresh.len(),
                    )
                    .await;
                    let (refetched_snapshot, _) = fetch_snapshot(
                        &rpc,
                        &lanes,
                        expected_targets,
                        expected_bundle_capacities,
                        tracker.current_refresh_ms(),
                        snapshot.rpc_status,
                        &request_token_cache,
                    )
                    .await?;
                    snapshot = refetched_snapshot;
                }

                let lane_changes = collect_lane_changes(last_snapshot.as_ref(), &snapshot);
                let mut new_events = diff_snapshots(last_snapshot.as_ref(), &snapshot);
                if previous_status.is_some_and(|status| status != snapshot.rpc_status) {
                    new_events.push(system_event(
                        "rpc_status_changed",
                        format!("feed status changed to {}", snapshot.rpc_status.label()),
                    ));
                }
                let new_event_count = new_events.len();
                let new_events = push_events(&mut events, new_events, args.events);
                logs.write_snapshot(&snapshot)?;
                logs.write_events(&new_events)?;

                if args.once {
                    if args.json {
                        println!("{}", serde_json::to_string_pretty(&snapshot)?);
                    } else {
                        print!(
                            "{}",
                            render_dashboard(
                                &snapshot,
                                &events,
                                &lane_changes,
                                new_event_count,
                                poll_count,
                                max_rows,
                                false,
                                use_color,
                            )
                        );
                    }
                    return Ok(());
                }

                print!(
                    "{}",
                    render_dashboard(
                        &snapshot,
                        &events,
                        &lane_changes,
                        new_event_count,
                        poll_count,
                        max_rows,
                        interactive,
                        use_color,
                    )
                );
                std::io::stdout().flush()?;
                last_snapshot = Some(snapshot);

                if interactive {
                    refresh_request_token_cache(
                        &rpc,
                        &bundles_needing_request_refresh,
                        &mut request_token_cache,
                        LIVE_REQUEST_BRICK_TIMEOUT_MS,
                        REQUEST_BRICK_REFRESH_LIMIT,
                    )
                    .await;
                }
            }
            Err(error) => {
                poll_count = poll_count.saturating_add(1);
                let max_rows = if interactive { terminal_rows() } else { None };
                if args.once {
                    return Err(anyhow!(error));
                }

                let previous_status = last_snapshot.as_ref().map(|value| value.rpc_status);
                let rpc_status = tracker.record_failure(classify_client_error(&error));
                let mut fallback_snapshot = last_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.with_status(rpc_status, tracker.current_refresh_ms()))
                    .unwrap_or_else(|| empty_snapshot(&lanes, tracker.current_refresh_ms(), rpc_status));
                fallback_snapshot.rpc_status = rpc_status;
                fallback_snapshot.refresh_ms = tracker.current_refresh_ms();

                let mut new_events = Vec::new();
                if previous_status.is_none_or(|status| status != rpc_status) {
                    new_events.push(system_event(
                        "rpc_status_changed",
                        format!("feed status changed to {}", rpc_status.label()),
                    ));
                }
                new_events.push(system_event(
                    "rpc_poll_error",
                    format!("rpc polling error: {error}"),
                ));
                let new_event_count = new_events.len();
                let new_events = push_events(&mut events, new_events, args.events);
                logs.write_snapshot(&fallback_snapshot)?;
                logs.write_events(&new_events)?;

                print!(
                    "{}",
                    render_dashboard(
                        &fallback_snapshot,
                        &events,
                        &BTreeMap::new(),
                        new_event_count,
                        poll_count,
                        max_rows,
                        interactive,
                        use_color,
                    )
                );
                std::io::stdout().flush()?;
                last_snapshot = Some(fallback_snapshot);
            }
        }

        if interactive {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(tracker.current_refresh_ms())) => {}
                _ = tokio::signal::ctrl_c() => {
                    return Ok(());
                }
            }
        } else {
            tokio::time::sleep(Duration::from_millis(tracker.current_refresh_ms())).await;
        }
    }
}

async fn watch_with_yellowstone(
    args: &Args,
    lanes: &[LaneKey],
    expected_targets: ExpectedTargets,
    expected_bundle_capacities: ExpectedBundleCapacities,
) -> Result<()> {
    let yellowstone_url = args
        .yellowstone_url
        .clone()
        .ok_or_else(|| anyhow!("yellowstone live mode requires --yellowstone-url"))?;
    let mut tracker = HealthTracker::new(args.refresh_ms);
    let mut events = VecDeque::with_capacity(args.events);
    let mut last_snapshot: Option<ClusterSnapshot> = None;
    let mut logs = LogWriters::new(args.snapshot_log.as_ref(), args.events_log.as_ref())?;
    let mut state = YellowstoneState::default();
    let mut request_token_cache = RequestTokenCache::default();
    let rpc = new_rpc_client(args);
    let started_at = Instant::now();
    let mut poll_count = 0u64;
    let interactive = !args.once && std::io::stdout().is_terminal();
    let use_color = std::io::stdout().is_terminal() && !args.json;
    let _terminal = if interactive {
        Some(TerminalSession::enter()?)
    } else {
        None
    };
    if interactive {
        print!(
            "\x1b[2J\x1b[Hwatch-bundles\nyellowstone live mode\nloading initial stream state from {} ...\n",
            yellowstone_url
        );
        std::io::stdout().flush()?;
    }

    let geyser = CloneableGeyserGrpcClient::new_with_options(
        yellowstone_url,
        args.yellowstone_x_token.clone(),
        true,
    )
    .await?;

    if let Err(error) =
        seed_yellowstone_state_from_rpc(&rpc, lanes, &mut state, &mut request_token_cache).await
    {
        push_event(
            &mut events,
            system_event("rpc_poll_error", format!("yellowstone rpc seed failed: {error}")),
            args.events,
        );
    } else if let Some(slot) = state.slot {
        tracker.record_success(slot, started_at.elapsed());
    }

    let (sender, mut receiver): (
        UnboundedSender<YellowstoneEvent>,
        UnboundedReceiver<YellowstoneEvent>,
    ) = unbounded_channel();
    let registry_pubkeys: Vec<_> = lanes
        .iter()
        .map(|lane| registry_pubkey(lane.context_tier, lane.duration_tier))
        .collect();
    let _slot_subscription = spawn_slot_subscription(geyser.clone(), sender.clone());
    let _registry_subscription =
        spawn_registry_subscription(geyser.clone(), registry_pubkeys, sender.clone());
    let mut bundle_subscription: Option<JoinHandle<()>> = None;
    let mut request_subscription: Option<JoinHandle<()>> = None;
    let mut subscribed_latest_bundles = BTreeSet::new();

    let mut render_interval = tokio::time::interval(Duration::from_millis(args.refresh_ms));
    render_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_event = receiver.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                match event {
                    YellowstoneEvent::Slot(slot) => {
                        state.slot = Some(slot);
                        tracker.record_success(slot, started_at.elapsed());
                    }
                    YellowstoneEvent::Registry(pubkey, registry) => {
                        state.registries.insert(pubkey, registry);
                        let latest_bundles = latest_bundles_for_lanes(&state, lanes);
                        if latest_bundles != subscribed_latest_bundles {
                            subscribed_latest_bundles = latest_bundles.clone();
                            state.current_request_bundles = latest_bundles.clone();
                            if let Some(handle) = bundle_subscription.take() {
                                handle.abort();
                            }
                            if let Some(handle) = request_subscription.take() {
                                handle.abort();
                            }
                            let bundle_pubkeys: Vec<_> = latest_bundles.iter().copied().collect();
                            bundle_subscription = spawn_bundle_subscription(
                                geyser.clone(),
                                bundle_pubkeys.clone(),
                                sender.clone(),
                            );
                            request_subscription = spawn_job_request_subscription(
                                geyser.clone(),
                                bundle_pubkeys,
                                sender.clone(),
                            );
                            if let Err(error) = hydrate_latest_bundles_from_rpc(
                                &rpc,
                                &subscribed_latest_bundles,
                                &mut state,
                                &mut request_token_cache,
                            )
                            .await
                            {
                                push_event(
                                    &mut events,
                                    system_event(
                                        "rpc_poll_error",
                                        format!("yellowstone rpc hydrate failed: {error}"),
                                    ),
                                    args.events,
                                );
                            }
                        }
                    }
                    YellowstoneEvent::Bundle(pubkey, bundle) => {
                        state.bundles.insert(pubkey, bundle);
                    }
                    YellowstoneEvent::JobRequest(pubkey, job_request) => {
                        state
                            .requests_by_bundle
                            .entry(Pubkey::new_from_array(job_request.bundle.inner()))
                            .or_default()
                            .insert(pubkey, job_request.input_token_count);
                    }
                    YellowstoneEvent::StreamError { source, message } => {
                        tracker.record_failure(PollFailureKind::Offline);
                        push_event(
                            &mut events,
                            system_event("rpc_poll_error", format!("{source}: {message}")),
                            args.events,
                        );
                    }
                }
            }
            _ = render_interval.tick() => {
                poll_count = poll_count.saturating_add(1);
                let rpc_status = tracker.current_status(started_at.elapsed());
                let snapshot = snapshot_from_yellowstone_state(
                    &state,
                    &request_token_cache,
                    lanes,
                    expected_targets,
                    expected_bundle_capacities,
                    args.refresh_ms,
                    rpc_status,
                );
                let max_rows = if interactive { terminal_rows() } else { None };
                let previous_status = last_snapshot.as_ref().map(|value| value.rpc_status);
                let lane_changes = collect_lane_changes(last_snapshot.as_ref(), &snapshot);
                let mut new_events = diff_snapshots(last_snapshot.as_ref(), &snapshot);
                if previous_status.is_some_and(|status| status != snapshot.rpc_status) {
                    new_events.push(system_event(
                        "rpc_status_changed",
                        format!("feed status changed to {}", snapshot.rpc_status.label()),
                    ));
                }
                let new_event_count = new_events.len();
                let new_events = push_events(&mut events, new_events, args.events);
                logs.write_snapshot(&snapshot)?;
                logs.write_events(&new_events)?;

                print!(
                    "{}",
                    render_dashboard(
                        &snapshot,
                        &events,
                        &lane_changes,
                        new_event_count,
                        poll_count,
                        max_rows,
                        interactive,
                        use_color,
                    )
                );
                std::io::stdout().flush()?;
                last_snapshot = Some(snapshot);
            }
            _ = tokio::signal::ctrl_c() => {
                return Ok(());
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let lanes = selected_lanes(&args.context_tier, &args.duration_tier);
    let mut expected_targets = ExpectedTargets::default();
    let mut expected_bundle_capacities = ExpectedBundleCapacities::default();
    for override_arg in &args.expected_max_context {
        expected_targets.apply_override(override_arg);
    }
    for override_arg in &args.expected_bundle_capacity {
        expected_bundle_capacities.apply_override(override_arg);
    }

    if !args.once && args.yellowstone_url.is_some() {
        watch_with_yellowstone(&args, &lanes, expected_targets, expected_bundle_capacities).await
    } else {
        watch_with_rpc(&args, &lanes, expected_targets, expected_bundle_capacities).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lane_snapshot(
        lane: &str,
        latest_bundle: Option<&str>,
        bundle_status: Option<&'static str>,
        classification: &'static str,
        max_context_length: Option<u64>,
        requests_len: Option<u64>,
    ) -> LaneSnapshot {
        LaneSnapshot {
            lane: lane.to_string(),
            context_tier: "eco",
            duration_tier: "eco",
            registry: "registry".to_string(),
            latest_bundle: latest_bundle.map(str::to_string),
            bundle_status,
            classification,
            max_context_length,
            bundle_capacity: Some(24),
            requests_len,
            total_input_tokens: Some(1_000),
            maximum_output_tokens: Some(128),
            child_bundle: None,
            request_input_tokens: vec![12_000, 12_000, 12_000],
            request_brick_status: RequestBrickStatus::Live,
        }
    }

    #[test]
    fn parses_expected_max_context_override() {
        let parsed = parse_expected_max_context("pro=202752").unwrap();
        assert_eq!(parsed.tier, TierArg::Pro);
        assert_eq!(parsed.value, 202_752);
        assert!(parse_expected_max_context("wat=1").is_err());
        assert!(parse_expected_max_context("eco").is_err());
    }

    #[test]
    fn parses_expected_bundle_capacity_override() {
        let parsed = parse_expected_bundle_capacity("pro=30").unwrap();
        assert_eq!(parsed.tier, TierArg::Pro);
        assert_eq!(parsed.value, 30);
        assert!(parse_expected_bundle_capacity("wat=1").is_err());
        assert!(parse_expected_bundle_capacity("eco").is_err());
    }

    #[test]
    fn selects_lane_cross_product() {
        let lanes = selected_lanes(&[TierArg::Eco], &[TierArg::Standard, TierArg::Pro]);
        assert_eq!(
            lanes,
            vec![
                LaneKey {
                    context_tier: TierArg::Eco,
                    duration_tier: TierArg::Standard,
                },
                LaneKey {
                    context_tier: TierArg::Eco,
                    duration_tier: TierArg::Pro,
                },
            ]
        );
    }

    #[test]
    fn applies_expected_targets_override() {
        let mut expected = ExpectedTargets::default();
        expected.apply_override(&ExpectedMaxContextArg {
            tier: TierArg::Pro,
            value: 999_999,
        });
        assert_eq!(expected.for_tier(TierArg::Eco), 12_000);
        assert_eq!(expected.for_tier(TierArg::Pro), 999_999);
    }

    #[test]
    fn applies_expected_bundle_capacity_override() {
        let mut expected = ExpectedBundleCapacities::default();
        expected.apply_override(&ExpectedBundleCapacityArg {
            tier: TierArg::Pro,
            value: 30,
        });
        assert_eq!(expected.for_tier(TierArg::Eco), 24);
        assert_eq!(expected.for_tier(TierArg::Pro), 30);
    }

    #[test]
    fn summarizes_target_and_legacy_rows() {
        let summary = summarize_lanes(&[
            lane_snapshot("eco/eco", Some("bundle-a"), Some("active"), "target", Some(12_000), Some(3)),
            lane_snapshot(
                "pro/pro",
                Some("bundle-b"),
                Some("full"),
                "legacy/custom",
                Some(200_000),
                Some(2),
            ),
            lane_snapshot("eco/pro", None, None, "missing", None, None),
        ]);
        assert_eq!(summary.target_lanes, 1);
        assert_eq!(summary.legacy_custom_lanes, 1);
        assert_eq!(summary.missing_lanes, 1);
        assert_eq!(summary.target_requests_len, 3);
        assert_eq!(summary.legacy_custom_requests_len, 2);
    }

    #[test]
    fn diffs_latest_bundle_and_request_changes() {
        let previous = ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(10),
            rpc_status: RpcStatus::Healthy,
            refresh_ms: 1_000,
            lanes: vec![lane_snapshot(
                "eco/eco",
                Some("bundle-a"),
                Some("active"),
                "legacy/custom",
                Some(43_000),
                Some(24),
            )],
            summary: SnapshotSummary::default(),
        };
        let current = ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(11),
            rpc_status: RpcStatus::Healthy,
            refresh_ms: 1_000,
            lanes: vec![lane_snapshot(
                "eco/eco",
                Some("bundle-b"),
                Some("full"),
                "target",
                Some(12_000),
                Some(1),
            )],
            summary: SnapshotSummary::default(),
        };

        let events = diff_snapshots(Some(&previous), &current);
        assert!(events.iter().any(|event| event.event_type == "latest_bundle_shift"));
        assert!(events.iter().any(|event| event.event_type == "requests_len_changed"));
        assert!(events.iter().any(|event| event.event_type == "bundle_status_changed"));
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "max_context_length_changed")
        );
    }

    #[test]
    fn collects_per_lane_change_flags() {
        let previous = ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(10),
            rpc_status: RpcStatus::Healthy,
            refresh_ms: 1_000,
            lanes: vec![lane_snapshot(
                "eco/eco",
                Some("bundle-a"),
                Some("active"),
                "legacy/custom",
                Some(43_000),
                Some(24),
            )],
            summary: SnapshotSummary::default(),
        };
        let current = ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(11),
            rpc_status: RpcStatus::Healthy,
            refresh_ms: 1_000,
            lanes: vec![lane_snapshot(
                "eco/eco",
                Some("bundle-b"),
                Some("full"),
                "target",
                Some(12_000),
                Some(1),
            )],
            summary: SnapshotSummary::default(),
        };

        let changes = collect_lane_changes(Some(&previous), &current);
        assert_eq!(
            changes.get("eco/eco"),
            Some(&LaneChange {
                latest_bundle_changed: true,
                requests_len_changed: true,
                bundle_status_changed: true,
                max_context_length_changed: true,
            })
        );
    }

    #[test]
    fn tracks_healthy_lagging_rate_limited_and_offline() {
        let mut tracker = HealthTracker::new(1_000);
        let mut cold_tracker = HealthTracker::new(1_000);
        assert_eq!(
            cold_tracker.record_failure(PollFailureKind::Offline),
            RpcStatus::Offline
        );

        assert_eq!(
            tracker.record_success(100, Duration::from_secs(0)),
            RpcStatus::Healthy
        );
        assert_eq!(
            tracker.record_success(100, Duration::from_secs(1)),
            RpcStatus::Healthy
        );
        assert_eq!(
            tracker.record_success(100, Duration::from_secs(3)),
            RpcStatus::Lagging
        );
        assert_eq!(
            tracker.record_failure(PollFailureKind::RateLimited),
            RpcStatus::RateLimited
        );
        assert_eq!(tracker.current_refresh_ms(), RATE_LIMIT_REFRESH_MS);
        assert_eq!(
            tracker.record_success(101, Duration::from_secs(4)),
            RpcStatus::Healthy
        );
        assert_eq!(tracker.current_refresh_ms(), 1_000);
        assert_eq!(
            tracker.record_failure(PollFailureKind::Offline),
            RpcStatus::Healthy
        );
        assert_eq!(
            tracker.record_failure(PollFailureKind::Offline),
            RpcStatus::Offline
        );
    }

    #[test]
    fn clap_rejects_json_without_once() {
        assert!(Args::try_parse_from(["watch-bundles", "--json"]).is_err());
        assert!(Args::try_parse_from(["watch-bundles", "--once", "--json"]).is_ok());
    }

    #[test]
    fn once_json_snapshot_serializes() {
        let snapshot = ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(123),
            rpc_status: RpcStatus::Healthy,
            refresh_ms: 1_000,
            lanes: vec![lane_snapshot(
                "eco/eco",
                Some("bundle-a"),
                Some("active"),
                "target",
                Some(12_000),
                Some(1),
            )],
            summary: SnapshotSummary::default(),
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("\"slot\":123"));
        assert!(json.contains("\"rpc_status\":\"healthy\""));
        assert!(json.contains("\"lane\":\"eco/eco\""));
    }

    #[test]
    fn compact_token_label_uses_readable_brick_values() {
        assert_eq!(compact_token_label(999), "999");
        assert_eq!(compact_token_label(1_200), "1.2k");
        assert_eq!(compact_token_label(12_000), "12k");
        assert_eq!(compact_token_label(202_752), "203k");
        assert_eq!(compact_token_label(1_250_000), "1.3m");
    }

    #[test]
    fn short_lane_label_uses_std_abbreviation() {
        assert_eq!(short_tier_label("standard"), "std");
        assert_eq!(short_lane_label("standard", "standard"), "std/std");
        assert_eq!(short_lane_label("standard", "pro"), "std/pro");
    }

    #[test]
    fn yellowstone_request_tokens_prefer_cache_when_stream_is_incomplete() {
        let bundle_pubkey = Pubkey::new_unique();
        let mut state = YellowstoneState::default();
        state.current_request_bundles.insert(bundle_pubkey);
        state
            .requests_by_bundle
            .entry(bundle_pubkey)
            .or_default()
            .insert(Pubkey::new_unique(), 369);

        let mut cache = RequestTokenCache::default();
        cache.insert(bundle_pubkey, vec![501, 395, 369], 3, 1_265);

        let (tokens, status) = request_tokens_for_bundle(&state, &cache, bundle_pubkey, 3, 1_265);
        assert_eq!(tokens, vec![501, 395, 369]);
        assert_eq!(status, RequestBrickStatus::Live);
    }

    #[test]
    fn lane_matrix_renders_request_bricks() {
        let snapshot = ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(123),
            rpc_status: RpcStatus::Healthy,
            refresh_ms: 1_000,
            lanes: vec![lane_snapshot(
                "eco/eco",
                Some("bundle-a"),
                Some("active"),
                "target",
                Some(12_000),
                Some(3),
            )],
            summary: SnapshotSummary::default(),
        };

        let matrix = render_lane_matrix(&snapshot, &BTreeMap::new(), false);
        assert!(matrix.contains("bundle matrix"));
        assert!(matrix.contains("eco/eco"));
        assert!(matrix.contains("12"));
    }

    #[test]
    fn lane_matrix_uses_short_size_speed_labels_in_tiles() {
        let snapshot = ClusterSnapshot {
            observed_at: now_string(),
            slot: Some(123),
            rpc_status: RpcStatus::Healthy,
            refresh_ms: 1_000,
            lanes: vec![LaneSnapshot {
                lane: "standard/standard".to_string(),
                context_tier: "standard",
                duration_tier: "standard",
                registry: "registry".to_string(),
                latest_bundle: Some("bundle-a".to_string()),
                bundle_status: Some("active"),
                classification: "target",
                max_context_length: Some(32_000),
                bundle_capacity: Some(6),
                requests_len: Some(3),
                total_input_tokens: Some(12_000),
                maximum_output_tokens: Some(128),
                child_bundle: None,
                request_input_tokens: vec![12_000, 11_000, 9_000],
                request_brick_status: RequestBrickStatus::Live,
            }],
            summary: SnapshotSummary::default(),
        };

        let matrix = render_lane_matrix(&snapshot, &BTreeMap::new(), false);
        assert!(matrix.contains("std/std"));
        assert!(!matrix.contains("standard/standard ["));
    }
}
