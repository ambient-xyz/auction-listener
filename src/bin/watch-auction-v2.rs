use ambient_auction_api_v2::{BundleEscrowV2, BundleVerifierPageV2, Pubkey as ApiPubkey};
use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use serde_json::{json, Value};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Duration,
};

const DEFAULT_RPC_URL: &str = "http://rpc.ambient.xyz:8899";
const DEFAULT_PROGRAM_ID: &str = "Auction111111111111111111111111111111111111";

#[derive(Parser, Debug)]
struct Cli {
    /// Solana RPC URL
    #[arg(long, default_value = DEFAULT_RPC_URL)]
    rpc_url: String,
    /// Auction program id
    #[arg(long, default_value = DEFAULT_PROGRAM_ID)]
    program_id: String,
    /// Run one bounded poll and exit
    #[arg(long)]
    once: bool,
    /// Poll interval when not using --once
    #[arg(long, default_value_t = 5_000)]
    interval_ms: u64,
    /// Recent successful program signatures to inspect each poll
    #[arg(long, default_value_t = 100)]
    signature_limit: usize,
    /// Maximum unique v2 accounts to fetch per poll
    #[arg(long, default_value_t = 200)]
    max_accounts: usize,
    /// Accounts per getMultipleAccounts request
    #[arg(long, default_value_t = 100)]
    rpc_batch_size: usize,
    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Pretty)]
    format: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Pretty,
    Jsonl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V2AccountKind {
    BundleEscrow,
    BundleVerifierPage,
}

impl V2AccountKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::BundleEscrow => "bundle_escrow_v2",
            Self::BundleVerifierPage => "bundle_verifier_page_v2",
        }
    }
}

#[derive(Clone, Debug)]
struct V2Candidate {
    pubkey: Pubkey,
    kind_hint: V2AccountKind,
    source_signature: String,
    source_slot: Option<u64>,
    source_instruction: &'static str,
}

#[derive(Default)]
struct V2CandidateCollector {
    candidates: Vec<V2Candidate>,
    seen: HashSet<Pubkey>,
}

impl V2CandidateCollector {
    fn insert(
        &mut self,
        pubkey: &str,
        kind_hint: V2AccountKind,
        signature: &str,
        slot: Option<u64>,
        instruction: &'static str,
    ) {
        let Ok(pubkey) = Pubkey::from_str(pubkey) else {
            return;
        };
        if self.seen.insert(pubkey) {
            self.candidates.push(V2Candidate {
                pubkey,
                kind_hint,
                source_signature: signature.to_string(),
                source_slot: slot,
                source_instruction: instruction,
            });
        }
    }
}

#[derive(Serialize)]
struct OutputRow {
    source_signature: String,
    source_slot: Option<u64>,
    source_instruction: String,
    account_address: String,
    account_type: String,
    layout_version: String,
    status: String,
    issue: String,
    account_lamports: Option<u64>,
    account_data_len: Option<usize>,
    summary: Value,
    decoded_account: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let program_id = Pubkey::from_str(&cli.program_id).context("parse --program-id")?;
    let client = RpcClient::new_with_commitment(cli.rpc_url.clone(), CommitmentConfig::confirmed());
    let http = reqwest::Client::new();
    let mut seen_signatures = HashSet::new();

    loop {
        let rows = poll_once(&cli, &client, &http, program_id, &mut seen_signatures).await?;
        write_rows(&cli, &rows)?;
        if cli.once {
            break;
        }
        tokio::time::sleep(Duration::from_millis(cli.interval_ms)).await;
    }

    Ok(())
}

async fn poll_once(
    cli: &Cli,
    client: &RpcClient,
    http: &reqwest::Client,
    program_id: Pubkey,
    seen_signatures: &mut HashSet<String>,
) -> Result<Vec<OutputRow>> {
    let signatures =
        fetch_recent_successful_signatures(http, &cli.rpc_url, program_id, cli.signature_limit)
            .await?;
    let new_signatures = signatures
        .into_iter()
        .filter(|signature| seen_signatures.insert(signature.clone()))
        .collect::<Vec<_>>();
    if new_signatures.is_empty() {
        return Ok(Vec::new());
    }

    let discovery =
        discover_recent_v2_candidates(http, &cli.rpc_url, program_id, &new_signatures).await?;
    if discovery.len() > cli.max_accounts {
        bail!(
            "{} discovered v2 accounts exceeded --max-accounts {}",
            discovery.len(),
            cli.max_accounts
        );
    }

    let keys = discovery
        .iter()
        .map(|candidate| candidate.pubkey)
        .collect::<Vec<_>>();
    let accounts = fetch_accounts(client, cli.rpc_batch_size, &keys).await?;
    Ok(discovery
        .into_iter()
        .map(|candidate| {
            let account = accounts.get(&candidate.pubkey).and_then(Option::as_ref);
            output_row(candidate, account, program_id)
        })
        .collect())
}

async fn fetch_recent_successful_signatures(
    http: &reqwest::Client,
    rpc_url: &str,
    program_id: Pubkey,
    limit: usize,
) -> Result<Vec<String>> {
    let result = rpc_json(
        http,
        rpc_url,
        "getSignaturesForAddress",
        json!([
            program_id.to_string(),
            {
                "commitment": "confirmed",
                "limit": limit.min(1_000)
            }
        ]),
    )
    .await?;
    let page = result
        .as_array()
        .ok_or_else(|| anyhow!("getSignaturesForAddress returned non-array result"))?;
    Ok(page
        .iter()
        .filter(|entry| entry.get("err").is_some_and(Value::is_null))
        .filter_map(|entry| entry.get("signature").and_then(Value::as_str))
        .map(str::to_string)
        .collect())
}

async fn discover_recent_v2_candidates(
    http: &reqwest::Client,
    rpc_url: &str,
    program_id: Pubkey,
    signatures: &[String],
) -> Result<Vec<V2Candidate>> {
    let mut collector = V2CandidateCollector::default();
    let program_id = program_id.to_string();
    for chunk in signatures.chunks(20) {
        let calls = chunk
            .iter()
            .map(|signature| {
                (
                    "getTransaction",
                    json!([
                        signature,
                        {
                            "commitment": "confirmed",
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]),
                )
            })
            .collect::<Vec<_>>();
        let transactions = rpc_json_batch(http, rpc_url, calls).await?;
        for (signature, transaction) in chunk.iter().zip(transactions) {
            if let Some(transaction) = transaction {
                collect_v2_candidates_from_transaction(
                    signature,
                    &program_id,
                    &transaction,
                    &mut collector,
                );
            }
        }
    }
    Ok(collector.candidates)
}

async fn fetch_accounts(
    client: &RpcClient,
    rpc_batch_size: usize,
    keys: &[Pubkey],
) -> Result<HashMap<Pubkey, Option<Account>>> {
    let mut out = HashMap::with_capacity(keys.len());
    for chunk in keys.chunks(rpc_batch_size.clamp(1, 100)) {
        let accounts = client
            .get_multiple_accounts(chunk)
            .await
            .with_context(|| format!("getMultipleAccounts batch of {}", chunk.len()))?;
        for (key, account) in chunk.iter().copied().zip(accounts) {
            out.insert(key, account);
        }
    }
    Ok(out)
}

async fn rpc_json(
    http: &reqwest::Client,
    rpc_url: &str,
    method: &str,
    params: Value,
) -> Result<Value> {
    let response = http
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }))
        .send()
        .await
        .with_context(|| format!("send {method}"))?
        .error_for_status()
        .with_context(|| format!("HTTP status for {method}"))?
        .json::<Value>()
        .await
        .with_context(|| format!("decode {method} JSON"))?;
    if let Some(error) = response.get("error") {
        bail!("{method} RPC error: {error}");
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
}

async fn rpc_json_batch(
    http: &reqwest::Client,
    rpc_url: &str,
    calls: Vec<(&'static str, Value)>,
) -> Result<Vec<Option<Value>>> {
    let request = calls
        .iter()
        .enumerate()
        .map(|(id, (method, params))| {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            })
        })
        .collect::<Vec<_>>();
    let response = http
        .post(rpc_url)
        .json(&request)
        .send()
        .await
        .context("send JSON-RPC batch")?
        .error_for_status()
        .context("HTTP status for JSON-RPC batch")?
        .json::<Value>()
        .await
        .context("decode JSON-RPC batch")?;
    let items = response
        .as_array()
        .ok_or_else(|| anyhow!("JSON-RPC batch returned non-array result"))?;
    let mut results = vec![None; calls.len()];
    for item in items {
        let Some(id) = item.get("id").and_then(Value::as_u64).map(|id| id as usize) else {
            continue;
        };
        if id < results.len() && item.get("error").is_none() {
            results[id] = item.get("result").cloned();
        }
    }
    Ok(results)
}

fn collect_v2_candidates_from_transaction(
    signature: &str,
    program_id: &str,
    transaction: &Value,
    collector: &mut V2CandidateCollector,
) -> usize {
    let slot = transaction.get("slot").and_then(Value::as_u64);
    let keys = transaction_account_keys(transaction);
    let Some(instructions) = transaction
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
    else {
        return 0;
    };
    let mut v2_instruction_count = 0;
    for instruction in instructions {
        let Some(program_id_index) = instruction.get("programIdIndex").and_then(Value::as_u64)
        else {
            continue;
        };
        if keys.get(program_id_index as usize).map(String::as_str) != Some(program_id) {
            continue;
        }
        let Some(instruction_value) = instruction_discriminator(instruction) else {
            continue;
        };
        let Some(instruction_name) = v2_instruction_name(instruction_value) else {
            continue;
        };
        v2_instruction_count += 1;
        let account_keys = instruction
            .get("accounts")
            .and_then(Value::as_array)
            .map(|accounts| {
                accounts
                    .iter()
                    .filter_map(Value::as_u64)
                    .filter_map(|index| keys.get(index as usize).map(String::as_str))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        match instruction_value {
            12 | 13 => {
                if let Some(pubkey) = account_keys.get(1) {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleEscrow,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
            }
            14 => {
                if let Some(pubkey) = account_keys.get(1) {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleEscrow,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
                if let Some(pubkey) = account_keys.get(3) {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleVerifierPage,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
            }
            15 => {
                if let Some(pubkey) = account_keys.get(1) {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleEscrow,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
                for pubkey in account_keys.iter().skip(6) {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleVerifierPage,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
            }
            16 | 17 | 18 => {
                if let Some(pubkey) = account_keys.first() {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleEscrow,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
                if instruction_value == 17 {
                    for pubkey in account_keys.iter().skip(5) {
                        collector.insert(
                            pubkey,
                            V2AccountKind::BundleVerifierPage,
                            signature,
                            slot,
                            instruction_name,
                        );
                    }
                }
            }
            21 => {
                if let Some(pubkey) = account_keys.get(1) {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleEscrow,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
                if let Some(pubkey) = account_keys.get(2) {
                    collector.insert(
                        pubkey,
                        V2AccountKind::BundleVerifierPage,
                        signature,
                        slot,
                        instruction_name,
                    );
                }
            }
            _ => {}
        }
    }
    v2_instruction_count
}

fn transaction_account_keys(transaction: &Value) -> Vec<String> {
    let mut keys = transaction
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .map(|keys| {
            keys.iter()
                .filter_map(|key| {
                    key.as_str()
                        .or_else(|| key.get("pubkey").and_then(Value::as_str))
                })
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for path in [
        "/meta/loadedAddresses/writable",
        "/meta/loadedAddresses/readonly",
    ] {
        if let Some(loaded) = transaction.pointer(path).and_then(Value::as_array) {
            keys.extend(loaded.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    keys
}

fn instruction_discriminator(instruction: &Value) -> Option<u8> {
    let data = instruction.get("data").and_then(|data| {
        data.as_str()
            .or_else(|| data.get(0).and_then(Value::as_str))
    })?;
    bs58::decode(data).into_vec().ok()?.first().copied()
}

fn v2_instruction_name(instruction: u8) -> Option<&'static str> {
    match instruction {
        12 => Some("OpenBundleEscrowV2"),
        13 => Some("CommitAuctionSettlementV2"),
        14 => Some("PostBundleResultV2"),
        15 => Some("FinalizeBundleVerificationV2"),
        16 => Some("ClaimWinnerLstakeV2"),
        17 => Some("ClaimVerifierLstakeV2"),
        18 => Some("ExpireBundleEscrowV2"),
        19 => Some("InitConfigPolicyV2"),
        20 => Some("SetConfigPolicyV2"),
        21 => Some("InitBundleVerifierPageV2"),
        _ => None,
    }
}

fn output_row(candidate: V2Candidate, account: Option<&Account>, program_id: Pubkey) -> OutputRow {
    let account_address = candidate.pubkey.to_string();
    let mut row = OutputRow {
        source_signature: candidate.source_signature,
        source_slot: candidate.source_slot,
        source_instruction: candidate.source_instruction.to_string(),
        account_address,
        account_type: candidate.kind_hint.as_str().to_string(),
        layout_version: String::new(),
        status: String::new(),
        issue: String::new(),
        account_lamports: account.map(|account| account.lamports),
        account_data_len: account.map(|account| account.data.len()),
        summary: Value::Null,
        decoded_account: Value::Null,
    };

    let Some(account) = account else {
        row.issue = "account_missing_now".to_string();
        return row;
    };
    if account.owner != program_id {
        row.issue = format!("owner_not_program:{}", account.owner);
        return row;
    }
    match decode_v2_account(account) {
        Ok(decoded) => {
            row.account_type = decoded.account_type;
            row.layout_version = decoded.layout_version;
            row.status = decoded.status;
            row.summary = decoded.summary;
            row.decoded_account = decoded.decoded_account;
        }
        Err(issue) => row.issue = issue,
    }
    row
}

struct DecodedV2Account {
    account_type: String,
    layout_version: String,
    status: String,
    summary: Value,
    decoded_account: Value,
}

fn decode_v2_account(account: &Account) -> Result<DecodedV2Account, String> {
    if let Some(decoded) = BundleEscrowV2::from_bytes(&account.data) {
        let raw = decoded.as_raw();
        return Ok(DecodedV2Account {
            account_type: "bundle_escrow_v2".to_string(),
            layout_version: format!("{:?}", decoded.layout().version),
            status: escrow_status_label(raw.status.into_u64()).to_string(),
            summary: json!({
                "reward_tier": raw.reward_tier,
                "coordinator": api_pubkey_string(raw.coordinator),
                "requester_refund_recipient": api_pubkey_string(raw.requester_refund_recipient),
                "bundle_version": raw.bundle_version,
                "total_input_tokens": raw.total_input_tokens,
                "max_output_tokens": raw.max_output_tokens,
                "escrow_lamports": raw.escrow_lamports,
                "winner_node_pubkey": api_pubkey_string(raw.winner_node_pubkey),
                "winner_vote_account": api_pubkey_string(raw.winner_vote_account),
                "clearing_price_per_output_token": raw.clearing_price_per_output_token,
                "selected_verifiers": api_pubkey_array(raw.selected_verifiers),
                "posted_output_tokens": raw.posted_output_tokens,
                "accepted_output_tokens": raw.accepted_output_tokens,
                "winner_payout_lamports": raw.winner_payout_lamports,
                "verifier_page_count": raw.verifier_page_count,
                "winner_reward_claimed": raw.winner_reward_claimed,
                "verifier_reward_claimed_bitmap": raw.verifier_reward_claimed_bitmap,
                "quorum_verifier_bitmap": raw.quorum_verifier_bitmap,
            }),
            decoded_account: serde_json::to_value(raw)
                .map_err(|_| "decode_failed_current_v2_account".to_string())?,
        });
    }

    if let Some(decoded) = BundleVerifierPageV2::from_bytes(&account.data) {
        let raw = decoded.as_raw();
        let entry_count = usize::from(raw.entry_count);
        let entries = raw
            .entries
            .iter()
            .take(entry_count)
            .enumerate()
            .map(|(index, entry)| {
                json!({
                    "entry_index": index,
                    "job_id": api_pubkey_string(entry.job_id),
                    "posted_output_tokens": entry.posted_output_tokens,
                    "accepted_output_tokens": entry.accepted_output_tokens,
                    "verdict": verdict_label(u8::from(entry.verdict)),
                    "verifier_claimed_bitmap": entry.verifier_claimed_bitmap,
                    "verifier_reward_tokens": entry.verifier_reward_tokens,
                    "assigned_verifiers_token_ranges": entry.assigned_verifiers_token_ranges,
                })
            })
            .collect::<Vec<_>>();
        return Ok(DecodedV2Account {
            account_type: "bundle_verifier_page_v2".to_string(),
            layout_version: format!("{:?}", decoded.layout().version),
            status: String::new(),
            summary: json!({
                "bundle_escrow": api_pubkey_string(raw.bundle_escrow),
                "page_index": raw.page_index,
                "entry_count": raw.entry_count,
                "entries": entries,
            }),
            decoded_account: serde_json::to_value(raw)
                .map_err(|_| "decode_failed_current_v2_account".to_string())?,
        });
    }

    Err("decode_failed_current_v2_account".to_string())
}

fn write_rows(cli: &Cli, rows: &[OutputRow]) -> Result<()> {
    match cli.format {
        OutputFormat::Jsonl => {
            for row in rows {
                println!("{}", serde_json::to_string(row)?);
            }
        }
        OutputFormat::Pretty => {
            for row in rows {
                print_pretty_row(row)?;
            }
        }
    }
    Ok(())
}

fn print_pretty_row(row: &OutputRow) -> Result<()> {
    println!(
        "[slot {}] {} {}",
        row.source_slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "?".to_string()),
        row.source_instruction,
        row.source_signature
    );
    println!(
        "  {} {} data_len={}",
        row.account_type,
        row.account_address,
        row.account_data_len
            .map(|len| len.to_string())
            .unwrap_or_else(|| "?".to_string())
    );
    if !row.issue.is_empty() {
        println!("    issue={}", row.issue);
        return Ok(());
    }
    if !row.layout_version.is_empty() {
        println!("    layout={}", row.layout_version);
    }
    if !row.status.is_empty() {
        println!("    status={}", row.status);
    }
    match row.account_type.as_str() {
        "bundle_escrow_v2" => print_pretty_escrow(&row.summary),
        "bundle_verifier_page_v2" => print_pretty_page(&row.summary),
        _ => {}
    }
    Ok(())
}

fn print_pretty_escrow(summary: &Value) {
    println!(
        "    tokens input={} max_output={} posted={} accepted={}",
        field(summary, "total_input_tokens"),
        field(summary, "max_output_tokens"),
        field(summary, "posted_output_tokens"),
        field(summary, "accepted_output_tokens")
    );
    println!(
        "    winner node={} vote={} clearing_price={}",
        field(summary, "winner_node_pubkey"),
        field(summary, "winner_vote_account"),
        field(summary, "clearing_price_per_output_token")
    );
    println!(
        "    verifier_pages={} winner_claimed={} verifier_claimed_bitmap={} quorum_bitmap={}",
        field(summary, "verifier_page_count"),
        field(summary, "winner_reward_claimed"),
        field(summary, "verifier_reward_claimed_bitmap"),
        field(summary, "quorum_verifier_bitmap")
    );
}

fn print_pretty_page(summary: &Value) {
    println!(
        "    bundle_escrow={} page_index={} entry_count={}",
        field(summary, "bundle_escrow"),
        field(summary, "page_index"),
        field(summary, "entry_count")
    );
    if let Some(entries) = summary.get("entries").and_then(Value::as_array) {
        for entry in entries {
            println!(
                "    entry {} job={} verdict={} posted={} accepted={}",
                field(entry, "entry_index"),
                field(entry, "job_id"),
                field(entry, "verdict"),
                field(entry, "posted_output_tokens"),
                field(entry, "accepted_output_tokens")
            );
        }
    }
}

fn field(value: &Value, key: &str) -> String {
    match value.get(key) {
        Some(Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

fn api_pubkey_string(pubkey: ApiPubkey) -> String {
    Pubkey::new_from_array(pubkey.inner()).to_string()
}

fn api_pubkey_array<const N: usize>(pubkeys: [ApiPubkey; N]) -> Vec<String> {
    pubkeys.into_iter().map(api_pubkey_string).collect()
}

fn escrow_status_label(value: u64) -> &'static str {
    match value {
        0 => "Open",
        1 => "Awarded",
        2 => "ResultPosted",
        3 => "FinalizedVerified",
        4 => "FinalizedRejected",
        5 => "Expired",
        _ => "Invalid",
    }
}

fn verdict_label(value: u8) -> &'static str {
    match value {
        0 => "Pending",
        1 => "Verified",
        2 => "Rejected",
        _ => "Invalid",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ambient_auction_api_v2::{BundleVerifierPageV2Entry, VerificationVerdictV2};

    fn program_id() -> Pubkey {
        Pubkey::from_str(DEFAULT_PROGRAM_ID).unwrap()
    }

    #[test]
    fn watch_auction_v2_collects_loaded_address_candidates() {
        let program = DEFAULT_PROGRAM_ID;
        let bundle = Pubkey::new_unique().to_string();
        let page = Pubkey::new_unique().to_string();
        let transaction = json!({
            "slot": 123,
            "transaction": {
                "message": {
                    "accountKeys": [
                        Pubkey::new_unique().to_string(),
                        bundle,
                        program,
                    ],
                    "instructions": [{
                        "programIdIndex": 2,
                        "accounts": [0, 1, 3],
                        "data": bs58::encode([21u8]).into_string()
                    }]
                }
            },
            "meta": {
                "loadedAddresses": {
                    "writable": [page],
                    "readonly": []
                }
            }
        });
        let mut collector = V2CandidateCollector::default();

        let count =
            collect_v2_candidates_from_transaction("sig", program, &transaction, &mut collector);

        assert_eq!(count, 1);
        assert_eq!(collector.candidates.len(), 2);
        assert_eq!(
            collector.candidates[0].kind_hint,
            V2AccountKind::BundleEscrow
        );
        assert_eq!(
            collector.candidates[1].kind_hint,
            V2AccountKind::BundleVerifierPage
        );
        assert_eq!(
            collector.candidates[0].source_instruction,
            "InitBundleVerifierPageV2"
        );
    }

    #[test]
    fn watch_auction_v2_decodes_bundle_escrow() {
        let mut escrow = BundleEscrowV2::default();
        escrow.total_input_tokens = 10;
        escrow.max_output_tokens = 20;
        escrow.clearing_price_per_output_token = 30;
        let mut data = vec![0; BundleEscrowV2::LEN_V2];
        assert!(escrow.write_v2_bytes(&mut data));
        let account = Account {
            lamports: 1,
            data,
            owner: program_id(),
            executable: false,
            rent_epoch: 0,
        };

        let decoded = decode_v2_account(&account).unwrap();

        assert_eq!(decoded.account_type, "bundle_escrow_v2");
        assert_eq!(decoded.status, "Open");
        assert_eq!(decoded.summary["total_input_tokens"], 10);
        assert_eq!(decoded.summary["max_output_tokens"], 20);
        assert_eq!(decoded.summary["clearing_price_per_output_token"], 30);
    }

    #[test]
    fn watch_auction_v2_decodes_verifier_page() {
        let mut page = BundleVerifierPageV2::default();
        page.page_index = 2;
        page.entry_count = 1;
        page.entries[0] = BundleVerifierPageV2Entry {
            job_id: [9; 32].into(),
            posted_output_tokens: 11,
            accepted_output_tokens: 7,
            verdict: VerificationVerdictV2::Verified,
            ..BundleVerifierPageV2Entry::default()
        };
        let mut data = vec![0; BundleVerifierPageV2::LEN_V2];
        assert!(page.write_v2_bytes(&mut data));
        let account = Account {
            lamports: 1,
            data,
            owner: program_id(),
            executable: false,
            rent_epoch: 0,
        };

        let decoded = decode_v2_account(&account).unwrap();

        assert_eq!(decoded.account_type, "bundle_verifier_page_v2");
        assert_eq!(decoded.summary["page_index"], 2);
        assert_eq!(decoded.summary["entry_count"], 1);
        assert_eq!(decoded.summary["entries"][0]["verdict"], "Verified");
    }

    #[test]
    fn watch_auction_v2_reports_missing_wrong_owner_and_bad_data() {
        let candidate = V2Candidate {
            pubkey: Pubkey::new_unique(),
            kind_hint: V2AccountKind::BundleEscrow,
            source_signature: "sig".to_string(),
            source_slot: Some(1),
            source_instruction: "OpenBundleEscrowV2",
        };
        let missing = output_row(candidate.clone(), None, program_id());
        assert_eq!(missing.issue, "account_missing_now");

        let wrong_owner = Account {
            lamports: 1,
            data: Vec::new(),
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };
        let wrong_owner_row = output_row(candidate.clone(), Some(&wrong_owner), program_id());
        assert!(wrong_owner_row.issue.starts_with("owner_not_program:"));

        let bad_data = Account {
            owner: program_id(),
            ..wrong_owner
        };
        let bad_data_row = output_row(candidate, Some(&bad_data), program_id());
        assert_eq!(bad_data_row.issue, "decode_failed_current_v2_account");
    }
}
