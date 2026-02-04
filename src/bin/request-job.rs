use ambient_auction_api::{bundle::RequestBundle, state::RequestTier, *};
use ambient_auction_client::sdk::request_job;
use ambient_auction_listener::CLIENT_URL;
use base64::prelude::*;
use clap::Parser;

use reqwest_eventsource::CannotCloneRequestError;
use sha2::{Digest, Sha256};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    client_error::ClientError,
    nonblocking::{pubsub_client::PubsubClientError, rpc_client::RpcClient},
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use solana_sdk::{
    message::{v0::Message, CompileError, VersionedMessage},
    pubkey::Pubkey,
    signer::{keypair::read_keypair_file, Signer, SignerError},
    transaction::VersionedTransaction,
};
use std::io::{IsTerminal, Read};
use std::{io, path::PathBuf};
use thiserror::Error as ThisError;
use ExitCondition::*;

#[allow(clippy::upper_case_acronyms)]
#[repr(i32)]
enum ExitCondition {
    INSUFFICIENTBALANCE = 1,
    INVALIDINPUT = 2,
    // COMMUNICATION = 3,
    // NOBIDDERS = 4,
    // INVALIDOUTPUT = 5,
    NOBUNDLE = 6,
}

impl ExitCondition {
    fn terminate(self) -> ! {
        std::process::exit(self as i32)
    }
}

#[derive(Parser)]
struct Args {
    /// The keypair that will pay for the auction
    payer_keypair: PathBuf,
    /// How long the auction will run in slots
    expiry: u64,
    /// The maximum amount of Lamports to spend on the auction. This is paid into escrow.
    max_price: u64,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long)]
    cluster_rpc: Option<String>,
    /// The Solana PUBSUB cluster URL. Defaults to http://localhost:8900
    #[arg(short = 'p', long)]
    cluster_pubsub: Option<String>,
    /// The model to use. Defaults to deepseek-ai/DeepSeek-R1.
    /// This should only be used during development.
    #[arg(short = 'm', long)]
    model: Option<String>,
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("An Internal error ocurred: {0}")]
    Internal(String),
    #[error("Solana RPC Client error: {0}")]
    RPC(#[from] ClientError),
    #[error("Pubsub client error: {0}")]
    Pubsub(#[from] PubsubClientError),
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Got unexpected account bytes.")]
    UnexpectedBytes,
    #[error("Account data was the wrong size. Expected {expected}. Got {actual}")]
    InvalidAccountSize { actual: usize, expected: usize },
    #[error("Account with pubkey {0} does not exist")]
    AccountNotExist(Pubkey),
    #[error("Programmer bug: cannot clone request: {0}")]
    CannotCloneRequest(#[from] CannotCloneRequestError),
    #[error("BUG: Solana instruction compilation error: {0}")]
    Compilation(#[from] CompileError),
    #[error("BUG: Solana instruction signer error: {0}")]
    TransactionSignature(#[from] SignerError),
    #[error("HTTP client error: {0}")]
    HTTPReqwest(#[from] reqwest::Error),
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(value: Box<dyn std::error::Error>) -> Self {
        Self::Internal(format!("{}", value))
    }
}

async fn get_latest_bundle(
    rpc_client: &RpcClient,
    context_length_tier: RequestTier,
    expiry_duration_tier: RequestTier,
) -> Result<Option<Pubkey>, Error> {
    let program_config = RpcProgramAccountsConfig {
        // RequestBundle::LEN will not exceed u64::MAX because of account constrains
        filters: Some(vec![
            RpcFilterType::DataSize(RequestBundle::LEN.try_into().unwrap()),
            RpcFilterType::Memcmp(Memcmp::new(0, MemcmpEncodedBytes::Bytes(vec![0]))),
            RpcFilterType::Memcmp(Memcmp::new(
                8,
                MemcmpEncodedBytes::Bytes(vec![context_length_tier as u8]),
            )),
            RpcFilterType::Memcmp(Memcmp::new(
                16,
                MemcmpEncodedBytes::Bytes(vec![expiry_duration_tier as u8]),
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
    Ok(rpc_client
        .get_program_accounts_with_config(&ambient_auction_listener::ID, program_config)
        .await?
        .iter()
        .map(|(pubkey, _)| *pubkey)
        .next())
}

#[tokio::main]
#[allow(clippy::result_large_err)]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let payer = read_keypair_file(args.payer_keypair)?;
    // Read the prompt from stdin
    let mut stdin = io::stdin();
    let mut prompt = String::new();
    if stdin.is_terminal() {
        eprint!("Enter your LLM prompt (hit ctrl-d when done): ");
    }
    let prompt_length = stdin.read_to_string(&mut prompt)?;
    if prompt_length == 0 {
        eprintln!("No prompt provided to stdin.");
        INVALIDINPUT.terminate();
    }
    // TODO: this needs to be added to the auction TX's inputs when the state has been updated to
    // support that.
    let prompt_hash = Sha256::digest(&prompt);
    eprintln!("Prompt hash: {}", BASE64_STANDARD.encode(prompt_hash));

    let rpc = RpcClient::new_with_commitment(
        args.cluster_rpc.unwrap_or(CLIENT_URL.to_string()),
        CommitmentConfig::confirmed(),
    );

    let balance = rpc
        .get_balance_with_commitment(&payer.pubkey(), CommitmentConfig::confirmed())
        .await?
        .value;
    if balance < args.max_price {
        eprintln!(
            "Balance {balance} is too low for request auction price: {}",
            args.max_price
        );
        INSUFFICIENTBALANCE.terminate();
    }

    let mut input_hash = [0u8; 32];
    input_hash[..32].copy_from_slice(&prompt_hash);

    let auction_lamports = rpc
        .get_minimum_balance_for_rent_exemption(Auction::LEN)
        .await?;
    let context_length_tier = RequestTier::Standard;
    let expiry_duration_tier = RequestTier::Standard;
    let max_output_tokens = 100_000;
    let max_price_per_output_token = 100_000;

    let Some(bundle) = get_latest_bundle(&rpc, context_length_tier, expiry_duration_tier).await?
    else {
        eprintln!("There was no bundle with the requested tiers. This is likely a bug.");
        NOBUNDLE.terminate();
    };
    eprintln!("Bundle: {bundle}");

    let bundle_lamports = rpc
        .get_minimum_balance_for_rent_exemption(RequestBundle::LEN)
        .await?;
    let seed = Keypair::new().pubkey();
    let ix = request_job(
        payer.pubkey(),
        input_hash,
        None,
        seed.to_bytes(),
        prompt.len() as u64, // TODO(nap): is this not enforced yet, verifier needs to enforce internally
        max_output_tokens,
        bundle_lamports,
        auction_lamports,
        bundle,
        max_price_per_output_token,
        context_length_tier,
        expiry_duration_tier,
        None,
        None,
    );
    for (i, key) in ix.accounts.iter().enumerate() {
        println!("account {} in {}", i, key.pubkey);
    }
    let job_request_key = ix.accounts[5].pubkey;
    let payer_key = payer.pubkey();

    let recent_blockhash = rpc.get_latest_blockhash().await?;
    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(Message::try_compile(
            &payer_key,
            &[ix],
            &[],
            recent_blockhash,
        )?),
        &[&payer],
    )?;
    let sig = rpc.send_and_confirm_transaction_with_spinner(&tx).await?;
    eprintln!(
        "Sent job request: {} with signature: {sig}",
        job_request_key
    );

    Ok(())
}
