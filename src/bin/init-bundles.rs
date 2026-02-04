use ambient_auction_api::{BundleRegistry, RequestBundle, RequestTier};
use ambient_auction_listener::CLIENT_URL;
use clap::Parser;
use itertools::Itertools as _;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::{
    message::{v0::Message, VersionedMessage},
    signature::{read_keypair_file, Keypair},
    signer::Signer as _,
    transaction::VersionedTransaction,
};
use std::{fmt::Display, path::PathBuf};

#[derive(Parser)]
struct Args {
    /// The keypair that will pay for the auction
    payer_keypair: PathBuf,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long)]
    cluster_rpc: Option<String>,
}

fn strerr<E: Display>(arg: E) -> String {
    format!("There was an error: {arg}")
}

async fn initialize_bundle(client: &RpcClient, payer: &Keypair) -> Result<(), String> {
    let bundle_lamports = client
        .get_minimum_balance_for_rent_exemption(RequestBundle::LEN)
        .await
        .map_err(strerr)?;
    let registry_lamports = client
        .get_minimum_balance_for_rent_exemption(BundleRegistry::LEN)
        .await
        .map_err(strerr)?;

    eprintln!("Initializing all bundles...");
    let tiers = vec![RequestTier::Eco, RequestTier::Standard, RequestTier::Pro];
    // create bundle for every tier combination
    for (length_tier, duration_tier) in tiers.iter().cartesian_product(&tiers) {
        let ix = ambient_auction_client::sdk::init_bundle(
            payer.pubkey(),
            *length_tier,
            *duration_tier,
            bundle_lamports,
            registry_lamports,
        );
        let bundle_pubkey = ix.accounts[1].pubkey;
        eprintln!("Pubkey for length tier: {length_tier:?} and duration tier {duration_tier:?} bundle: {bundle_pubkey}");

        let tx = VersionedTransaction::try_new(
            VersionedMessage::V0(
                Message::try_compile(
                    &payer.pubkey(),
                    &[ix],
                    &[],
                    client.get_latest_blockhash().await.map_err(strerr)?,
                )
                .map_err(strerr)?,
            ),
            &[&payer],
        )
        .map_err(strerr)?;

        let sig = match client
            .send_and_confirm_transaction_with_spinner(&tx)
            .await
            .map_err(strerr)
        {
            Ok(sig) => sig,
            Err(e) => {
                eprintln!("Error submitting create bundle txn: {e}");
                continue;
            }
        };
        eprintln!("Signature: {sig}");
    }
    Ok(())
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let payer = read_keypair_file(args.payer_keypair).map_err(strerr)?;
    let rpc = RpcClient::new_with_commitment(
        args.cluster_rpc.unwrap_or(CLIENT_URL.to_string()),
        CommitmentConfig::confirmed(),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(strerr)?
        .block_on(initialize_bundle(&rpc, &payer))
}
