use ambient_auction_api::BundleRegistry;
use ambient_auction_api::BUNDLE_REGISTRY_SEED;
use ambient_auction_api::{RequestBundle, RequestTier};
use ambient_auction_client::sdk;
use ambient_auction_client::ID as AUCTION_PROGRAM;
use clap::Parser;
use itertools::Itertools;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::ReadableAccount;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    message::{v0::Message, VersionedMessage},
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};
use std::fmt::Display;
use std::path::PathBuf;
use ambient_auction_listener::CLIENT_URL;

#[derive(Parser)]
struct Args {
    /// The keypair that will pay for the transaction
    payer_keypair: PathBuf,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long)]
    cluster_rpc: Option<String>,
}

fn strerr<E: Display>(arg: E) -> String {
    format!("There was an error: {arg}")
}

async fn get_latest_bundle(
    rpc_client: &RpcClient,
    context_length_tier: RequestTier,
    expiry_duration_tier: RequestTier,
) -> Result<(Pubkey, RequestBundle), String> {
    let (registry, _) = Pubkey::find_program_address(
        &[
            BUNDLE_REGISTRY_SEED,
            (context_length_tier as u64).to_le_bytes().as_ref(),
            (expiry_duration_tier as u64).to_le_bytes().as_ref(),
        ],
        &AUCTION_PROGRAM,
    );
    let registry = rpc_client.get_account(&registry).await.map_err(strerr)?;
    let registry: &BundleRegistry = bytemuck::try_from_bytes(registry.data()).map_err(strerr)?;
    let bundle_pubkey = Pubkey::new_from_array(registry.latest_bundle.inner());
    let bundle = rpc_client
        .get_account(&bundle_pubkey)
        .await
        .map_err(strerr)?;
    let bundle: &RequestBundle = bytemuck::try_from_bytes(bundle.data()).map_err(strerr)?;
    Ok((bundle_pubkey, *bundle))
}

async fn cancel_bundles(payer: Keypair, client: &RpcClient) -> Result<(), String> {
    let tiers = vec![RequestTier::Eco, RequestTier::Standard, RequestTier::Pro];
    let current_slot = client.get_slot().await.map_err(strerr)?;

    // create bundle for every tier combination
    for (length_tier, duration_tier) in tiers.iter().cartesian_product(&tiers) {
        let (latest_bundle_key, bundle) =
            match get_latest_bundle(client, *length_tier, *duration_tier).await {
                Ok(b) => b,
                Err(_) => {
                    eprintln!("Unable to get latest bundle for {length_tier:?}/{duration_tier:?}");
                    continue;
                }
            };

        if bundle.requests_len < bundle.context_length_tier.get_request_per_bundle() && bundle.expiry_slot <= current_slot {
            let bundle_lamports = client
                .get_minimum_balance_for_rent_exemption(RequestBundle::LEN)
                .await
                .map_err(strerr)?;

            let ix = sdk::cancel_bundle(
                payer.pubkey(),
                bundle.parent_bundle_key.inner().into(),
                latest_bundle_key,
                bundle.bump as u8,
                *length_tier,
                *duration_tier,
                bundle_lamports,
            );

            let payer_key = payer.pubkey();
            let recent_blockhash = client.get_latest_blockhash().await.unwrap();
            let tx = VersionedTransaction::try_new(
                VersionedMessage::V0(
                    Message::try_compile(&payer_key, &[ix], &[], recent_blockhash).unwrap(),
                ),
                &[&payer],
            )
            .map_err(strerr)?;

            let sig = match client
                .send_and_confirm_transaction_with_spinner_and_commitment(
                    &tx,
                    CommitmentConfig::confirmed(),
                )
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Failed to cancel bundle {length_tier:?}/{duration_tier:?}: {e}");
                    continue;
                }
            };
            println!("Submitted with signature: {}", sig);
        }
    }

    Ok(())
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let payer = read_keypair_file(args.payer_keypair).map_err(strerr)?;
    let rpc = RpcClient::new_with_commitment(
        args.cluster_rpc.unwrap_or(CLIENT_URL.to_string()),
        CommitmentConfig::processed(),
    );

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(strerr)?
        .block_on(cancel_bundles(payer, &rpc))
}
