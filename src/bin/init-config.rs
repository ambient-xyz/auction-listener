#![cfg(feature = "global-config")]
use ambient_auction_api::{Config, InitConfigArgs};
use clap::Parser;
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
    /// The keypair that will be the update authority for the config account
    authority_keypair: PathBuf,
    /// Minimum pairs of auction-bundle accounts to be supplied per request. Defaults to 2
    #[arg(short = 'm', long)]
    minimum_bundle_auction_pairs: Option<u64>,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long)]
    cluster_rpc: Option<String>,
}

fn strerr<E: Display>(arg: E) -> String {
    format!("There was an error: {arg}")
}

async fn initialize_config(
    client: &RpcClient,
    update_authority: &Keypair,
    minimum_bundle_auction_pairs: Option<u64>,
) -> Result<(), String> {
    let config_lamports = client
        .get_minimum_balance_for_rent_exemption(Config::LEN)
        .await
        .map_err(strerr)?;

    let args = InitConfigArgs {
        minimum_bundle_auction_pairs: minimum_bundle_auction_pairs.unwrap_or(2),
        update_authority: update_authority.pubkey().to_bytes().into(),
        config_lamports,
    };

    let ix = ambient_auction_client::sdk::init_config(update_authority.pubkey(), args);

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            Message::try_compile(
                &update_authority.pubkey(),
                &[ix],
                &[],
                client.get_latest_blockhash().await.map_err(strerr)?,
            )
            .map_err(strerr)?,
        ),
        &[&update_authority],
    )
    .map_err(strerr)?;

    let sig = client
        .send_and_confirm_transaction_with_spinner(&tx)
        .await
        .map_err(strerr)?;

    eprintln!("Signature: {sig}");

    Ok(())
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let payer = read_keypair_file(args.authority_keypair).map_err(strerr)?;
    let rpc = RpcClient::new_with_commitment(
        args.cluster_rpc
            .unwrap_or(ambient_auction_listener::CLIENT_URL.to_string()),
        CommitmentConfig::confirmed(),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(strerr)?
        .block_on(initialize_config(
            &rpc,
            &payer,
            args.minimum_bundle_auction_pairs,
        ))
}
