use ambient_auction_listener::CLIENT_URL;
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
    /// The keypair that will pay for the transaction
    payer_keypair: PathBuf,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long)]
    cluster_rpc: Option<String>,
}

fn strerr<E: Display>(arg: E) -> String {
    format!("There was an error: {arg}")
}

async fn init_auction_verifiers(client: &RpcClient, payer: &Keypair) -> Result<(), String> {
    eprintln!("Initializing auction verifiers account...");
    let ix = ambient_auction_client::sdk::init_auction_verifiers(payer.pubkey());
    let auction_verifiers = ix.accounts[1].pubkey;

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
        &[payer],
    )
    .map_err(strerr)?;

    let sig = client
        .send_and_confirm_transaction_with_spinner(&tx)
        .await
        .map_err(strerr)?;

    eprintln!("Initialized auction verifiers account: {auction_verifiers} with signature: {sig}");
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
        .block_on(init_auction_verifiers(&rpc, &payer))
}
