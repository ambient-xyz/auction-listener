use ambient_auction_api::Metadata;
use ambient_auction_listener::CLIENT_URL;
use clap::Parser;
use rand::distr::Alphanumeric;
use rand::Rng;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
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
    /// data to be put onchain
    data_file: PathBuf,
}

fn strerr<E: Display>(arg: E) -> String {
    format!("There was an error: {arg}")
}

async fn create_data_account(
    client: &RpcClient,
    payer: &Keypair,
    data: &[u8],
) -> Result<(), String> {
    let space = data.len() + Metadata::LEN;
    let account_lamports = client
        .get_minimum_balance_for_rent_exemption(space)
        .await
        .map_err(strerr)?;

    let seed: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(32) // length
        .map(char::from)
        .collect();

    let data_account =
        Pubkey::create_with_seed(&payer.pubkey(), &seed, &ambient_auction_client::ID)
            .map_err(strerr)?;
    let mut instructions = vec![
        solana_system_interface::instruction::create_account_with_seed(
            &payer.pubkey(),
            &data_account,
            &payer.pubkey(),
            &seed,
            account_lamports,
            space as u64,
            &ambient_auction_client::ID,
        ),
    ];
    eprintln!("creating new data account");

    let mut offset = Metadata::LEN as u64;

    for (i, data) in data.chunks(1000).enumerate() {
        offset += i as u64;
        let ix = ambient_auction_client::sdk::append_data(
            payer.pubkey(),
            data,
            &seed,
            offset,
            data_account,
            None,
        );

        instructions.push(ix);
        let tx = VersionedTransaction::try_new(
            VersionedMessage::V0(
                Message::try_compile(
                    &payer.pubkey(),
                    &instructions,
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
                eprintln!("Error submitting append data txn: {e}");
                continue;
            }
        };
        eprintln!("Appended data to account: {data_account} with Signature: {sig}");
        instructions.clear();
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
    let data = std::fs::read(args.data_file).map_err(strerr)?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(strerr)?
        .block_on(create_data_account(&rpc, &payer, &data))
}
