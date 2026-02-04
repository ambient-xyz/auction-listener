use ambient_auction_api::{Auction, JobRequest, JobRequestStatus, RequestBundle};
use ambient_auction_listener::{listener::AuctionClient, CLIENT_URL, ID, YELLOWSTONE_URL};
use clap::Parser;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair};
use solana_sdk::signer::Signer as _;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinSet;

use ambient_auction_listener::run::Error;

#[derive(Parser)]
struct Args {
    /// The keypair that has been used for auctions
    payer_keypair: PathBuf,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long, default_value_t = CLIENT_URL.to_string())]
    cluster_rpc: String,
    /// The Solana Yellowstone URL. Defaults to http://localhost:10000
    #[arg(short = 'y', long, default_value_t = YELLOWSTONE_URL.to_string())]
    yellowstone_url: String,
    /// The number of workers to spawn to submit transactions in parallel
    #[arg(short = 'w', long, default_value_t = 4)]
    workers: usize,
}

async fn reclaim_job_request(
    auction_client: &AuctionClient,
    payer: &Keypair,
    job_request_pubkey: Pubkey,
    job_request_acct: JobRequest,
) -> Result<(), Error> {
    let JobRequest { bundle, .. } = job_request_acct;
    let bundle_pubkey = Pubkey::new_from_array(bundle.inner());
    let bundle: RequestBundle = auction_client.get_account(&bundle_pubkey).await?;
    let RequestBundle {
        payer: bundle_payer,
        context_length_tier,
        expiry_duration_tier,
        auction,
        ..
    } = bundle;
    let auction_key = Pubkey::new_from_array(auction.get().unwrap_or_default().inner());
    let auction_data: Auction = auction_client.get_account(&auction_key).await?;

    println!("{job_request_pubkey}");
    eprintln!("Reclaiming...");
    let recent_blockhash = auction_client.rpc_client.get_latest_blockhash().await?;
    let bundle_payer_pubkey = Pubkey::new_from_array(bundle_payer.inner());

    let new_bundle_lamports = auction_client
        .rpc_client
        .get_minimum_balance_for_rent_exemption(RequestBundle::LEN)
        .await?;
    let new_auction_lamports = auction_client
        .rpc_client
        .get_minimum_balance_for_rent_exemption(Auction::LEN)
        .await?;
    if let Err(e) = auction_client
        .reclaim_job_request(
            payer,
            job_request_pubkey,
            bundle_payer_pubkey,
            bundle_pubkey,
            Pubkey::new_from_array(auction.get().unwrap_or_default().inner()),
            Pubkey::new_from_array(auction_data.payer.inner()),
            recent_blockhash,
            context_length_tier,
            expiry_duration_tier,
            new_bundle_lamports,
            new_auction_lamports,
        )
        .await
    {
        eprintln!("Unable to reclaim job request account ({job_request_pubkey}): {e}");
    };
    Ok(())
}

#[tokio::main]
#[allow(clippy::result_large_err)]
async fn main() -> Result<(), Error> {
    env_logger::init();
    let cli_args = Args::parse();
    let payer = read_keypair_file(&cli_args.payer_keypair)?;
    let cluster_rpc = cli_args.cluster_rpc.clone();
    let yellowstone_url = cli_args.yellowstone_url.clone();
    let keypair_path = cli_args.payer_keypair.clone();
    let auction_client = Arc::new(
        AuctionClient::new(
            ID,
            cluster_rpc,
            Some(keypair_path),
            Pubkey::default(),
            None,
            yellowstone_url,
        )
        .await?,
    );
    let job_requests = auction_client
        .get_job_requests_for_authority(&payer.pubkey(), Some(JobRequestStatus::OutputVerified))
        .await?;

    eprintln!("Found {} job_requests to clean up.", job_requests.len());

    let mut join_set: JoinSet<Result<(), Error>> = JoinSet::new();
    for chunk in job_requests.chunks(job_requests.len() / cli_args.workers) {
        join_set.spawn({
            let auction_client = auction_client.clone();
            let payer = payer.insecure_clone();
            let chunk = chunk.to_owned();
            async move {
                for (job_request_pubkey, job_request_acct) in chunk {
                    reclaim_job_request(
                        &auction_client,
                        &payer,
                        job_request_pubkey,
                        job_request_acct,
                    )
                    .await?;
                }
                Ok(())
            }
        });
    }
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(_) => (),
            Err(e) => {
                tracing::error!(e = %e, "Error in worker task. Aborting remaining tasks.");
                join_set.abort_all();
            }
        }
    }
    Ok(())
}
