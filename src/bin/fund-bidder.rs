use ambient_auction_listener::CLIENT_URL;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcRequestAirdropConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::io;

#[tokio::main]
async fn main() {
    let client = RpcClient::new(CLIENT_URL.to_string());

    println!("Please enter the bidder pubkey: ");
    let mut input_text = String::new();
    io::stdin()
        .read_line(&mut input_text)
        .expect("read line failed");
    let bidder_pubkey: Pubkey = Pubkey::from_str_const(input_text.trim());

    let bidding_budget_lamports: u64 = 3000000000;
    // println!("Please enter the airdrop amount (in lamports): ");
    // let mut input_text = String::new();
    // io::stdin()
    //     .read_line(&mut input_text)
    //     .expect("read line failed");
    // bidding_budget_lamports = input_text.trim().parse::<u64>().unwrap();

    let _ = client
        .request_airdrop_with_config(
            &bidder_pubkey,
            bidding_budget_lamports,
            RpcRequestAirdropConfig {
                recent_blockhash: None,
                commitment: Some(CommitmentConfig::confirmed()),
            },
        )
        .await
        .inspect_err(|e| eprintln!("Failed to airdrop to the bidder keypair: {e:?}"));

    println!(
        "Funded bidder {} with {} lamports",
        bidder_pubkey, bidding_budget_lamports
    );
}
