use ambient_auction_api::Auction;
use anyhow::Result;
use ambient_auction_listener::yellowstone_grpc::{auction_update_request, decode_account_update};
use futures_util::StreamExt;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("YELLOWSTONE_URL").unwrap_or("http://localhost:10000".to_string());
    let mut grpc_client = GeyserGrpcClient::build_from_shared(url)?
        .connect()
        .await
        .expect("Failed to connect to gRPC server");

    let subscription_req =
        auction_update_request(ambient_auction_listener::ID, CommitmentLevel::Processed, None);

    let (_sink, mut stream) = grpc_client
        .subscribe_with_request(Some(subscription_req))
        .await
        .expect("Failed to subscribe to auction account updates");

    println!("Listening to auction accounts updates");

    while let Some(Ok(update)) = stream.next().await {
        let res =
            decode_account_update::<Auction>(update).expect("Failed to decode auction update");
        if let Some((auction_acc_pk, auction)) = res {
            println!("Auction account: {} updated: {:?}", auction_acc_pk, auction);
        }
    }
    Ok(())
}
