use ambient_auction_api::Auction;
use bytemuck::Pod;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use yellowstone_grpc_client::{GeyserGrpcBuilderError, GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter::Filter, subscribe_update::UpdateOneof::Account,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeUpdateAccountInfo,
};

#[derive(Debug, thiserror::Error)]
pub enum YellowstoneGrpcError {
    #[error("Yellowstone client error: {0}")]
    Client(#[from] GeyserGrpcClientError),
    #[error("Yellowstone builder error: {0}")]
    Builder(#[from] GeyserGrpcBuilderError),
    #[error("Yellowstone connection error: {0}")]
    Connection(#[from] yellowstone_grpc_proto::tonic::transport::Error),
    #[error("Yellowstone subscription error: {0}")]
    Subscription(#[from] yellowstone_grpc_proto::tonic::Status),
    #[error("Yellowstone decode error: {0}")]
    Decode(String),
}

/// A wrapper around `[yellowstone_grpc_client::GeyserGrpcClient]` that allows for cheap cloning
/// via the underlying `tonic::Client` clone. This is a workaround until the
/// `[yellowstone_grpc_client::GeyserGrpcClient]` ditches its opaque `Interceptor` type
pub struct CloneableGeyserGrpcClient(
    pub GeyserGrpcClient<yellowstone_grpc_client::InterceptorXToken>,
);

impl CloneableGeyserGrpcClient {
    pub async fn new(url: String) -> Result<Self, YellowstoneGrpcError> {
        let endpoint =
            yellowstone_grpc_proto::tonic::transport::Endpoint::from_shared(url.clone())?;

        let channel = endpoint.connect().await?;

        let interceptor = yellowstone_grpc_client::InterceptorXToken {
            x_token: None,
            x_request_snapshot: false,
        };
        let geyser = yellowstone_grpc_proto::geyser::geyser_client::GeyserClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );
        let health_client = tonic_health::pb::health_client::HealthClient::with_interceptor(
            channel.clone(),
            interceptor,
        );
        let geyser_grpc_client = GeyserGrpcClient::new(health_client, geyser.clone());
        Ok(CloneableGeyserGrpcClient(geyser_grpc_client))
    }

    pub fn into_0(self) -> GeyserGrpcClient<yellowstone_grpc_client::InterceptorXToken> {
        self.0
    }
}

impl Clone for CloneableGeyserGrpcClient {
    fn clone(&self) -> Self {
        let geyser_clone = self.0.geyser.clone();
        let health_client_clone = self.0.health.clone();
        let geyser = GeyserGrpcClient {
            geyser: geyser_clone,
            health: health_client_clone,
        };
        CloneableGeyserGrpcClient(geyser)
    }
}

/// Handles yellowstone SubscribeUpdate messages and decodes them into Auction accounts
/// The outer `Result` is for decode errors, and the 0 Option is for yellowstone upgrades
/// that do not represent an auction account update
#[allow(clippy::result_large_err)]
pub fn decode_account_update<A: Pod>(
    subscribe_update: yellowstone_grpc_proto::geyser::SubscribeUpdate,
) -> Result<Option<(Pubkey, A)>, YellowstoneGrpcError> {
    // The vast majority of this work is because the `subscribe` service is a multi-way stream that
    // accepts a Sink of `SubscribeRequest` and returns a Stream of `SubscribeUpdate`. The updates
    // can be in response to any of the requests, even though we are only using a one-way streaming
    // subscription
    subscribe_update
        .update_oneof
        .and_then(|update| match update {
            Account(account_update) => Some(account_update),
            // yellowstone subscriptions will also send pings, which can be ignored
            _ => None,
        })
        .and_then(|account_update| account_update.account)
        // The account_update.account may be None if the account was removed
        .map(|account_info| decode_account_info::<A>(account_info))
        .transpose()
}

#[allow(clippy::result_large_err)]
pub fn decode_account_info<A: Pod>(
    account_info: SubscribeUpdateAccountInfo,
) -> Result<(Pubkey, A), YellowstoneGrpcError> {
    // The account_update.account may be None if the account was removed
    let acc = bytemuck::try_pod_read_unaligned::<A>(account_info.data.as_slice())
        .map_err(|e| YellowstoneGrpcError::Decode(e.to_string()));

    let pk = Pubkey::try_from(account_info.pubkey.as_slice())
        .map_err(|e| YellowstoneGrpcError::Decode(e.to_string()));

    pk.and_then(|pk| acc.map(|acc| (pk, acc)))
}

pub fn auction_update_request(
    account_id: Pubkey,
    commitment: yellowstone_grpc_proto::geyser::CommitmentLevel,
    subscriber: Option<String>,
) -> yellowstone_grpc_proto::geyser::SubscribeRequest {
    let auction_size = Auction::LEN.try_into().unwrap();
    let filters = vec![SubscribeRequestFilterAccountsFilter {
        filter: Some(Filter::Datasize(auction_size)),
    }];
    let account_filters = HashMap::from([(
        subscriber.unwrap_or("client".to_string()),
        SubscribeRequestFilterAccounts {
            owner: vec![account_id.to_string()],
            filters,
            ..Default::default()
        },
    )]);

    yellowstone_grpc_proto::geyser::SubscribeRequest {
        accounts: account_filters,
        commitment: Some(commitment.into()),
        ..Default::default()
    }
}
