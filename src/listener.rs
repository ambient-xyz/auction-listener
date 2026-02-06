use ambient_auction_api::bundle::RequestBundle;
use ambient_auction_api::instruction::{SubmitJobOutputArgs, SubmitValidationArgs};
use ambient_auction_api::{
    error::AuctionError, Auction, AuctionStatus, Bid, JobRequest, JobRequestStatus,
    JobVerificationState, Metadata, RequestTier, PUBKEY_BYTES,
};
use ambient_auction_api::{BundleRegistry, RevealBidArgs};
use ambient_auction_api::{BundleStatus, BUNDLE_REGISTRY_SEED};
use ambient_auction_client::ID as AUCTION_PROGRAM;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytemuck::Pod;
use futures_util::SinkExt as _;
use futures_util::{Stream, StreamExt};
use prometheus::register_gauge;
use prometheus::Gauge;
use rand::distr::SampleString as _;
use sha2::{Digest, Sha256};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::RpcFilterType,
};
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::hash::Hash;
use solana_sdk::message::v0::Message;
use solana_sdk::message::VersionedMessage;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    entrypoint::ProgramResult,
    program_error::ProgramError,
    pubkey::Pubkey,
    signature::{EncodableKey, Keypair},
    signer::Signer,
    transaction::Transaction,
};
use std::any::type_name;
use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug_span, info_span, instrument, Instrument as _, Span};
use wolf_crypto::buf::Iv;
use x25519_dalek::{PublicKey, StaticSecret};
use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter::Filter;
use yellowstone_grpc_proto::geyser::subscribe_request_filter_accounts_filter_memcmp::Data;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel as GrpcCommitmentLevel, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp, SubscribeUpdate,
};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterAccounts};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
};
macro_rules! auctionerr {
    ($variant:ident) => {
        Err(ProgramError::Custom(AuctionError::$variant.code()))
    };
}
use ambient_auction_client::sdk;

use crate::error::Error::Custom;
use crate::run::{
    completion, encrypt_with_iv, retry, stream_completion, wait_for_verification, InferenceRequest,
    InferenceResponse, LifecycleEvent, RunAuction, StreamingResponse, SubmitJobArgs,
};
use crate::yellowstone_grpc::{decode_account_info, CloneableGeyserGrpcClient};
use crate::{error::Error, run};
use ambient_auction_client::sdk::request_job;
use prometheus::{register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec};
use solana_sdk::pubkey::MAX_SEED_LEN;
use std::sync::LazyLock;

static RUN_AUCTION_TIMINGS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "run_auction_timings",
        "Time for various parts of the auction to run.",
        &["type"],
        vec![
            0.001,
            0.01,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            2.5,
            5.0,
            10.0,
            30.0,
            60.0,
            120.0,
            f64::INFINITY
        ]
    )
    .unwrap()
});
static RPC_CLIENT_TIMINGS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "auction_solana_client_timings",
        "Time for various solana actions.",
        &["type"],
        vec![
            0.001,
            0.01,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            2.5,
            5.0,
            10.0,
            20.0,
            40.0,
            80.0,
            f64::INFINITY
        ]
    )
    .unwrap()
});
static RPC_CLIENT_COUNTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "auction_solana_client_counts",
        "Number of attempted requests",
        &["type"] // label names
    )
    .unwrap()
});

static RECLAIM_JOB_TASKS: LazyLock<Gauge> = LazyLock::new(|| {
    register_gauge!(
        "auction_listener:reclaim_job_tasks",
        "The number of active tasks "
    )
    .unwrap()
});

struct GaugeHandle {
    gauge: &'static Gauge,
}

impl GaugeHandle {
    fn new(gauge: &'static Gauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for GaugeHandle {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

pub struct AuctionClient {
    pub program_id: Pubkey,
    pub rpc_client: RpcClient,
    pub keypair: Keypair,
    pub vote_account: Pubkey,
    pub vote_authority: Keypair,
    // Yellowstone gRPC client: access with self.yellowstone_client()
    yellowstone_client: CloneableGeyserGrpcClient,
    bundle_registry_cache: Arc<papaya::HashMap<Pubkey, BundleRegistry>>,
}
impl std::fmt::Debug for AuctionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AuctionClient").finish()
    }
}
/// Chains a step onto a [`Stream`] to block on and produce an event for inference verification.
pub fn chain_verification(
    client: Arc<AuctionClient>,
    job: Pubkey,
    s: impl Stream<Item = Result<StreamingResponse, crate::run::Error>>,
) -> impl Stream<Item = Result<StreamingResponse, crate::run::Error>> {
    let (verified_tx, verified_rx) = oneshot::channel();
    tokio::spawn(async move {
        let verified = wait_for_verification(client, job).await.unwrap_or(false);
        if verified_tx.send(verified).is_err() {
            tracing::warn!(job_request_id = ?job, "Failed to notify listener of job verifiecation");
        };
    });
    tracing::debug!("Chaining verification...");
    s.chain(futures_util::stream::once(async move {
        Ok(StreamingResponse::Verification {
            verified: verified_rx
                .await
                .map_err(|_| crate::run::Error::Internal("Verification job died.".to_string()))?,
        })
    }))
}

/// Maintains a cache with the latest bundle for each tier.
async fn maintain_bundle_registry_cache(
    mut geyser: CloneableGeyserGrpcClient,
    cache: Arc<papaya::HashMap<Pubkey, BundleRegistry>>,
) -> Result<(), crate::run::Error> {
    let (_, mut stream) = geyser
        .0
        .subscribe_with_request(Some(SubscribeRequest {
            accounts: HashMap::from([(
                "listener-bundle-cache".to_string(),
                SubscribeRequestFilterAccounts {
                    owner: vec![crate::ID.to_string()],
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Datasize(BundleRegistry::LEN as u64)),
                    }],
                    ..Default::default()
                },
            )]),
            commitment: Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed.into()),
            ..Default::default()
        }))
        .await
        .inspect_err(|e| {
            tracing::error!(error = %e, "Error subscribing to request bundles");
        })?;

    while let Some(Ok(upd)) = stream.next().await {
        let Some(UpdateOneof::Account(account)) = upd.update_oneof else {
            continue;
        };

        let Some(account) = account.account else {
            continue;
        };

        let Ok(registry) = bytemuck::try_pod_read_unaligned::<BundleRegistry>(&account.data) else {
            tracing::warn!("Failed to deserialize bundle registry");
            continue;
        };

        cache.pin().insert(
            Pubkey::new_from_array(account.pubkey.try_into().unwrap()),
            registry,
        );
    }

    Ok(())
}

impl AuctionClient {
    /// Create a new [`AuctionClient`].
    ///
    /// `program_id` should be the Ambient Auction program ID found in the [`ambient_auction_api`] crate.
    ///
    /// `json_rpc_url` is the URI pointing to the Solana/Ambient RPC server to use for requests.
    ///
    /// `keypair_path`: an optional keypair to use for paying and signing transactions. Defaults to
    /// `~/.config/solana/id.json` (TODO(nap): change this to be XDG compatible).
    ///
    /// `vote_account`: the public key of the vote account this client is associated with. This is
    /// only used by certain methods in on the client and can be set to any valid pubkey if not
    /// acting on behalf of a miner. TODO(nap): Split this into a different client/API.
    ///
    ///
    /// `vote_authority_keypair_path`: An optional path pointing to the vote authority keypair. If
    /// unspecified, the keypair loaded from `keypair_path` (above) is used.
    ///
    /// `yellowstone_grpc_url`: the URI for connecting to a Yellowstone gRPC server.
    pub async fn new(
        program_id: Pubkey,
        json_rpc_url: String,
        keypair_path: Option<std::path::PathBuf>,
        vote_account: Pubkey,
        vote_authority_keypair_path: Option<std::path::PathBuf>,
        yellowstone_grpc_url: String,
    ) -> Result<Self, Error> {
        let keypair_path = keypair_path.unwrap_or_else(|| {
            #[allow(deprecated)]
            std::env::home_dir()
                .unwrap()
                .join(".config")
                .join("solana")
                .join("id.json")
        });

        let keypair = match keypair_path.exists() {
            true => Keypair::read_from_file(keypair_path).unwrap(),
            false => {
                let keypair = Keypair::new();
                match keypair.write_to_file(keypair_path.clone()) {
                    Ok(_) => {
                        tracing::info!("Wrote new Solana client keypair to {keypair_path:?}");
                        Ok(keypair)
                    }
                    Err(_) => Err("Unable to write Solana client keypair to file"),
                }?
            }
        };

        let vote_authority = if let Some(keypair_path) = vote_authority_keypair_path.clone() {
            match Keypair::read_from_file(keypair_path) {
                Ok(vote_authority) => Ok(vote_authority),
                Err(_) => Err("Unable to read Solana vote authority keypair from file"),
            }?
        } else {
            // TODO: remove insecure clone
            keypair.insecure_clone()
        };

        let bundle_registry_cache = Arc::new(papaya::HashMap::with_capacity(1024));
        let yellowstone_client =
            CloneableGeyserGrpcClient::new(yellowstone_grpc_url.clone()).await?;

        let rpc_client = RpcClient::new_with_timeout(json_rpc_url, Duration::from_secs(300));

        Ok(AuctionClient {
            program_id,
            rpc_client,
            keypair,
            vote_account,
            vote_authority,
            yellowstone_client,
            bundle_registry_cache,
        })
    }

    #[instrument]
    pub async fn keep_cache_warm(&self) -> Result<(), crate::run::Error> {
        maintain_bundle_registry_cache(
            self.yellowstone_client.clone(),
            self.bundle_registry_cache.clone(),
        )
        .await
    }

    pub async fn wait_for_auction_winner(
        self: Arc<AuctionClient>,
        auction_pubkey: Pubkey,
    ) -> Result<Auction, crate::run::Error> {
        let client = self.clone();

        client
            .wait_for_account_condition::<Auction, Auction>(
                auction_pubkey,
                Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Confirmed),
                None,
                async move |auction| match auction.status {
                    AuctionStatus::Ended => Ok(Some(auction)),
                    AuctionStatus::Canceled => Err(crate::run::Error::NoBidders(auction_pubkey)),
                    // The auction is accepting bids
                    AuctionStatus::RevealingBids | AuctionStatus::Active => Ok(None),
                },
            )
            .instrument(info_span!(
                "wait_for_account_condition",
                condition = "auction_winner",
                address = auction_pubkey.to_string()
            ))
            .await
    }

    #[instrument]
    async fn get_latest_bundle(
        &self,
        context_length_tier: RequestTier,
        expiry_duration_tier: RequestTier,
    ) -> Result<Pubkey, crate::run::Error> {
        let (registry, _) = Pubkey::find_program_address(
            &[
                BUNDLE_REGISTRY_SEED,
                (context_length_tier as u64).to_le_bytes().as_ref(),
                (expiry_duration_tier as u64).to_le_bytes().as_ref(),
            ],
            &AUCTION_PROGRAM,
        );
        tracing::debug!(registry_id = %registry, ?context_length_tier, ?expiry_duration_tier, "Fetching registry account.");
        let registry = self.get_bundle_registry_cached(&registry).await?;
        let latest_bundle = Pubkey::new_from_array(registry.latest_bundle.inner());
        tracing::debug!(registry_latest_bundle = %latest_bundle, ?context_length_tier, ?expiry_duration_tier, "Fetched bundle registry.");
        Ok(latest_bundle)
    }

    /// A generic method for waiting until some condition is true. The async test function `f` is
    /// called with the current account state of the account mapped to by `pubkey`. `f` is
    /// fallible and errors are propagated immediately. If there are no errors, but the condition
    /// is not true, `f` should return `Ok(None)` to indicate the condition was not met. `f` will
    /// then be called again once an update to the specified account has occured according to the
    /// RPC subscription model of Solana / Ambient. Once the condition is true, `f` should return
    /// `Ok(Some(T))` where `T` is then returned in `Ok(T)` from this function. Note: this *can* block indefinitely if there are no account updates
    // TODO(nap): this should really return Result<Result<T, E>, YellowstoneError>
    pub async fn wait_for_account_condition<A: Pod, T>(
        &self,
        pubkey: Pubkey,
        commitment: Option<yellowstone_grpc_proto::geyser::CommitmentLevel>,
        ready: Option<oneshot::Sender<()>>,
        f: impl AsyncFn(A) -> Result<Option<T>, crate::run::Error>,
    ) -> Result<T, run::Error> {
        let random_id = rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 8);
        let account_filters = HashMap::from([(
            format!("auction-{pubkey}-{random_id}"),
            SubscribeRequestFilterAccounts {
                account: [pubkey.to_string()].to_vec(),
                ..Default::default()
            },
        )]);
        let commitment =
            commitment.unwrap_or(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed);
        let (mut sink, mut subscription) = self
            .yellowstone_client
            .clone()
            .0
            .subscribe_with_request(Some(SubscribeRequest {
                accounts: account_filters,
                commitment: Some(commitment.into()),
                ..Default::default()
            }))
            .await?;
        if let Some(ready) = ready {
            let _ = ready.send(());
        }

        // Get the current account state _after_ we've subscribed to the state, in case the
        // condition we're waiting for already happened.
        // We will make 3 tries to fetch the account
        let mut acct = {
            let mut attempts = 0;
            loop {
                match self.get_account::<A>(&pubkey).await {
                    Ok(account) => break account,
                    Err(e @ Error::AccountNotExist(_, _)) => {
                        attempts += 1;
                        if attempts > 3 {
                            return Err(e.into());
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        };
        let mut checks = 0usize;
        // Loop until the test function returns some or the subscription breaks
        loop {
            // execute our test
            if let Some(out) = f(acct)
                .instrument(info_span!("wait_for_account_condition_check", checks))
                .await?
            {
                let _r = sink.close().await;
                return Ok(out);
            };
            checks += 1;
            // Wait for an update from the event stream
            let Some(Ok(update)) = subscription
                .next()
                .instrument(info_span!("wait_for_account_condition_update"))
                .await
            else {
                tracing::warn!(
                    "Subscription to account ({pubkey}) ended prematurely.",
                    pubkey = pubkey
                );
                break Err(crate::run::Error::Internal(format!(
                    "Condition never met for account ({pubkey}) of type ({})",
                    type_name::<A>()
                )));
            };
            if let Some(updated_acct) = Self::decode_from_geyser::<A>(update, pubkey) {
                // If the account decodes into the type we expect
                acct = updated_acct;
            }
        }
    }

    fn decode_from_geyser<A: Pod>(update: SubscribeUpdate, pubkey: Pubkey) -> Option<A> {
        update
            .update_oneof
            .and_then(|update| match update {
                UpdateOneof::Account(update) => Some(update.account),
                // yellowstone subscriptions will also send pings, which can be ignored
                _ => None,
            })
            .flatten()
            .and_then(|account| {
                Some(
                    decode_account_info::<A>(account)
                        .inspect_err(|error| {
                            tracing::error!(?error, "Unable to decode account ({pubkey})")
                        })
                        .ok()?
                        .1,
                )
            })
    }

    /// Waits for an [`RequestBundle`] to be auctioned off. Returns the pubkey of the resulting
    /// [`Auction`].
    #[instrument]
    pub async fn wait_for_bundle_to_be_auctioned(
        &self,
        bundle_key: Pubkey,
        ready: oneshot::Sender<()>,
    ) -> Result<Pubkey, crate::run::Error> {
        self.wait_for_account_condition::<RequestBundle, Pubkey>(
            bundle_key,
            Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed),
            Some(ready),
            async |bundle| {
                Ok(bundle
                    .auction
                    .get()
                    .map(|auction_key| Pubkey::from(auction_key.inner())))
            },
        )
        .instrument(info_span!(
            "wait_for_account_condition",
            condition = "bundle_auction",
            address = bundle_key.to_string()
        ))
        .await
    }

    /// Waits for a [`JobRequest`] to be verified. Note: this *can* block indefinitely if the
    /// inference input is never received, which prevents the output from being verified. In
    /// general, assuming well behaved clients, this won't happen.
    #[instrument]
    pub async fn wait_for_job_verification(
        &self,
        job_request_key: Pubkey,
        ready: Option<oneshot::Sender<()>>,
    ) -> Result<(bool, JobRequest), crate::run::Error> {
        self.wait_for_account_condition(
            job_request_key,
            Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed),
            ready,
            async |job_request: JobRequest| {
                let verifications_completed = job_request
                    .verification
                    .verifier_states
                    .iter()
                    .filter(|s| **s == JobVerificationState::Completed)
                    .count();

                tracing::debug!("Job status: {:?}", job_request.status);
                tracing::debug!("Verifications completed: {verifications_completed}");

                if job_request.status == JobRequestStatus::OutputVerified {
                    return Ok(Some((true, job_request)));
                } else if verifications_completed == job_request.verification.verifier_states.len()
                {
                    return Ok(Some((false, job_request)));
                }
                Ok(None)
            },
        )
        .instrument(info_span!(
            "wait_for_account_condition",
            condition = "job_verification",
            address = job_request_key.to_string()
        ))
        .await
    }

    /// Waits for a [`RequestBundle`] to hit a [`BundleStatus::Canceled`] or
    /// [`BundleStatus::Verified`] state. From there is when we can reclaim job requests and the
    /// bundle itself.
    #[instrument]
    pub async fn wait_for_bundle_to_be_done(
        &self,
        bundle_key: Pubkey,
    ) -> Result<(), crate::run::Error> {
        self.wait_for_account_condition::<RequestBundle, ()>(
            bundle_key,
            Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Confirmed),
            None,
            async |bundle| match bundle.status {
                BundleStatus::Verified | BundleStatus::Canceled => Ok(Some(())),
                _ => Ok(None),
            },
        )
        .instrument(info_span!(
            "wait_for_account_condition",
            condition = "bundle_auction",
            address = bundle_key.to_string()
        ))
        .await
    }

    /// Place a bid on a [`Auction`]. Returns the pubkey of the bid account created.
    #[allow(clippy::too_many_arguments)]
    #[instrument]
    pub async fn place_bid(
        &self,
        auction_pubkey: Pubkey,
        price_per_output_token: u64,
        price_hash_seed: Option<[u8; 32]>,
        endpoint: (IpAddr, u16),
        node_encryption_publickey: Option<[u8; 32]>,
        recent_blockhash: Hash,
        commitment_config: Option<CommitmentLevel>,
    ) -> Result<Pubkey, Error> {
        let price_hash_seed = price_hash_seed.unwrap_or(self.keypair.pubkey().to_bytes());

        tracing::debug!("using {:?} as the price hash seed", price_hash_seed);

        let ix = sdk::place_bid(
            self.keypair.pubkey(),
            auction_pubkey,
            price_per_output_token,
            price_hash_seed,
            endpoint,
            node_encryption_publickey,
        );
        let bid_account_pubkey = ix.accounts[1].pubkey;
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.keypair.pubkey()));
        tx.sign(&[&self.keypair], recent_blockhash);
        let sig = self
            .rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(
                        commitment_config.unwrap_or(CommitmentLevel::Processed),
                    ),
                    encoding: None,
                    max_retries: None,
                    min_context_slot: None,
                },
            )
            .instrument(debug_span!(
                "send_transaction_with_config",
                intent = "place_bid"
            ))
            .await
            .map_err(|e| {
                tracing::error!("Failed to place bid: {}", e);
                e
            })?;

        tracing::debug!("Placed bid on auction with signature: {sig}", sig = sig);

        Ok(bid_account_pubkey)
    }

    pub fn yellowstone_client(&self) -> GeyserGrpcClient<impl Interceptor> {
        self.yellowstone_client.clone().0
    }

    pub async fn get_bundle_registry_cached(
        &self,
        pubkey: &Pubkey,
    ) -> Result<BundleRegistry, Error> {
        if let Some(registry) = self.bundle_registry_cache.pin().get(pubkey) {
            tracing::info!("Bundle registry found in cache");
            return Ok(*registry);
        }

        tracing::warn!("Bundle registry not found in cache");
        let registry = self
            .get_account_with_commitment::<BundleRegistry>(
                pubkey,
                Some(CommitmentConfig::processed()),
            )
            .await?;
        self.bundle_registry_cache.pin().insert(*pubkey, registry);
        Ok(registry)
    }

    /// Attempts to close an [`Auction`], resolving it's escrow balances.
    #[instrument(skip_all, fields(bundle_id = %bundle_key))]
    pub async fn end_auction(&self, bidder: &Keypair, bundle_key: &Pubkey) -> ProgramResult {
        let ix = sdk::end_auction(bidder.pubkey(), *bundle_key, self.vote_account);
        let mut tx = Transaction::new_with_payer(&[ix], Some(&bidder.pubkey()));
        tx.sign(
            &[bidder],
            self.rpc_client
                .get_latest_blockhash()
                .await
                .or(auctionerr!(SolanaRpc))?,
        );
        match self
            .rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    encoding: None,
                    max_retries: Some(3),
                    min_context_slot: None,
                },
            )
            .instrument(debug_span!(
                "send_transaction_with_config",
                intent = "end_auction"
            ))
            .await
        {
            Ok(sig) => {
                tracing::debug!("Ended auction with signature: {}", sig);
            }
            Err(e) => tracing::error!(error = %e, "Error ending auction."),
        };
        Ok(())
    }

    /// Used by verifiers to submit their verification result.
    #[instrument(skip_all)]
    pub async fn submit_validation_result(
        &self,
        bundle_key: Pubkey,
        job_request_key: Pubkey,
        data: SubmitValidationArgs,
        commitment_config: Option<CommitmentLevel>,
    ) -> Result<(), Error> {
        let ix = sdk::submit_validation(
            bundle_key,
            self.vote_account,
            self.vote_authority.pubkey(),
            job_request_key,
            data,
        );
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.keypair.pubkey()));
        tx.sign(
            &[&self.keypair, &self.vote_authority],
            self.rpc_client
                .get_latest_blockhash()
                .await
                .or(auctionerr!(SolanaRpc))?,
        );
        Ok(self
            .rpc_client
            .send_and_confirm_transaction_with_spinner_and_config(
                &tx,
                CommitmentConfig {
                    commitment: commitment_config.unwrap_or(CommitmentLevel::Processed),
                },
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    ..Default::default()
                },
            )
            .instrument(debug_span!(
                "send_transaction_with_config",
                intent = "submit_job_validation_result",
                validated_tokens = data.num_successes,
                invalid_tokens = data.num_failures
            ))
            .await
            .map(|sig| {
                tracing::debug!(
                    signature = ?sig, "Submitted auction validation result with signature",
                );
            })
            .inspect_err(|e| {
                tracing::error!(error = ?e, "Failed to submit auction validation");
            })?)
    }

    /// Used by clients who receive inference directly from the miner to post the results of that
    /// inference on-chain for verification.
    #[instrument]
    pub async fn submit_job_output(
        &self,
        bundle_key: Pubkey,
        job_request_key: Pubkey,
        data: SubmitJobOutputArgs,
        commitment_config: Option<CommitmentConfig>,
        // account to be used as the job output data account.
        // this is required if an input data account is used for the request
        output_data_account: Option<Pubkey>,
    ) -> Result<(), Error> {
        let ix = sdk::submit_job(
            self.keypair.pubkey(),
            bundle_key,
            job_request_key,
            data,
            output_data_account,
        );
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.keypair.pubkey()));
        tx.try_sign(
            &[&self.keypair],
            self.rpc_client
                .get_latest_blockhash()
                .await
                .or(auctionerr!(SolanaRpc))?,
        )?;
        let sig = self
            .rpc_client
            .send_and_confirm_transaction_with_spinner_and_config(
                &tx,
                commitment_config.unwrap_or(CommitmentConfig::processed()),
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    encoding: None,
                    max_retries: None,
                    min_context_slot: None,
                },
            )
            .instrument(debug_span!(
                "send_transaction_with_config",
                intent = "submit_job_output",
                job_request_id = job_request_key.to_string(),
                bundle_id = bundle_key.to_string()
            ))
            .await
            .inspect_err(|e| tracing::warn!("Failed to submit job result: {}", e))
            .map_err(Error::SolanaClient)?;

        tracing::debug!("Submitted job result with signature: {}", sig);
        Ok(())
    }

    pub async fn create_data_account(
        &self,
        commitment_config: Option<CommitmentConfig>,
        // Seed used to derive the data account upon creation.
        //
        // NOTE: The `AuctionClient` keypair is used as the base key for `CreateAccountWithSeed`
        seed: &str,
        data_size: usize,
    ) -> Result<(Signature, Pubkey), Error> {
        if seed.len() > MAX_SEED_LEN {
            return Err(Error::Custom("max seed length exceeded".to_string()));
        };

        let data_account_key =
            Pubkey::create_with_seed(&self.keypair.pubkey(), seed, &crate::ID)
                .map_err(|_| Custom("Failed to create pubkey with seed".to_string()))?;

        let ix = solana_system_interface::instruction::create_account_with_seed(
            &self.keypair.pubkey(),
            &data_account_key,
            &self.keypair.pubkey(),
            seed,
            self.rpc_client
                .get_minimum_balance_for_rent_exemption(data_size)
                .await?,
            data_size as u64,
            &crate::ID,
        );
        tracing::debug!("Creating new data account: {:?}", data_account_key);
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.keypair.pubkey()));

        tx.try_sign(
            &[&self.keypair],
            self.rpc_client
                .get_latest_blockhash()
                .await
                .or(auctionerr!(SolanaRpc))?,
        )?;
        match self
            .rpc_client
            .send_and_confirm_transaction_with_spinner_and_config(
                &tx,
                commitment_config.unwrap_or(CommitmentConfig::processed()),
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    encoding: None,
                    max_retries: None,
                    min_context_slot: None,
                },
            )
            .instrument(debug_span!(
                "send_transaction_with_config",
                intent = "create_data_account",
            ))
            .await
        {
            Ok(sig) => {
                tracing::info!("created data account with signature: {}", sig);
                Ok((sig, data_account_key))
            }
            Err(e) => {
                tracing::error!("Failed to create data account: {}", e);
                // return the created account and the failing offset
                Err(Error::SolanaClient(e))
            }
        }
    }

    #[instrument]
    pub async fn append_data_to_account(
        &self,
        // Seed used to derive the data account upon creation.
        //
        // NOTE: The `AuctionClient` keypair is used as the base key for `CreateAccountWithSeed`
        seed: &str,
        uncompressed_data: Vec<u8>,
        use_compression: bool,
        data_account_key: Pubkey,
        commitment_config: Option<CommitmentConfig>,
    ) -> Result<(), Error> {
        let chunk_size = 1000;

        let (data, len) = if use_compression {
            let data_len = uncompressed_data.len();
            (
                tokio::task::spawn_blocking(move || lz4_flex::block::compress(&uncompressed_data))
                    .await
                    .map_err(|e| Error::Custom(format!("Failed to spawn compression task: {e}")))?,
                NonZeroU64::new(data_len as u64),
            )
        } else {
            (uncompressed_data.to_vec(), None)
        };

        let data_chunks = data.chunks(chunk_size).enumerate();
        let data_chunks_len = data_chunks.len();

        for (i, data_chunk) in data_chunks {
            let offset = (i * chunk_size + size_of::<Metadata>()) as u64;

            let ix = sdk::append_data(
                self.keypair.pubkey(),
                data_chunk,
                seed,
                offset,
                data_account_key,
                len,
            );
            let mut tx = Transaction::new_with_payer(&[ix], Some(&self.keypair.pubkey()));

            tx.try_sign(
                &[&self.keypair],
                self.rpc_client
                    .get_latest_blockhash()
                    .await
                    .or(auctionerr!(SolanaRpc))?,
            )?;

            match if i == 0 || i == data_chunks_len - 1 {
                self.rpc_client
                    .send_and_confirm_transaction_with_spinner_and_config(
                        &tx,
                        commitment_config.unwrap_or(CommitmentConfig::processed()),
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            preflight_commitment: Some(CommitmentLevel::Processed),
                            encoding: None,
                            max_retries: None,
                            min_context_slot: None,
                        },
                    )
                    .instrument(debug_span!(
                        "send_transaction_with_config",
                        intent = "append_data_to_account",
                    ))
                    .await
            } else {
                self.rpc_client
                    .send_transaction_with_config(
                        &tx,
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            preflight_commitment: Some(CommitmentLevel::Processed),
                            encoding: None,
                            max_retries: None,
                            min_context_slot: None,
                        },
                    )
                    .instrument(debug_span!(
                        "send_transaction_with_config",
                        intent = "append_data_to_account",
                    ))
                    .await
            } {
                Ok(sig) => {
                    tracing::debug!("appended data to account with signature: {}", sig);
                }
                Err(e) => {
                    let message =
                        format!("Failed to append data to account: {} at offset {offset}", e);
                    tracing::error!(message);
                    return Err(Error::Custom(message));
                }
            }
        }
        tracing::info!("Data appended successfully");
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument]
    pub async fn reveal_bid(
        &self,
        bidder: Pubkey,
        bundle_key: Pubkey,
        auction_key: Pubkey,
        price_per_output_token: u64,
        price_hash_seed: [u8; 32],
        recent_blockhash: Hash,
    ) -> Result<Signature, Error> {
        let ix = sdk::reveal_bid(
            bidder,
            auction_key,
            bundle_key,
            self.vote_account,
            self.vote_authority.pubkey(),
            RevealBidArgs {
                price_per_output_token,
                price_hash_seed,
            },
        );
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.keypair.pubkey()));
        tx.sign(&[&self.keypair, &self.vote_authority], recent_blockhash);
        let sig = self
            .rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    ..Default::default()
                },
            )
            .instrument(debug_span!(
                "send_transaction_with_config",
                intent = "reveal_bid",
                bidder = bidder.to_string(),
                bundle_id = bundle_key.to_string(),
                auction_id = auction_key.to_string(),
                price_per_output_token = price_per_output_token
            ))
            .await?;

        tracing::info!("Revealed bid  with signature: {}", sig);

        Ok(sig)
    }

    /// Retrieves an account associated with `address` and decodes it into a type `A` using
    /// [`bytemuck`] using the specified commitment level. Defaults to `CommitmentConfig::processed()`
    #[instrument(level = "debug")]
    pub async fn get_account_with_commitment<A: Pod>(
        &self,
        address: &Pubkey,
        commitment: Option<CommitmentConfig>,
    ) -> Result<A, Error> {
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            data_slice: None,
            commitment: Some(commitment.unwrap_or(CommitmentConfig::processed())),
            min_context_slot: None,
        };
        self.get_account_with_config(address, config).await
    }

    /// Retrieves an account associated with `address` and decodes it into a type `A` using
    /// [`bytemuck`] using the provided [`RpcAccountInfoConfig`].
    #[instrument]
    pub async fn get_account_with_config<A: Pod>(
        &self,
        address: &Pubkey,
        config: RpcAccountInfoConfig,
    ) -> Result<A, Error> {
        let Some(acct) = self
            .rpc_client
            .get_account_with_config(address, config)
            .instrument(info_span!("solana_rpc_get_account_with_config"))
            .await?
            .value
        else {
            return Err(Error::AccountNotExist(std::any::type_name::<A>(), *address));
        };

        bytemuck::try_pod_read_unaligned(&acct.data).map_err(|_| {
            Error::Custom(format!(
                "Account ({address}) did not decode into expected type: {}",
                type_name::<A>()
            ))
        })
    }

    /// Retrieves an account associated with `address` and decodes it into a type `A` using
    /// [`bytemuck`] using default options and a commitement level of "processed."
    #[instrument(level = "debug")]
    pub async fn get_account<A: Pod>(&self, address: &Pubkey) -> Result<A, Error> {
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            data_slice: None,
            commitment: Some(CommitmentConfig::processed()),
            min_context_slot: None,
        };
        self.get_account_with_config(address, config).await
    }

    #[instrument(level = "debug")]
    pub async fn get_account_with_slot<A: Pod>(
        &self,
        address: &Pubkey,
        slot: Option<u64>,
    ) -> Result<A, Error> {
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            data_slice: None,
            commitment: Some(CommitmentConfig::processed()),
            min_context_slot: slot,
        };
        self.get_account_with_config(address, config).await
    }

    #[instrument(level = "debug")]
    pub async fn get_jobs_for_bundle(&self, bundle_key: Pubkey) -> Result<Vec<Pubkey>, Error> {
        let program_config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new(
                0,
                MemcmpEncodedBytes::Bytes(bundle_key.to_bytes().to_vec()),
            ))]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: None,
                commitment: Some(CommitmentConfig::processed()),
                min_context_slot: None,
            },
            with_context: None,
            sort_results: None,
        };
        Ok(self
            .rpc_client
            .get_program_accounts_with_config(&crate::ID, program_config)
            .await?
            .iter()
            .map(|(pubkey, _)| *pubkey)
            .collect())
    }

    #[instrument]
    pub async fn get_job_requests_for_authority(
        &self,
        request_authority: &Pubkey,
        state: Option<JobRequestStatus>,
    ) -> Result<Vec<(Pubkey, JobRequest)>, Error> {
        let mut filter = vec![
            RpcFilterType::DataSize(size_of::<JobRequest>() as u64),
            RpcFilterType::Memcmp(Memcmp::new(
                std::mem::offset_of!(JobRequest, authority),
                MemcmpEncodedBytes::Bytes(request_authority.to_bytes().to_vec()),
            )),
        ];
        if let Some(state) = state {
            filter.push(RpcFilterType::Memcmp(Memcmp::new(
                std::mem::offset_of!(JobRequest, status),
                MemcmpEncodedBytes::Bytes(bytemuck::bytes_of(&state).to_vec()),
            )))
        }
        let program_config = RpcProgramAccountsConfig {
            filters: Some(filter),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: None,
                commitment: Some(CommitmentConfig::processed()),
                min_context_slot: None,
            },
            with_context: None,
            sort_results: None,
        };
        Ok(self
            .rpc_client
            .get_program_accounts_with_config(&crate::ID, program_config)
            .await?
            .iter()
            .filter_map(|(pubkey, acct)| {
                Some((
                    *pubkey,
                    bytemuck::try_pod_read_unaligned::<JobRequest>(&acct.data)
                        .map_err(|_| {
                            Error::Custom(format!(
                                "Account ({pubkey}) did not decode into expected type: {}",
                                type_name::<JobRequest>()
                            ))
                        })
                        .ok()?
                        .to_owned(),
                ))
            })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(request_authority))]
    pub async fn reclaim_job_request(
        &self,
        request_authority: &Keypair,
        job_request_key: Pubkey,
        bundle_payer: Pubkey,
        bundle_key: Pubkey,
        auction_key: Pubkey,
        auction_payer: Pubkey,
        recent_blockhash: Hash,
        context_length_tier: RequestTier,
        expiry_duration_tier: RequestTier,
        new_bundle_lamports: u64,
        new_auction_lamports: u64,
    ) -> Result<(), Error> {
        let ix = sdk::close_request(
            ambient_auction_client::sdk::CloseRequest{
               request_authority:              request_authority.pubkey(),
                job_request_key,
                bundle_payer,
                bundle_key,
                auction_key,
                auction_payer,
                context_length_tier,
                expiry_duration_tier,
                new_bundle_lamports,
                new_auction_lamports,
            }
        );
        let mut tx = Transaction::new_with_payer(&[ix], Some(&request_authority.pubkey()));
        tx.sign(&[request_authority], recent_blockhash);
        let sig = self
            .rpc_client
            .send_and_confirm_transaction_with_spinner_and_config(
                &tx,
                CommitmentConfig::confirmed(),
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Confirmed),
                    encoding: None,
                    max_retries: None,
                    min_context_slot: None,
                },
            )
            .instrument(debug_span!(
                "send_transaction_with_config",
                intent = "reclaim_job_request"
            ))
            .await
            .map_err(|e| {
                tracing::warn!(
                    bundle_id = %bundle_key,
                    job_request_id = %job_request_key,
                    auction_id = %auction_key,
                    error = %e,
                    "Failed to reclaim account");
                e
            })?;
        tracing::debug!(job_request_id = %job_request_key, "Closed job request account with signature {sig}");
        Ok(())
    }
}

#[instrument(skip(retry_client, subscribed))]
async fn wait_for_job_request_to_complete(
    retry_client: Arc<AuctionClient>,
    job_request_id: Pubkey,
    subscribed: oneshot::Sender<()>,
) -> Result<JobRequest, crate::run::Error> {
    let mut yellowstone_client = retry_client.yellowstone_client();
    tracing::info!(?job_request_id, "Subscribing to transaction status");
    let mut sub = yellowstone_client
        .subscribe_once(SubscribeRequest {
            transactions_status: HashMap::from([(
                format!("{job_request_id}-tx-status"),
                SubscribeRequestFilterTransactions {
                    failed: Some(true),
                    //signature: Some(job_request_signature.to_string()),
                    account_include: vec![job_request_id.to_string()],
                    ..Default::default()
                },
            )]),
            accounts: HashMap::from([(
                format!("{job_request_id}-account-update"),
                SubscribeRequestFilterAccounts {
                    account: vec![job_request_id.to_string()],
                    owner: vec![AUCTION_PROGRAM.to_string()],
                    ..Default::default()
                },
            )]),
            // TODO(nap): this cast is _probably_ safe, but double check
            commitment: Some(CommitmentLevel::Processed as i32),
            slots: HashMap::from([(
                format!("{job_request_id}-slots-update"),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(false),
                },
            )]),
            ..Default::default()
        })
        .await?;
    let mut job_request_timer = Some(
        RPC_CLIENT_TIMINGS
            .with_label_values(&["grpc_job_request_tx"])
            .start_timer(),
    );
    let mut job_request_tx_success = Some(
        RPC_CLIENT_TIMINGS
            .with_label_values(&["grpc_job_request_tx_success"])
            .start_timer(),
    );
    let mut job_request_tx_failure = Some(
        RPC_CLIENT_TIMINGS
            .with_label_values(&["grpc_job_request_tx_failure"])
            .start_timer(),
    );
    let mut job_request = None;
    if subscribed.send(()).is_err() {
        return Err(crate::run::Error::Internal(
            "Skipping wait for auction success, parent task died.".to_string(),
        ));
    };
    while let Some(update_result) = sub.next().await {
        let update = match update_result {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    ?job_request_id,
                    "Error in event from geyser"
                );
                job_request_timer.take().map(|t| t.stop_and_discard());
                return Err(crate::run::Error::Internal(
                    "Got error while waiting for update to job request".to_string(),
                ));
            }
        };
        tracing::debug!(?update, "Received event from gRPC");
        // TODO(nap): change this to debug or remote
        // keyword: printline, debugging
        tracing::info!(
            "Received update from transaction and job request subscription for job request."
        );
        match update.update_oneof {
            Some(UpdateOneof::Account(a)) if a.account.is_some() => {
                let acct = a.account.unwrap();
                let j: JobRequest = bytemuck::try_pod_read_unaligned(&acct.data).map_err(|_| {
                    Error::Custom(format!(
                        "Account ({job_request_id}) did not decode into expected type: JobRequset",
                    ))
                })?;

                job_request = Some((a.slot, j));

                job_request_timer.take().map(|t| t.stop_and_record());
                job_request_tx_success.take().map(|s| s.stop_and_discard());
                job_request_tx_failure.take().map(|s| s.stop_and_discard());
            }
            Some(UpdateOneof::Account(_)) => {
                tracing::warn!(account = ?job_request_id, "BUG: Received account update but content was empty.");
            }
            Some(UpdateOneof::TransactionStatus(s)) if s.err.is_some() => {
                job_request_tx_failure.take().map(|f| f.stop_and_record());
                job_request_tx_success.take().map(|s| s.stop_and_discard());
                let tx_err = s.err.unwrap();
                tracing::warn!(
                    job_request_id = ?job_request_id,
                    error = ?tx_err,
                    "Failed to submit transaction to create job."
                );
                job_request_timer.take().map(|t| t.stop_and_discard());
                return Err(crate::run::Error::BundleSaturated);
            }
            Some(UpdateOneof::TransactionStatus(_)) => {
                job_request_tx_failure.take().map(|f| f.stop_and_discard());
                job_request_tx_success.take().map(|s| s.stop_and_record());
                tracing::debug!(
                    ?job_request_id,
                    "Job request transaction succeeded. Awaiting update to JobRequestAccount"
                );
            }
            Some(UpdateOneof::Ping(_)) => (),
            Some(UpdateOneof::Slot(slot))
                if job_request.is_some()
                        // if the slot is at least the slot the job request was in
                    && slot.slot >= job_request.unwrap().0
                        // if the commitment is confirmed or greater
                    && slot.status >= CommitmentLevel::Confirmed as i32 =>
            {
                return Ok(job_request.unwrap().1);
            }
            Some(UpdateOneof::Slot(_)) => {
                tracing::debug!(
                    "Reached slot while waiting for job request to reach confirmed status"
                );
            }
            e => {
                tracing::error!(event = ?e, "BUG: Got unexpected event. Check your subscription filters");
            }
        }
    }
    job_request_timer.take().map(|t| t.stop_and_discard());
    return Err(crate::run::Error::AccountNotExist(
        "BUG: the JobRequest account did not exist after subscription loop.",
        job_request_id,
    ));
}

/// Submits a job request and waits for the corresponding auction, returning the metadata
/// associated with the resulting job, auction, and connection information.
#[instrument(skip_all)]
pub async fn run_auction_and_get_data(
    args: SubmitJobArgs,
    self_private_key: Option<[u8; 32]>,
    lifecycle_tx: UnboundedSender<Result<StreamingResponse, run::Error>>,
) -> Result<RunAuction, crate::run::Error> {
    tracing::debug!("Running auction client flow.");

    let mut end_to_end_run_auction = Some(
        RUN_AUCTION_TIMINGS
            .with_label_values(&["e2e"])
            .start_timer(),
    );

    let client = args.client.clone();
    // Read the prompt from stdin
    if args.inference_args.messages.is_empty() {
        tracing::error!(?args, "No prompt provided");
        return Err(crate::run::Error::InvalidInput);
    }
    // TODO: this needs to be added to the auction TX's inputs when the state has been updated to
    // support that.
    let prompt_hash = Sha256::digest(&serde_json::to_string(&args.inference_args.messages)?);
    tracing::info!("Prompt hash: {}", BASE64_STANDARD.encode(prompt_hash));
    let prompt_len = tokenizer::glm_chat(
        args.inference_args
            .messages
            .iter()
            .map(tokenizer::ChatMessage::from)
            .collect(),
    )
    .instrument(info_span!("glm_tokenizer"))
    .await? as u64;
    tracing::info!(prompt_length = prompt_len, "Tokenized and calculated prompt length.");

    let balance = {
        let _timer = RPC_CLIENT_TIMINGS
            .with_label_values(&["get_balance_with_commitment"])
            .start_timer();
        RPC_CLIENT_COUNTS
            .with_label_values(&["get_balance_with_commitment"])
            .inc();
        client
            .rpc_client
            .get_balance_with_commitment(
                &args.payer_keypair.pubkey(),
                CommitmentConfig::confirmed(),
            )
            .await?
            .value
    };
    if balance < args.max_price {
        tracing::info!(
            "Balance {balance} is too low for request auction price: {}",
            args.max_price
        );
        return Err(crate::run::Error::InsufficientBalance);
    }
    let mut input_hash = [0u8; 32];
    input_hash[..32].copy_from_slice(&prompt_hash);

    let (input_hash, input_hash_iv) = if let Some((node_private, client_public_key)) =
        self_private_key.zip(args.encrypt_with_client_publickey)
    {
        let shared_secret =
            StaticSecret::from(node_private).diffie_hellman(&PublicKey::from(client_public_key));
        let iv = rand::random();
        let mut encrypted_hash_output = [0u8; 32];
        encrypted_hash_output.copy_from_slice(&encrypt_with_iv(
            input_hash.as_slice(),
            shared_secret.to_bytes(),
            Iv::new(iv),
        )?);
        (encrypted_hash_output, iv)
    } else {
        (input_hash, Default::default())
    };

    let auction_lamports = {
        let _timer = RPC_CLIENT_TIMINGS
            .with_label_values(&["get_minimum_balance_for_rent_exemption"])
            .start_timer();
        RPC_CLIENT_COUNTS
            .with_label_values(&["get_minimum_balance_for_rent_exemption"])
            .inc();
        client
            .rpc_client
            .get_minimum_balance_for_rent_exemption(Auction::LEN)
            .await?
    };

    // if we have an override defined, use that. This should only be used for debugging purposes,
    // bot doesn't really hurt anything.
    let context_length_tier = if let Some(c) = args.context_tier_override {
        c
    } else {
        // otherwise, calculate based on the prompt length.
        RequestTier::context_tier_for_tokens(prompt_len)
            .ok_or_else(|| run::Error::TooManyTokens(prompt_len))?
    };

    let expiry_duration_tier = args.duration_tier.unwrap_or(RequestTier::Pro);
    let max_output_tokens = 100;
    let retry_client = client.clone();
    let stream_tx_auction = lifecycle_tx.clone();
    let (sig, bundle_id, auction_id, job_request_key) = retry(
        |attempt, err| {
            tracing::warn!("Error submitting job request (attempt: {attempt}): {err}");
            //tokio::time::sleep(Duration::from_millis(500))
            async {}
        },
        1,
        async move || -> Result<(Signature, Pubkey, Pubkey, Pubkey), crate::run::Error> {
            let bundle = {
                let _timer = RPC_CLIENT_TIMINGS
                    .with_label_values(&["get_latest_bundle"])
                    .start_timer();
                RPC_CLIENT_COUNTS
                    .with_label_values(&["get_latest_bundle"])
                    .inc();
                retry_client
                    .get_latest_bundle(context_length_tier, expiry_duration_tier)
                    .await?
            };
            tracing::debug!("Bundle: {bundle}");

            let bundle_lamports = {
                let _timer = RPC_CLIENT_TIMINGS
                    .with_label_values(&["get_minimum_balance_for_rent_exemption"])
                    .start_timer();
                RPC_CLIENT_COUNTS
                    .with_label_values(&["get_minimum_balance_for_rent_exemption"])
                    .inc();
                retry_client
                    .rpc_client
                    .get_minimum_balance_for_rent_exemption(RequestBundle::LEN)
                    .await?
            };
            let seed = Keypair::new().pubkey();
            let ix = request_job(
                args.payer_keypair.pubkey(),
                input_hash,
                args.encrypt_with_client_publickey.map(|_| input_hash_iv),
                seed.to_bytes(),
                prompt_len,
                max_output_tokens,
                bundle_lamports,
                auction_lamports,
                bundle,
                args.max_price_per_output_token,
                context_length_tier,
                expiry_duration_tier,
                args.input_data_account,
                args.additional_bundles,
            );
            ix.accounts.iter().enumerate().for_each(|(i, acct)| {
                tracing::trace!("Account {i}: {}", acct.pubkey);
            });
            let job_request_key = ix.accounts[1].pubkey;
            // so we only allocate the string once :)
            let job_request_id = job_request_key.to_string();
            let payer_key = args.payer_keypair.pubkey();

            let recent_blockhash = {
                let _timer = RPC_CLIENT_TIMINGS
                    .with_label_values(&["get_latest_blockhash"])
                    .start_timer();
                retry_client
                    .yellowstone_client
                    .clone()
                    .0
                    .get_latest_blockhash(Some(GrpcCommitmentLevel::Processed))
                    .await?
                    .blockhash
                    .parse::<Hash>()
                    .expect("to parse hash string")
            };
            let tx = VersionedTransaction::try_new(
                VersionedMessage::V0(Message::try_compile(
                    &payer_key,
                    &[ix],
                    &[],
                    recent_blockhash,
                )?),
                &[&args.payer_keypair],
            )?;
            let job_requests_task = {
                let _timer = RPC_CLIENT_TIMINGS
                    .with_label_values(&["grpc_subscribe_wait_for_job_request"])
                    .start_timer();
                let (subscribed_tx, subscribed_rx) = oneshot::channel();
                let job_requests_task = tokio::spawn(wait_for_job_request_to_complete(
                    retry_client.clone(),
                    job_request_key,
                    subscribed_tx,
                ));
                // wait for the task to report that it has subscribed
                subscribed_rx.await.map_err(|_| {
                    crate::run::Error::Internal(
                        "Failed to subscribe to job request events.".to_string(),
                    )
                })?;
                job_requests_task
            };
            let sig = {
                let _timer = RPC_CLIENT_TIMINGS
                    .with_label_values(&["send_transaction_with_config"])
                    .start_timer();
                RPC_CLIENT_COUNTS
                    .with_label_values(&["send_transaction_with_config"])
                    .inc();
                retry_client
                    .rpc_client
                    .send_transaction_with_config(
                        &tx,
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            max_retries: Some(0),
                            ..Default::default()
                        },
                    )
                    .instrument(info_span!(
                        "send_transaction_with_config",
                        intent = "request_job",
                        job_request_id = job_request_id,
                        payer = payer_key.to_string()
                    ))
                    .await?
            };
            let job_request = {
                let _timer = RPC_CLIENT_TIMINGS
                    .with_label_values(&["wait_for_job_request_to_complete"])
                    .start_timer();
                job_requests_task.await??
            };
            tracing::info!(job_request_id = %job_request_key, "Job request tx succeeded.");

            let _ = stream_tx_auction.send(Ok(StreamingResponse::lifecycle(
                LifecycleEvent::JobRequested(Box::new(job_request)),
            )));

            let bundle_id = Pubkey::new_from_array(job_request.bundle.inner());
            let bundle = retry_client
                .get_account_with_commitment::<RequestBundle>(
                    &bundle_id,
                    Some(CommitmentConfig::processed()),
                )
                .instrument(info_span!("get_bundle_account"))
                .await?;
            let _ = stream_tx_auction.send(Ok(StreamingResponse::lifecycle(
                LifecycleEvent::Bundled(bundle),
            )));

            tracing::info!(
                job_request_id = job_request_id,
                bundle_id = %bundle_id,
                "Bundle id for the request. Waiting for it to be auctioned off.",
            );
            // Wait for the bundle to be auctioned off.
            let auction_id = Pubkey::new_from_array(
                bundle
                .auction
                .get()
                .ok_or(
                    crate::run::Error::Internal(
                        "The auction was expected to have been created at the same time as the bundle but was not".to_string()
                        )
                    )?
                .inner()
                );

            // Spawn task in the background to fetch the auction details and
            // emit the auction lifecycle event.
            // This helps prevent any slowdowns in the main thread.
            tokio::spawn({
                let stream_tx_auction = stream_tx_auction.clone();
                let retry_client = retry_client.clone();

                async move {
                    let auction = retry_client.get_account(&auction_id).await?;

                    let _ = stream_tx_auction.send(Ok(StreamingResponse::lifecycle(
                        LifecycleEvent::AuctionStarted(auction),
                    )));

                    Ok::<_, crate::run::Error>(())
                }
            });

            tracing::info!(
                job_request_id = job_request_id,
                "Auction for bundle ({bundle_id}) is {auction_id}"
            );
            Ok((sig, bundle_id, auction_id, job_request_key))
        },
    )
    .instrument(info_span!("job_request_retry_loop"))
    .await?;

    tracing::info!("Sent job request: {job_request_key} with signature: {sig}",);

    let bids_handle = tokio::spawn(
        watch_bids(
            lifecycle_tx.clone(),
            client.program_id.to_string(),
            client.yellowstone_client.clone(),
            auction_id,
        )
        .instrument(debug_span!(
            "watch_bids",
            auction_id = auction_id.to_string()
        )),
    );

    tracing::info!("Waiting for auction ({auction_id}) winner");
    let auction = {
        let _timer = RPC_CLIENT_TIMINGS
            .with_label_values(&["wait_for_auction_winner"])
            .start_timer();
        RPC_CLIENT_COUNTS
            .with_label_values(&["wait_for_auction_winner"])
            .inc();
        client
            .clone()
            .wait_for_auction_winner(auction_id)
            .instrument(info_span!("wait_for_auction_winner", job_request_id = %job_request_key, bundle_id = %bundle_id))
            .await
            .inspect_err(|e| tracing::error!(
                    error = %e,
                    job_request_id = %job_request_key,
                    bundle_id = %bundle_id,
                    "Failure when waiting for auction winner"
            ))?
    };
    tracing::info!(
        auction_id = auction_id.to_string(),
        "Auction {auction_id} completed: "
    );

    let _ = lifecycle_tx.send(Ok(StreamingResponse::lifecycle(
        LifecycleEvent::AuctionEnded(auction),
    )));

    // Stop watching bids
    bids_handle.abort();

    if auction.winning_bid == [0u8; PUBKEY_BYTES] || auction.winning_bid == auction_id.to_bytes() {
        tracing::error!("There was no bids on the auction ({auction_id})");
        return Err(crate::run::Error::NoBidders(auction_id));
    }

    let winning_bid_pubkey = Pubkey::from(auction.winning_bid.inner());
    tracing::debug!("Auction ({auction_id}) winning bidder: {winning_bid_pubkey}");
    // Get the Bid account of the winning bidder, we want the data IP and port from that
    let winning_bidder_state: Bid = {
        let _timer = RPC_CLIENT_TIMINGS
            .with_label_values(&["get_account_with_commitment"])
            .start_timer();
        RPC_CLIENT_COUNTS
            .with_label_values(&["get_account_with_commitment"])
            .inc();
        client
            .get_account_with_commitment(&winning_bid_pubkey, None)
            .instrument(info_span!("get_account_with_commitment", job_request_id = %job_request_key, %bundle_id, intent = "winning_bid_account"))
            .await?
    };

    let _ = lifecycle_tx.send(Ok(StreamingResponse::lifecycle(
        LifecycleEvent::WinningBid(winning_bidder_state.into()),
    )));

    let data_ip = IpAddr::from(winning_bidder_state.ip);
    let data_port = winning_bidder_state.port;
    tracing::debug!(data_ip = %data_ip, data_port = data_port);
    let inference_request = InferenceRequest {
        request_id: job_request_key.to_string(),
        args: args.inference_args,
        slot: Some(auction.expiry_slot + 1),
    };

    if let Some(end_to_end_timer) = end_to_end_run_auction.take() {
        end_to_end_timer.stop_and_record();
    }

    Ok(RunAuction {
        bundle: bundle_id,
        data_ip,
        data_port,
        encryption_publickey: winning_bidder_state.public_key,
        inference_request,
        job_request_id: job_request_key,
        winning_bidder: auction.winning_bid.inner().into(),
        winning_bid_price: auction.winning_bid_price.map(|p| p.get()),
        context_length_tier,
        expiry_duration_tier,
    })
}

async fn watch_bids(
    tx: UnboundedSender<Result<StreamingResponse, run::Error>>,
    program_id: String,
    client: CloneableGeyserGrpcClient,
    auction_id: Pubkey,
) -> Result<(), crate::run::Error> {
    let mut geyser = client.clone().0;
    let (_, mut stream) = geyser
        .subscribe_with_request(Some(SubscribeRequest {
            accounts: HashMap::from([(
                "listener-bid-cache".to_string(),
                SubscribeRequestFilterAccounts {
                    owner: vec![program_id],
                    filters: vec![
                        SubscribeRequestFilterAccountsFilter {
                            filter: Some(Filter::Datasize(Bid::LEN as u64)),
                        },
                        SubscribeRequestFilterAccountsFilter {
                            filter: Some(Filter::Memcmp(
                                SubscribeRequestFilterAccountsFilterMemcmp {
                                    // TODO: add e2e testing, because this can break
                                    // if the fields in the `Bid` struct ever get re-ordered.
                                    offset: 32,
                                    // Watch bids for specific auction
                                    data: Some(Data::Bytes(auction_id.to_bytes().to_vec())),
                                },
                            )),
                        },
                    ],
                    ..Default::default()
                },
            )]),
            commitment: Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed.into()),
            ..Default::default()
        }))
        .await?;

    while let Some(Ok(upd)) = stream.next().await {
        let Some(UpdateOneof::Account(account)) = upd.update_oneof else {
            continue;
        };

        let Some(account) = account.account else {
            continue;
        };

        let bid: Bid = bytemuck::try_pod_read_unaligned(&account.data)?;
        let _ = tx.send(Ok(StreamingResponse::lifecycle(LifecycleEvent::Bid(
            bid.into(),
        ))));
    }

    Ok::<_, run::Error>(())
}

#[instrument(skip(client, payer))]
pub async fn reclaim_job_request(
    client: Arc<AuctionClient>,
    payer: Arc<Keypair>,
    job_request_id: Pubkey,
    bundle_key: Pubkey,
    context_length_tier: RequestTier,
    expiry_duration_tier: RequestTier,
) -> Result<(), crate::run::Error> {
    tracing::info!(job_request_id = %job_request_id, bundle_id = %bundle_key, "Reclaiming job request.");
    let gauge = GaugeHandle::new(&RECLAIM_JOB_TASKS);
    tokio::time::timeout(
        Duration::from_secs(60 * 10),
        client.wait_for_job_verification(job_request_id, None),
    )
    .await??;

    let blockhash = client
        .yellowstone_client()
        .get_latest_blockhash(None)
        .await?
        .blockhash
        .parse::<Hash>()
        .map_err(|e| Error::Custom(format!("Failed to parse recent blockhash: {e}")))?;

    let bundle: RequestBundle = client.get_account(&bundle_key).await?;
    let RequestBundle { auction, .. } = bundle;

    let auction_key = Pubkey::new_from_array(auction.get().unwrap_or_default().inner());
    let auction_data: Auction = client.get_account(&auction_key).await?;

    let new_bundle_lamports = client
        .rpc_client
        .get_minimum_balance_for_rent_exemption(RequestBundle::LEN)
        .await?;
    let new_auction_lamports = client
        .rpc_client
        .get_minimum_balance_for_rent_exemption(Auction::LEN)
        .await?;
    client
        .reclaim_job_request(
            &payer,
            job_request_id,
            payer.pubkey(),
            bundle_key,
            auction_key,
            Pubkey::new_from_array(auction_data.payer.inner()),
            blockhash,
            context_length_tier,
            expiry_duration_tier,
            new_bundle_lamports,
            new_auction_lamports,
        )
        .await?;
    tracing::info!(job_request_id = %job_request_id, bundle_id = %bundle_key, auction_id = %auction_key, "Successfully reclaimed job.");
    drop(gauge);
    Ok(())
}

#[instrument(skip_all)]
pub async fn submit_job_async<'a>(
    args: SubmitJobArgs,
    self_private_key: Option<[u8; 32]>,
) -> Result<InferenceResponse<'a>, crate::run::Error> {
    // Create a channel, which will contain all the lifecycle events
    // in addition to the completion responses.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<StreamingResponse, _>>();

    // Oneshot channel to block until we know the auction succeeded
    let (auction_err_tx, auction_err_rx) = oneshot::channel::<Option<run::Error>>();

    let task_span = info_span!("submit_job_main_task");
    let reclaim = args.reclaim;
    task_span.follows_from(Span::current());
    // Spawn the auction to run in the background
    tokio::spawn(
        async move {
            let client = args.client.clone();
            let payer = args.payer_keypair.clone();
            let input_data_account = args.input_data_account;

            let run_auction_result = timeout(
                // 30s timeout to run the auction
                Duration::from_secs(10),
                run_auction_and_get_data(args, self_private_key, tx.clone()),
            )
            .instrument(debug_span!("timeout", intent = "run_auction_and_get_data"))
            .await;


            let RunAuction {
                data_ip,
                data_port,
                inference_request,
                job_request_id,
                bundle,
                context_length_tier,
                expiry_duration_tier,
                ..
            } = match run_auction_result {
                // Success
                Ok(Ok(result)) => {
                    auction_err_tx
                        .send(None)
                        .map_err(|_| run::Error::AuctionErrorChannel)?;

                    result
                }
                // No timeout, but auction errored
                Ok(Err(e)) => {
                    auction_err_tx
                        .send(Some(e))
                        .map_err(|_| run::Error::AuctionErrorChannel)?;
                    // Since this runs in a background task, returned error is not
                    // propagated regardless.
                    return Err(run::Error::Internal(Default::default()));
                }
                // timeout
                Err(e) => {
                    auction_err_tx
                        .send(Some(e.into()))
                        .map_err(|_| run::Error::AuctionErrorChannel)?;
                    return Err(run::Error::Internal(Default::default()));
                }
            };
            if reclaim.unwrap_or(true) {
                tracing::info!("Spawning background task to reclaim job when complete.");
                tokio::spawn(reclaim_job_request(client, payer, job_request_id, bundle, context_length_tier, expiry_duration_tier));
            }
            if input_data_account.is_none() {
                let _ = tx.send(
                    Ok(StreamingResponse::lifecycle(LifecycleEvent::RequestForwarded(data_ip, data_port)))
                );
                let mut stream =
                    Box::pin(stream_completion(inference_request, data_ip, data_port).await?);

                // TODO(nap) count tokens here and _always_ report on-chain
                // Forward all events from the stream to the channel
                let mut erroring = false;
                // ugly match so we only produce a single warning instead of 5000000000
                while let Some(m) = stream.next().await {
                    match (erroring, tx.send(m)) {
                        (true, Err(_)) => (),
                        (false, Err(e)) => {
                            erroring = true;
                            tracing::warn!(error = ?e, "Failed to send submit job event via channel.")
                        }
                        (false, Ok(_)) | (true, Ok(_)) => (),
                    }
                }
            }
            Ok::<_, run::Error>(())
        }
        .instrument(task_span),
    );

    // Wait and ensure there were no errors with the auction
    if let Some(e) = auction_err_rx
        .await
        .map_err(|_| run::Error::AuctionFailed)?
    {
        return Err(e);
    };

    // Convert the channel to a stream
    Ok(InferenceResponse::Stream(Box::pin(
        UnboundedReceiverStream::new(rx),
    )))
}

/// Submits a job to be bundled and auctioned on the Ambient network using the supplied arguments.
pub async fn submit_job<'a>(
    args: SubmitJobArgs,
    self_private_key: Option<[u8; 32]>,
) -> Result<InferenceResponse<'a>, crate::run::Error> {
    if let Some(true) = args.inference_args.stream {
        submit_job_async(args, self_private_key).await
    } else {
        submit_job_sync(args, self_private_key).await
    }
}

#[instrument(skip_all)]
async fn submit_job_sync<'a>(
    args: SubmitJobArgs,
    self_private_key: Option<[u8; 32]>,
) -> Result<InferenceResponse<'a>, crate::run::Error> {
    let reclaim = args.reclaim;
    tokio::spawn(
        async move {
            // Ignore auction events for synchronous inference
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Result<StreamingResponse, _>>();
            let client = args.client.clone();
            let payer = args.payer_keypair.clone();

            let RunAuction {
                data_ip,
                data_port,
                inference_request,
                bundle,
                job_request_id,
                winning_bidder,
                winning_bid_price,
                expiry_duration_tier,
                context_length_tier,
                ..
            } = run_auction_and_get_data(args, self_private_key, tx).await?;
            if reclaim.unwrap_or(true) {
                // trigger a background task to reclaim this job request when it gets verified.
                tokio::spawn(reclaim_job_request(
                    client,
                    payer,
                    job_request_id,
                    bundle,
                    context_length_tier,
                    expiry_duration_tier,
                ));
            }
            let mut out = completion(inference_request, data_ip, data_port).await?;
            out.winning_bidder = Some(winning_bidder);
            out.winning_bid_price = winning_bid_price;

            Ok(InferenceResponse::Sync(out))
        }
        .instrument(info_span!("submit_job_sync_main_task")),
    )
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn get_bundle_benchmark() {
        let Ok(client) = AuctionClient::new(
            ID,
            "http://localhost:8899".into(),
            None,
            Pubkey::default(),
            None,
            "http://localhost:10000".into(),
        )
        .await
        else {
            return;
        };

        let auction_client = Arc::new(client);

        let now = tokio::time::Instant::now();
        let successes = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(AtomicUsize::new(0));

        futures_util::future::join_all((0..5000).map(|_| {
            let client = auction_client.clone();
            let successes = successes.clone();
            let errors = errors.clone();

            async move {
                let _r = client
                    .get_latest_bundle(RequestTier::Pro, RequestTier::Pro)
                    .await
                    .inspect_err(|_| {
                        errors.fetch_add(1, Ordering::SeqCst);
                    })
                    .inspect(|_| {
                        successes.fetch_add(1, Ordering::SeqCst);
                    });
            }
        }))
        .await;

        eprintln!("Successes: {}", successes.load(Ordering::SeqCst));
        eprintln!("Errors: {}", errors.load(Ordering::SeqCst));
        eprintln!("Time: {}", now.elapsed().as_secs_f64());
    }
}
