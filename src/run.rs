use crate::listener::AuctionClient;
use crate::yellowstone_grpc::YellowstoneGrpcError;
use ambient_auction_api::{Auction, Bid, BidStatus, JobRequest, RequestBundle, RequestTier};
use axum::http::HeaderMap;
use base64::prelude::BASE64_STANDARD;
use base64::Engine as _;
use futures_util::StreamExt as _;
use futures_util::{stream::BoxStream, Stream};
use reqwest_eventsource::{CannotCloneRequestError, Event, EventSource};
use serde::{Deserialize, Serialize};
use solana_client::{client_error::ClientError, nonblocking::pubsub_client::PubsubClientError};
use solana_sdk::pubkey::ParsePubkeyError;
use solana_sdk::signature::Keypair;
use solana_sdk::{message::CompileError, pubkey::Pubkey, signer::SignerError};
use std::collections::HashMap;
use std::error::Error as _;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;
use std::vec;
use std::{future::Future, net::IpAddr, sync::Arc};
use thiserror::Error as ThisError;
use tokenizer::ChatMessage;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tracing::{info_span, instrument, Instrument as _};
use wolf_crypto::buf::Iv;
use yellowstone_grpc_client::GeyserGrpcClientError;

pub const TIMEOUT: Duration = Duration::from_secs(1200);

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(
    tag = "role",
    rename_all = "snake_case",
    rename_all_fields = "snake_case"
)]
/// InferenceMessage is the input datastructure used for formulating requests to an inference
/// engine. Not to be confused with `SyncMessage` which is the response counterpart.
pub enum InferenceMessage {
    Developer {
        content: String,
    },
    System {
        content: Option<MessageContent>,
    },
    User {
        content: Option<MessageContent>,
    },
    Assistant {
        content: Option<MessageContent>,
    },
    Tool {
        content: String,
        tool_call_id: String,
    },
}

// Converts a synchronous message response into one or more InferenceMessage to be used in
// subsequent requests.
impl TryFrom<SyncMessage> for Vec<InferenceMessage> {
    type Error = String;

    fn try_from(message: SyncMessage) -> Result<Self, Self::Error> {
        let SyncMessage {
            content: Some(content),
            tool_calls,
            role,
            ..
        } = message
        else {
            return Err("No content in provided `SyncMessage` (content is None).".to_string());
        };
        match role.as_str() {
            "developer" => Ok(vec![InferenceMessage::Developer { content }]),
            "system" => Ok(vec![InferenceMessage::System {
                content: Some(MessageContent::Text(content)),
            }]),
            "user" => Ok(vec![InferenceMessage::User {
                content: Some(MessageContent::Text(content)),
            }]),
            "assistant" => {
                let mut out = vec![InferenceMessage::Assistant {
                    content: Some(MessageContent::Text(content)),
                }];
                for tool_call in tool_calls {
                    out.push(InferenceMessage::Tool {
                        content: serde_json::to_string(&tool_call).map_err(|e| {
                            format!("Unable to serialize tool call to JSON string: {e}")
                        })?,
                        tool_call_id: tool_call.id,
                    })
                }
                Ok(out)
            }
            _ => Err(format!("Unexpected role: {role}")),
        }
    }
}

impl InferenceMessage {
    pub fn content(&self) -> MessageContent {
        use InferenceMessage::*;
        match self {
            Developer { content } => MessageContent::Text(content.to_owned()),
            System { content } => content.to_owned().unwrap_or_default(),
            User { content } => content.to_owned().unwrap_or_default(),
            Assistant { content, .. } => content.to_owned().unwrap_or_default(),
            Tool { content, .. } => MessageContent::Text(content.to_owned()),
        }
    }

    pub fn set_content(&mut self, new_content: String) {
        match self {
            Self::Developer { ref mut content } => *content = new_content,
            Self::System { ref mut content } => *content = Some(MessageContent::Text(new_content)),
            Self::User { ref mut content } => *content = Some(MessageContent::Text(new_content)),
            Self::Assistant { ref mut content } => {
                *content = Some(MessageContent::Text(new_content))
            }
            Self::Tool {
                ref mut content, ..
            } => *content = new_content,
        }
    }
}

impl From<&InferenceMessage> for ChatMessage {
    fn from(msg: &InferenceMessage) -> Self {
        match msg {
            InferenceMessage::Developer { content } => ChatMessage::system(content.clone()),
            InferenceMessage::System { content } => {
                ChatMessage::system(content.clone().unwrap_or_default().into_text().join(""))
            }
            InferenceMessage::User { content } => {
                ChatMessage::user(content.clone().unwrap_or_default().into_text().join(""))
            }
            InferenceMessage::Assistant { content, .. } => {
                ChatMessage::assistant(content.clone().unwrap_or_default().into_text().join(""))
            }
            InferenceMessage::Tool { content, .. } => ChatMessage::tool(content.clone()),
        }
    }
}

/// Either a plain string **or** an array of `{ type, text }` objects.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)] // <-- key: no extra tag field in JSON
pub enum MessageContent {
    Text(String),
    Segments(Vec<ContentSegment>),
}

impl Default for MessageContent {
    fn default() -> Self {
        MessageContent::Text(String::default())
    }
}

impl MessageContent {
    pub fn len(&self) -> usize {
        match self {
            MessageContent::Text(text) => text.len(),
            MessageContent::Segments(segments) => segments.iter().map(|s| s.text.len()).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_text(self) -> Vec<String> {
        match self {
            MessageContent::Text(text) => vec![text],
            MessageContent::Segments(segments) => segments.into_iter().map(|s| s.text).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ContentSegment {
    #[serde(rename = "type")]
    pub type_: String,
    pub text: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FunctionArgDefinition {
    pub name: String,
    pub description: Option<String>,
    pub parameters: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ChatCompletionToolsParam {
    #[serde(rename = "type")]
    pub type_: String,
    pub function: FunctionArgDefinition,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum ToolChoice {
    String(String),
    Object(HashMap<String, serde_json::Value>),
}

#[derive(Debug)]
pub struct SubmitJobArgs {
    /// The keypair that will pay for the auction
    pub payer_keypair: Arc<Keypair>,
    /// The maximum amount of Lamports to spend on the auction. This is paid into escrow.
    pub max_price: u64,
    pub max_price_per_output_token: u64,
    /// A pubsub + RPC client wrapped in an Arc for convenient sharing.
    pub client: Arc<AuctionClient>,
    pub additional_bundles: Option<u64>,
    pub encrypt_with_client_publickey: Option<[u8; 32]>,
    // pub node_private_key: Option<[u8; 32]>,
    pub inference_args: InferenceArgs,
    pub duration_tier: Option<RequestTier>,
    pub context_tier_override: Option<RequestTier>,
    pub input_data_account: Option<Pubkey>,
    pub reclaim: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InferenceRequest {
    /// The job request key
    pub request_id: String,
    #[serde(default)]
    pub slot: Option<u64>,
    #[serde(flatten)]
    pub args: InferenceArgs,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct StreamOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_usage: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuous_usage_stats: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct InferenceArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Whether to use the SSE-based streaming response or the
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    pub messages: Vec<InferenceMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>, // b/w compatibility with some clients
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_budget: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<StreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ChatCompletionToolsParam>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled_tools: Option<Vec<ToolChoice>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<serde_json::Value>,
    /// The JSON schema used to constrain the output (structured output).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guided_json: Option<serde_json::Value>,
    /// Wait for verification to complete before returning the response to the caller.
    /// Right now this is only used in the node API, but the logic should be moved here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_for_verification: Option<bool>,
    /// Whether to return usage information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emit_usage: Option<bool>,
    /// Whether to return verification status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emit_verified: Option<bool>,
    /// Client public key for encryption
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypt_with: Option<[u8; 32]>,
    /// Maximum price per output token
    #[serde(default)]
    pub is_paid: bool,
}

impl InferenceArgs {
    pub fn max_lamports_per_output_token(&self) -> u64 {
        if self.is_paid {
            10
        } else {
            0
        }
    }
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("An Internal error ocurred: {0}")]
    Internal(String),
    #[error("Solana RPC Client error: {0}")]
    RPC(#[from] ClientError),
    #[error("Pubsub client error: {0}")]
    Pubsub(#[from] PubsubClientError),
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Got unexpected account bytes.")]
    UnexpectedBytes,
    #[error("Account data was the wrong size. Expected {expected}. Got {actual}")]
    InvalidAccountSize { actual: usize, expected: usize },
    #[error("Account {0} with pubkey {1} does not exist")]
    AccountNotExist(&'static str, Pubkey),
    #[error("Programmer bug: cannot clone request: {0}")]
    CannotCloneRequest(#[from] CannotCloneRequestError),
    #[error("BUG: Solana instruction compilation error: {0}")]
    Compilation(#[from] CompileError),
    #[error("BUG: Solana instruction signer error: {0}")]
    TransactionSignature(#[from] SignerError),
    #[error("HTTP client error: {error}{cause}")]
    HTTPReqwest {
        error: reqwest::Error,
        cause: String,
    },
    #[error("Insufficient balance to pay for auction and job.")]
    InsufficientBalance,
    #[error("The input prompt was either empty or malformed")]
    InvalidInput,
    #[error("Event source connection error: {0}")]
    EventSource(#[from] reqwest_eventsource::Error),
    #[error("BUG: JSON encoder/decoder error, there's a malformed request: {0}")]
    JsonEncode(#[from] serde_json::Error),
    #[error("There were no bidders for this auction. Please try again.")]
    NoBidders(Pubkey),
    #[error("Bundle saturated. Could not get into bundle for requested tiers. Please try again.")]
    BundleSaturated,
    #[error("BUG: Account data bytes mismatch: {0}")]
    Bytemuck(bytemuck::PodCastError),
    #[error("Yellowstone Client Error: {0}")]
    YellowstoneGrpc(#[from] YellowstoneGrpcError),
    #[error("Yellowstone GRPC Client Error: {0}")]
    Grpc(#[from] GeyserGrpcClientError),
    #[error("Auction Listener Error: {0}")]
    AuctionListener(#[from] crate::error::Error),
    #[error("Error encrypting inference")]
    InferenceEncryption,
    #[error("Failed to send inference response")]
    InferenceChannel,
    #[error("Timeout. Something took too long. Elapsed: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Tokenizing error: {0}")]
    Tokenizing(#[from] tokenizer::Error),
    #[error("Exceeded maximum token limit: {0}")]
    TooManyTokens(u64),
    #[error("Join error: failed to join a background task: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("Auction aborted")]
    AuctionFailed,
    #[error("Auction status receiver dropped")]
    AuctionErrorChannel,
}

impl From<bytemuck::PodCastError> for Error {
    fn from(value: bytemuck::PodCastError) -> Self {
        Self::Bytemuck(value)
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(value: Box<dyn std::error::Error>) -> Self {
        Self::Internal(format!("{}", value))
    }
}

impl From<SendError<Result<StreamingResponse, Error>>> for Error {
    fn from(_: SendError<Result<StreamingResponse, Error>>) -> Self {
        Self::InferenceChannel
    }
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        let cause = error
            .source()
            .map(|cause| format!(": {cause}"))
            .unwrap_or_default();
        Error::HTTPReqwest { error, cause }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AuctionError {
    pub error: String,
}

impl AuctionError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
        }
    }

    pub fn message(s: impl ToString) -> Self {
        Self {
            error: s.to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(untagged)]
pub enum FallibleResponse<T, E> {
    Ok(T),
    Err(E),
}

impl std::fmt::Display for AuctionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuctionError: {}", self.error)
    }
}
impl std::error::Error for AuctionError {}

impl From<reqwest::Error> for AuctionError {
    fn from(value: reqwest::Error) -> Self {
        Self {
            error: format!("Internal HTTP client request error: {value}"),
        }
    }
}

impl From<ParsePubkeyError> for AuctionError {
    fn from(value: ParsePubkeyError) -> Self {
        Self {
            error: format!("Unable to parse pubkey: {value}"),
        }
    }
}

impl From<Error> for AuctionError {
    fn from(value: Error) -> Self {
        Self {
            error: format!("AuctionError({value})"),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, Eq, PartialEq)]
pub struct UsageInfo {
    pub prompt_tokens: u64,
    pub total_tokens: u64,
    pub completion_tokens: u64,
    pub prompt_tokens_details: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct StreamingFunctionResponseDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub arguments: Option<serde_json::Value>,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct StreamingToolCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "type")]
    pub type_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<usize>,
    pub function: StreamingFunctionResponseDefinition,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(untagged)]
pub enum ContentDelta {
    Reasoning {
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        #[serde(deserialize_with = "Option::deserialize")]
        reasoning_content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<StreamingToolCall>>,
    },
    Output {
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        #[serde(deserialize_with = "Option::deserialize")]
        content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<StreamingToolCall>>,
    },
    ToolCall {
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        tool_calls: Vec<StreamingToolCall>,
    },
    Finished {},
}

impl ContentDelta {
    pub fn encrypt(&mut self, shared_secret: [u8; 32], iv: Iv) -> Result<(), AuctionError> {
        let plaintext = match self {
            ContentDelta::Reasoning {
                reasoning_content: content,
                ..
            }
            | ContentDelta::Output { content, .. } => content,
            ContentDelta::ToolCall { .. } | ContentDelta::Finished {} => {
                // We don't encrypt tool calls for now.
                return Ok(());
            }
        };
        *plaintext = match plaintext {
            Some(plaintext) => Some(BASE64_STANDARD.encode(encrypt_with_iv(
                plaintext.as_bytes(),
                shared_secret,
                iv,
            )?)),
            None => None,
        };
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ContentChoice {
    pub index: usize,
    pub delta: ContentDelta,
    pub logprobs: Option<serde_json::Value>,
    pub finish_reason: Option<String>,
    pub stop_reason: Option<serde_json::Value>,
    pub token_ids: Option<serde_json::Value>,
}

/// Usage information returned by the OpenAI/vLLM API
#[derive(Deserialize, Serialize, Debug)]
pub struct Usage {
    pub id: String,
    pub object: String,
    pub merkle_root: String,
    pub usage: UsageInfo,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(untagged)]
pub enum StreamingResponse {
    Lifecycle {
        object: String,
        #[serde(flatten)]
        event: Box<LifecycleEvent>,
    },
    Content {
        id: String,
        object: String,
        created: u64,
        model: String,
        choices: Vec<ContentChoice>,
        #[serde(skip_serializing_if = "Option::is_none")]
        encryption_iv: Option<[u8; 16]>,
    },
    Usage(Usage),
    Done,
    Verification {
        verified: bool,
    },
}

impl StreamingResponse {
    pub fn lifecycle(event: LifecycleEvent) -> Self {
        Self::Lifecycle {
            object: "ambient.lifecycle".to_owned(),
            event: Box::new(event),
        }
    }
}

/// Stripped down version of the `Bid` struct that can be
/// shown to end users.
#[derive(Deserialize, Serialize, Debug)]
pub struct BidInfo {
    authority: Pubkey,
    auction: Pubkey,
    price_hash: Pubkey,
    price_per_output_token: Option<NonZeroU64>,
    status: BidStatus,
    public_key: [u8; 32],
}

impl From<Bid> for BidInfo {
    fn from(bid: Bid) -> Self {
        BidInfo {
            authority: bid.authority.inner().into(),
            auction: bid.auction.inner().into(),
            price_hash: bid.price_hash.into(),
            price_per_output_token: bid.price_per_output_token,
            status: bid.status,
            public_key: bid.public_key,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", content = "content", rename_all = "camelCase")]
pub enum LifecycleEvent {
    JobRequested(Box<JobRequest>),
    Bundled(RequestBundle),
    AuctionStarted(Auction),
    Bid(BidInfo),
    WinningBid(BidInfo),
    AuctionEnded(Auction),
    RequestForwarded(IpAddr, u16),
}

/// Wrapper for serializing and deserializing Pubkey's as strings.
#[derive(Debug, Clone, Copy)]
pub struct PubkeyString(Pubkey);

impl From<Pubkey> for PubkeyString {
    fn from(pubkey: Pubkey) -> Self {
        PubkeyString(pubkey)
    }
}

impl From<ambient_auction_api::Pubkey> for PubkeyString {
    fn from(pubkey: ambient_auction_api::Pubkey) -> Self {
        PubkeyString(pubkey.inner().into())
    }
}

impl Serialize for PubkeyString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for PubkeyString {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(PubkeyString(
            Pubkey::from_str(&s).map_err(serde::de::Error::custom)?,
        ))
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct FunctionResponseDefinition {
    pub name: String,
    pub arguments: Option<serde_json::Value>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub function: FunctionResponseDefinition,
    // we don't care what the actual JSON structure is, but we want to preserve it
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<serde_json::Value>,
}

/// The synchronous response message type.
#[derive(Deserialize, Serialize, Debug, Default)]
pub struct SyncMessage {
    pub role: String,
    pub reasoning_content: Option<String>,
    pub content: Option<String>,
    pub refusal: Option<String>,
    pub annotations: Option<serde_json::Value>,
    pub audio: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_call: Option<serde_json::Value>, // deprecated but leave it for now
    #[serde(default)]
    pub tool_calls: Vec<ToolCall>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SyncChoice {
    pub index: usize,
    pub message: SyncMessage,
    pub logprobs: Option<serde_json::Value>,

    pub finish_reason: Option<String>,
    pub stop_reason: Option<serde_json::Value>,
}
#[derive(Deserialize, Serialize, Debug)]
pub struct SyncResponse {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<SyncChoice>,
    // TODO(nap): make this not optional again
    pub usage: Option<UsageInfo>,
    pub merkle_root: Option<String>,
    pub verified: Option<bool>,
    pub service_tier: Option<String>,
    pub system_fingerprint: Option<String>,
    pub prompt_logprobs: Option<serde_json::Value>,
    pub winning_bidder: Option<Pubkey>,
    pub winning_bid_price: Option<u64>,
}

pub enum InferenceResponse<'a> {
    Sync(Box<SyncResponse>),
    Stream(BoxStream<'a, Result<StreamingResponse, Error>>),
}

/// Results from running an auction.
pub struct RunAuction {
    pub inference_request: InferenceRequest,
    pub bundle: Pubkey,
    pub data_ip: IpAddr,
    pub data_port: u16,
    pub encryption_publickey: [u8; 32],
    pub job_request_id: Pubkey,
    pub winning_bidder: Pubkey,
    pub winning_bid_price: Option<u64>,
    pub context_length_tier: RequestTier,
    pub expiry_duration_tier: RequestTier,
}

pub fn tracing_headers() -> HeaderMap {
    use reqwest::header::{HeaderName, HeaderValue};

    sentry::configure_scope(|scope| scope.get_span())
        .map(|s| {
            s.iter_headers()
                .filter_map(|(k, v)| {
                    Some((
                        HeaderName::from_str(k).ok()?,
                        HeaderValue::from_str(&v).ok()?,
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::ClientBuilder::new()
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .read_timeout(TIMEOUT)
        .timeout(TIMEOUT)
        .build()
        .unwrap()
});

///Retries an async function calling `wait` after each failed attempt.
///[`wait`] is expected to return a future with the intent of adding a delay between attempts. To
///allow for dynamic waits (such as exponential backoff or error-condition specific waits), the
///current attempt number and the error that was last returned by `f` is passed to this function.
///If `f` returns `Ok(T)` on its first call, `wait` is never invoked.
///[`attempts`] is the number of times to attempt calling `f`. `f` is always called at least once,
///even if `attempts` is `0`.
///[`f`] is the fallible async function to be retried. All `Err(E)` conditions are retried up to
///`attempts` times. An `Ok(T)` result is returned to the caller and `Err(E)` results are returned
///if an `Ok(T)` result was not returned before `attempts` tries.
pub async fn retry<T, E, Fut: Future + Send>(
    wait: impl FnOnce(usize, &E) -> Fut + Copy,
    attempts: usize,
    f: impl AsyncFn() -> Result<T, E> + Send,
) -> Result<T, E> {
    let mut attempt = 0;
    loop {
        match f().instrument(info_span!("retry_loop_body", attempt)).await {
            Ok(result) => break Ok(result),
            Err(e) => {
                attempt += 1;
                if attempt >= attempts {
                    break Err(e);
                }
                wait(attempt - 1, &e).await;
            }
        }
    }
}

/// A streaming wrapper around OpenAI compatible `/v1/chat/completions` endpoints.
///
///Bearer token-based authorization is supported. The `Authorization` header will be set to `Bearer
///` followed by the contents of the `INFERENCE_TOKEN` environment variable.
///
///`prompt` is the sequence of input messages.
///`job` is the pubkey of the `JobRequest` this inference is the input for. The corresponding
///auction must have been won before calling this function and the data IP and ports must match the
///fields provided in the winning bidders bid Account
///`data_ip` is the IP address provided by the winning bidder which will be used to perform the
///inference.
///`data_port` the port to send this inference request to
#[instrument(skip(request), fields(job_request_id = request.request_id))]
pub async fn stream_completion(
    mut request: InferenceRequest,
    data_ip: IpAddr,
    data_port: u16,
) -> Result<impl Stream<Item = Result<StreamingResponse, Error>>, Error> {
    tracing::debug!("Connecting to inference server using {data_ip}:{data_port}");
    request.args.stream = Some(true);
    let mut request = HTTP_CLIENT
        .post(format!("http://{data_ip}:{data_port}/v1/chat/completions"))
        .header("Accept", "application/json")
        .headers(tracing_headers())
        .json(&request);
    // If we have an env var set, we should use that token.
    if let Ok(token) = std::env::var("INFERENCE_TOKEN") {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let mut es = EventSource::new(request)?;
    es.set_retry_policy(Box::new(reqwest_eventsource::retry::Never));
    // Get the first result from the stream, if it's an error
    match es.next().await {
        // this is the happy case, we continus as normal here.
        Some(Ok(Event::Open)) => (),
        Some(Err(e)) => {
            tracing::error!(error = ?e, "Error in upstream completions endpoint.");
            return Err(e.into());
        }
        Some(Ok(event)) => {
            tracing::warn!(
                event = ?event,
                "BUG: Accidentally ate the first event from the upstream completions."
            );
        }
        None => {
            tracing::error!("Received no response from upstream completions.");
            return Err(Error::InferenceChannel);
        }
    }
    Ok(es.filter_map(async move |event| match event {
        Ok(Event::Open) => None,
        Ok(Event::Message(event)) => {
            log::trace!("Event data: {}", &event.data);
            if event.data == "[DONE]" {
                tracing::info!("Received end of stream.");

                Some(Ok(StreamingResponse::Done))
            } else {
                match serde_json::from_str::<FallibleResponse<StreamingResponse, AuctionError>>(
                    &event.data,
                ) {
                    Ok(FallibleResponse::Ok(resp)) => Some(Ok(resp)),
                    Ok(FallibleResponse::Err(e)) => Some(Err(Error::Internal(e.error))),
                    Err(e) => {
                        tracing::error!(
                            "Unable to decode response from inference host: {e}. Response: {}",
                            &event.data
                        );
                        Some(Err(e.into()))
                    }
                }
            }
        }
        Err(reqwest_eventsource::Error::StreamEnded) => {
            log::info!("Stream ended");
            None
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                error_debug = ?e,
                source = ?e.source(),
                "Received error from event client"
            );
            Some(Err(e.into()))
        }
    }))
}

///A basic wrapper around OpenAI compatible `/v1/chat/completions` endpoints using `stream: false`
///(ie. synchronous).
///
///Bearer token-based authorization is supported. The `Authorization` header will be set to `Bearer
///` followed by the contents of the `INFERENCE_TOKEN` environment variable.
///
///`prompt` is the sequence of input messages.
///`job` is the pubkey of the `JobRequest` this inference is the input for. The corresponding
///auction must have been won before calling this function and the data IP and ports must match the
///fields provided in the winning bidders bid Account
///`data_ip` is the IP address provided by the winning bidder which will be used to perform the
///inference.
///`data_port` the port to send this inference request to
///`wait_for_verification`: whether to wait for verification or not. The specifics of how this is
///handled are determined by the endpoint. The *intent* is to prevent blocking while waiting for
///the job to be verified.
#[instrument(skip(request))]
pub async fn completion(
    mut request: InferenceRequest,
    data_ip: IpAddr,
    data_port: u16,
) -> Result<Box<SyncResponse>, Error> {
    log::debug!("Connecting to inference server using {data_ip}:{data_port}");
    request.args.stream = Some(false);
    match &request.args.encrypt_with {
        Some(key) if *key != <[u8; 32]>::default() => {
            log::warn!("BUG: Forcing encrypt_with field to `None`. We do not support one-shot encryption at the moment.")
        }
        _ => {}
    }
    request.args.encrypt_with = None;
    let mut http_request = HTTP_CLIENT
        .post(format!("http://{data_ip}:{data_port}/v1/chat/completions"))
        .header("Accept", "application/json")
        .headers(tracing_headers())
        .json(&request);
    // If we have an env var set, we should use that token.
    if let Ok(token) = std::env::var("INFERENCE_TOKEN") {
        http_request = http_request.header("Authorization", format!("Bearer {token}"));
    }
    let resp: FallibleResponse<Box<SyncResponse>, AuctionError> = http_request
        .send()
        .instrument(info_span!("completions_http", job_request_id = request.request_id, data_ip = %data_ip, data_port = data_port))
        .await?
        .json()
        .await
        .inspect_err(|e| tracing::error!(error = ?e, data_ip = ?data_ip, data_port, "Error in completions endpoint"))?;
    match resp {
        FallibleResponse::Ok(resp) => Ok(resp),
        FallibleResponse::Err(_) => Err(Error::Internal(
            "Error calling completions endpoint.".to_string(),
        )),
    }
}

///Waits for a `JobRequest` with pubkey `job` to be verified.
#[instrument]
pub async fn wait_for_verification(client: Arc<AuctionClient>, job: Pubkey) -> Result<bool, Error> {
    log::info!("Waiting for verification");
    let (ready_tx, ready_rx) = oneshot::channel();
    let task =
        tokio::spawn(async move { client.wait_for_job_verification(job, Some(ready_tx)).await });
    if ready_rx.await.is_err() {
        return Ok(false);
    }
    Ok(task
        .await?
        .inspect_err(|e| {
            log::info!("Failed while waiting for verification: {e}");
        })
        .map(|v| v.0)
        .unwrap_or(false))
}

/// Encrypts `plaintext` with `shared_secret` using AES-256-CTR.
///
/// The initialization vector is always randomly generated to prevent potential misuse.
#[allow(clippy::result_large_err)]
pub fn encrypt(plaintext: &[u8], shared_secret: [u8; 32]) -> Result<(Vec<u8>, [u8; 16]), Error> {
    let iv = rand::random();
    let encrypted = encrypt_with_iv(plaintext, shared_secret, Iv::new(iv))?;

    Ok((encrypted, iv))
}

/// Encrypts `plaintext` with `shared_secret` using AES-256-CTR and the provided
/// initialization vector.
#[allow(clippy::result_large_err)]
pub fn encrypt_with_iv(
    plaintext: &[u8],
    shared_secret: [u8; 32],
    iv: Iv,
) -> Result<Vec<u8>, Error> {
    use wolf_crypto::aes::{AesCtr, Key};
    let key = Key::Aes256(shared_secret);
    let mut encrypted = vec![0; plaintext.len()];
    AesCtr::new(&key, &iv)
        .map_err(|_| Error::InferenceEncryption)?
        .try_apply_keystream(plaintext, &mut encrypted)
        .unit_err(())
        .map_err(|_| Error::InferenceEncryption)?;

    Ok(encrypted)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_broken_response() {
        let resp_raw = r#"
            {
                "id":"chatcmpl-48gPwqrN5ux8UEABWdFPdj1nCXFZeMMxNmXbZ4SD8FGV",
                "object":"chat.completion.chunk",
                "created":1770051839,
                "model":"zai-org/GLM-4.6",
                "choices": [
                    {
                        "index": 0,
                        "delta": {},
                        "logprobs": null,
                        "finish_reason": "tool_calls",
                        "stop_reason": null,
                        "token_ids": null
                    }
                ]
            }"#;
        // this should decode properly
        let _decoded: StreamingResponse = serde_json::from_str(resp_raw).unwrap();
    }
}
