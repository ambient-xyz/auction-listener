use ambient_auction_listener::run::{
    ContentChoice, ContentDelta, InferenceArgs, StreamingResponse, SyncChoice, SyncMessage,
    SyncResponse, UsageInfo,
};
use axum::{
    extract::Json as ExtractJson,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures_util::stream::Stream;
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "40")]
    tps: u64,

    #[arg(long, default_value = "1")]
    min_delay: u64,

    #[arg(long, default_value = "60")]
    max_delay: u64,

    #[arg(long, default_value = "1.0")]
    max_load: f64,

    #[arg(long, default_value_t = 15)]
    max_verification_delay: u64,

    /// Probability (0.0-1.0) that a request returns an error.
    /// Errors include HTTP 429/500/502/503, mid-stream error events, and connection drops.
    #[arg(long, default_value = "0.0")]
    error_rate: f64,
}

static ARGS: std::sync::OnceLock<Args> = std::sync::OnceLock::new();

#[derive(Clone, Copy)]
enum ErrorKind {
    /// HTTP 500 with OpenAI-style JSON error body
    InternalServerError,
    /// HTTP 502 with HTML body (Cloudflare-style, no JSON)
    BadGateway,
    /// HTTP 503 with OpenAI-style JSON error body (model overloaded)
    ServiceUnavailable,
    /// HTTP 429 with OpenAI-style JSON error body
    RateLimitExceeded,
    /// vLLM-style: error JSON sent as SSE event mid-stream, followed by [DONE]
    MidStreamErrorEvent,
    /// OpenAI-style: stream abruptly ends with no error event and no [DONE]
    MidStreamDrop,
}

fn random_error(error_rate: f64, is_streaming: bool) -> Option<ErrorKind> {
    if error_rate <= 0.0 || rand::random::<f64>() >= error_rate {
        return None;
    }
    let kind = if is_streaming {
        match rand::random::<u8>() % 6 {
            0 => ErrorKind::InternalServerError,
            1 => ErrorKind::BadGateway,
            2 => ErrorKind::ServiceUnavailable,
            3 => ErrorKind::RateLimitExceeded,
            4 => ErrorKind::MidStreamErrorEvent,
            _ => ErrorKind::MidStreamDrop,
        }
    } else {
        match rand::random::<u8>() % 4 {
            0 => ErrorKind::InternalServerError,
            1 => ErrorKind::BadGateway,
            2 => ErrorKind::ServiceUnavailable,
            _ => ErrorKind::RateLimitExceeded,
        }
    };
    Some(kind)
}

fn make_http_error_response(kind: ErrorKind) -> axum::response::Response {
    use axum::http::StatusCode;

    match kind {
        ErrorKind::InternalServerError => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": {
                    "message": "The server had an error while processing your request. Sorry about that!",
                    "type": "server_error",
                    "param": null,
                    "code": null
                }
            })),
        )
            .into_response(),
        ErrorKind::BadGateway => (
            StatusCode::BAD_GATEWAY,
            axum::response::Html(
                "<html><head><title>502 Bad Gateway</title></head>\
                 <body><center><h1>502 Bad Gateway</h1></center>\
                 <hr><center>cloudflare</center></body></html>",
            ),
        )
            .into_response(),
        ErrorKind::ServiceUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": {
                    "message": "That model is currently overloaded with other requests. You can retry your request, or contact us through our help center if the error persists.",
                    "type": "server_error",
                    "param": null,
                    "code": null
                }
            })),
        )
            .into_response(),
        ErrorKind::RateLimitExceeded => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": {
                    "message": "Rate limit reached for default-model in organization on requests per min (RPM): Limit 60, Used 60, Requested 1. Please try again in 1s.",
                    "type": "rate_limit_error",
                    "param": null,
                    "code": "rate_limit_exceeded"
                }
            })),
        )
            .into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": {
                    "message": "The server had an error while processing your request. Sorry about that!",
                    "type": "server_error",
                    "param": null,
                    "code": null
                }
            })),
        )
            .into_response(),
    }
}

async fn completions_handler(
    ExtractJson(request): ExtractJson<InferenceArgs>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let args = ARGS.get().unwrap();
    let is_streaming = request.stream == Some(true);

    let error = random_error(args.error_rate, is_streaming);

    if let Some(kind) = error {
        if !matches!(kind, ErrorKind::MidStreamErrorEvent | ErrorKind::MidStreamDrop) {
            return Ok(make_http_error_response(kind));
        }
    }

    match request.stream {
        Some(true) => Ok(
            streaming_handler(request, args.tps, args.min_delay, args.max_delay, error)
                .await
                .into_response(),
        ),
        _ => Ok(Json(sync_handler(args.min_delay, args.max_delay).await).into_response()),
    }
}

async fn streaming_handler(
    request: InferenceArgs,
    tps: u64,
    min_delay: u64,
    max_delay: u64,
    error: Option<ErrorKind>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let delay_ms = 1000 / tps;
    let total_duration = rand::random::<u64>() % (max_delay - min_delay + 1) + min_delay;
    let total_tokens = request
        .max_completion_tokens
        .unwrap_or(tps * total_duration);
    let request_id = format!("chatcmpl-{}", rand::random::<u64>());

    let messages = generate_streaming_messages(total_tokens, delay_ms, request_id, error);
    Sse::new(messages).keep_alive(KeepAlive::default())
}

async fn sync_handler(min_delay: u64, max_delay: u64) -> SyncResponse {
    let delay = rand::random::<u64>() % (max_delay - min_delay + 1) + min_delay;
    sleep(Duration::from_secs(delay)).await;

    SyncResponse {
        id: format!("chatcmpl-{}", Pubkey::new_unique()),
        object: "chat.completion".to_string(),
        created: 1589478378,
        model: "z-ai/glm-4.6".to_string(),
        choices: vec![SyncChoice {
            index: 0,
            message: SyncMessage {
                role: "assistant".to_string(),
                content: Some("This is a complete response from the mock server.".to_string()),
                reasoning_content: None,
                refusal: None,
                annotations: None,
                audio: None,
                function_call: None,
                tool_calls: vec![],
            },
            logprobs: None,
            finish_reason: Some("stop".to_string()),
            stop_reason: None,
        }],
        usage: Some(UsageInfo {
            prompt_tokens: 10,
            completion_tokens: 50,
            total_tokens: 60,
            prompt_tokens_details: None,
        }),
        merkle_root: Some(
            "1018ffbd513a6b5a5fe77dce4b47989c3d5b56d7cf3c00f3523dc3d2ea5cb21d".to_string(),
        ),
        verified: Some(true),
        service_tier: Some("default".to_string()),
        system_fingerprint: Some("mock_system".to_string()),
        prompt_logprobs: None,
        winning_bidder: Some(Pubkey::new_unique()),
        winning_bid_price: Some(25),
    }
}

fn generate_streaming_messages(
    total_tokens: u64,
    delay_ms: u64,
    request_id: String,
    error: Option<ErrorKind>,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    use futures_util::stream::{self, StreamExt};

    let words = vec![
        "Hello",
        "world",
        "this",
        "is",
        "a",
        "streaming",
        "response",
        "from",
        "the",
        "mock",
        "server",
        "with",
        "configurable",
        "timing",
        "and",
        "token",
        "rate",
        "control",
        ".",
    ];

    // If injecting a mid-stream error, cut off after a random number of tokens
    let emit_count = match error {
        Some(_) if total_tokens > 1 => rand::random::<u64>() % (total_tokens - 1) + 1,
        _ => total_tokens,
    };

    let mut events = Vec::new();

    for i in 0..emit_count {
        let word = words[i as usize % words.len()];
        let content = format!("{word} ");
        let is_last = error.is_none() && i == total_tokens - 1;

        let event = StreamingResponse::Content {
            id: request_id.clone(),
            object: "chat.completion.chunk".into(),
            created: 1589478378,
            model: "z-ai/glm-4.6".into(),
            choices: vec![ContentChoice {
                index: 0,
                delta: ContentDelta::Output {
                    role: if i == 0 {
                        Some("assistant".to_string())
                    } else {
                        None
                    },
                    content: Some(content),
                    tool_calls: None,
                },
                logprobs: None,
                finish_reason: if is_last {
                    Some("stop".to_string())
                } else {
                    None
                },
                stop_reason: None,
                token_ids: None,
            }],
            encryption_iv: None,
        };

        events.push(Ok(Event::default().json_data(&event).unwrap()));
    }

    match error {
        Some(ErrorKind::MidStreamErrorEvent) => {
            // vLLM style: send error JSON as SSE data event, then [DONE]
            let error_data = serde_json::json!({
                "error": {
                    "message": "Internal server error",
                    "type": "InternalServerError",
                    "param": null,
                    "code": 500
                }
            });
            events.push(Ok(Event::default().json_data(&error_data).unwrap()));
            events.push(Ok(Event::default().data("[DONE]")));
        }
        Some(ErrorKind::MidStreamDrop) => {
            // OpenAI style: stream abruptly ends — no error event, no [DONE]
        }
        _ => {
            // Normal completion: usage event then [DONE]
            let usage_event =
                StreamingResponse::Usage(ambient_auction_listener::run::Usage {
                    id: request_id.clone(),
                    object: "chat.completion.usage".to_string(),
                    merkle_root:
                        "1018ffbd513a6b5a5fe77dce4b47989c3d5b56d7cf3c00f3523dc3d2ea5cb21d"
                            .to_string(),
                    usage: UsageInfo {
                        prompt_tokens: 12,
                        completion_tokens: total_tokens,
                        total_tokens: 12 + total_tokens,
                        prompt_tokens_details: None,
                    },
                });
            events.push(Ok(Event::default().json_data(&usage_event).unwrap()));
            events.push(Ok(Event::default().data("[DONE]")));
        }
    }

    stream::iter(events).then(move |event| async move {
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }
        event
    })
}

async fn verify() -> Json<serde_json::Value> {
    let random_delay_seconds =
        rand::random_range::<u64, _>(0..=ARGS.get().unwrap().max_verification_delay);
    sleep(Duration::from_secs(random_delay_seconds)).await;
    Json(serde_json::json!({"success": true, "status": "Verified" }))
}

async fn health_handler() -> Json<serde_json::Value> {
    let args = ARGS.get().unwrap();
    let load = rand::random_range::<f64, _>(0.0..args.max_load);
    Json(serde_json::json!({"ok": true, "load": load}))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    ARGS.set(args).unwrap();

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/chat/completions", post(completions_handler))
        .route("/ambient/v1/inference/verify", post(verify));

    println!(
        "Mock server starting with TPS: {}, Delay range: {}-{} seconds, Error rate: {:.0}%",
        ARGS.get().unwrap().tps,
        ARGS.get().unwrap().min_delay,
        ARGS.get().unwrap().max_delay,
        ARGS.get().unwrap().error_rate * 100.0,
    );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3002").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
