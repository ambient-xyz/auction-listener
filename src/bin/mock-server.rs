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
}

static ARGS: std::sync::OnceLock<Args> = std::sync::OnceLock::new();

async fn completions_handler(
    ExtractJson(request): ExtractJson<InferenceArgs>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let args = ARGS.get().unwrap();

    match request.stream {
        Some(true) => Ok(
            streaming_handler(request, args.tps, args.min_delay, args.max_delay)
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
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let delay_ms = 1000 / tps;
    let total_duration = rand::random::<u64>() % (max_delay - min_delay + 1) + min_delay;
    let total_tokens = request
        .max_completion_tokens
        .unwrap_or(tps * total_duration);
    let request_id = format!("chatcmpl-{}", rand::random::<u64>());

    let messages = generate_streaming_messages(total_tokens, delay_ms, request_id);
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

    let mut events = Vec::new();

    for i in 0..total_tokens {
        let word = words[i as usize % words.len()];
        let content = format!("{word} ");

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
                finish_reason: if i == total_tokens - 1 {
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

    // Add usage message before [DONE]
    let usage_event = StreamingResponse::Usage(ambient_auction_listener::run::Usage {
        id: request_id.clone(),
        object: "chat.completion.usage".to_string(),
        merkle_root: "1018ffbd513a6b5a5fe77dce4b47989c3d5b56d7cf3c00f3523dc3d2ea5cb21d".to_string(),
        usage: UsageInfo {
            prompt_tokens: 12,
            completion_tokens: total_tokens,
            total_tokens: 12 + total_tokens,
            prompt_tokens_details: None,
        },
    });
    events.push(Ok(Event::default().json_data(&usage_event).unwrap()));

    events.push(Ok(Event::default().data("[DONE]")));

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
        "Mock server starting with TPS: {}, Delay range: {}-{} seconds",
        ARGS.get().unwrap().tps,
        ARGS.get().unwrap().min_delay,
        ARGS.get().unwrap().max_delay
    );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3002").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
