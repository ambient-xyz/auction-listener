use ambient_auction_listener::listener::{submit_job, AuctionClient};
use ambient_auction_listener::run::{
    self, ContentDelta, Error, InferenceArgs, InferenceMessage, InferenceResponse, MessageContent,
    StreamingResponse, Usage,
};
use ambient_auction_listener::YELLOWSTONE_URL;
use ambient_auction_listener::{CLIENT_URL, ID};
use base64::Engine;
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::keypair::read_keypair_file;
use std::fmt::{Display, Formatter};
use std::io::{IsTerminal, Read, Write as _};
use std::sync::Arc;
use std::{io, path::PathBuf};
use tokio::pin;
use ExitCondition::*;

#[allow(clippy::upper_case_acronyms)]
#[repr(i32)]
enum ExitCondition {
    INSUFFICIENTBALANCE = 1,
    INVALIDINPUT = 2,
    COMMUNICATION = 3,
    NOBIDDERS = 4,
    INVALIDOUTPUT = 5,
    UNEXPECTEDSTATE = 6,
    VERIFICATIONFAILED = 7,
}

impl ExitCondition {
    fn terminate(self) -> ! {
        std::process::exit(self as i32)
    }
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum RequestTier {
    Eco,
    Standard,
    Pro,
}

impl From<RequestTier> for ambient_auction_api::RequestTier {
    fn from(value: RequestTier) -> Self {
        use ambient_auction_api::RequestTier::*;
        match value {
            RequestTier::Eco => Eco,
            RequestTier::Standard => Standard,
            RequestTier::Pro => Pro,
        }
    }
}

impl Display for RequestTier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RequestTier::Eco => "eco",
            RequestTier::Standard => "standard",
            RequestTier::Pro => "pro",
        })
    }
}

#[derive(Parser)]
struct Args {
    /// The keypair that will pay for the auction
    payer_keypair: PathBuf,
    /// The maximum amount of Lamports to spend on the auction. This is paid into escrow.
    max_price: u64,
    /// Additional bundles to use if the minimum bundles where filled
    #[arg(short = 'b', long, default_value = None)]
    additional_bundles: Option<u64>,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long, default_value_t = CLIENT_URL.to_string())]
    cluster_rpc: String,
    /// The Solana Yellowstone URL. Defaults to http://localhost:10000
    #[arg(short = 'y', long, default_value_t = YELLOWSTONE_URL.to_string())]
    yellowstone_url: String,
    /// The model to use. Defaults to deepseek-ai/DeepSeek-R1.
    /// This should only be used during development.
    #[arg(short = 'm', long)]
    model: Option<String>,
    #[arg(short = 't', long)]
    /// Temperature passed directly to inference engine
    temperature: Option<f64>,
    #[arg(short = 'M', long)]
    /// The max tokens the model is allowed to generate of output, including reasoning.
    max_completion_tokens: Option<u64>,
    #[arg(short = 'D', long, default_value_t = RequestTier::Standard)]
    duration_tier: RequestTier,
    #[arg(short = 'C', long)]
    context_tier: Option<RequestTier>,
    #[arg(short = 'd', long, default_value = None)]
    input_data_account_key: Option<Pubkey>,
    #[arg(short = 'R', long, default_value_t = true)]
    reclaim_job: bool,
}

impl Args {
    fn into_run_auction_args(
        self,
        prompt: String,
        payer_keypair: Arc<Keypair>,
        client: Arc<AuctionClient>,
        input_data_account: Option<Pubkey>,
    ) -> run::SubmitJobArgs {
        let inference_args = InferenceArgs {
            stream: Some(true),
            messages: vec![InferenceMessage::User {
                content: Some(MessageContent::Text(prompt)),
            }],
            wait_for_verification: Some(true),
            encrypt_with: None,
            max_completion_tokens: self.max_completion_tokens,
            temperature: self.temperature,
            ..Default::default()
        };
        run::SubmitJobArgs {
            payer_keypair,
            max_price: self.max_price,
            max_price_per_output_token: 55,
            client,
            additional_bundles: self.additional_bundles,
            encrypt_with_client_publickey: None,
            inference_args,
            duration_tier: Some(self.duration_tier.into()),
            context_tier_override: self
                .context_tier
                .map(ambient_auction_api::RequestTier::from),
            input_data_account,
            reclaim: Some(self.reclaim_job),
        }
    }
}

#[tokio::main]
#[allow(clippy::result_large_err)]
async fn main() -> Result<(), Error> {
    env_logger::init();
    let cli_args = Args::parse();
    let payer = Arc::new(read_keypair_file(&cli_args.payer_keypair)?);
    // Read the prompt from stdin
    let mut stdin = io::stdin();
    let mut prompt = String::new();
    let input_data_account = cli_args.input_data_account_key;
    if input_data_account.is_none() {
        if stdin.is_terminal() {
            eprint!("Enter your LLM prompt (hit ctrl-d when done): ");
        }
        let prompt_length = stdin.read_to_string(&mut prompt)?;
        if prompt_length == 0 {
            eprintln!("No prompt provided to stdin.");
            INVALIDINPUT.terminate();
        }
    }
    let cluster_rpc = cli_args.cluster_rpc.clone();
    let yellowstone_url = cli_args.yellowstone_url.clone();
    let keypair_path = cli_args.payer_keypair.clone();
    let args = cli_args.into_run_auction_args(
        prompt,
        payer,
        Arc::new(
            AuctionClient::new(
                ID,
                cluster_rpc,
                Some(keypair_path),
                Pubkey::default(),
                None,
                yellowstone_url,
            )
            .await?,
        ),
        input_data_account,
    );

    let stream = match submit_job(args, None).await {
        Ok(InferenceResponse::Stream(stream)) => stream,
        Ok(InferenceResponse::Sync(_)) => unreachable!(),
        Err(e) => {
            eprintln!("Error: {e}");
            use Error::*;
            match e {
                InsufficientBalance => INSUFFICIENTBALANCE.terminate(),
                InvalidInput => INVALIDOUTPUT.terminate(),
                HTTPReqwest { .. } | RPC(_) | Pubsub(_) => COMMUNICATION.terminate(),
                NoBidders(_) => NOBIDDERS.terminate(),
                _ => UNEXPECTEDSTATE.terminate(),
            }
        }
    };
    pin!(stream);
    enum OutputState {
        NotStarted,
        Thinking,
        Outputting,
    }
    use OutputState::*;
    let mut state = OutputState::NotStarted;
    let mut hasher = Sha256::new();
    while let Some(event) = stream.next().await {
        let event = event?;
        match event {
            StreamingResponse::Content { choices, .. } => {
                for choice in choices {
                    match choice.delta {
                        ContentDelta::Output { content, .. } => {
                            let content = content.unwrap_or_default();
                            hasher.update(&content);
                            if matches!(state, Thinking) {
                                state = Outputting;
                                eprint!("</think>");
                            }
                            std::io::stdout().write_all(content.as_bytes())?;
                        }
                        ContentDelta::Reasoning {
                            reasoning_content, ..
                        } => {
                            let reasoning_content = reasoning_content.unwrap_or_default();
                            hasher.update(&reasoning_content);
                            if matches!(state, NotStarted) {
                                state = Thinking;
                                eprint!("<think>")
                            }
                            std::io::stdout().write_all(reasoning_content.as_bytes())?;
                        }
                        ContentDelta::ToolCall { .. } => {
                            // No output for tool calls, just ignore them.
                        }
                        ContentDelta::Finished {} => {
                            // No output, we're done!
                        }
                    }
                }
            }
            StreamingResponse::Usage(Usage {
                merkle_root, usage, ..
            }) => {
                // Flush stdout so we don't get these printed _before_ the final output text.
                std::io::stdout().flush()?;
                let digest = hasher.clone().finalize();
                let digest_b64 = base64::prelude::BASE64_STANDARD.encode(digest);
                eprintln!("\nMerkle Root: {merkle_root}");
                eprintln!("Completion tokens: {}", usage.completion_tokens);
                eprintln!("Output hash (sha256 | base64): {digest_b64}",);
            }
            StreamingResponse::Done => (),
            StreamingResponse::Verification { verified } => {
                if verified {
                    println!("\n");
                } else {
                    VERIFICATIONFAILED.terminate();
                }
            }
            event @ StreamingResponse::Lifecycle { .. } => {
                log::info!(
                    "Lifecycle event: {}",
                    serde_json::to_string(&event).unwrap()
                );
            }
        }
    }
    Ok(())
}
