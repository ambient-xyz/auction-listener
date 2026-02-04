use std::io;

use ambient_auction_api::instruction::SubmitValidationArgs;
use ambient_auction_client::sdk::submit_validation;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcRequestAirdropConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    message::{v0::Message, VersionedMessage},
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    signature::Keypair,
    signer::{EncodableKey, Signer},
    transaction::VersionedTransaction,
};

#[tokio::main]
async fn main() {
    println!("Enter the signer keypair path: ");
    let mut input_text = String::new();
    io::stdin()
        .read_line(&mut input_text)
        .expect("read line failed");
    let payer_path = input_text.trim();

    let payer = Keypair::read_from_file(payer_path).unwrap_or(Keypair::new());

    log::info!("Using signer: {}", payer.pubkey());

    println!("Enter the vote pubkey: ");
    let mut input_text = String::new();
    io::stdin()
        .read_line(&mut input_text)
        .expect("read line failed");
    let vote_pubkey = input_text.trim().parse::<Pubkey>().unwrap();

    println!("Enter the job request key: ");
    let mut input_text = String::new();
    io::stdin()
        .read_line(&mut input_text)
        .expect("read line failed");
    let job_request_key = input_text.trim().parse::<Pubkey>().unwrap();

    let client = RpcClient::new("http://localhost:8899".to_string());

    let airdrop_sig = match client
        .request_airdrop_with_config(
            &payer.pubkey(),
            LAMPORTS_PER_SOL / 100,
            RpcRequestAirdropConfig {
                recent_blockhash: match client.get_latest_blockhash().await {
                    Ok(hash) => Some(hash.to_string()),
                    Err(e) => {
                        eprintln!("Failed to get latest blockhash: {}", e);
                        return;
                    }
                },
                commitment: Some(CommitmentConfig::confirmed()),
            },
        )
        .await
    {
        Ok(sig) => sig,
        Err(e) => {
            eprintln!("Failed to request airdrop: {}", e);
            return;
        }
    };

    loop {
        if client
            .get_signature_status_with_commitment(&airdrop_sig, CommitmentConfig::confirmed())
            .await
            .unwrap()
            .is_some()
        {
            break;
        }
    }

    let ix = submit_validation(
        vote_pubkey,
        vote_pubkey,
        vote_pubkey,
        job_request_key,
        SubmitValidationArgs {
            num_successes: 100,
            num_failures: 0,
        },
    );

    let payer_key = payer.pubkey();

    let recent_blockhash = client.get_latest_blockhash().await.unwrap();
    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            Message::try_compile(&payer_key, &[ix], &[], recent_blockhash).unwrap(),
        ),
        &[&payer],
    )
    .unwrap();

    let sig = client
        .send_and_confirm_transaction_with_spinner_and_commitment(
            &tx,
            CommitmentConfig::confirmed(),
        )
        .await
        .unwrap();
    println!("Submitted with signature: {}", sig);
}
