use ambient_auction_api::BundleVerificationDisputeV2Kind;
use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    message::{v0::Message, VersionedMessage},
    pubkey::Pubkey,
    signature::read_keypair_file,
    signer::Signer,
    transaction::VersionedTransaction,
};
use std::path::PathBuf;

const DEFAULT_RPC_URL: &str = "http://localhost:8899";

#[derive(Parser, Debug)]
struct Cli {
    /// Solana RPC cluster URL
    #[arg(short = 'r', long, default_value = DEFAULT_RPC_URL)]
    cluster_rpc: String,
    /// Keypair that pays for the dispute account and any configured dispute bond
    #[arg(long)]
    dispute_payer_keypair: PathBuf,
    /// V2 bundle escrow account to dispute
    #[arg(long)]
    bundle_escrow: Pubkey,
    /// Dispute type to submit
    #[arg(long, value_enum)]
    kind: DisputeKindArg,
    /// Account that receives the rent refund and any refundable dispute bond
    #[arg(long)]
    bond_refund_recipient: Option<Pubkey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum DisputeKindArg {
    #[value(name = "missed-verification")]
    MissedVerification,
    #[value(name = "paid-verdict-dispute")]
    PaidVerdictDispute,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let dispute_payer = read_keypair_file(&cli.dispute_payer_keypair)
        .map_err(|err| anyhow!("failed to read dispute payer keypair: {err}"))?;
    let dispute_payer_pubkey = dispute_payer.pubkey();
    let bond_refund_recipient = cli.bond_refund_recipient.unwrap_or(dispute_payer_pubkey);
    let bundle_verification_dispute =
        ambient_auction_client::sdk::find_bundle_verification_dispute_v2(
            ambient_auction_client::ID,
            cli.bundle_escrow,
        );
    let ix = ambient_auction_client::sdk::dispute_bundle_verification_v2(
        ambient_auction_client::ID,
        dispute_payer_pubkey,
        cli.bundle_escrow,
        bond_refund_recipient,
        match cli.kind {
            DisputeKindArg::MissedVerification => {
                BundleVerificationDisputeV2Kind::MissedVerification
            }
            DisputeKindArg::PaidVerdictDispute => {
                BundleVerificationDisputeV2Kind::PaidVerdictDispute
            }
        },
    );

    let client = RpcClient::new_with_commitment(cli.cluster_rpc, CommitmentConfig::confirmed());
    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(Message::try_compile(
            &dispute_payer_pubkey,
            &[ix],
            &[],
            client.get_latest_blockhash().await?,
        )?),
        &[&dispute_payer],
    )?;
    let sig = client
        .send_and_confirm_transaction_with_spinner(&tx)
        .await?;

    println!("Bundle escrow: {}", cli.bundle_escrow);
    println!("Bundle verification dispute: {bundle_verification_dispute}");
    println!("Dispute payer: {dispute_payer_pubkey}");
    println!("Bond refund recipient: {bond_refund_recipient}");
    println!("Signature: {sig}");

    Ok(())
}
