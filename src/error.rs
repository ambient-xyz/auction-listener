use crate::yellowstone_grpc::YellowstoneGrpcError;
use solana_client::client_error::ClientError;
use solana_sdk::{program_error::ProgramError, pubkey::Pubkey, signer::SignerError};
use thiserror::Error;

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
#[repr(u32)]
pub enum Error {
    #[error("{0}")]
    Custom(String),
    #[error("Solana client error: {0}")]
    SolanaClient(#[from] ClientError),
    #[error("Solana program error: {0}")]
    SolanaProgram(#[from] ProgramError),
    #[error("I/O Error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Yellowstone Client Error: {0}")]
    YellowstoneGrpc(#[from] YellowstoneGrpcError),
    #[error("Pubkey Parsing Error: {0}")]
    ParsePubkeyError(#[from] solana_sdk::pubkey::ParsePubkeyError),
    #[error("Signer error: {0}")]
    Signer(#[from] SignerError),
    #[error("{0} Account does not exist: {1}")]
    AccountNotExist(&'static str, Pubkey),
}

impl From<&str> for Error {
    fn from(error: &str) -> Self {
        Error::Custom(error.to_string())
    }
}
