use solana_sdk::pubkey::Pubkey;

pub mod error;
pub mod listener;
pub mod run;
pub mod yellowstone_grpc;

#[cfg(test)]
pub mod tests;

pub const CLIENT_URL: &str = "http://localhost:8899";
pub const YELLOWSTONE_URL: &str = "http://localhost:10000";
pub const ID: Pubkey = Pubkey::new_from_array(ambient_auction_api::ID);
