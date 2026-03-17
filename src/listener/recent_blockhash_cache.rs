use solana_sdk::hash::Hash;
use std::time::{Duration, Instant};
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta;

pub(super) const RECENT_BLOCKHASH_CACHE_MAX_AGE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CachedRecentBlockhash {
    pub(super) slot: u64,
    pub(super) blockhash: Hash,
    pub(super) observed_at: Instant,
}

impl CachedRecentBlockhash {
    pub(super) fn from_block_meta(block: &SubscribeUpdateBlockMeta) -> Result<Self, String> {
        Ok(Self {
            slot: block.slot,
            blockhash: block
                .blockhash
                .parse()
                .map_err(|e| format!("Failed to parse blockhash `{}`: {e}", block.blockhash))?,
            observed_at: Instant::now(),
        })
    }

    pub(super) fn from_rpc_blockhash(blockhash: Hash) -> Self {
        Self {
            // RPC does not provide the observed slot here; any Yellowstone update should win.
            slot: 0,
            blockhash,
            observed_at: Instant::now(),
        }
    }

    pub(super) fn is_fresh(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.observed_at) <= RECENT_BLOCKHASH_CACHE_MAX_AGE
    }

    pub(super) fn supersedes(&self, current: Option<&Self>) -> bool {
        match current {
            Some(current) => self.slot > current.slot,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::solana::storage::confirmed_block::BlockHeight;

    #[test]
    fn block_meta_update_parses_cached_value() {
        let blockhash = Hash::new_unique();
        let block = SubscribeUpdateBlockMeta {
            slot: 42,
            blockhash: blockhash.to_string(),
            rewards: None,
            block_time: None,
            block_height: Some(BlockHeight { block_height: 9001 }),
            parent_slot: 41,
            parent_blockhash: String::new(),
            executed_transaction_count: 0,
            entries_count: 0,
        };

        let cached =
            CachedRecentBlockhash::from_block_meta(&block).expect("block meta should parse");

        assert_eq!(cached.slot, 42);
        assert_eq!(cached.blockhash, blockhash);
    }

    #[test]
    fn rpc_blockhash_starts_at_slot_zero() {
        let blockhash = Hash::new_unique();
        let cached = CachedRecentBlockhash::from_rpc_blockhash(blockhash);

        assert_eq!(cached.slot, 0);
        assert_eq!(cached.blockhash, blockhash);
        assert!(cached.is_fresh(Instant::now()));
    }

    #[test]
    fn stale_block_meta_does_not_replace_newer_cache() {
        let current = CachedRecentBlockhash {
            slot: 101,
            blockhash: Hash::new_unique(),
            observed_at: Instant::now(),
        };
        let stale = CachedRecentBlockhash {
            slot: 100,
            blockhash: Hash::new_unique(),
            observed_at: Instant::now(),
        };

        assert!(!stale.supersedes(Some(&current)));
        assert!(current.supersedes(Some(&stale)));
    }

    #[test]
    fn same_slot_different_blockhash_does_not_replace() {
        let current = CachedRecentBlockhash {
            slot: 101,
            blockhash: Hash::new_unique(),
            observed_at: Instant::now(),
        };
        let next = CachedRecentBlockhash {
            slot: 101,
            blockhash: Hash::new_unique(),
            observed_at: Instant::now(),
        };

        assert!(!next.supersedes(Some(&current)));
    }

    #[test]
    fn stale_cached_blockhash_is_not_fresh() {
        let cached = CachedRecentBlockhash {
            slot: 12,
            blockhash: Hash::new_unique(),
            observed_at: Instant::now() - RECENT_BLOCKHASH_CACHE_MAX_AGE - Duration::from_secs(1),
        };

        assert!(!cached.is_fresh(Instant::now()));
    }
}
