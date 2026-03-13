use solana_sdk::hash::Hash;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::time::timeout;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta;

pub(super) const RECENT_BLOCKHASH_CACHE_MAX_AGE: Duration = Duration::from_secs(30);
pub(super) const RECENT_BLOCKHASH_CACHE_WARMUP_WAIT: Duration = Duration::from_millis(200);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CachedRecentBlockhash {
    pub(super) slot: u64,
    pub(super) blockhash: Hash,
    pub(super) observed_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RecentBlockhashLookup {
    Return(Hash),
    WaitForWarmup,
    FallbackToRpc,
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

pub(super) fn decide_recent_blockhash_lookup(
    current: Option<&CachedRecentBlockhash>,
    warmer_running: bool,
    now: Instant,
) -> RecentBlockhashLookup {
    match current {
        Some(cached) if cached.is_fresh(now) => RecentBlockhashLookup::Return(cached.blockhash),
        None if warmer_running => RecentBlockhashLookup::WaitForWarmup,
        _ => RecentBlockhashLookup::FallbackToRpc,
    }
}

pub(super) async fn wait_for_fresh_cached_blockhash(
    mut rx: watch::Receiver<Option<CachedRecentBlockhash>>,
    timeout_duration: Duration,
) -> Option<CachedRecentBlockhash> {
    if let Some(cached) = rx
        .borrow()
        .clone()
        .filter(|cached| cached.is_fresh(Instant::now()))
    {
        return Some(cached);
    }

    timeout(timeout_duration, async move {
        loop {
            if rx.changed().await.is_err() {
                return None;
            }

            if let Some(cached) = rx
                .borrow_and_update()
                .clone()
                .filter(|cached| cached.is_fresh(Instant::now()))
            {
                return Some(cached);
            }
        }
    })
    .await
    .ok()
    .flatten()
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

    #[tokio::test]
    async fn wait_helper_returns_after_sender_publish() {
        let (tx, rx) = watch::channel(None);
        let expected = CachedRecentBlockhash {
            slot: 77,
            blockhash: Hash::new_unique(),
            observed_at: Instant::now(),
        };
        let expected_clone = expected.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tx.send(Some(expected_clone))
                .expect("watch receiver should still be alive");
        });

        let cached = wait_for_fresh_cached_blockhash(rx, Duration::from_millis(100))
            .await
            .expect("cache wait should resolve");

        assert_eq!(cached, expected);
    }

    #[tokio::test]
    async fn wait_helper_times_out_when_no_value_arrives() {
        let (_tx, rx) = watch::channel(None);

        let cached = wait_for_fresh_cached_blockhash(rx, Duration::from_millis(20)).await;

        assert_eq!(cached, None);
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

    #[test]
    fn no_warmer_running_falls_back_immediately() {
        let decision = decide_recent_blockhash_lookup(None, false, Instant::now());

        assert_eq!(decision, RecentBlockhashLookup::FallbackToRpc);
    }

    #[test]
    fn empty_cache_with_warmer_running_waits_briefly() {
        let decision = decide_recent_blockhash_lookup(None, true, Instant::now());

        assert_eq!(decision, RecentBlockhashLookup::WaitForWarmup);
    }

    #[test]
    fn stale_cache_falls_back_to_rpc() {
        let stale = CachedRecentBlockhash {
            slot: 42,
            blockhash: Hash::new_unique(),
            observed_at: Instant::now() - RECENT_BLOCKHASH_CACHE_MAX_AGE - Duration::from_secs(1),
        };

        let decision = decide_recent_blockhash_lookup(Some(&stale), true, Instant::now());

        assert_eq!(decision, RecentBlockhashLookup::FallbackToRpc);
    }
}
