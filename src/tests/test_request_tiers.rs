#[cfg(test)]
mod tests {
    use ambient_auction_api::RequestTier;

    #[test]
    fn prompt_length_maps_to_expected_context_tier_boundaries() {
        assert_eq!(
            RequestTier::context_tier_for_tokens(12_000),
            Some(RequestTier::Eco)
        );
        assert_eq!(
            RequestTier::context_tier_for_tokens(12_001),
            Some(RequestTier::Standard)
        );
        assert_eq!(
            RequestTier::context_tier_for_tokens(32_000),
            Some(RequestTier::Standard)
        );
        assert_eq!(
            RequestTier::context_tier_for_tokens(32_001),
            Some(RequestTier::Pro)
        );
        assert_eq!(
            RequestTier::context_tier_for_tokens(202_752),
            Some(RequestTier::Pro)
        );
        assert_eq!(RequestTier::context_tier_for_tokens(202_753), None);
    }
}
