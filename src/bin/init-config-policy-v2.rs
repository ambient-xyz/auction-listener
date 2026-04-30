use ambient_auction_api::{ConfigPolicyV2, ConfigPolicyV2Flag, ConfigPolicyV2Flags};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::Parser;
use serde::Serialize;
use solana_account_decoder_client_types::{UiAccount, UiAccountData, UiAccountEncoding};
use solana_sdk::{pubkey::Pubkey, rent::Rent};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

const STANDARD_TIER_CONFIG_INDEX: usize = 2;

#[derive(Parser, Debug)]
struct Args {
    /// Write a solana-test-validator --account JSON file for local bootstrap
    #[arg(long)]
    test_validator_account_file: PathBuf,
    /// The service signer pubkey to place in service_authorities[0]
    #[arg(long)]
    service_authority: Pubkey,
    /// Add AllowServicePageBackedFinalizePayout to the default page-backed finalize bypass policy
    #[arg(long)]
    enable_page_backed_finalize_payout: bool,
    /// Override the Standard tier settlement window in slots
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    standard_settlement_window_slots: Option<u64>,
    /// Override the Standard tier result window in slots
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    standard_result_window_slots: Option<u64>,
    /// Override the Standard tier verification window in slots
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    standard_verification_window_slots: Option<u64>,
    /// Override the Standard tier claim window in slots
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    standard_claim_window_slots: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
struct StandardTierWindowOverrides {
    settlement: Option<u64>,
    result: Option<u64>,
    verification: Option<u64>,
    claim: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct StandardTierWindows {
    settlement: u64,
    result: u64,
    verification: u64,
    claim: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TestValidatorAccountFile {
    pubkey: String,
    account: UiAccount,
}

fn strerr<E: Display>(arg: E) -> String {
    format!("There was an error: {arg}")
}

fn standard_tier_window_overrides(args: &Args) -> StandardTierWindowOverrides {
    StandardTierWindowOverrides {
        settlement: args.standard_settlement_window_slots,
        result: args.standard_result_window_slots,
        verification: args.standard_verification_window_slots,
        claim: args.standard_claim_window_slots,
    }
}

fn apply_standard_tier_window_overrides(
    policy: &mut ConfigPolicyV2,
    overrides: StandardTierWindowOverrides,
) {
    let tier_config = &mut policy.tier_configs[STANDARD_TIER_CONFIG_INDEX];
    if let Some(settlement) = overrides.settlement {
        tier_config.settlement_window_slots = settlement;
    }
    if let Some(result) = overrides.result {
        tier_config.result_window_slots = result;
    }
    if let Some(verification) = overrides.verification {
        tier_config.verification_window_slots = verification;
    }
    if let Some(claim) = overrides.claim {
        tier_config.claim_window_slots = claim;
    }
}

fn standard_tier_windows(policy: &ConfigPolicyV2) -> StandardTierWindows {
    let tier_config = &policy.tier_configs[STANDARD_TIER_CONFIG_INDEX];
    StandardTierWindows {
        settlement: tier_config.settlement_window_slots,
        result: tier_config.result_window_slots,
        verification: tier_config.verification_window_slots,
        claim: tier_config.claim_window_slots,
    }
}

fn desired_policy_flags(enable_page_backed_finalize_payout: bool) -> ConfigPolicyV2Flags {
    let flags =
        ConfigPolicyV2Flags::from_flag(ConfigPolicyV2Flag::AllowServicePageBackedFinalizeBypass);
    if enable_page_backed_finalize_payout {
        flags.union(ConfigPolicyV2Flags::from_flag(
            ConfigPolicyV2Flag::AllowServicePageBackedFinalizePayout,
        ))
    } else {
        flags
    }
}

fn desired_policy(
    service_authority: Pubkey,
    enable_page_backed_finalize_payout: bool,
    standard_tier_window_overrides: StandardTierWindowOverrides,
) -> ConfigPolicyV2 {
    let mut policy = ConfigPolicyV2::default();
    policy.service_authorities[0] = service_authority.to_bytes().into();
    policy.policy_flags = desired_policy_flags(enable_page_backed_finalize_payout);
    apply_standard_tier_window_overrides(&mut policy, standard_tier_window_overrides);
    policy
}

fn print_policy_summary(
    policy_pda: Pubkey,
    policy_flags: ConfigPolicyV2Flags,
    windows: StandardTierWindows,
) {
    println!("Config policy PDA: {policy_pda}");
    println!("Policy flags: {}", policy_flags.bits());
    println!(
        "Standard tier windows: settlement={} result={} verification={} claim={}",
        windows.settlement, windows.result, windows.verification, windows.claim
    );
}

fn write_test_validator_account_file(
    path: &Path,
    policy_pda: Pubkey,
    policy: ConfigPolicyV2,
) -> Result<(), String> {
    let encoded_policy = BASE64_STANDARD.encode(bytemuck::bytes_of(&policy));
    let account_file = TestValidatorAccountFile {
        pubkey: policy_pda.to_string(),
        account: UiAccount {
            lamports: Rent::default().minimum_balance(ConfigPolicyV2::LEN),
            data: UiAccountData::Binary(encoded_policy, UiAccountEncoding::Base64),
            owner: ambient_auction_client::ID.to_string(),
            executable: false,
            rent_epoch: u64::MAX,
            space: Some(ConfigPolicyV2::LEN as u64),
        },
    };

    let encoded = serde_json::to_vec_pretty(&account_file).map_err(strerr)?;
    std::fs::write(path, encoded).map_err(strerr)
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let standard_tier_window_overrides = standard_tier_window_overrides(&args);
    let policy = desired_policy(
        args.service_authority,
        args.enable_page_backed_finalize_payout,
        standard_tier_window_overrides,
    );
    let policy_pda = ambient_auction_client::sdk::find_config_policy_v2(ambient_auction_client::ID);

    write_test_validator_account_file(&args.test_validator_account_file, policy_pda, policy)?;

    print_policy_summary(
        policy_pda,
        policy.policy_flags,
        standard_tier_windows(&policy),
    );
    println!(
        "Test validator account file: {}",
        args.test_validator_account_file.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_config_policy_v2_desired_policy_sets_primary_service_authority_and_bypass_flag() {
        let service_authority = Pubkey::new_unique();
        let policy = desired_policy(
            service_authority,
            false,
            StandardTierWindowOverrides::default(),
        );

        assert_eq!(
            policy.service_authorities[0].inner(),
            service_authority.to_bytes()
        );
        assert!(policy
            .policy_flags
            .contains(ConfigPolicyV2Flag::AllowServicePageBackedFinalizeBypass));
        assert!(!policy
            .policy_flags
            .contains(ConfigPolicyV2Flag::AllowServicePageBackedFinalizePayout));
    }

    #[test]
    fn init_config_policy_v2_desired_policy_adds_payout_flag_when_requested() {
        let service_authority = Pubkey::new_unique();
        let policy = desired_policy(
            service_authority,
            true,
            StandardTierWindowOverrides::default(),
        );

        assert_eq!(policy.policy_flags.bits(), 48);
        assert!(policy.policy_flags.contains_all(
            ConfigPolicyV2Flags::from_flag(
                ConfigPolicyV2Flag::AllowServicePageBackedFinalizeBypass
            )
            .union(ConfigPolicyV2Flags::from_flag(
                ConfigPolicyV2Flag::AllowServicePageBackedFinalizePayout
            ))
        ));
    }

    #[test]
    fn init_config_policy_v2_desired_policy_applies_standard_tier_window_overrides() {
        let service_authority = Pubkey::new_unique();
        let policy = desired_policy(
            service_authority,
            false,
            StandardTierWindowOverrides {
                settlement: Some(40),
                result: Some(120),
                verification: Some(120),
                claim: Some(5),
            },
        );
        let windows = standard_tier_windows(&policy);

        assert_eq!(windows.settlement, 40);
        assert_eq!(windows.result, 120);
        assert_eq!(windows.verification, 120);
        assert_eq!(windows.claim, 5);
    }
}
