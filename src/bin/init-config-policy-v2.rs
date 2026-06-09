use ambient_auction_api::{ConfigPolicyV2, ConfigPolicyV2Flag, ConfigPolicyV2Flags};
use ambient_auction_listener::CLIENT_URL;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::Parser;
use serde::Serialize;
use solana_account_decoder_client_types::{UiAccount, UiAccountData, UiAccountEncoding};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    message::{v0::Message, VersionedMessage},
    pubkey::Pubkey,
    rent::Rent,
    signature::{read_keypair_file, Keypair},
    signer::Signer as _,
    transaction::VersionedTransaction,
};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

const STANDARD_TIER_CONFIG_INDEX: usize = 2;

#[derive(Parser, Debug)]
struct Args {
    /// Write a solana-test-validator --account JSON file for local bootstrap
    #[arg(long)]
    test_validator_account_file: Option<PathBuf>,
    /// The keypair that pays for the config-policy account when initializing a live cluster
    #[arg(long)]
    payer_keypair: Option<PathBuf>,
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899 when --payer-keypair is used
    #[arg(short = 'r', long)]
    cluster_rpc: Option<String>,
    /// The first admin authority to place in admin_authorities[0]
    #[arg(long)]
    initial_admin_authority: Pubkey,
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
    /// Override the minimum bundle/auction account pairs per request
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    minimum_bundle_auction_pairs: Option<u64>,
    /// Override the maximum auction credits applied by one update
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    max_auction_credits_per_update: Option<u64>,
    /// Override the verifier count drawn for each V2 auction
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..))]
    v2_verifiers_per_auction: Option<u8>,
    /// Override the verifier quorum required for each V2 auction
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..))]
    v2_verifier_quorum: Option<u8>,
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
    initial_admin_authority: Pubkey,
    service_authority: Pubkey,
    enable_page_backed_finalize_payout: bool,
    standard_tier_window_overrides: StandardTierWindowOverrides,
    args: &Args,
) -> ConfigPolicyV2 {
    let mut policy = ConfigPolicyV2::default();
    policy.admin_authorities[0] = initial_admin_authority.to_bytes().into();
    policy.service_authorities[0] = service_authority.to_bytes().into();
    policy.policy_flags = desired_policy_flags(enable_page_backed_finalize_payout);
    if let Some(minimum_bundle_auction_pairs) = args.minimum_bundle_auction_pairs {
        policy.minimum_bundle_auction_pairs = minimum_bundle_auction_pairs;
    }
    if let Some(max_auction_credits_per_update) = args.max_auction_credits_per_update {
        policy.max_auction_credits_per_update = max_auction_credits_per_update;
    }
    if let Some(v2_verifiers_per_auction) = args.v2_verifiers_per_auction {
        policy.v2_verifiers_per_auction = v2_verifiers_per_auction;
    }
    if let Some(v2_verifier_quorum) = args.v2_verifier_quorum {
        policy.v2_verifier_quorum = v2_verifier_quorum;
    }
    apply_standard_tier_window_overrides(&mut policy, standard_tier_window_overrides);
    policy
}

fn print_policy_summary(
    policy_pda: Pubkey,
    initial_admin_authority: Pubkey,
    service_authority: Pubkey,
    minimum_bundle_auction_pairs: u64,
    max_auction_credits_per_update: u64,
    v2_verifiers_per_auction: u8,
    v2_verifier_quorum: u8,
    policy_flags: ConfigPolicyV2Flags,
    windows: StandardTierWindows,
) {
    println!("Config policy PDA: {policy_pda}");
    println!("Initial admin authority: {initial_admin_authority}");
    println!("Service authority: {service_authority}");
    println!("Policy flags: {}", policy_flags.bits());
    println!(
        "Runtime settings: minimum_bundle_auction_pairs={} max_auction_credits_per_update={} v2_verifiers_per_auction={} v2_verifier_quorum={}",
        minimum_bundle_auction_pairs,
        max_auction_credits_per_update,
        v2_verifiers_per_auction,
        v2_verifier_quorum,
    );
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

async fn initialize_config_policy(
    client: &RpcClient,
    payer: &Keypair,
    initial_admin_authority: Pubkey,
    service_authority: Pubkey,
    policy: ConfigPolicyV2,
) -> Result<(), String> {
    let config_policy_lamports = client
        .get_minimum_balance_for_rent_exemption(ConfigPolicyV2::LEN)
        .await
        .map_err(strerr)?;
    let ix = ambient_auction_client::sdk::init_config_policy_v2(
        ambient_auction_client::ID,
        payer.pubkey(),
        config_policy_lamports,
        initial_admin_authority,
        service_authority,
        policy,
    );
    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            Message::try_compile(
                &payer.pubkey(),
                &[ix],
                &[],
                client.get_latest_blockhash().await.map_err(strerr)?,
            )
            .map_err(strerr)?,
        ),
        &[payer],
    )
    .map_err(strerr)?;
    let sig = client
        .send_and_confirm_transaction_with_spinner(&tx)
        .await
        .map_err(strerr)?;

    eprintln!("Signature: {sig}");
    Ok(())
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    if args.test_validator_account_file.is_none() && args.payer_keypair.is_none() {
        return Err("pass --test-validator-account-file, --payer-keypair, or both".to_string());
    }
    let standard_tier_window_overrides = standard_tier_window_overrides(&args);
    let policy = desired_policy(
        args.initial_admin_authority,
        args.service_authority,
        args.enable_page_backed_finalize_payout,
        standard_tier_window_overrides,
        &args,
    );
    let policy_pda = ambient_auction_client::sdk::find_config_policy_v2(ambient_auction_client::ID);

    print_policy_summary(
        policy_pda,
        args.initial_admin_authority,
        args.service_authority,
        policy.minimum_bundle_auction_pairs,
        policy.max_auction_credits_per_update,
        policy.v2_verifiers_per_auction,
        policy.v2_verifier_quorum,
        policy.policy_flags,
        standard_tier_windows(&policy),
    );
    if let Some(test_validator_account_file) = args.test_validator_account_file.as_deref() {
        write_test_validator_account_file(test_validator_account_file, policy_pda, policy)?;
        println!(
            "Test validator account file: {}",
            test_validator_account_file.display()
        );
    }
    if let Some(payer_keypair) = args.payer_keypair.as_deref() {
        let payer = read_keypair_file(payer_keypair).map_err(strerr)?;
        let rpc = RpcClient::new_with_commitment(
            args.cluster_rpc.unwrap_or(CLIENT_URL.to_string()),
            CommitmentConfig::confirmed(),
        );
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(strerr)?
            .block_on(initialize_config_policy(
                &rpc,
                &payer,
                args.initial_admin_authority,
                args.service_authority,
                policy,
            ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_config_policy_v2_desired_policy_sets_primary_service_authority_and_bypass_flag() {
        let initial_admin_authority = Pubkey::new_unique();
        let service_authority = Pubkey::new_unique();
        let args = Args::parse_from([
            "init-config-policy-v2",
            "--initial-admin-authority",
            &initial_admin_authority.to_string(),
            "--service-authority",
            &service_authority.to_string(),
            "--test-validator-account-file",
            "policy.json",
        ]);
        let policy = desired_policy(
            initial_admin_authority,
            service_authority,
            false,
            StandardTierWindowOverrides::default(),
            &args,
        );

        assert_eq!(
            policy.admin_authorities[0].inner(),
            initial_admin_authority.to_bytes()
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
        let initial_admin_authority = Pubkey::new_unique();
        let service_authority = Pubkey::new_unique();
        let args = Args::parse_from([
            "init-config-policy-v2",
            "--initial-admin-authority",
            &initial_admin_authority.to_string(),
            "--service-authority",
            &service_authority.to_string(),
            "--test-validator-account-file",
            "policy.json",
        ]);
        let policy = desired_policy(
            initial_admin_authority,
            service_authority,
            true,
            StandardTierWindowOverrides::default(),
            &args,
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
        let initial_admin_authority = Pubkey::new_unique();
        let service_authority = Pubkey::new_unique();
        let args = Args::parse_from([
            "init-config-policy-v2",
            "--initial-admin-authority",
            &initial_admin_authority.to_string(),
            "--service-authority",
            &service_authority.to_string(),
            "--test-validator-account-file",
            "policy.json",
        ]);
        let policy = desired_policy(
            initial_admin_authority,
            service_authority,
            false,
            StandardTierWindowOverrides {
                settlement: Some(40),
                result: Some(120),
                verification: Some(120),
                claim: Some(5),
            },
            &args,
        );
        let windows = standard_tier_windows(&policy);

        assert_eq!(windows.settlement, 40);
        assert_eq!(windows.result, 120);
        assert_eq!(windows.verification, 120);
        assert_eq!(windows.claim, 5);
    }

    #[test]
    fn init_config_policy_v2_desired_policy_applies_runtime_overrides() {
        let initial_admin_authority = Pubkey::new_unique();
        let service_authority = Pubkey::new_unique();
        let args = Args::parse_from([
            "init-config-policy-v2",
            "--initial-admin-authority",
            &initial_admin_authority.to_string(),
            "--service-authority",
            &service_authority.to_string(),
            "--test-validator-account-file",
            "policy.json",
            "--minimum-bundle-auction-pairs",
            "3",
            "--max-auction-credits-per-update",
            "42",
            "--v2-verifiers-per-auction",
            "2",
            "--v2-verifier-quorum",
            "1",
        ]);
        let policy = desired_policy(
            initial_admin_authority,
            service_authority,
            false,
            StandardTierWindowOverrides::default(),
            &args,
        );

        assert_eq!(policy.minimum_bundle_auction_pairs, 3);
        assert_eq!(policy.max_auction_credits_per_update, 42);
        assert_eq!(policy.v2_verifiers_per_auction, 2);
        assert_eq!(policy.v2_verifier_quorum, 1);
    }
}
