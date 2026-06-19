use ambient_auction_api::{
    ConfigPolicyV2, ConfigPolicyV2Flag, ConfigPolicyV2Flags, Pubkey as ApiPubkey, RequestTier,
    RequestTierConfigV2, CONFIG_POLICY_V2_ADMIN_CAPACITY, CONFIG_POLICY_V2_SERVICE_CAPACITY,
};
use anyhow::{anyhow, bail, Result};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::Instruction,
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
    /// Admin keypair used as both config authority and fee payer
    #[arg(long)]
    authority_keypair: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Patch config policy flags
    Flags(FlagsArgs),
    /// Replace one admin authority slot
    AdminAuthority(AdminAuthorityArgs),
    /// Replace one service authority slot
    ServiceAuthority(ServiceAuthorityArgs),
    /// Patch V2 verifier settings
    VerifierSettings(VerifierSettingsArgs),
    /// Patch max auction credits per update
    MaxAuctionCreditsPerUpdate(MaxAuctionCreditsArgs),
    /// Patch V2 verification dispute settings
    DisputeSettings(DisputeSettingsArgs),
    /// Patch one request tier config, preserving unspecified fields
    TierConfig(TierConfigArgs),
}

#[derive(ClapArgs, Debug)]
struct FlagsArgs {
    /// Replace the whole flag bitset
    #[arg(long, conflicts_with_all = ["enable", "disable"])]
    set_bits: Option<u64>,
    /// Enable a named flag
    #[arg(long, value_enum)]
    enable: Vec<PolicyFlagArg>,
    /// Disable a named flag
    #[arg(long, value_enum)]
    disable: Vec<PolicyFlagArg>,
}

#[derive(ClapArgs, Debug)]
struct AdminAuthorityArgs {
    /// Admin authority slot index
    #[arg(long, value_parser = clap::value_parser!(u8).range(0..CONFIG_POLICY_V2_ADMIN_CAPACITY as i64))]
    index: u8,
    /// Replacement admin authority pubkey
    #[arg(long)]
    pubkey: Pubkey,
}

#[derive(ClapArgs, Debug)]
struct ServiceAuthorityArgs {
    /// Service authority slot index
    #[arg(long, value_parser = clap::value_parser!(u8).range(0..CONFIG_POLICY_V2_SERVICE_CAPACITY as i64))]
    index: u8,
    /// Replacement service authority pubkey
    #[arg(long)]
    pubkey: Pubkey,
}

#[derive(ClapArgs, Debug)]
struct VerifierSettingsArgs {
    /// Number of verifiers drawn for each V2 auction
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..))]
    v2_verifiers_per_auction: u8,
    /// Verifier quorum required for each V2 auction
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..))]
    v2_verifier_quorum: u8,
}

#[derive(ClapArgs, Debug)]
struct MaxAuctionCreditsArgs {
    /// Maximum auction credits that one update can apply
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    value: u64,
}

#[derive(ClapArgs, Debug)]
struct DisputeSettingsArgs {
    /// Slots after verification deadline when a missed verification can be disputed
    #[arg(long)]
    missed_verification_dispute_window_slots: u64,
    /// Slots replacement verifiers have to resolve an opened dispute
    #[arg(long)]
    dispute_verification_window_slots: u64,
    /// Slots after provisional finalize when a paid verdict dispute can be opened
    #[arg(long)]
    paid_verification_dispute_window_slots: u64,
    /// Lamports paid to open a paid verdict dispute
    #[arg(long)]
    paid_verification_dispute_bond_lamports: u64,
}

#[derive(ClapArgs, Debug)]
struct TierConfigArgs {
    /// Request tier to patch
    #[arg(long, value_enum)]
    tier: TierArg,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    bid_reveal_duration: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    active_auction_duration: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    bundle_duration: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    requests_per_bundle: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    max_context_length_tokens: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    job_submission_duration_slots: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    bid_commitment_amount_multiplier: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    auction_credits_multiplier: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    settlement_window_slots: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    result_window_slots: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    verification_window_slots: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    claim_window_slots: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum PolicyFlagArg {
    #[value(name = "open-escrow-args-bypass")]
    OpenEscrowArgsBypass,
    #[value(name = "commit-override")]
    CommitOverride,
    #[value(name = "result-post-override")]
    ResultPostOverride,
    #[value(name = "finalize-override")]
    FinalizeOverride,
    #[value(name = "page-backed-finalize-bypass")]
    PageBackedFinalizeBypass,
    #[value(name = "page-backed-finalize-payout")]
    PageBackedFinalizePayout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TierArg {
    Eco,
    Small,
    Standard,
    Pro,
    Large,
}

struct PatchPlan {
    kind: &'static str,
    before: String,
    after: String,
    instruction: Option<Instruction>,
}

fn flags_from_bits(bits: u64) -> ConfigPolicyV2Flags {
    bytemuck::cast(bits)
}

fn api_pubkey_to_solana(pubkey: ApiPubkey) -> Pubkey {
    Pubkey::new_from_array(pubkey.inner())
}

fn flag_from_arg(flag: PolicyFlagArg) -> ConfigPolicyV2Flag {
    match flag {
        PolicyFlagArg::OpenEscrowArgsBypass => ConfigPolicyV2Flag::AllowServiceOpenEscrowArgsBypass,
        PolicyFlagArg::CommitOverride => ConfigPolicyV2Flag::AllowServiceCommitOverride,
        PolicyFlagArg::ResultPostOverride => ConfigPolicyV2Flag::AllowServiceResultPostOverride,
        PolicyFlagArg::FinalizeOverride => ConfigPolicyV2Flag::AllowServiceFinalizeOverride,
        PolicyFlagArg::PageBackedFinalizeBypass => {
            ConfigPolicyV2Flag::AllowServicePageBackedFinalizeBypass
        }
        PolicyFlagArg::PageBackedFinalizePayout => {
            ConfigPolicyV2Flag::AllowServicePageBackedFinalizePayout
        }
    }
}

fn request_tier_from_arg(tier: TierArg) -> RequestTier {
    match tier {
        TierArg::Eco => RequestTier::Eco,
        TierArg::Small => RequestTier::Small,
        TierArg::Standard => RequestTier::Standard,
        TierArg::Pro => RequestTier::Pro,
        TierArg::Large => RequestTier::Large,
    }
}

fn request_tier_config_index(tier: RequestTier) -> usize {
    match tier {
        RequestTier::Eco => 0,
        RequestTier::Small => 1,
        RequestTier::Standard => 2,
        RequestTier::Pro => 3,
        RequestTier::Large => 4,
    }
}

fn is_admin(policy: &ConfigPolicyV2, authority: Pubkey) -> bool {
    policy
        .admin_authorities
        .iter()
        .any(|admin| admin.inner() == authority.to_bytes())
}

fn ensure_admin(policy: &ConfigPolicyV2, authority: Pubkey) -> Result<()> {
    if is_admin(policy, authority) {
        Ok(())
    } else {
        bail!("{authority} is not in config policy admin_authorities")
    }
}

fn desired_flags(args: &FlagsArgs, current: ConfigPolicyV2Flags) -> Result<ConfigPolicyV2Flags> {
    if let Some(bits) = args.set_bits {
        return Ok(flags_from_bits(bits));
    }
    if args.enable.is_empty() && args.disable.is_empty() {
        bail!("pass --set-bits or at least one --enable/--disable flag");
    }
    if args
        .enable
        .iter()
        .any(|enabled| args.disable.contains(enabled))
    {
        bail!("the same flag cannot be both enabled and disabled");
    }

    let mut bits = current.bits();
    for flag in &args.enable {
        bits |= ConfigPolicyV2Flags::from_flag(flag_from_arg(*flag)).bits();
    }
    for flag in &args.disable {
        bits &= !ConfigPolicyV2Flags::from_flag(flag_from_arg(*flag)).bits();
    }
    Ok(flags_from_bits(bits))
}

fn apply_tier_overrides(
    mut config: RequestTierConfigV2,
    args: &TierConfigArgs,
) -> RequestTierConfigV2 {
    if let Some(value) = args.bid_reveal_duration {
        config.bid_reveal_duration = value;
    }
    if let Some(value) = args.active_auction_duration {
        config.active_auction_duration = value;
    }
    if let Some(value) = args.bundle_duration {
        config.bundle_duration = value;
    }
    if let Some(value) = args.requests_per_bundle {
        config.requests_per_bundle = value;
    }
    if let Some(value) = args.max_context_length_tokens {
        config.max_context_length_tokens = value;
    }
    if let Some(value) = args.job_submission_duration_slots {
        config.job_submission_duration_slots = value;
    }
    if let Some(value) = args.bid_commitment_amount_multiplier {
        config.bid_commitment_amount_multiplier = value;
    }
    if let Some(value) = args.auction_credits_multiplier {
        config.auction_credits_multiplier = value;
    }
    if let Some(value) = args.settlement_window_slots {
        config.settlement_window_slots = value;
    }
    if let Some(value) = args.result_window_slots {
        config.result_window_slots = value;
    }
    if let Some(value) = args.verification_window_slots {
        config.verification_window_slots = value;
    }
    if let Some(value) = args.claim_window_slots {
        config.claim_window_slots = value;
    }
    config
}

fn tier_config_summary(config: &RequestTierConfigV2) -> String {
    format!(
        "bid_reveal_duration={} active_auction_duration={} bundle_duration={} requests_per_bundle={} max_context_length_tokens={} job_submission_duration_slots={} bid_commitment_amount_multiplier={} auction_credits_multiplier={} settlement_window_slots={} result_window_slots={} verification_window_slots={} claim_window_slots={}",
        config.bid_reveal_duration,
        config.active_auction_duration,
        config.bundle_duration,
        config.requests_per_bundle,
        config.max_context_length_tokens,
        config.job_submission_duration_slots,
        config.bid_commitment_amount_multiplier,
        config.auction_credits_multiplier,
        config.settlement_window_slots,
        config.result_window_slots,
        config.verification_window_slots,
        config.claim_window_slots,
    )
}

fn build_patch_plan(
    command: &Command,
    policy: &ConfigPolicyV2,
    authority: Pubkey,
) -> Result<PatchPlan> {
    let program_id = ambient_auction_client::ID;
    match command {
        Command::Flags(args) => {
            let before = policy.policy_flags;
            let after = desired_flags(args, before)?;
            Ok(PatchPlan {
                kind: "flags",
                before: format!("policy_flags={}", before.bits()),
                after: format!("policy_flags={}", after.bits()),
                instruction: (before != after).then(|| {
                    ambient_auction_client::sdk::set_config_policy_v2_flags(
                        program_id, authority, after,
                    )
                }),
            })
        }
        Command::AdminAuthority(args) => {
            let before = api_pubkey_to_solana(policy.admin_authorities[usize::from(args.index)]);
            Ok(PatchPlan {
                kind: "admin-authority",
                before: format!("admin_authorities[{}]={before}", args.index),
                after: format!("admin_authorities[{}]={}", args.index, args.pubkey),
                instruction: (before != args.pubkey).then(|| {
                    ambient_auction_client::sdk::set_config_policy_v2_admin_authority(
                        program_id,
                        authority,
                        args.index,
                        args.pubkey,
                    )
                }),
            })
        }
        Command::ServiceAuthority(args) => {
            let before = api_pubkey_to_solana(policy.service_authorities[usize::from(args.index)]);
            Ok(PatchPlan {
                kind: "service-authority",
                before: format!("service_authorities[{}]={before}", args.index),
                after: format!("service_authorities[{}]={}", args.index, args.pubkey),
                instruction: (before != args.pubkey).then(|| {
                    ambient_auction_client::sdk::set_config_policy_v2_service_authority(
                        program_id,
                        authority,
                        args.index,
                        args.pubkey,
                    )
                }),
            })
        }
        Command::VerifierSettings(args) => {
            let changed = policy.v2_verifiers_per_auction != args.v2_verifiers_per_auction
                || policy.v2_verifier_quorum != args.v2_verifier_quorum;
            Ok(PatchPlan {
                kind: "verifier-settings",
                before: format!(
                    "v2_verifiers_per_auction={} v2_verifier_quorum={}",
                    policy.v2_verifiers_per_auction, policy.v2_verifier_quorum
                ),
                after: format!(
                    "v2_verifiers_per_auction={} v2_verifier_quorum={}",
                    args.v2_verifiers_per_auction, args.v2_verifier_quorum
                ),
                instruction: changed.then(|| {
                    ambient_auction_client::sdk::set_config_policy_v2_verifier_settings(
                        program_id,
                        authority,
                        args.v2_verifiers_per_auction,
                        args.v2_verifier_quorum,
                    )
                }),
            })
        }
        Command::MaxAuctionCreditsPerUpdate(args) => {
            let before = policy.max_auction_credits_per_update;
            Ok(PatchPlan {
                kind: "max-auction-credits-per-update",
                before: format!("max_auction_credits_per_update={before}"),
                after: format!("max_auction_credits_per_update={}", args.value),
                instruction: (before != args.value).then(|| {
                    ambient_auction_client::sdk::set_config_policy_v2_max_auction_credits_per_update(
                        program_id,
                        authority,
                        args.value,
                    )
                }),
            })
        }
        Command::DisputeSettings(args) => {
            let changed = policy.missed_verification_dispute_window_slots
                != args.missed_verification_dispute_window_slots
                || policy.dispute_verification_window_slots
                    != args.dispute_verification_window_slots
                || policy.paid_verification_dispute_window_slots
                    != args.paid_verification_dispute_window_slots
                || policy.paid_verification_dispute_bond_lamports
                    != args.paid_verification_dispute_bond_lamports;
            Ok(PatchPlan {
                kind: "dispute-settings",
                before: format!(
                    "missed_verification_dispute_window_slots={} dispute_verification_window_slots={} paid_verification_dispute_window_slots={} paid_verification_dispute_bond_lamports={}",
                    policy.missed_verification_dispute_window_slots,
                    policy.dispute_verification_window_slots,
                    policy.paid_verification_dispute_window_slots,
                    policy.paid_verification_dispute_bond_lamports
                ),
                after: format!(
                    "missed_verification_dispute_window_slots={} dispute_verification_window_slots={} paid_verification_dispute_window_slots={} paid_verification_dispute_bond_lamports={}",
                    args.missed_verification_dispute_window_slots,
                    args.dispute_verification_window_slots,
                    args.paid_verification_dispute_window_slots,
                    args.paid_verification_dispute_bond_lamports
                ),
                instruction: changed.then(|| {
                    ambient_auction_client::sdk::set_config_policy_v2_dispute_settings(
                        program_id,
                        authority,
                        args.missed_verification_dispute_window_slots,
                        args.dispute_verification_window_slots,
                        args.paid_verification_dispute_window_slots,
                        args.paid_verification_dispute_bond_lamports,
                    )
                }),
            })
        }
        Command::TierConfig(args) => {
            let tier = request_tier_from_arg(args.tier);
            let before = policy.tier_configs[request_tier_config_index(tier)];
            let after = apply_tier_overrides(before, args);
            Ok(PatchPlan {
                kind: "tier-config",
                before: tier_config_summary(&before),
                after: tier_config_summary(&after),
                instruction: (before != after).then(|| {
                    ambient_auction_client::sdk::set_config_policy_v2_tier_config(
                        program_id, authority, tier, after,
                    )
                }),
            })
        }
    }
}

async fn fetch_config_policy(client: &RpcClient, policy_pda: Pubkey) -> Result<ConfigPolicyV2> {
    let account = client
        .get_account(&policy_pda)
        .await
        .map_err(|err| anyhow!("failed to fetch config policy {policy_pda}: {err}"))?;
    if account.owner != ambient_auction_client::ID {
        bail!(
            "config policy owner mismatch: expected {}, got {}",
            ambient_auction_client::ID,
            account.owner
        );
    }
    if account.data.len() != ConfigPolicyV2::LEN {
        bail!(
            "config policy size mismatch: expected {}, got {}",
            ConfigPolicyV2::LEN,
            account.data.len()
        );
    }
    bytemuck::try_pod_read_unaligned::<ConfigPolicyV2>(&account.data)
        .map_err(|err| anyhow!("failed to decode ConfigPolicyV2: {err}"))
}

async fn send_patch(
    client: &RpcClient,
    authority: &solana_sdk::signature::Keypair,
    ix: Instruction,
) -> Result<()> {
    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(Message::try_compile(
            &authority.pubkey(),
            &[ix],
            &[],
            client.get_latest_blockhash().await?,
        )?),
        &[authority],
    )?;
    let sig = client
        .send_and_confirm_transaction_with_spinner(&tx)
        .await?;
    eprintln!("Signature: {sig}");
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let authority = read_keypair_file(&cli.authority_keypair)
        .map_err(|err| anyhow!("failed to read authority keypair: {err}"))?;
    let authority_pubkey = authority.pubkey();
    let policy_pda = ambient_auction_client::sdk::find_config_policy_v2(ambient_auction_client::ID);
    let client = RpcClient::new_with_commitment(cli.cluster_rpc, CommitmentConfig::confirmed());
    let policy = fetch_config_policy(&client, policy_pda).await?;

    ensure_admin(&policy, authority_pubkey)?;
    let patch = build_patch_plan(&cli.command, &policy, authority_pubkey)?;

    println!("Config policy PDA: {policy_pda}");
    println!("Authority: {authority_pubkey}");
    println!("Patch: {}", patch.kind);
    println!("Before: {}", patch.before);
    println!("After: {}", patch.after);

    if let Some(ix) = patch.instruction {
        send_patch(&client, &authority, ix).await?;
    } else {
        println!("No-op: requested value already matches current policy");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ambient_auction_api::{ConfigPolicyV2PatchKind, SetConfigPolicyV2Args};

    fn args_for(command: Command) -> Cli {
        Cli {
            cluster_rpc: DEFAULT_RPC_URL.to_string(),
            authority_keypair: "authority.json".into(),
            command,
        }
    }

    #[test]
    fn set_config_policy_v2_flags_enable_disable_preserves_unrelated_bits() {
        let current = flags_from_bits(
            ConfigPolicyV2Flag::AllowServiceCommitOverride.mask().bits()
                | ConfigPolicyV2Flag::AllowServiceFinalizeOverride
                    .mask()
                    .bits(),
        );
        let args = FlagsArgs {
            set_bits: None,
            enable: vec![PolicyFlagArg::PageBackedFinalizePayout],
            disable: vec![PolicyFlagArg::FinalizeOverride],
        };
        let flags = desired_flags(&args, current).unwrap();

        assert!(flags.contains(ConfigPolicyV2Flag::AllowServiceCommitOverride));
        assert!(flags.contains(ConfigPolicyV2Flag::AllowServicePageBackedFinalizePayout));
        assert!(!flags.contains(ConfigPolicyV2Flag::AllowServiceFinalizeOverride));
    }

    #[test]
    fn set_config_policy_v2_flags_set_bits_replaces_flags() {
        let args = FlagsArgs {
            set_bits: Some(48),
            enable: Vec::new(),
            disable: Vec::new(),
        };

        assert_eq!(
            desired_flags(&args, ConfigPolicyV2Flags::empty())
                .unwrap()
                .bits(),
            48
        );
    }

    #[test]
    fn set_config_policy_v2_tier_overrides_preserve_unspecified_fields() {
        let current = RequestTierConfigV2::production_default_for_tier(RequestTier::Standard);
        let args = TierConfigArgs {
            tier: TierArg::Standard,
            bid_reveal_duration: None,
            active_auction_duration: None,
            bundle_duration: None,
            requests_per_bundle: None,
            max_context_length_tokens: None,
            job_submission_duration_slots: None,
            bid_commitment_amount_multiplier: None,
            auction_credits_multiplier: None,
            settlement_window_slots: Some(40),
            result_window_slots: None,
            verification_window_slots: None,
            claim_window_slots: Some(8),
        };
        let patched = apply_tier_overrides(current, &args);

        assert_eq!(patched.settlement_window_slots, 40);
        assert_eq!(patched.claim_window_slots, 8);
        assert_eq!(patched.result_window_slots, current.result_window_slots);
        assert_eq!(
            patched.max_context_length_tokens,
            current.max_context_length_tokens
        );
    }

    #[test]
    fn set_config_policy_v2_invalid_authority_indexes_are_rejected() {
        let replacement = Pubkey::new_unique().to_string();

        assert!(Cli::try_parse_from([
            "set-config-policy-v2",
            "--authority-keypair",
            "authority.json",
            "admin-authority",
            "--index",
            "8",
            "--pubkey",
            &replacement,
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "set-config-policy-v2",
            "--authority-keypair",
            "authority.json",
            "service-authority",
            "--index",
            "16",
            "--pubkey",
            &replacement,
        ])
        .is_err());
    }

    #[test]
    fn set_config_policy_v2_non_admin_signer_preflight_is_rejected() {
        let policy = ConfigPolicyV2::production_default();

        assert!(ensure_admin(&policy, Pubkey::new_unique()).is_err());
    }

    #[test]
    fn set_config_policy_v2_noop_patch_skips_instruction() {
        let mut policy = ConfigPolicyV2::production_default();
        policy.policy_flags = flags_from_bits(48);
        let cli = args_for(Command::Flags(FlagsArgs {
            set_bits: Some(48),
            enable: Vec::new(),
            disable: Vec::new(),
        }));
        let plan = build_patch_plan(&cli.command, &policy, Pubkey::new_unique()).unwrap();

        assert!(plan.instruction.is_none());
    }

    #[test]
    fn set_config_policy_v2_dispute_settings_builds_patch() {
        let policy = ConfigPolicyV2::production_default();
        let cli = args_for(Command::DisputeSettings(DisputeSettingsArgs {
            missed_verification_dispute_window_slots: 5,
            dispute_verification_window_slots: 7,
            paid_verification_dispute_window_slots: 11,
            paid_verification_dispute_bond_lamports: 13,
        }));
        let plan = build_patch_plan(&cli.command, &policy, Pubkey::new_unique()).unwrap();

        assert_eq!(plan.kind, "dispute-settings");
        let instruction = plan.instruction.unwrap();
        let args = SetConfigPolicyV2Args::try_from(&instruction.data[1..]).unwrap();
        assert_eq!(args.patch_kind, ConfigPolicyV2PatchKind::DISPUTE_SETTINGS);
        assert_eq!(args.missed_verification_dispute_window_slots, 5);
        assert_eq!(args.dispute_verification_window_slots, 7);
        assert_eq!(args.paid_verification_dispute_window_slots, 11);
        assert_eq!(args.paid_verification_dispute_bond_lamports, 13);
    }

    #[test]
    fn set_config_policy_v2_dispute_settings_noop_skips_instruction() {
        let mut policy = ConfigPolicyV2::production_default();
        policy.missed_verification_dispute_window_slots = 5;
        policy.dispute_verification_window_slots = 7;
        policy.paid_verification_dispute_window_slots = 11;
        policy.paid_verification_dispute_bond_lamports = 13;
        let cli = args_for(Command::DisputeSettings(DisputeSettingsArgs {
            missed_verification_dispute_window_slots: 5,
            dispute_verification_window_slots: 7,
            paid_verification_dispute_window_slots: 11,
            paid_verification_dispute_bond_lamports: 13,
        }));
        let plan = build_patch_plan(&cli.command, &policy, Pubkey::new_unique()).unwrap();

        assert!(plan.instruction.is_none());
    }
}
