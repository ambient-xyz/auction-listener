use ambient_auction_api::{
    ConfigPolicyV2, CONFIG_POLICY_V2_FLAG_ALLOW_SERVICE_PAGE_BACKED_FINALIZE_BYPASS,
    CONFIG_POLICY_V2_SERVICE_CAPACITY,
};
use ambient_auction_listener::CLIENT_URL;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::Parser;
use serde::Serialize;
use solana_account_decoder_client_types::{UiAccount, UiAccountData, UiAccountEncoding};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    message::{v0::Message, VersionedMessage},
    pubkey::Pubkey,
    rent::Rent,
    signature::{read_keypair_file, Keypair, Signature},
    signer::Signer as _,
    transaction::VersionedTransaction,
};
use std::{
    env,
    fmt::Display,
    path::{Path, PathBuf},
};

const DEFAULT_AUTHORITY_KEYPAIR: &str = "~/.config/solana/id.json";

#[derive(Parser, Debug)]
struct Args {
    /// The Solana RPC cluster URL. Defaults to http://localhost:8899
    #[arg(short = 'r', long, default_value = CLIENT_URL)]
    cluster_rpc: String,
    /// The authority keypair used to initialize or update the config policy
    #[arg(long, default_value = DEFAULT_AUTHORITY_KEYPAIR)]
    authority_keypair: String,
    /// The service signer pubkey to place in service_authorities[0]
    #[arg(long)]
    service_authority: Option<Pubkey>,
    /// Write a solana-test-validator --account JSON file for local bootstrap instead of sending the oversized instruction
    #[arg(long)]
    test_validator_account_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigPolicyAction {
    Initialized,
    Updated,
}

#[derive(Debug)]
struct ConfigPolicyOutcome {
    action: ConfigPolicyAction,
    policy_pda: Pubkey,
    signature: Signature,
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

fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        return env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(path));
    }

    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }

    PathBuf::from(path)
}

fn read_policy(account: &Account) -> Result<ConfigPolicyV2, String> {
    if account.owner != ambient_auction_client::ID {
        return Err(format!(
            "Config policy account has unexpected owner {}; expected {}",
            account.owner,
            ambient_auction_client::ID
        ));
    }

    bytemuck::try_from_bytes::<ConfigPolicyV2>(&account.data)
        .copied()
        .map_err(strerr)
}

fn has_authority(authorities: &[ambient_auction_api::Pubkey], authority: Pubkey) -> bool {
    authorities
        .iter()
        .any(|entry| entry.inner() == authority.to_bytes())
}

fn service_authorities_with_primary(
    existing: &[ambient_auction_api::Pubkey; CONFIG_POLICY_V2_SERVICE_CAPACITY],
    service_authority: Pubkey,
) -> [ambient_auction_api::Pubkey; CONFIG_POLICY_V2_SERVICE_CAPACITY] {
    let mut updated = [ambient_auction_api::Pubkey::default(); CONFIG_POLICY_V2_SERVICE_CAPACITY];
    updated[0] = service_authority.to_bytes().into();

    let mut next_index = 1;
    for existing_authority in existing {
        let existing_key = existing_authority.inner();
        if existing_key == [0; 32] || existing_key == service_authority.to_bytes() {
            continue;
        }
        if next_index >= updated.len() {
            break;
        }
        updated[next_index] = existing_key.into();
        next_index += 1;
    }

    updated
}

fn desired_policy(service_authority: Pubkey) -> ConfigPolicyV2 {
    let mut policy = ConfigPolicyV2::default();
    policy.service_authorities[0] = service_authority.to_bytes().into();
    policy.policy_flags |= CONFIG_POLICY_V2_FLAG_ALLOW_SERVICE_PAGE_BACKED_FINALIZE_BYPASS;
    policy
}

fn updated_policy(current: ConfigPolicyV2, service_authority: Pubkey) -> ConfigPolicyV2 {
    let mut policy = current;
    policy.service_authorities =
        service_authorities_with_primary(&policy.service_authorities, service_authority);
    policy.policy_flags |= CONFIG_POLICY_V2_FLAG_ALLOW_SERVICE_PAGE_BACKED_FINALIZE_BYPASS;
    policy
}

async fn submit_policy_transaction(
    client: &RpcClient,
    authority: &Keypair,
    instruction: solana_sdk::instruction::Instruction,
) -> Result<Signature, String> {
    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            Message::try_compile(
                &authority.pubkey(),
                &[instruction],
                &[],
                client.get_latest_blockhash().await.map_err(strerr)?,
            )
            .map_err(strerr)?,
        ),
        &[authority],
    )
    .map_err(strerr)?;

    client
        .send_and_confirm_transaction_with_spinner(&tx)
        .await
        .map_err(strerr)
}

async fn initialize_or_update_config_policy_v2(
    client: &RpcClient,
    authority: &Keypair,
    service_authority: Pubkey,
) -> Result<ConfigPolicyOutcome, String> {
    let policy_pda = ambient_auction_client::sdk::find_config_policy_v2();
    let existing = client
        .get_account_with_commitment(&policy_pda, CommitmentConfig::confirmed())
        .await
        .map_err(strerr)?
        .value;

    match existing {
        None => {
            let lamports = client
                .get_minimum_balance_for_rent_exemption(ConfigPolicyV2::LEN)
                .await
                .map_err(strerr)?;
            let signature = submit_policy_transaction(
                client,
                authority,
                ambient_auction_client::sdk::init_config_policy_v2(
                    authority.pubkey(),
                    lamports,
                    desired_policy(service_authority),
                ),
            )
            .await?;

            Ok(ConfigPolicyOutcome {
                action: ConfigPolicyAction::Initialized,
                policy_pda,
                signature,
            })
        }
        Some(account) => {
            let current_policy = read_policy(&account)?;
            let authority_key = authority.pubkey();
            if !has_authority(&current_policy.admin_authorities, authority_key)
                && !has_authority(&current_policy.service_authorities, authority_key)
            {
                return Err(format!(
                    "Authority {} is neither an admin nor a service authority on existing config policy {}",
                    authority_key, policy_pda
                ));
            }

            let signature = submit_policy_transaction(
                client,
                authority,
                ambient_auction_client::sdk::set_config_policy_v2(
                    authority.pubkey(),
                    updated_policy(current_policy, service_authority),
                ),
            )
            .await?;

            Ok(ConfigPolicyOutcome {
                action: ConfigPolicyAction::Updated,
                policy_pda,
                signature,
            })
        }
    }
}

async fn config_policy_rent_lamports(client: &RpcClient) -> u64 {
    match client
        .get_minimum_balance_for_rent_exemption(ConfigPolicyV2::LEN)
        .await
    {
        Ok(lamports) => lamports,
        Err(err) => {
            eprintln!(
                "Falling back to default rent calculation after RPC rent lookup failed: {err}"
            );
            Rent::default().minimum_balance(ConfigPolicyV2::LEN)
        }
    }
}

fn write_test_validator_account_file(
    path: &Path,
    policy_pda: Pubkey,
    policy: ConfigPolicyV2,
    lamports: u64,
) -> Result<(), String> {
    let encoded_policy = BASE64_STANDARD.encode(bytemuck::bytes_of(&policy));
    let account_file = TestValidatorAccountFile {
        pubkey: policy_pda.to_string(),
        account: UiAccount {
            lamports,
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

fn load_authority_keypair(path: &str) -> Result<Keypair, String> {
    read_keypair_file(expand_tilde(path)).map_err(strerr)
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    let authority = load_authority_keypair(&args.authority_keypair)?;
    let service_authority = args.service_authority.unwrap_or_else(|| authority.pubkey());
    let rpc = RpcClient::new_with_commitment(args.cluster_rpc, CommitmentConfig::confirmed());

    if let Some(account_file) = args.test_validator_account_file {
        let policy_pda = ambient_auction_client::sdk::find_config_policy_v2();
        let lamports = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(strerr)?
            .block_on(config_policy_rent_lamports(&rpc));

        write_test_validator_account_file(
            &account_file,
            policy_pda,
            desired_policy(service_authority),
            lamports,
        )?;

        println!("Config policy PDA: {policy_pda}");
        println!("Test validator account file: {}", account_file.display());
        return Ok(());
    }

    let outcome = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(strerr)?
        .block_on(initialize_or_update_config_policy_v2(
            &rpc,
            &authority,
            service_authority,
        ));

    match outcome {
        Ok(outcome) => {
            println!("Action: {:?}", outcome.action);
            println!("Config policy PDA: {}", outcome.policy_pda);
            println!("Transaction signature: {}", outcome.signature);
            Ok(())
        }
        Err(err) if err.contains("too large") => Err(format!(
            "{err}\nUse --test-validator-account-file <PATH> for local solana-test-validator bootstrap."
        )),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desired_policy_sets_primary_service_authority_and_flag() {
        let service_authority = Pubkey::new_unique();
        let policy = desired_policy(service_authority);

        assert_eq!(
            policy.service_authorities[0].inner(),
            service_authority.to_bytes()
        );
        assert_ne!(
            policy.policy_flags & CONFIG_POLICY_V2_FLAG_ALLOW_SERVICE_PAGE_BACKED_FINALIZE_BYPASS,
            0
        );
    }

    #[test]
    fn updated_policy_promotes_service_authority_without_dropping_existing_entries() {
        let existing_a = Pubkey::new_unique();
        let existing_b = Pubkey::new_unique();
        let chosen = existing_b;

        let mut current = ConfigPolicyV2::default();
        current.service_authorities[0] = existing_a.to_bytes().into();
        current.service_authorities[1] = existing_b.to_bytes().into();

        let updated = updated_policy(current, chosen);

        assert_eq!(updated.service_authorities[0].inner(), chosen.to_bytes());
        assert_eq!(
            updated.service_authorities[1].inner(),
            existing_a.to_bytes()
        );
        assert_ne!(
            updated.policy_flags & CONFIG_POLICY_V2_FLAG_ALLOW_SERVICE_PAGE_BACKED_FINALIZE_BYPASS,
            0
        );
    }
}
