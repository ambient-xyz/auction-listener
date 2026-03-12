use anyhow::{bail, Context, Result};
use reqwest::Url;
use serde_json::{json, Value};
use std::fs::{self, File};
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct RepoPaths {
    pub auction_listener_root: PathBuf,
    pub validator_root: PathBuf,
    pub agave_root: PathBuf,
}

impl RepoPaths {
    pub fn discover(
        auction_listener_root: Option<PathBuf>,
        validator_root: Option<PathBuf>,
        agave_root: Option<PathBuf>,
    ) -> Result<Self> {
        let auction_listener_root = canonicalize_existing(
            &auction_listener_root.unwrap_or_else(default_auction_listener_root),
            "AUCTION_LISTENER_ROOT",
        )?;
        let validator_root = canonicalize_existing(
            &validator_root.unwrap_or_else(|| auction_listener_root.join("../ambient")),
            "VALIDATOR_ROOT",
        )?;
        let agave_root = canonicalize_existing(
            &agave_root.unwrap_or_else(|| validator_root.join("../agave")),
            "AGAVE_ROOT",
        )?;
        Ok(Self {
            auction_listener_root,
            validator_root,
            agave_root,
        })
    }

    pub fn validator_script(&self) -> PathBuf {
        self.validator_root.join("auction/run-validator.sh")
    }

    pub fn yellowstone_plugin(&self) -> PathBuf {
        self.validator_root.join(
            "auction/yellowstone-config/yellowstone-grpc-geyser-release/lib/libyellowstone_grpc_geyser.so",
        )
    }

    pub fn payer_keypair(&self) -> PathBuf {
        self.validator_root
            .join("auction/program/target/test-ledger/faucet-keypair.json")
    }
}

pub struct PreparedCargoContext {
    pub config_path: PathBuf,
    temp_root: PathBuf,
}

impl PreparedCargoContext {
    pub fn prepare(repo_paths: &RepoPaths, run_dir: &Path) -> Result<Self> {
        require_command("cargo")?;
        require_command("git")?;
        require_command("git-lfs")?;

        let temp_root = run_dir.join("tmp");
        fs::create_dir_all(&temp_root)
            .with_context(|| format!("failed to create {}", temp_root.display()))?;

        let tokenizer_sha = resolve_tokenizer_sha(repo_paths)?;
        let tokenizer_root = temp_root.join("tokenizer");
        let cargo_config_root = temp_root.join("cargo-config");
        let config_path = cargo_config_root.join("harness-config.toml");

        if tokenizer_root.exists() {
            fs::remove_dir_all(&tokenizer_root)
                .with_context(|| format!("failed to reset {}", tokenizer_root.display()))?;
        }
        if cargo_config_root.exists() {
            fs::remove_dir_all(&cargo_config_root)
                .with_context(|| format!("failed to reset {}", cargo_config_root.display()))?;
        }
        fs::create_dir_all(&cargo_config_root)
            .with_context(|| format!("failed to create {}", cargo_config_root.display()))?;

        run_command(
            Command::new("git")
                .arg("clone")
                .arg("https://github.com/ambient-xyz/tokenizer.git")
                .arg(&tokenizer_root),
            "clone tokenizer repo",
        )?;
        run_command(
            Command::new("git")
                .arg("-C")
                .arg(&tokenizer_root)
                .arg("checkout")
                .arg(&tokenizer_sha),
            "checkout tokenizer revision",
        )?;
        run_command(
            Command::new("git")
                .arg("-C")
                .arg(&tokenizer_root)
                .arg("lfs")
                .arg("pull"),
            "hydrate tokenizer LFS assets",
        )?;

        let glm_path = find_file_named(&tokenizer_root, "glm.json").with_context(|| {
            format!("failed to find glm.json under {}", tokenizer_root.display())
        })?;
        validate_tokenizer_asset(&glm_path)?;

        fs::write(
            &config_path,
            format!(
                "[patch.\"https://github.com/ambient-xyz/tokenizer\"]\ntokenizer = {{ path = \"{}\" }}\n",
                tokenizer_root.display()
            ),
        )
        .with_context(|| format!("failed to write {}", config_path.display()))?;

        Ok(Self {
            config_path,
            temp_root,
        })
    }
}

impl Drop for PreparedCargoContext {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.temp_root);
    }
}

pub struct ValidatorStack {
    child: Child,
    pub rpc_url: String,
    pub yellowstone_url: String,
    pub payer_keypair: PathBuf,
    pub validator_log: PathBuf,
    pub init_bundles_log: PathBuf,
}

impl ValidatorStack {
    pub async fn start(
        repo_paths: &RepoPaths,
        cargo_context: &PreparedCargoContext,
        run_dir: &Path,
        rpc_url: &str,
        yellowstone_url: &str,
    ) -> Result<Self> {
        let validator_log = run_dir.join("validator.log");
        let init_bundles_log = run_dir.join("init-bundles.log");
        let payer_keypair = repo_paths.payer_keypair();

        let validator_stdout = File::create(&validator_log)
            .with_context(|| format!("failed to create {}", validator_log.display()))?;
        let validator_stderr = validator_stdout
            .try_clone()
            .with_context(|| format!("failed to clone {}", validator_log.display()))?;

        let mut child = Command::new(repo_paths.validator_script())
            .current_dir(&repo_paths.validator_root)
            .stdout(Stdio::from(validator_stdout))
            .stderr(Stdio::from(validator_stderr))
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start validator helper {}",
                    repo_paths.validator_script().display()
                )
            })?;

        wait_for_rpc(rpc_url, Duration::from_secs(120))
            .await
            .with_context(|| format!("timed out waiting for RPC at {rpc_url}"))?;
        wait_for_tcp(yellowstone_url, Duration::from_secs(120))
            .await
            .with_context(|| format!("timed out waiting for Yellowstone at {yellowstone_url}"))?;

        let init_bundles_stdout = File::create(&init_bundles_log)
            .with_context(|| format!("failed to create {}", init_bundles_log.display()))?;
        let init_bundles_stderr = init_bundles_stdout
            .try_clone()
            .with_context(|| format!("failed to clone {}", init_bundles_log.display()))?;

        let init_bundles_status = Command::new("cargo")
            .arg("--config")
            .arg(&cargo_context.config_path)
            .arg("run")
            .arg("--manifest-path")
            .arg(repo_paths.auction_listener_root.join("Cargo.toml"))
            .arg("--bin")
            .arg("init-bundles")
            .arg("--")
            .arg("-r")
            .arg(rpc_url)
            .arg(&payer_keypair)
            .env("CARGO_TARGET_DIR", run_dir.join("init-bundles-target"))
            .stdout(Stdio::from(init_bundles_stdout))
            .stderr(Stdio::from(init_bundles_stderr))
            .status()
            .context("failed to run init-bundles")?;

        if !init_bundles_status.success() {
            let _ = stop_child(&mut child);
            bail!("init-bundles failed; see {}", init_bundles_log.display());
        }

        Ok(Self {
            child,
            rpc_url: rpc_url.to_string(),
            yellowstone_url: yellowstone_url.to_string(),
            payer_keypair,
            validator_log,
            init_bundles_log,
        })
    }

    pub fn stop(&mut self) -> Result<()> {
        stop_child(&mut self.child)
    }
}

impl Drop for ValidatorStack {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub fn default_auction_listener_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn default_run_dir(root: &Path, prefix: &str) -> PathBuf {
    root.join("target").join(prefix).join(timestamp_string())
}

pub fn preflight_linux(repo_paths: &RepoPaths) -> Result<()> {
    if std::env::consts::OS != "linux" {
        bail!("the local validator harness is supported only on Linux");
    }

    require_command("cargo")?;
    require_command("git")?;
    require_command("git-lfs")?;
    require_command("solana")?;

    let solana_status = Command::new("solana")
        .arg("address")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to run `solana address`")?;
    if !solana_status.success() {
        bail!("unable to resolve a default Solana signer via `solana address`");
    }

    if !repo_paths
        .agave_root
        .join("sdk/program/Cargo.toml")
        .is_file()
    {
        bail!(
            "expected Agave source tree at {}",
            repo_paths
                .agave_root
                .join("sdk/program/Cargo.toml")
                .display()
        );
    }

    if !repo_paths.validator_script().is_file() {
        bail!(
            "missing validator helper at {}",
            repo_paths.validator_script().display()
        );
    }

    if !repo_paths.yellowstone_plugin().is_file() {
        bail!(
            "missing Yellowstone plugin at {}",
            repo_paths.yellowstone_plugin().display()
        );
    }

    if !repo_paths.payer_keypair().is_file() {
        bail!(
            "missing payer keypair at {}",
            repo_paths.payer_keypair().display()
        );
    }

    Ok(())
}

fn canonicalize_existing(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.exists() {
        bail!("{label} does not exist at {}", path.display());
    }
    path.canonicalize()
        .with_context(|| format!("failed to canonicalize {}", path.display()))
}

fn find_file_named(root: &Path, filename: &str) -> Result<PathBuf> {
    for entry in fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))? {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", root.display()))?;
        let path = entry.path();
        if path.is_file() && path.file_name().and_then(|name| name.to_str()) == Some(filename) {
            return Ok(path);
        }
        if path.is_dir() {
            if let Ok(found) = find_file_named(&path, filename) {
                return Ok(found);
            }
        }
    }
    bail!("unable to find {filename} under {}", root.display())
}

fn require_command(name: &str) -> Result<()> {
    let status = Command::new("which")
        .arg(name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("failed to look up `{name}`"))?;
    if !status.success() {
        bail!("required command `{name}` is not available on PATH");
    }
    Ok(())
}

fn resolve_tokenizer_sha(repo_paths: &RepoPaths) -> Result<String> {
    let output = Command::new("cargo")
        .arg("metadata")
        .arg("--manifest-path")
        .arg(repo_paths.auction_listener_root.join("Cargo.toml"))
        .arg("--format-version")
        .arg("1")
        .output()
        .context("failed to run cargo metadata")?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let metadata: Value =
        serde_json::from_slice(&output.stdout).context("failed to parse cargo metadata JSON")?;
    let packages = metadata["packages"]
        .as_array()
        .context("cargo metadata did not include packages array")?;

    for package in packages {
        let Some(name) = package["name"].as_str() else {
            continue;
        };
        if name != "tokenizer" {
            continue;
        }
        let Some(source) = package["source"].as_str() else {
            continue;
        };
        if !source.starts_with("git+https://github.com/ambient-xyz/tokenizer") {
            continue;
        }
        let Some((_, sha)) = source.rsplit_once('#') else {
            bail!("tokenizer source did not include a git revision: {source}");
        };
        return Ok(sha.to_string());
    }

    bail!("unable to resolve tokenizer git source from cargo metadata")
}

fn run_command(command: &mut Command, context: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("failed to {context}"))?;
    if !status.success() {
        bail!("failed to {context}: exit status {status}");
    }
    Ok(())
}

fn stop_child(child: &mut Child) -> Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

fn timestamp_string() -> String {
    if let Ok(output) = Command::new("date").arg("+%Y%m%dT%H%M%S").output() {
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
        .to_string()
}

fn validate_tokenizer_asset(path: &Path) -> Result<()> {
    let mut prefix = String::new();
    File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?
        .take(128)
        .read_to_string(&mut prefix)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if prefix.starts_with("version https://git-lfs.github.com/spec") {
        bail!("{} is still a Git LFS pointer", path.display());
    }

    let full_text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str::<Value>(&full_text)
        .with_context(|| format!("{} is not valid JSON", path.display()))?;
    Ok(())
}

async fn wait_for_rpc(rpc_url: &str, timeout_duration: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + timeout_duration;

    while tokio::time::Instant::now() < deadline {
        let response = client
            .post(rpc_url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"getSlot"}))
            .send()
            .await;
        if matches!(response, Ok(ref resp) if resp.status().is_success()) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    bail!("RPC endpoint did not become ready in time")
}

async fn wait_for_tcp(endpoint: &str, timeout_duration: Duration) -> Result<()> {
    let parsed = Url::parse(endpoint).with_context(|| format!("invalid URL `{endpoint}`"))?;
    let host = parsed.host_str().context("Yellowstone URL missing host")?;
    let port = parsed
        .port_or_known_default()
        .context("Yellowstone URL missing port")?;
    let socket_addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .with_context(|| format!("failed to parse {host}:{port}"))?;

    let deadline = tokio::time::Instant::now() + timeout_duration;
    while tokio::time::Instant::now() < deadline {
        if TcpStream::connect_timeout(&socket_addr, Duration::from_millis(250)).is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    bail!("Yellowstone gRPC endpoint did not become ready in time")
}
