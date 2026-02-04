# auction-listener

Rust tooling for observing and reacting to **Ambient Auction** on-chain activity on Solana, backed by **Yellowstone gRPC Geyser**.

This crate provides:
- a production-oriented `AuctionClient` combining **Solana JSON RPC** + **Yellowstone gRPC** subscriptions,
- fast, typed decoding of program account updates via `bytemuck::Pod`,
- end-to-end flows to **submit a job**, **watch the auction**, and optionally **stream inference output** from the winning bidder.

---

## Contents

- [Repository layout](#repository-layout)
- [Defaults](#defaults)
- [Prerequisites](#prerequisites)
- [Build and test](#build-and-test)
- [Workspace assumptions](#workspace-assumptions)
- [Configuration](#configuration)
- [Core concepts](#core-concepts)
- [CLI tools](#cli-tools)
  - [`watch-auction`](#watch-auction)
  - [`run-auction`](#run-auction)
- [Using as a library](#using-as-a-library)
- [Observability](#observability)
- [Operational notes](#operational-notes)
---

## Repository layout

- `src/lib.rs` — crate entrypoint + default constants
- `src/listener.rs` — `AuctionClient` (RPC + gRPC), cache warming, wait primitives, job submit flow
- `src/run.rs` — request/response models, sync + streaming inference helpers, encryption helpers
- `src/yellowstone_grpc.rs` — Yellowstone client wrapper + account decode helpers
- `src/error.rs` — crate-level error types
- `src/bin/*` — CLI utilities (e.g. `watch-auction`, `run-auction`, etc.)
- `yellowstone-config/` — local geyser plugin artifact/config (optional)

---

## Defaults

From `src/lib.rs`:

- `CLIENT_URL`: `http://localhost:8899` (Solana JSON RPC)
- `YELLOWSTONE_URL`: `http://localhost:10000` (Yellowstone gRPC)
- `ID`: Ambient Auction program id (`ambient_auction_api::ID`)

---

## Prerequisites

- Rust stable toolchain (edition 2021)
- Solana JSON RPC endpoint (local validator or remote RPC)
- Yellowstone gRPC endpoint (local geyser-enabled validator or hosted geyser)

Typical local setup:
- Solana JSON RPC: `http://localhost:8899`
- Yellowstone gRPC: `http://localhost:10000`

---

## Build and test

```bash
cargo build
cargo test
````

---

## Workspace assumptions

This crate depends on local path crates:

* `../auction-api` (`ambient-auction-api`)
* `../auction-client` (`ambient-auction-client`)
* `../tokenizer` (`tokenizer`)

It is expected to live inside the Ambient/Auction workspace (monorepo or multi-repo checkout) with these paths available.

---

## Configuration

### Endpoints

**Library usage:** endpoints are passed explicitly into `AuctionClient::new(...)`.

**CLI usage:** some binaries provide flags, others read environment variables.

* `run-auction` uses flags:

  * `--cluster-rpc` (default: `CLIENT_URL`)
  * `--yellowstone-url` (default: `YELLOWSTONE_URL`)
* `watch-auction` uses the environment variable:

  * `YELLOWSTONE_URL` (default: `http://localhost:10000`)

### Inference authorization (optional)

When forwarding inference requests to a winning bidder’s OpenAI-compatible endpoint, the library will set:

* `Authorization: Bearer $INFERENCE_TOKEN`

if `INFERENCE_TOKEN` is present:

```bash
export INFERENCE_TOKEN="..."
```

---

## Core concepts

### Yellowstone streaming + typed decoding

Yellowstone returns a multiplexed stream of updates. This crate subscribes with `SubscribeRequest` filters and decodes account bytes into `bytemuck::Pod` structs (e.g. `Auction`, `Bid`, `JobRequest`, `RequestBundle`, `BundleRegistry`).

Key decode helpers (`src/yellowstone_grpc.rs`):

* `decode_account_update<A: Pod>(SubscribeUpdate) -> Result<Option<(Pubkey, A)>, YellowstoneGrpcError>`
* `decode_account_info<A: Pod>(SubscribeUpdateAccountInfo) -> Result<(Pubkey, A), YellowstoneGrpcError>`

### Waiting on-chain state transitions

`AuctionClient::wait_for_account_condition` is the primary primitive:

* subscribes to account updates for a specific pubkey (Yellowstone),
* fetches the latest state after subscribing,
* repeatedly evaluates an async predicate until it returns `Some(T)` or errors.

Used by:

* `wait_for_bundle_to_be_auctioned`
* `wait_for_auction_winner` (waits for `AuctionStatus::Ended` and returns the `Auction` account)
* `wait_for_job_verification`
* `wait_for_bundle_to_be_done`

### Bundle registry cache

`AuctionClient::keep_cache_warm()` subscribes to `BundleRegistry` accounts and maintains a `papaya::HashMap` cache keyed by registry pubkey. This is used to quickly resolve the current `latest_bundle` for a given tier combination.

### Watching bids (layout-sensitive filter)

`watch_bids` subscribes to `Bid` accounts using:

* a datasize filter (`Bid::LEN`)
* a memcmp filter at a fixed offset (`offset: 32`) to match a specific `auction_id`

This offset assumes the current `Bid` account layout. If the on-chain struct layout changes (field reordering), the offset must be updated.

---

## CLI tools

List binaries:

```bash
ls src/bin
```

Run help:

```bash
cargo run --bin <name> -- --help
```

### watch-auction

`watch-auction` listens for **Auction account updates** via Yellowstone gRPC and prints decoded `Auction` states.

Configuration:

* `YELLOWSTONE_URL` environment variable (defaults to `http://localhost:10000`)

Run:

```bash
export YELLOWSTONE_URL="http://localhost:10000"
cargo run --bin watch-auction
```

Behavior:

* builds a Yellowstone gRPC client,
* subscribes to account updates filtered by `Auction::LEN` and program owner `ambient_auction_listener::ID`,
* prints `(pubkey, Auction)` for each decoded update.

### run-auction

`run-auction` is an end-to-end CLI that:

1. submits a job request on-chain (paid by a local keypair),
2. waits for the auction to end and reads the on-chain winning bid,
3. forwards the inference request to the winning bidder’s OpenAI-compatible endpoint (`/v1/chat/completions`) using SSE streaming,
4. prints model reasoning/output in a `<think>...</think>` style stream,
5. optionally spawns a background task to reclaim the job request after verification.

Invocation style:

```bash
cargo run --release --bin run-auction -- <PAYER_KEYPAIR_PATH> <MAX_PRICE_LAMPORTS> < prompt.txt
```

Key flags (from `src/bin/run-auction.rs`):

* `-r, --cluster-rpc <URL>`: Solana RPC URL (default: `http://localhost:8899`)
* `-y, --yellowstone-url <URL>`: Yellowstone gRPC URL (default: `http://localhost:10000`)
* `-b, --additional-bundles <N>`: extra bundles to try if initial bundle is saturated
* `-m, --model <MODEL>`: model name forwarded to inference (dev only)
* `-t, --temperature <FLOAT>`
* `-M, --max-completion-tokens <N>`
* `-D, --duration-tier <eco|standard|pro>` (default: `standard`)
* `-C, --context-tier <eco|standard|pro>` (optional override; otherwise derived via tokenization)
* `-d, --input-data-account-key <PUBKEY>`: skip stdin prompt and use an input data account
* `-R, --reclaim-job <true|false>` (default: `true`)

Notes:

* If `--input-data-account-key` is **not** provided, `run-auction` reads the prompt from stdin (interactive if the terminal is attached).
* The payer keypair must be **funded** on the target cluster.
* The CLI returns non-zero exit codes for common failures (insufficient balance, no bidders, verification failed, etc.).

---

## Using as a library

### Create an `AuctionClient`

```rust
use solana_sdk::pubkey::Pubkey;
use ambient_auction_listener::{listener::AuctionClient, ID};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = AuctionClient::new(
        ID,                               // auction program id (ambient_auction_api::ID)
        "http://localhost:8899".into(),   // solana json rpc url
        None,                             // keypair path (defaults to ~/.config/solana/id.json)
        Pubkey::default(),                // vote account (required by some APIs; can be any pubkey if not mining)
        None,                             // vote authority keypair path (defaults to payer keypair)
        "http://localhost:10000".into(),  // yellowstone grpc url
    ).await?;

    // Optional: keep bundle registry cache warm
    tokio::spawn(async move {
        let _ = client.keep_cache_warm().await;
    });

    Ok(())
}
```

### Submit a job (streaming)

The main entrypoint is:

* `listener::submit_job(args, self_private_key) -> InferenceResponse`

It:

1. derives tiers (or uses overrides),
2. submits `request_job`,
3. waits for the JobRequest tx/account to reach confirmed status,
4. waits for the auction to end and reads winning bid data,
5. forwards an OpenAI-compatible request to the winning bidder endpoint,
6. optionally spawns a reclaim task to close/reclaim the job request after verification.

**About `Verification` events:**
`StreamingResponse::Verification { verified: bool }` exists. In practice, verification events may be emitted by the inference backend’s SSE stream. If you want a *local* on-chain verification event appended to an arbitrary stream, use `listener::chain_verification(...)` (not enabled by default inside `submit_job_async`).

---

## Observability

The listener registers Prometheus metrics (it registers; it does not expose an HTTP endpoint):

* `run_auction_timings` (histogram vec)
* `auction_solana_client_timings` (histogram vec)
* `auction_solana_client_counts` (counter vec)
* `ambient_auction_listener:reclaim_job_tasks` (gauge)

Embedding services should expose these metrics via their own metrics endpoint.

---

## Operational notes

* **Commitment levels:** several subscriptions use `Processed` for lower latency; use `Confirmed`/`Finalized` when you need stronger guarantees.
* **Idempotency:** reconnects and resubscriptions can occur; downstream effects should be idempotent (keyed by pubkey/signature/slot).
* **Key management:** default keypair handling writes to `~/.config/solana/id.json`. For production, pass explicit paths and manage secrets.
* **Blocking behavior:** some waits can block indefinitely if the target accounts never update; apply service-level timeouts where appropriate.
* **Bid filter offset:** `watch_bids` uses a memcmp offset tied to the current `Bid` layout; update it if the layout changes.

---