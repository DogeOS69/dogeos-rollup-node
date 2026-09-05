# Running a Scroll Rollup Node

This guide covers how to run a Scroll rollup node as a follower node.

## Hardware Requirements

The following are the recommended hardware specifications for running a Scroll rollup node:

### Recommended Requirements

- **CPU**: 2 cores @ 3 GHz
- **Memory**: 16 GB RAM

These specifications are based on production deployments and should provide sufficient resources for stable operation as
a follower node.

## Building the Node

### Prerequisites

- Rust toolchain (stable)
- Cargo package manager

### Compilation

To build the rollup node binary:

```bash
cargo build --release --bin rollup-node
```

This will create an optimized production binary at `target/release/rollup-node`.

For development builds (faster compilation, slower runtime):

```bash
cargo build --bin rollup-node
```

## Download Snapshot (Optional)

For faster initial sync, you can optionally download a snapshot of the blockchain data instead of syncing from genesis.

**This step is recommended but not required.** Without a snapshot, the node will sync from genesis, which can take considerably longer.

**Step 1:** Download the snapshot for your target network:

For Scroll Mainnet:
```bash
wget https://scroll-geth-snapshot.s3.us-west-2.amazonaws.com/reth/latest.tar
```

For Scroll Sepolia:
```bash
wget https://scroll-sepolia-l2geth-snapshots.s3.us-west-2.amazonaws.com/reth/latest.tar
```

**Step 2:** Extract the snapshot to your data directory:

```bash
tar -xvf latest.tar -C <DATADIR_PATH>
```

Replace `<DATADIR_PATH>` with your node's data directory path. This should match the `--datadir` flag you'll use when running the node (see configuration section below).

**Step 3:** Clean up the downloaded archive:

```bash
rm latest.tar
```

## Running the Node

### Basic Command

To run the node as a follower:

```bash
./target/release/rollup-node node \
  --chain <CHAIN_NAME> \
  --l1.url <L1_RPC_URL> \
  --blob.s3_url <BLOB_SOURCE_URL> \
  --http \
  --http.addr 0.0.0.0 \
  --http.port 8545
```

Replace:

- `<CHAIN_NAME>`: The chain to follow (e.g., `scroll-mainnet`, `scroll-sepolia`, or `dev`)
- `<L1_RPC_URL>`: HTTP(S) URL of an Ethereum L1 RPC endpoint
- `<BLOB_SOURCE_URL>`: Blob data source URL (use Scroll's S3 URLs or your own beacon node)

### Essential Configuration Flags

#### Chain Configuration

- `--chain <CHAIN>`: Specify the chain to sync (`scroll-mainnet`, `scroll-sepolia`, or `dev`)
- `-d, --datadir <PATH>`: Directory for node data storage (default: platform-specific)

#### L1 Provider Configuration

- `--l1.url <URL>`: L1 Ethereum RPC endpoint URL (required for follower nodes)
- `--l1.cups <NUMBER>`: Compute units per second for rate limiting (default: 10000)
- `--l1.max-retries <NUMBER>`: Maximum retry attempts for L1 requests (default: 10)
- `--l1.initial-backoff <MS>`: Initial backoff duration for retries in milliseconds (default: 100)
- `--l1.query-range <BLOCKS>`: Block range for querying L1 logs (default: 500)

#### Blob Provider Configuration

The node requires access to blob data for derivation. Configure one or more blob sources:

- `--blob.beacon_node_urls <URL>`: Beacon node URLs for fetching blobs (comma-separated for multiple)
- `--blob.s3_url <URL>`: S3-compatible storage URL for blob data
- `--blob.anvil_url <URL>`: Anvil blob provider URL (for testing)

**Scroll-Provided S3 Blob Storage:**

Scroll provides public S3 blob storage endpoints for both networks:

- **Mainnet**: `https://scroll-mainnet-blob-data.s3.us-west-2.amazonaws.com/`
- **Sepolia**: `https://scroll-sepolia-blob-data.s3.us-west-2.amazonaws.com/`

These can be used as reliable blob sources without requiring your own beacon node infrastructure.

#### Consensus Configuration

- `--consensus.algorithm <ALGORITHM>`: Consensus algorithm to use
    - `system-contract` (default): Validates blocks against authorized signer from L1
    - `noop`: No consensus validation (testing only)
- `--consensus.authorized-signer <ADDRESS>`: Static authorized signer address (when using system-contract without L1
  provider)

#### Database Configuration

- `--rollup-node-db.path <PATH>`: Custom database path (default: `<datadir>/scroll.db`)

#### Network Configuration

- `--network.bridge`: Enable bridging blocks from eth wire to scroll wire protocol (default: true)
- `--network.scroll-wire`: Enable the scroll wire protocol (default: true)
- `--network.sequencer-url <URL>`: Sequencer RPC URL for following the sequencer
- `--network.valid_signer <ADDRESS>`: Valid signer address for network validation

#### Chain Orchestrator Configuration

- `--chain.optimistic-sync-trigger <BLOCKS>`: Block gap that triggers optimistic sync (default: 1000)
- `--chain.chain-buffer-size <SIZE>`: In-memory chain buffer size (default: 2000)

#### Engine Configuration

- `--engine.sync-at-startup`: Attempt to sync on startup (default: true)

#### HTTP RPC Configuration

- `--http`: Enable HTTP RPC server
- `--http.addr <ADDRESS>`: HTTP server listening address (default: 127.0.0.1)
- `--http.port <PORT>`: HTTP server port (default: 8545)
- `--http.api <APIS>`: Enabled RPC API namespaces (comma-separated)
    - Available: `admin`, `debug`, `eth`, `net`, `trace`, `txpool`, `web3`, `rpc`, `reth`, `ots`, `flashbots`, `miner`, `mev`
- `--http.corsdomain <ORIGINS>`: CORS allowed origins (comma-separated)

#### Rollup Node RPC

- `--rpc.rollup-node=false`: Disable the rollup node basic RPC namespace(default: enabled) (provides rollup-specific methods)
- `--rpc.rollup-node-admin`: Enable the rollup node admin RPC namespace (provides rollup-specific methods)

#### Experimental Parent-State Multiproof RPC

`--rpc.experimental-multiproof` enables `dogeos_getProofs` on transports whose API
selection includes `eth`. It defaults off and is independent of the rollup basic
and admin flags. For a local HTTP experiment, add
`--http --http.addr 127.0.0.1 --http.api eth,debug --rpc.experimental-multiproof`
to the node's normal chain and data-directory arguments. Set
`--rpc.eth-proof-window` to cover the retained parent states you intend to query;
its default of zero permits only the latest state. The local fixtures use 16.
The method is never
automatically added to the authenticated Engine endpoint. Removing the flag
disables it after restart.

The method accepts one object parameter:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "dogeos_getProofs",
  "params": [{
    "blockHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "targets": [{
      "address": "0x5300000000000000000000000000000000000002",
      "storageKeys": ["0x0000000000000000000000000000000000000000000000000000000000000009"]
    }]
  }]
}
```

Replace the example hash with a retained canonical block hash. It selects that
block's **post-state**; a block materializer passes its parent hash. The result is
an array of ordinary EIP-1186 proofs in target order, with storage proofs in key
order. Tags, block numbers, empty target lists, duplicate accounts and duplicate
keys are rejected. Zero-key targets are supported, including accounts with
nonempty storage.

The initial limits are four accounts, four keys per account, eight keys total,
64 KiB raw JSON params before typed deserialization, and a 4 MiB serialized
response. Existing global transport limits still apply. Two jobs share admission
across transports, with fail-fast overload handling; they also use the ordinary
proof guard and history window. The shared-guard wait is at most one second and
the RPC deadline is 30 seconds. Cancellation does not preempt synchronous proof
work: the actual worker retains its permits until it exits. The response cap
bounds output, not intermediate historical reconstruction memory.

Only method-not-found (`-32601`) permits a compatible client to fall back to
individual `eth_getProof` calls. Invalid params (`-32602`), resource errors
(`-32005` with `data.kind`), historical-state errors and invalid proofs must remain
explicit failures. The node checks canonicality at resolution; clients still
authenticate returned proofs against their expected parent header's state root.

The local integration target is `cargo test -p rollup-node --test multiproof`.
It includes real-node transport checks, exact proof comparisons and a Tsuki
fixture. The ignored `serve_multiproof_fixture` and
`serve_tsuki_multiproof_fixture` tests expose temporary local nodes for external
comparisons. Set `MULTIPROOF_FIXTURE_DIR` to a fresh directory and run the selected
test with `-- --ignored --exact <test-name> --nocapture`. It writes `manifest.json`
and the matching `genesis.json`; create a `stop` file in that directory to finish
early, otherwise it stops after 15 minutes. The Tsuki fixture enables `debug`
for real execution witnesses. Core artifact equality and performance require
separate comparisons against this running node.

During the unpublished experiment, use the orchestrator's external Cargo patch
configuration for the entire compact dependency family and its shared build-lock
wrapper. Keep local path patches and their lockfile changes uncommitted. The
fixed image in the existing compose example does not contain these source edits;
use the experiment binary with a fresh temporary data directory.

### Example Configurations

#### Scroll Mainnet Follower

```bash
./target/release/rollup-node node \
  --chain scroll-mainnet \
  --datadir /var/lib/scroll-node \
  --l1.url https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY \
  --blob.s3_url https://scroll-mainnet-blob-data.s3.us-west-2.amazonaws.com/ \
  --http \
  --http.addr 0.0.0.0 \
  --http.port 8545 \
  --http.api eth,net,web3,debug,trace
```

#### Scroll Sepolia Testnet Follower

```bash
./target/release/rollup-node node \
  --chain scroll-sepolia \
  --datadir /var/lib/scroll-node-sepolia \
  --l1.url https://eth-sepolia.g.alchemy.com/v2/YOUR_API_KEY \
  --blob.s3_url https://scroll-sepolia-blob-data.s3.us-west-2.amazonaws.com/ \
  --http \
  --http.addr 0.0.0.0 \
  --http.port 8545 \
  --http.api eth,net,web3
```

## Logging and Debugging

The node uses Rust's `tracing` framework for structured logging. Configure log levels using the `RUST_LOG` environment
variable.

### Log Level Configuration

The general format is: `RUST_LOG=<default_level>,<target>=<level>,...`

#### Recommended Log Configuration

For production with detailed rollup-specific logging:

```bash
RUST_LOG=info,scroll=trace,rollup=trace,sqlx=off,scroll_txpool=trace ./target/release/rollup-node node ...
```

This configuration:

- Sets default log level to `info`
- Enables detailed `trace` logging for scroll-specific components
- Enables detailed `trace` logging for rollup components
- Disables verbose sqlx database query logging
- Enables detailed `trace` logging for transaction pool operations

### Useful Log Targets

For debugging specific components, you can adjust individual log targets:

#### L1 Watcher

```bash
RUST_LOG=scroll::watcher=debug
```

Monitor L1 event watching, batch commits, and chain reorganizations.

#### Derivation Pipeline

```bash
RUST_LOG=scroll::derivation_pipeline=debug
```

Track batch decoding and payload attribute streaming.

#### Engine Driver

```bash
RUST_LOG=scroll::engine=debug
```

Debug engine API interactions and forkchoice updates.

#### Network Layer

```bash
RUST_LOG=scroll::network=debug
```

Monitor P2P networking and block propagation.

#### Database Operations

```bash
RUST_LOG=scroll::db=debug
```

Debug database queries and operations.

### Log Level Options

Available log levels (from least to most verbose):

- `error`: Only error messages
- `warn`: Warnings and errors
- `info`: General informational messages (recommended for production)
- `debug`: Detailed debugging information
- `trace`: Very detailed trace information (use for specific debugging)

### Combined Example with Logging

```bash
RUST_LOG=info,scroll=debug,rollup=debug,sqlx=off \
./target/release/rollup-node node \
  --chain scroll-mainnet \
  --datadir /var/lib/scroll-node \
  --l1.url https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY \
  --blob.s3_url https://scroll-mainnet-blob-data.s3.us-west-2.amazonaws.com/ \
  --http \
  --http.addr 0.0.0.0 \
  --http.port 8545 \
  --http.api eth,net,web3
```

## Verifying Node Operation

### Check Sync Status

Once the node is running, you can verify it's syncing properly:

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_syncing",
    "params": [],
    "id": 1
  }'
```

### Get Latest Block

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_blockNumber",
    "params": [],
    "id": 1
  }'
```

### Check Node Health

Monitor the node logs for:

- Successful L1 event processing
- Block derivation progress
- Engine API health
- Database operations

Look for log entries indicating successful block processing and chain advancement.

## Troubleshooting

### Common Issues

**Node not syncing:**

- Verify L1 RPC endpoint is accessible and synced
- Ensure beacon node URLs are correct and responsive
- Check network connectivity
- Review logs with `RUST_LOG=debug` for detailed error messages

**High memory usage:**

- Adjust `--chain.chain-buffer-size` to reduce memory footprint
- Ensure system has adequate RAM (16GB recommended)

**Database errors:**

- Verify disk space is available
- Check file permissions on datadir
- Consider using a different database path with `--datadir`

**L1 connection errors:**

- Verify L1 RPC endpoint is reliable and has sufficient rate limits
- Adjust `--l1.max-retries` and `--l1.initial-backoff` for unstable connections
- Consider using a dedicated or archive L1 node

For additional support and detailed implementation information, refer to the project's CLAUDE.md and source code
documentation.
