# sundae-sync-v2

[![Test Suite](https://github.com/SundaeSwap-finance/sundae-sync-v2/workflows/Test%20Suite/badge.svg)](https://github.com/SundaeSwap-finance/sundae-sync-v2/actions)
[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)

A robust, production-ready Cardano blockchain indexer with UtxoRPC support, automatic failover, and flexible event filtering.

This is a ground-up rewrite of [sundae-sync](https://github.com/SundaeSwap-finance/sundae-sync), designed for high availability and efficient resource utilization.

## Features

- **Robust Failover** - Automatic failover across multiple nodes using DynamoDB-based locking
- **UtxoRPC Support** - Reduced resource consumption via [Dolos](https://github.com/TxPipe/dolos) and other UtxoRPC providers
- **Simplified Consumers** - Rollforward and Undo messages eliminate cursor management for downstream consumers
- **Flexible Filtering** - Configurable filtering for replication to multiple destinations
- **Block Archiving** - Archive blocks to S3 for fast bulk replay and historical lookup
- **UTXO Indexing** - Complete indexable record of every UTXO in DynamoDB

## Quick Start

### Prerequisites

- Rust 1.75+ (managed via `rust-toolchain.toml`)
- AWS credentials with permissions for:
  - DynamoDB (tables: `{env}-sundae-sync-v2--lock`, `{env}-sundae-sync-v2--lookup`)
  - S3 (bucket: `{env}-sundae-sync-v2-{account}-{region}`)
  - Kinesis (stream: `{env}-sundae-sync-v2--{destination}`)
- Access to a UtxoRPC endpoint (e.g., Dolos)

### Installation

#### From Source

```bash
git clone https://github.com/SundaeSwap-finance/sundae-sync-v2.git
cd sundae-sync-v2
cargo build --release
```

The binary will be available at `target/release/sundae-sync-v2`.

#### Using cargo-dist (Releases)

Pre-built binaries are available on the [Releases page](https://github.com/SundaeSwap-finance/sundae-sync-v2/releases).

### Infrastructure Setup

Deploy the required AWS infrastructure using the provided CloudFormation template:

```bash
aws cloudformation create-stack \
  --stack-name sundae-sync-v2-preview \
  --template-body file://resources.template \
  --parameters ParameterKey=Env,ParameterValue=preview
```

This creates:

- DynamoDB tables for locking and UTXO lookup
- S3 bucket for block archiving
- Kinesis streams for event replication

### Configuration

Configure via environment variables or command-line arguments:

```bash
# Required
export UTXO_RPC_URI=http://localhost:50051
export UTXO_RPC_API_KEY=your-api-key  # if required by provider

# Optional
export LOCK_TABLE=preview-sundae-sync-v2--lock
export LOOKUP_TABLE=preview-sundae-sync-v2--lookup
export ARCHIVE_BUCKET=preview-sundae-sync-v2-{account}-{region}
export KINESIS_STREAM=preview-sundae-sync-v2--mainnet
```

### Running

```bash
# Start the indexer
./target/release/sundae-sync-v2 \
  --utxo-rpc-uri http://localhost:50051 \
  --utxo-rpc-api-key your-key
```

The service will:

1. Acquire a distributed lock via DynamoDB
2. Connect to the UtxoRPC endpoint
3. Start syncing from the chain tip (or resume from cursor)
4. Archive blocks to S3
5. Index UTXOs to DynamoDB
6. Replicate events to Kinesis streams

## Architecture

### Components

```
sundae-sync-v2
├── src/main.rs          - Entrypoint and orchestration
├── src/lock/            - Distributed locking for failover
├── src/worker/          - UtxoRPC connection and event processing
├── src/archive/         - Block and UTXO archiving to S3/DynamoDB
└── src/broadcast/       - Event replication with filtering
```

### Lock Management

- Acquires exclusive, expiring lock via DynamoDB
- Automatically fails over to standby nodes on:
  - Lock expiration
  - Worker errors
  - Node failures
- Prevents split-brain scenarios

### Worker Thread

- Connects to UtxoRPC and processes:
  - `RollForward` - New blocks
  - `RollBackward` - Chain reorganizations
- Respects lock deadlines to prevent stale writes

### Archive System

- Stores full block data in S3 at: `s3://{bucket}/{block-hash}`
- Indexes UTXOs in DynamoDB for fast lookup by transaction
- Enables:
  - Historical block replay
  - UTXO resolution for wallets and applications
  - Bulk data export

### Broadcast System

- Replicates events to multiple Kinesis streams
- Configurable per-destination filters:
  - Policy-based (all assets under a policy)
  - Asset-based (specific token)
  - Address-based (payment/staking credentials)
  - Withdrawal-based (reward withdrawals)
- Maintains cursor per destination for recovery

## Client Libraries

### Go Client

A Go client library is provided for accessing indexed transaction data from DynamoDB:

```bash
go get github.com/SundaeSwap-finance/sundae-sync-v2/clients/go
```

**Example:**

```go
import "github.com/SundaeSwap-finance/sundae-sync-v2/clients/go/txdao"

dao := txdao.New(ddbClient, "preview-sundae-sync-v2--lookup", logger, false)
tx, err := dao.Get(ctx, "tx-hash")
utxo, err := dao.GetOutput(ctx, "tx-hash", 0)
```

See [clients/go/README.md](clients/go/README.md) for full documentation.

## Configuration Options

```
Usage: sundae-sync-v2 [OPTIONS]

Options:
  --utxo-rpc-uri <URI>            UtxoRPC endpoint URL
  --utxo-rpc-api-key <KEY>        API key for UtxoRPC (if required)
  --socket-path <PATH>            Unix socket path (alternative to URI)
  --lock-table <TABLE>            DynamoDB lock table
  --lookup-table <TABLE>          DynamoDB UTXO lookup table
  --archive-bucket <BUCKET>       S3 bucket for block archiving
  --kinesis-stream <STREAM>       Kinesis stream for events
  --filter <FILTER>               Event filter configuration (JSON)
  -h, --help                      Print help
  -V, --version                   Print version
```

## Event Filtering

Configure filters in JSON format:

```json
{
  "Spent": {
    "Policy": { "policy": "policy_id_hex" }
  }
}
```

Filter types:

- `Spent(TokenFilter)` - Transactions spending/creating matching assets
- `Mint(TokenFilter)` - Transactions minting/burning matching assets
- `Withdraw { credential }` - Transactions withdrawing from staking credential
- `Delegate { credential }` - Transactions delegating staking credential

Token filter types:

- `Policy { policy }` - All assets under a policy
- `AssetId { policy, asset_name }` - Specific asset

## Development

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Linting

```bash
cargo clippy --all-targets
cargo fmt --check
```

### Running Locally

For local development without AWS infrastructure:

```bash
# Use localstack or real AWS with preview environment
cargo run -- \
  --utxo-rpc-uri http://localhost:50051 \
  --lock-table local-sundae-sync-v2--lock \
  --lookup-table local-sundae-sync-v2--lookup
```

## Monitoring

The service exposes metrics via standard Rust tracing. Key events:

- Lock acquisition/renewal
- Block processing (forward/backward)
- Archive operations
- Broadcast delivery
- Error conditions

## License

This project is licensed under the Business Source License 1.1.

- **Production Use**: Requires an Additional Use Grant (see [LICENSE-GRANTS.md](LICENSE-GRANTS.md))
- **Non-Production Use**: Freely available for development, testing, and evaluation
- **Change Date**: 2029-01-14 (converts to GPL v2.0+)

For production use grants, contact: legal@sundaeswap.finance

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run `cargo clippy` and `cargo fmt`
5. Submit a pull request

## Support

- **Issues**: [GitHub Issues](https://github.com/SundaeSwap-finance/sundae-sync-v2/issues)
- **Discussions**: [GitHub Discussions](https://github.com/SundaeSwap-finance/sundae-sync-v2/discussions)
- **Commercial Support**: contact@sundaeswap.finance

## Related Projects

- [sundae-sync](https://github.com/SundaeSwap-finance/sundae-sync) - Original implementation
- [Dolos](https://github.com/TxPipe/dolos) - UtxoRPC node implementation
- [Pallas](https://github.com/txpipe/pallas) - Rust Cardano libraries
