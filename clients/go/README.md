# Sundae Sync V2 - Go Client

Go client library for accessing transaction data indexed by [sundae-sync-v2](https://github.com/SundaeSwap-finance/sundae-sync-v2).

## Overview

This package provides a Go interface for reading Cardano transaction data from DynamoDB that has been indexed by the sundae-sync-v2 Rust service. It includes:

- Data structure definitions matching the Rust indexer's output format
- DynamoDB DAO (Data Access Object) for querying transactions
- Utilities for working with UTxOs and transaction outputs

## Installation

```bash
go get github.com/SundaeSwap-finance/sundae-sync-v2/clients/go
```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/SundaeSwap-finance/sundae-sync-v2/clients/go/txdao"
    "github.com/rs/zerolog"
)

func main() {
    // Create AWS session
    sess := session.Must(session.NewSession(&aws.Config{
        Region: aws.String("us-east-1"),
    }))

    // Create DynamoDB client
    ddbClient := dynamodb.New(sess)

    // Create DAO
    logger := zerolog.New(os.Stdout)
    dao := txdao.New(ddbClient, "mainnet-sundae-sync-v2--lookup", logger, false)

    // Get a transaction
    ctx := context.Background()
    tx, err := dao.Get(ctx, "your-tx-hash-here")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Transaction: %+v\n", tx)
    fmt.Printf("UTxOs: %d\n", len(tx.Utxos))
}
```

### Getting a Specific Output

```go
// Get the output at index 0 from a transaction
utxo, err := dao.GetOutput(ctx, "tx-hash", 0)
if err != nil {
    log.Fatal(err)
}

// Convert to ogmigo Value type for further processing
value := utxo.Value()
fmt.Printf("ADA: %v\n", value.Coins())
```

### Using with sundae-go-utils

For convenience when building services, you can use the `Build` function which integrates with sundae-go-utils common flags:

```go
import (
    sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
    "github.com/SundaeSwap-finance/sundae-sync-v2/clients/go/txdao"
)

// Automatically uses environment from sundae-cli flags
dao := txdao.Build(ddbClient)
```

## Data Structures

### Tx (Transaction)

```go
type Tx struct {
    Pk         string  // Primary key: "tx:{hash}"
    Sk         string  // Sort key: "tx"
    Block      string  // Block hash
    InChain    bool    // Whether tx is in current chain
    Location   string  // S3 location of full block data
    Successful bool    // Whether tx succeeded
    Utxos      []UTxO  // Transaction outputs
    Collateral UTxO    // Collateral output (for failed txs)
}
```

### UTxO (Unspent Transaction Output)

```go
type UTxO struct {
    Address string      // Cardano address (base64 encoded)
    Coin    string      // Lovelace amount
    Assets  []Policy    // Native assets
}
```

### Policy & Asset

```go
type Policy struct {
    PolicyID string     // Policy ID (base64 encoded)
    Assets   []Asset    // Assets under this policy
}

type Asset struct {
    Name       string   // Asset name (base64 encoded)
    OutputCoin string   // Asset quantity
}
```

## Table Naming Convention

The DynamoDB table name follows the pattern: `{environment}-sundae-sync-v2--lookup`

Examples:
- `mainnet-sundae-sync-v2--lookup`
- `preview-sundae-sync-v2--lookup`
- `preprod-sundae-sync-v2--lookup`

## Requirements

- Go 1.24 or higher
- AWS credentials with DynamoDB read access
- Access to a sundae-sync-v2 DynamoDB table

## Development

### Running Tests

```bash
cd clients/go
go test ./...
```

### Building

```bash
cd clients/go
go build ./...
```

## License

See [LICENSE](../../LICENSE) in the repository root.

## Related

- [sundae-sync-v2](../../) - The Rust indexer service
- [sundae-go-utils](https://github.com/SundaeSwap-finance/sundae-go-utils) - Common Go utilities for SundaeSwap services
