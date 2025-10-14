# sundae-sync-v2

This repository is a ground-up rewrite of [sundae-sync](https://github.com/SundaeSwap-finance/sundae-sync).

Sundae-sync is responsible for mirroring RollForward and RollBackward messages to a single kinesis stream.

The rewrite seeks to address several goals:

- Robust failover across multiple nodes
- UtxoRPC support, for reduced resource consumption via [Dolos](https://github.com/TxPipe/dolos)
- Rollforward and Undo messages, to avoid downstream consumers having to manage a cursor
- Replication to multiple destinations with configurable filtering
- Archive blocks to S3, enabling lookup and fast bulk replay
- An indexable record of every UTXO in dynamodb

## Client Libraries

### Go Client

A Go client library is provided for accessing indexed transaction data from DynamoDB:

```bash
go get github.com/SundaeSwap-finance/sundae-sync-v2/clients/go
```

See [clients/go/README.md](clients/go/README.md) for detailed usage documentation and examples.

## Architecture

The main architecture of this codebase is as follows:

- src/main.rs
  - The entrypoint
  - Spawns various threads to operate in the background
  - Shared cancellation token for graceful shutdown
  - Waits for all tasks to finish
- src/lock
  - Responsible for obtaining an exclusive, expiring lock via dynamodb
  - Continually tries to acquire a lock for failover
  - Renews the lock in the background
  - Runs a unit of work so long as the lock is held
  - Worker thread can return an error, trigging a failover to another node
  - Communicates a "deadline" to the worker, preventing dangerous writes if
    we're close to the expiration
- src/worker
  - Responsible for connecting to a utxorpc API and responding to each event
- src/archive
  - Responsible for archiving a block and its UTXOs to S3/DynamoDB
- src/broadcast
  - Responsible for a set of destinations and filter conditions
  - Maintains a cursor for each destination, so we can recover and repair on
    failover
