pub mod archive;
pub mod args;
pub mod broadcast;
pub mod lock;
pub mod utils;
pub mod worker;

use std::time::Duration;

use archive::Archive;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use lock::LockThread;

use anyhow::Result;
use args::Args;
use tokio::{signal, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::info;
use worker::Worker;

#[tokio::main]
async fn main() -> Result<()> {
    let cancel = CancellationToken::new();
    tracing_subscriber::fmt::init();
    info!("Starting sundae-sync-v2");

    let args = Args::parse();
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let s3_client = S3Client::new(&config);
    let dynamo_client = DynamoClient::new(&config);
    let kinesis_client = KinesisClient::new(&config);

    {
        // Cancel our worker thread once we receive a Ctrl+C
        let cancel = cancel.clone();
        tasks.spawn(async move {
            info!("Press Ctrl+C or send SIGINT to gracefully shut down...");
            signal::ctrl_c().await?;
            cancel.cancel();
            Ok(())
        });
    }

    {
        // Spawn a thread that runs our worker thread *only* when we have a lock thread
        let lock_thread = LockThread {
            lock_duration: Duration::from_secs(20),
            lock_acquire_freq: Duration::from_secs(5),
            lock_renew_freq: Duration::from_secs(10),
            lock_stall_window: Duration::from_secs(5),
            dynamo: dynamo_client.clone(),
        };

        let archive = Archive {
            bucket: "preview-529991308818-sundae-sync-v2-test-bucket".to_string(),
            table: "sundae-sync-v2-test-table".to_string(),
            s3: s3_client,
            dynamo: dynamo_client.clone(),
        };

        let worker = Worker {
            dynamo: dynamo_client.clone(),
            kinesis: kinesis_client.clone(),
            uri: args.utxo_rpc_url.unwrap(),
            table: "sundae-sync-v2-test-table".to_string(),
            archive,
        };

        let cancel = cancel.clone();
        tasks.spawn(async move { lock_thread.maintain_lock(cancel, worker).await });
    }

    // Wait for all our tasks to finish
    while let Some(result) = tasks.join_next().await {
        match result? {
            Ok(_) => info!("Task finished succesfully"),
            Err(err) => {
                info!("Task finished with error: {:?}", err)
            }
        }
    }

    Ok(())
}
