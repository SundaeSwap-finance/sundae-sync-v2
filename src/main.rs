pub mod args;
pub mod broadcast;
pub mod filter;
pub mod lock;
pub mod logic;
pub mod node;
pub mod u5c;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use lock::LockThread;

use anyhow::Result;
use args::Args;
use tokio::{signal, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::info;

async fn worker_thread(
    lock_id: String,
    lock_deadline: tokio::sync::watch::Receiver<u64>,
) -> Result<()> {
    loop {
        info!("Lock {} doing work!", lock_id);
        loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_millis() as u64;
            if now + 5 * 1000 < *lock_deadline.borrow() {
                info!("Lock {} Work", lock_id)
            } else {
                info!("Lock {} not renewed, stalling", lock_id)
            }
            sleep(Duration::new(1, 0)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cancel = CancellationToken::new();
    tracing_subscriber::fmt::init();
    info!("Starting");

    let _args = Args::parse();
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let _s3_client = S3Client::new(&config);
    let dynamo_client = DynamoClient::new(&config);

    let lock_thread = LockThread {
        lock_duration: Duration::from_secs(20),
        lock_acquire_freq: Duration::from_secs(5),
        lock_renew_freq: Duration::from_secs(10),
        lock_stall_window: Duration::from_secs(5),
        dynamo: dynamo_client.clone(),
    };

    {
        // Cancel our worker thread once we receive a Ctrl+C
        let cancel = cancel.clone();
        tasks.spawn(async move {
            signal::ctrl_c().await?;
            cancel.cancel();
            Ok(())
        });
    }

    {
        // Spawn a thread that runs our worker thread
        let cancel = cancel.clone();
        tasks.spawn(async move { lock_thread.maintain_lock(cancel, worker_thread).await });
    }

    while let Some(result) = tasks.join_next().await {
        match result? {
            Ok(_) => info!("Task finished succesfully"),
            Err(err) => {
                info!("Task finished with error: {:?}", err)
            }
        }
    }

    /*
    let sundae_sync = SundaeSyncLogic {
        s3: s3_client,
        bucket: "preview-529991308818-sundae-sync-v2-test-bucket".to_string(),
    };

    // Read from either utxorpc, or an ouroboros socket
    if let Some(utxo_url) = args.utxo_rpc_url {
        sync_with_utxorpc(sundae_sync, utxo_url, args.point).await?;
    } else {
        sync_with_socket(
            sundae_sync,
            args.socket_path.unwrap(),
            args.network_magic.unwrap(),
            args.point,
        )
        .await?;
    }
    */

    Ok(())
}
