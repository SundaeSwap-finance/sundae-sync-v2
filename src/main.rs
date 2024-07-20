pub mod archive;
pub mod args;
pub mod broadcast;
pub mod lock;

use std::time::{Duration, SystemTime};

use archive::Archive;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_s3::Client as S3Client;
use broadcast::{BroadcastMessage, Broadcaster};
use bytes::Bytes;
use clap::Parser;
use hex::ToHex;
use lock::LockThread;

use anyhow::{bail, Context, Result};
use args::Args;
use tokio::{select, signal, sync::watch::Receiver, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};
use utxorpc::{
    spec::{
        cardano::{Block, BlockHeader},
        sync::BlockRef,
    },
    Cardano, CardanoSyncClient, ClientBuilder, LiveTip, TipEvent,
};

pub struct Follower {
    tip: LiveTip<Cardano>,
}

impl Follower {
    async fn new(uri: &String, points: Vec<BlockRef>) -> Result<Self> {
        let mut client = ClientBuilder::new()
            .uri(uri)?
            .build::<CardanoSyncClient>()
            .await;
        let mut tip = client.follow_tip(points).await?;
        // TODO: https://github.com/txpipe/dolos/issues/294
        _ = tip.event().await.context("skipping first")?;
        Ok(Self { tip })
    }

    async fn next_event(&mut self) -> Result<(bool, Bytes, Block, BlockHeader)> {
        let event = self.tip.event().await.context("failed to grab tip")?;
        let (bytes, block) = match &event {
            TipEvent::Apply(block) | TipEvent::Undo(block) => {
                let bytes = block.native.clone();
                let block = block.parsed.clone().expect("must include block");

                (bytes, block)
            }
            TipEvent::Reset(_) => todo!(),
        };
        let header = block.header.as_ref().expect("must include header").clone();

        Ok((matches!(event, TipEvent::Apply(_)), bytes, block, header))
    }
}

pub struct Worker {
    pub dynamo: DynamoClient,
    pub kinesis: KinesisClient,
    pub archive: Archive,
    pub table: String,
    pub uri: String,
}

fn elapsed(start: SystemTime) -> Duration {
    SystemTime::now()
        .duration_since(start)
        .expect("time went backwards")
}

impl Worker {
    async fn worker_thread(&self, _lock_id: String, lock_deadline: Receiver<u64>) -> Result<()> {
        // Fetch destinations from the database
        let mut broadcaster = Broadcaster::new(
            self.dynamo.clone(),
            self.table.clone(),
            self.kinesis.clone(),
            lock_deadline,
        )
        .await?;

        if broadcaster.destinations.len() == 0 {
            warn!("No enabled destinations, nothing to do");
            return Ok(());
        }

        broadcaster.repair().await?;

        let earliest_point = broadcaster
            .destinations
            .iter()
            .min_by_key(|d| d.last_seen_point.index)
            .unwrap();
        let intersect = earliest_point
            .recovery_points
            .iter()
            .cloned()
            .rev()
            .collect();

        info!(
            "Starting from {}/{}",
            earliest_point.last_seen_point.index,
            earliest_point
                .last_seen_point
                .hash
                .to_vec()
                .encode_hex::<String>()
        );
        let mut follower = Follower::new(&self.uri, intersect).await?;

        let mut undo_stack = vec![];

        loop {
            select! {
                _ = sleep(Duration::from_secs(5 * 60)) => {
                    warn!("No block in 5 minutes, failing over to another node");
                    bail!("No block in 5 minutes, failing over to another node");
                },
                result = follower.next_event() => {
                    let (is_roll_forward, bytes, block, header) = result?;
                    let block_hash: String = header.hash.encode_hex();

                    let start = SystemTime::now();
                    let point = BlockRef {
                        index: header.slot,
                        hash: header.hash,
                    };
                    if is_roll_forward {
                        trace!("Saving {}/{}", point.index, block_hash);
                        self.archive.save(&block, bytes.to_vec()).await?;
                        trace!("Block {}/{} saved (elapsed={:?})", point.index, block_hash, elapsed(start));

                        let start = SystemTime::now();
                        broadcaster.broadcast(block, BroadcastMessage {
                            undo: undo_stack.clone(),
                            advance: point.clone(),
                        }).await?;
                        trace!("Message broadcast (elapsed={:?})", SystemTime::now().duration_since(start)?);
                        undo_stack.clear();
                        info!("Roll forward block {}/{}", point.index, block_hash);
                    } else {
                        trace!("Unsaving {}/{}", point.index, block_hash);
                        self.archive.unsave(&block).await?;
                        trace!("Block {}/{} unsaved (elapsed={:?})", point.index, block_hash, elapsed(start));
                        undo_stack.push(point.clone());
                        info!("Undo block {}/{}", point.index, block_hash);
                    }
                }
            }
        }
    }
}

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
