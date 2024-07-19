pub mod archive;
pub mod args;
pub mod broadcast;
pub mod filter;
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

    async fn next_event(&mut self) -> Result<(Bytes, Block, BlockHeader, BroadcastMessage)> {
        let event = self.tip.event().await.context("failed to grab tip")?;
        Ok(match event {
            TipEvent::Apply(block) => {
                let bytes = block.native;
                let block = block.parsed.expect("must include block");
                let block_header = block.header.clone().expect("must have header");
                let (index, hash) = (block_header.slot, block_header.hash.clone());

                (
                    bytes,
                    block,
                    block_header,
                    BroadcastMessage::Roll(BlockRef { index, hash }),
                )
            }
            TipEvent::Undo(block) => {
                let bytes = block.native;
                let block = block.parsed.expect("must include block");
                let block_header = block.header.clone().expect("must have header");
                let (index, hash) = (block_header.slot, block_header.hash.clone());

                (
                    bytes,
                    block,
                    block_header,
                    BroadcastMessage::Undo(BlockRef { index, hash }),
                )
            }
            TipEvent::Reset(_) => todo!(),
        })
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
            return Ok(());
        }

        let earliest_point = broadcaster
            .destinations
            .iter()
            .min_by_key(|d| d.last_seen_point.index)
            .unwrap()
            .recovery_points
            .iter()
            .cloned()
            .rev()
            .collect();

        let mut follower = Follower::new(&self.uri, earliest_point).await?;

        loop {
            select! {
                _ = sleep(Duration::from_secs(5 * 60)) => {
                    warn!("No block in 5 minutes, failing over to another node");
                    bail!("No block in 5 minutes, failing over to another node");
                },
                result = follower.next_event() => {
                    let (bytes, block, block_header, message) = result?;
                    let block_hash: String = block_header.hash.encode_hex();

                    let start = SystemTime::now();
                    match &message {
                        BroadcastMessage::Roll(_) => {
                            info!("Roll forward block {}", block_hash);
                            self.archive.save(&block, bytes.to_vec()).await?;
                            trace!("Block {} saved (elapsed={:?})", block_hash, elapsed(start));
                        },
                        BroadcastMessage::Undo(_) => {
                            info!("Undo block {}", block_hash);
                            self.archive.unsave(&block).await?;
                            trace!("Block {} unsaved (elapsed={:?})", block_hash, elapsed(start));
                        },
                    }

                    let start = SystemTime::now();
                    broadcaster.broadcast(block, message).await?;
                    trace!("Message broadcast (elapsed={:?})", SystemTime::now().duration_since(start)?);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cancel = CancellationToken::new();
    tracing_subscriber::fmt::init();
    info!("Starting");

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

    // let dest = Destination {
    //     pk: "sundae-sync-v2-destination".into(),
    //     sk: "all".into(),
    //     filter: None,
    //     stream_arn:
    //         "arn:aws:kinesis:us-east-2:529991308818:stream/preview-sundae-sync-v2--test-stream"
    //             .into(),
    //     last_seen_point: point_to_string(BlockRef {
    //         index: 54260713,
    //         hash: hex::decode("e13ccc8cec1d4dffa5127bf6947485045b6c58cb61495f99df36b1e9151d4d56")?
    //             .into(),
    //     }),
    //     recovery_points: vec![],
    //     enabled: true,
    // };
    // dynamo_client
    //     .put_item()
    //     .set_item(Some(to_item(dest)?))
    //     .table_name("sundae-sync-v2-test-table")
    //     .send()
    //     .await?;
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
