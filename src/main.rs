pub mod archive;
pub mod args;
pub mod broadcast;
pub mod filter;
pub mod lock;
pub mod logic;
pub mod node;
pub mod u5c;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use archive::Archive;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_s3::{
    config::IntoShared,
    primitives::{Blob, ByteStream},
    Client as S3Client,
};
use broadcast::{load_destinations, Destination};
use clap::Parser;
use hex::ToHex;
use lock::LockThread;

use anyhow::{bail, Result};
use args::Args;
use pallas::{
    interop::utxorpc::{LedgerContext, Mapper, TxoRef, UtxoMap},
    network::{
        facades::NodeClient,
        miniprotocols::{chainsync::NextResponse, Point},
    },
};
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_1::to_item;
use tokio::{select, signal, sync::watch::Receiver, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};
use utxorpc::{spec::sync::BlockRef, CardanoSyncClient, ClientBuilder, TipEvent};

pub struct Worker {
    pub dynamo: DynamoClient,
    pub kinesis: KinesisClient,
    pub archive: Archive,

    pub uri: String,
}

#[derive(Serialize, Deserialize)]
pub enum BroadcastMessage {
    Roll(BlockRef),
    Undo(BlockRef),
}

impl Worker {
    async fn worker_thread(&self, lock_id: String, mut lock_deadline: Receiver<u64>) -> Result<()> {
        // Fetch destinations from the database
        let destinations = load_destinations(self.dynamo.clone()).await?;

        if destinations.len() == 0 {
            return Ok(());
        }
        // TODO: repair

        let min_point = destinations
            .iter()
            .min_by_key(|f| f.point.index)
            .unwrap()
            .point
            .clone();

        let mut client = ClientBuilder::new()
            .uri(&self.uri)?
            .build::<CardanoSyncClient>()
            .await;

        let mut tip = client.follow_tip(vec![min_point]).await?;
        loop {
            select! {
                _ = sleep(Duration::from_secs(5 * 60)) => {
                    warn!("No block in 5 minutes, failing over to another node");
                    bail!("No block in 5 minutes, failing over to another node");
                },
                result = tip.event() => {
                    let event = result?;
                    let (block, message) = match event {
                        TipEvent::Apply(block) => {
                            let bytes = block.native;
                            let block = block.parsed.expect("must include block");
                            let block_header = block.header.clone().expect("must have header");

                            let start = SystemTime::now();
                            info!("Roll forward block {}", block_header.hash.encode_hex::<String>());
                            self.archive.save(&block, bytes.to_vec()).await?;
                            trace!("Block {} saved (elapsed={:?})", block_header.hash.encode_hex::<String>(), SystemTime::now().duration_since(start)?);

                            (
                                block,
                                BroadcastMessage::Roll(BlockRef {
                                    index: block_header.slot,
                                    hash: block_header.hash,
                                }),
                            )
                        }
                        TipEvent::Undo(block) => {
                            let block = block.parsed.expect("must include block");
                            let block_header = block.header.clone().expect("must have header");

                            // Unsave the block, so the UTXOs get set correctly
                            let start = SystemTime::now();
                            info!("Undo block {}", block_header.hash.encode_hex::<String>());
                            self.archive.unsave(&block).await?;
                            trace!("Block {} unsaved (elapsed={:?})", block_header.hash.encode_hex::<String>(), SystemTime::now().duration_since(start)?);

                            (
                                block,
                                BroadcastMessage::Undo(BlockRef {
                                    index: block_header.slot,
                                    hash: block_header.hash,
                                }),
                            )
                        }
                        TipEvent::Reset(_) => todo!(),
                    };
                    let start = SystemTime::now();
                    let message_bytes = serde_json::to_vec(&message)?;
                    for destination in &destinations {
                        let applies = destination
                            .filter
                            .as_ref()
                            .map_or(true, |f| f.applies_block(&block));
                        if applies {
                            lock_deadline
                                .wait_for(|deadline| {
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .expect("time went backwards")
                                        .as_millis() as u64;
                                    now < *deadline
                                })
                                .await?;
                            let resp = self
                                .kinesis
                                .put_record()
                                .partition_key("partition")
                                .data(Blob::new(message_bytes.clone()))
                                .stream_arn(&destination.stream_arn)
                                .send()
                                .await?;
                        }
                    }
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

    let dest = Destination {
        pk: "sundae-sync-v2-destination".into(),
        sk: "all".into(),
        filter: None,
        stream_arn:
            "arn:aws:kinesis:us-east-2:529991308818:stream/preview-sundae-sync-v2--test-stream"
                .into(),
        point: BlockRef {
            index: 54260713,
            hash: hex::decode("e13ccc8cec1d4dffa5127bf6947485045b6c58cb61495f99df36b1e9151d4d56")?
                .into(),
        },
        enabled: true,
    };
    dynamo_client
        .put_item()
        .set_item(Some(to_item(dest)?))
        .table_name("sundae-sync-v2-test-table")
        .send()
        .await?;
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
