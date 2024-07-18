pub mod args;
pub mod broadcast;
pub mod filter;
pub mod lock;
pub mod logic;
pub mod node;
pub mod u5c;

use std::time::Duration;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_s3::{primitives::Blob, Client as S3Client};
use broadcast::{load_destinations, Destination};
use clap::Parser;
use lock::LockThread;

use anyhow::Result;
use args::Args;
use pallas::{
    interop::utxorpc::{LedgerContext, Mapper, TxoRef, UtxoMap},
    network::{facades::NodeClient, miniprotocols::Point},
};
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_1::to_item;
use tokio::{signal, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::info;
use utxorpc::spec::sync::BlockRef;

pub struct Worker {
    pub dynamo: DynamoClient,
    pub kinesis: KinesisClient,

    pub socket_path: String,
    pub magic: u64,
}

#[derive(Serialize, Deserialize)]
pub enum BroadcastMessage {
    Roll(BlockRef),
    Undo(BlockRef),
}

#[derive(Clone)]
struct NoContext;
impl LedgerContext for NoContext {
    fn get_utxos(&self, _refs: &[TxoRef]) -> Option<UtxoMap> {
        None
    }
}
impl Worker {
    async fn worker_thread(
        &self,
        lock_id: String,
        lock_deadline: tokio::sync::watch::Receiver<u64>,
    ) -> Result<()> {
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

        // let mut recent_blocks = VecDeque::new();
        let mapper = Mapper::new(NoContext {});

        let mut client = NodeClient::connect(&self.socket_path, self.magic).await?;
        let (point, _) = client
            .chainsync()
            .find_intersect(vec![Point::Specific(
                min_point.index,
                min_point.hash.into(),
            )])
            .await?;
        loop {
            let next = client.chainsync().request_or_await_next().await?;
            let block = match next {
                pallas::network::miniprotocols::chainsync::NextResponse::RollForward(block, _) => {
                    block
                }
                pallas::network::miniprotocols::chainsync::NextResponse::RollBackward(_, _) => {
                    continue
                }
                pallas::network::miniprotocols::chainsync::NextResponse::Await => continue,
            };
            let block = mapper.map_block_cbor(&block);
            let block_header = block.header.clone().unwrap();
            info!("Block {}", hex::encode(&block_header.hash));
            let message = BroadcastMessage::Roll(BlockRef {
                index: block_header.height,
                hash: block_header.hash,
            });
            let message_bytes = serde_json::to_vec(&message)?;
            for destination in &destinations {
                let applies = destination
                    .filter
                    .as_ref()
                    .map(|f| {
                        block
                            .body
                            .as_ref()
                            .unwrap()
                            .tx
                            .iter()
                            .any(|tx| f.applies(tx))
                    })
                    .unwrap_or(true);
                if applies {
                    let resp = self
                        .kinesis
                        .put_record()
                        .partition_key("partition")
                        .data(Blob::new(message_bytes.clone()))
                        .stream_arn(&destination.stream_arn)
                        .send()
                        .await?;
                    info!("Wrote message {}", resp.sequence_number);
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
    let _s3_client = S3Client::new(&config);
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

        let worker = Worker {
            dynamo: dynamo_client.clone(),
            kinesis: kinesis_client.clone(),
            magic: args.network_magic.unwrap(),
            socket_path: args.socket_path.unwrap(),
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
