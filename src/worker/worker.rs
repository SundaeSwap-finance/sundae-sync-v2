use std::time::{Duration, SystemTime};

use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_kinesis::Client as KinesisClient;
use hex::ToHex;
use tokio::{select, sync::watch::Receiver, time::sleep};
use tracing::{info, trace, warn};
use utxorpc::spec::sync::BlockRef;

use super::Follower;
use crate::{
    archive::Archive,
    broadcast::{BroadcastMessage, Broadcaster},
    utils::elapsed,
};
use anyhow::{bail, Result};

pub struct Worker {
    pub dynamo: DynamoClient,
    pub kinesis: KinesisClient,
    pub archive: Archive,
    pub table: String,
    pub uri: String,
    pub api_key: Option<String>,
}
impl Worker {
    pub async fn worker_thread(
        &self,
        _lock_id: String,
        lock_deadline: Receiver<u64>,
    ) -> Result<()> {
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
        let intersect: Vec<_> = earliest_point
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
        let mut follower = Follower::new(&self.uri, &self.api_key, intersect).await?;

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
                        self.archive.save(&block, bytes.to_vec()).await?;

                        let start = SystemTime::now();
                        let destinations = broadcaster.broadcast(block, BroadcastMessage {
                            undo: undo_stack.clone(),
                            advance: point.clone(),
                        }).await?;
                        trace!("Message broadcast (elapsed={:?})", SystemTime::now().duration_since(start)?);
                        undo_stack.clear();
                        info!("Roll forward {}/{} ({})", point.index, block_hash, destinations.join(", "));
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
