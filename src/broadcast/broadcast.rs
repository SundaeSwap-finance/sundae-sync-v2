use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_s3::primitives::Blob;
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use tokio::sync::watch::Receiver;
use utxorpc::spec::{cardano::Block, sync::BlockRef};

use super::Destination;

/// What one broadcast did: which destinations received the message, and
/// whether it moved any destination's cursor forward at all.
pub struct Broadcast {
    pub published_to: Vec<String>,
    pub advanced: bool,
}

/// Whether a block at `advance_slot` moves a destination whose cursor sits at
/// `cursor_slot`.
///
/// The follower re-delivers the intersect block itself after every restart —
/// the block the cursor already names. Publishing it again is a duplicate for
/// every consumer, and when the upstream node is stuck it is the ONLY thing
/// the producer ever publishes: on 2026-09-16 preprod re-emitted one block
/// every ~35s for three days, which kept the stream's idle alarm quiet while
/// nothing advanced. Equal is not an advance.
pub fn advances(cursor_slot: u64, advance_slot: u64) -> bool {
    advance_slot > cursor_slot
}

pub struct Broadcaster {
    pub destinations: Vec<Destination>,
    pub kinesis: KinesisClient,
    pub dynamo: DynamoClient,
    pub table: String,
    pub deadline: Receiver<u64>,
}

/// A sequence of blocks to undo, followed by one block to advance
/// Messages are structured this way to make sequences of undo's atomic
/// so that we can repair after a crash much easier
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BroadcastMessage {
    pub undo: Vec<BlockRef>,
    pub advance: BlockRef,
}

impl Broadcaster {
    pub async fn new(
        dynamo: DynamoClient,
        table: String,
        kinesis: KinesisClient,
        deadline: Receiver<u64>,
    ) -> Result<Self> {
        // Load destinations from dynamo
        let resp = dynamo
            .scan()
            .consistent_read(true)
            .filter_expression("enabled = :enabled")
            .expression_attribute_values(":enabled", AttributeValue::Bool(true))
            .table_name(&table)
            .send()
            .await?;

        let destinations = from_items(resp.items().to_vec())?;
        Ok(Self {
            destinations,
            kinesis,
            dynamo,
            table,
            deadline,
        })
    }

    pub async fn broadcast(
        &mut self,
        block: Block,
        message: BroadcastMessage,
    ) -> Result<Broadcast> {
        let message_bytes = serde_json::to_vec(&message)?;
        let mut destinations = vec![];
        let advanced = self
            .destinations
            .iter()
            .any(|d| advances(d.last_seen_point.slot, message.advance.slot));
        // For each destination
        for destination in &mut self.destinations {
            if !advances(destination.last_seen_point.slot, message.advance.slot) {
                continue;
            }
            // Check if we *should* send to this destination,
            // based on whether any of the transactions match the criteria
            let applies = destination
                .filter
                .as_ref()
                .is_none_or(|f| f.applies_block(&block));
            // If so
            if applies {
                // Safety check: ensure we're comfortably within the lock expiration deadline
                // before sending to Kinesis. This is a defensive backup to the primary safety
                // mechanism (lock renewal failure drops the worker future). This prevents
                // sending duplicate Kinesis events if the lock expires but the worker hasn't
                // been properly cancelled yet.
                // TODO: Verify that lock renewal failure in lock_thread.rs fully cancels the
                // worker future before relying solely on that mechanism.
                self.deadline
                    .wait_for(|deadline| {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("time went backwards")
                            .as_millis() as u64;
                        now < *deadline
                    })
                    .await
                    .context("failed checking for deadline")?;

                // then send to kinesis, and save the point/seq number back to the destination
                let result = self
                    .kinesis
                    .put_record()
                    .partition_key("sundae-sync-v2")
                    .data(Blob::new(message_bytes.clone()))
                    .stream_arn(&destination.stream_arn)
                    .send()
                    .await
                    .context("failed writing to kinesis")?;
                destination
                    .commit(
                        &self.dynamo,
                        &self.table,
                        message.advance.clone(),
                        Some(result.sequence_number),
                    )
                    .await
                    .context(format!("failed committing destination {}", destination.pk))?;
                destinations.push(destination.pk.clone());
            } else {
                // If the block doesn't apply, we still advance the point
                // with the same sequence number
                // so that we don't replay excessively if a filter makes
                // hits rare
                destination
                    .commit(
                        &self.dynamo,
                        &self.table,
                        message.advance.clone(),
                        destination.sequence_number.clone(),
                    )
                    .await
                    .context(format!(
                        "failed advancing sequence number for destination {}",
                        destination.pk
                    ))?;
            }
        }
        Ok(Broadcast {
            published_to: destinations,
            advanced,
        })
    }

    pub async fn repair(&mut self) -> Result<()> {
        for destination in &mut self.destinations {
            destination
                .repair(
                    self.kinesis.clone(),
                    self.dynamo.clone(),
                    self.table.clone(),
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::advances;

    #[test]
    fn a_block_ahead_of_the_cursor_advances_it() {
        assert!(advances(100, 101));
        assert!(advances(0, 1));
    }

    #[test]
    fn the_intersect_block_and_anything_behind_it_do_not() {
        // The intersect block re-delivered on restart sits exactly at the cursor.
        assert!(!advances(133_902_215, 133_902_215));
        assert!(!advances(100, 99));
    }
}
