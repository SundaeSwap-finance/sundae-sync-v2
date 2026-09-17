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
use tracing::warn;

/// Whether a destination should receive this broadcast.
///
/// A destination whose last_seen slot is already past this advance is
/// skipped so catch-up of a lagging peer does not rewind it. Slot is not
/// chain position: a one-block Praos fork can replace slot N+1 with slot N,
/// so a message that undoes the destination's current point is still sent.
pub(crate) fn destination_should_receive(last_seen: &BlockRef, message: &BroadcastMessage) -> bool {
    if last_seen.slot <= message.advance.slot {
        return true;
    }
    message.undo.iter().any(|u| u.hash == last_seen.hash)
}

/// Undo records ride the next roll-forward. Clearing the in-memory stack
/// after a broadcast that wrote to nobody drops those undos forever.
pub(crate) fn should_clear_undo_stack(written_destinations: &[String]) -> bool {
    !written_destinations.is_empty()
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
    ) -> Result<Vec<String>> {
        let message_bytes = serde_json::to_vec(&message)?;
        let mut destinations = vec![];
        // For each destination
        for destination in &mut self.destinations {
            if !destination_should_receive(&destination.last_seen_point, &message) {
                warn!(
                    dest = %destination.pk,
                    last_seen_slot = destination.last_seen_point.slot,
                    last_seen_hash = hex::encode(&destination.last_seen_point.hash),
                    advance_slot = message.advance.slot,
                    advance_hash = hex::encode(&message.advance.hash),
                    undo_count = message.undo.len(),
                    "skipping destination: last_seen is ahead of advance and undo does not include last_seen"
                );
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
        Ok(destinations)
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
    use super::*;
    use bytes::Bytes;

    fn point(slot: u64, hash: u8) -> BlockRef {
        BlockRef {
            slot,
            hash: Bytes::from(vec![hash]),
            height: 0,
            timestamp: 0,
        }
    }

    fn message(undo: Vec<BlockRef>, advance: BlockRef) -> BroadcastMessage {
        BroadcastMessage { undo, advance }
    }

    #[test]
    fn receives_when_advance_slot_is_ahead() {
        let last_seen = point(483, 0xaa);
        let msg = message(vec![], point(484, 0xbb));
        assert!(destination_should_receive(&last_seen, &msg));
    }

    #[test]
    fn receives_when_advance_slot_ties_last_seen() {
        let last_seen = point(484, 0xaa);
        let msg = message(vec![], point(484, 0xbb));
        assert!(destination_should_receive(&last_seen, &msg));
    }

    #[test]
    fn skips_catch_up_when_already_ahead_and_undo_is_empty() {
        let last_seen = point(1000, 0xaa);
        let msg = message(vec![], point(401, 0xbb));
        assert!(!destination_should_receive(&last_seen, &msg));
    }

    #[test]
    fn receives_one_block_fork_when_canonical_slot_is_lower() {
        // Doomed block at 484 already committed; canonical winner is 483.
        // Slot-only "already ahead" would drop the undo of 484.
        let doomed = point(484, 0xaa);
        let canonical = point(483, 0xbb);
        let msg = message(vec![doomed.clone()], canonical);
        assert!(destination_should_receive(&doomed, &msg));
    }

    #[test]
    fn skips_when_lower_slot_advance_does_not_undo_last_seen() {
        let last_seen = point(1000, 0xcc);
        let doomed = point(484, 0xaa);
        let canonical = point(483, 0xbb);
        let msg = message(vec![doomed], canonical);
        assert!(!destination_should_receive(&last_seen, &msg));
    }

    #[test]
    fn skips_empty_undo_when_advance_slot_goes_backwards() {
        let last_seen = point(484, 0xaa);
        let msg = message(vec![], point(483, 0xbb));
        assert!(!destination_should_receive(&last_seen, &msg));
    }

    #[test]
    fn clears_undo_stack_only_after_a_destination_was_written() {
        assert!(should_clear_undo_stack(&["all".to_string()]));
        assert!(!should_clear_undo_stack(&[]));
    }
}
