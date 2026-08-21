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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BroadcastMessage {
    pub undo: Vec<BlockRef>,
    pub advance: BlockRef,
}

#[derive(Clone, Debug, PartialEq)]
enum BroadcastAction {
    BroadcastAndCommit(BroadcastMessage),
    Commit,
    Ignore,
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

    fn next_action(
        destination: &Destination,
        message: &BroadcastMessage,
        matches_filter: bool,
    ) -> BroadcastAction {
        let mut undo = vec![];
        if let Some(rolled_back_before) = message.undo.last() {
            for published in destination.published_points.iter().rev() {
                if published.slot >= rolled_back_before.slot {
                    undo.push(published.clone());
                } else {
                    break;
                }
            }
        }

        if undo.is_empty() {
            if destination.last_seen_point.slot > message.advance.slot {
                // This destination has already seen this message
                BroadcastAction::Ignore
            } else if matches_filter {
                BroadcastAction::BroadcastAndCommit(message.clone())
            } else {
                BroadcastAction::Commit
            }
        } else {
            // if we're rolling back, we _must_ broadcast. Otherwise, consumers will never know.
            if undo.len() >= destination.published_points.len() {
                // We're trying to roll back farther than we've tracked published messages.
                // This will only happen when switching off of a catastrophically long fork.
                // If it does, ignore any filter and publish the "raw" message.
                // Consumers may see undos and advances for blocks we should have filtered out,
                // but that's safer than _not_ switching off of the fork.
                BroadcastAction::BroadcastAndCommit(message.clone())
            } else if matches_filter {
                BroadcastAction::BroadcastAndCommit(BroadcastMessage {
                    undo,
                    advance: message.advance.clone(),
                })
            } else {
                let rollback_to_index = destination.published_points.len() - undo.len() - 1;
                let rollback_to = destination.published_points[rollback_to_index].clone();
                // BroadcastMessage must always advance the chain, but we have nothing new to advance to.
                // So we "undo" the point we want to roll back to, and then re-"advance" to it.
                undo.push(rollback_to.clone());
                BroadcastAction::BroadcastAndCommit(BroadcastMessage {
                    undo,
                    advance: rollback_to,
                })
            }
        }
    }

    pub async fn broadcast(
        &mut self,
        block: Block,
        message: BroadcastMessage,
    ) -> Result<Vec<String>> {
        let mut destinations = vec![];
        // For each destination
        for destination in &mut self.destinations {
            let matches_filter = destination
                .filter
                .as_ref()
                .is_some_and(|f| f.applies_block(&block));

            let broadcast_message = match Self::next_action(destination, &message, matches_filter) {
                BroadcastAction::BroadcastAndCommit(message) => Some(message),
                BroadcastAction::Commit => None,
                BroadcastAction::Ignore => continue,
            };
            if let Some(message) = broadcast_message {
                let message_bytes = serde_json::to_vec(&message)?;
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
                        true,
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
                        false,
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

    fn test_destination(last_seen_point: BlockRef, published_points: Vec<BlockRef>) -> Destination {
        Destination {
            pk: "idgaf".to_string(),
            stream_arn: "idgaf".to_string(),
            shard_id: "idgaf".to_string(),
            filter: None,
            sequence_number: None,
            last_seen_point,
            recovery_points: vec![],
            published_points,
            enabled: true,
            skip_repair: true,
        }
    }

    fn make_point(index: u64) -> BlockRef {
        make_point_with_hash(index, &format!("hash-{}", index))
    }

    fn make_point_with_hash(index: u64, hash: &str) -> BlockRef {
        BlockRef {
            slot: index,
            hash: bytes::Bytes::from(hash.as_bytes().to_vec()),
            timestamp: 0,
            height: 0,
        }
    }

    #[test]
    fn next_action_should_broadcast_next_message() {
        let destination = test_destination(make_point(101), vec![make_point(100), make_point(101)]);
        let message = BroadcastMessage {
            undo: vec![],
            advance: make_point(102),
        };
        let matches_filter = true;
        assert_eq!(
            Broadcaster::next_action(&destination, &message, matches_filter),
            BroadcastAction::BroadcastAndCommit(message),
        );
    }

    #[test]
    fn next_action_should_not_broadcast_filtered_messages() {
        let destination = test_destination(make_point(101), vec![make_point(100), make_point(101)]);
        let message = BroadcastMessage {
            undo: vec![],
            advance: make_point(102),
        };
        let matches_filter = false;
        assert_eq!(
            Broadcaster::next_action(&destination, &message, matches_filter),
            BroadcastAction::Commit,
        );
    }

    #[test]
    fn next_action_should_ignore_older_messages_without_rollbacks() {
        let destination = test_destination(make_point(101), vec![make_point(100), make_point(101)]);
        let message = BroadcastMessage {
            undo: vec![],
            advance: make_point(97),
        };
        let matches_filter = true;
        assert_eq!(
            Broadcaster::next_action(&destination, &message, matches_filter),
            BroadcastAction::Ignore,
        );
    }

    #[test]
    fn next_action_should_roll_back_when_needed() {
        let destination = test_destination(make_point(101), vec![make_point(100), make_point(101)]);
        let message = BroadcastMessage {
            undo: vec![make_point(101)],
            advance: make_point_with_hash(101, "other 101"),
        };
        let matches_filter = true;
        assert_eq!(
            Broadcaster::next_action(&destination, &message, matches_filter),
            BroadcastAction::BroadcastAndCommit(message),
        );
    }

    #[test]
    fn next_action_should_roll_back_even_on_filtered_messages() {
        let destination = test_destination(make_point(202), vec![make_point(200), make_point(202)]);
        let message = BroadcastMessage {
            undo: vec![make_point(202), make_point(201)],
            advance: make_point_with_hash(201, "other 201"),
        };
        let matches_filter = false;
        assert_eq!(
            Broadcaster::next_action(&destination, &message, matches_filter),
            BroadcastAction::BroadcastAndCommit(BroadcastMessage {
                undo: vec![make_point(202), make_point(200)],
                advance: make_point(200),
            }),
        );
    }

    #[test]
    fn next_action_should_ignore_filtered_messages_from_future() {
        let destination = test_destination(make_point(204), vec![make_point(200), make_point(202)]);
        let message = BroadcastMessage {
            undo: vec![make_point(204), make_point(203)],
            advance: make_point_with_hash(203, "other 203"),
        };
        let matches_filter = false;
        assert_eq!(
            Broadcaster::next_action(&destination, &message, matches_filter),
            BroadcastAction::Ignore,
        );
    }

    #[test]
    fn next_action_should_roll_back_beyond_tracked_history() {
        let destination = test_destination(make_point(204), vec![make_point(200), make_point(202)]);
        let message = BroadcastMessage {
            undo: vec![
                make_point(204),
                make_point(203),
                make_point(202),
                make_point(201),
                make_point(200),
                make_point(199),
            ],
            advance: make_point_with_hash(199, "other 199"),
        };
        let matches_filter = true;
        assert_eq!(
            Broadcaster::next_action(&destination, &message, matches_filter),
            BroadcastAction::BroadcastAndCommit(message),
        );
    }
}
