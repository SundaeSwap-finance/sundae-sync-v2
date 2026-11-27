use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_s3::primitives::Blob;
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use tokio::{sync::watch::Receiver, time::timeout};
use utxorpc::spec::{cardano::Block, sync::BlockRef};

use super::{destination::point_to_string, Destination};

/// The longest a write to Kinesis may take, retries included.
///
/// The deadline check below runs before the write starts, so the write itself
/// must end before the lock can expire. The lock thread keeps the deadline
/// `lock_duration - lock_renew_freq - lock_stall_window` ahead of the lock
/// expiration; this timeout must stay below that margin (main.rs checks it).
pub const KINESIS_PUT_TIMEOUT: Duration = Duration::from_secs(10);

/// What one broadcast did: which destinations received the message, and
/// whether it moved any destination's cursor at all.
///
/// The upstream sends the block at the cursor again after every restart.
/// When the upstream node is stuck, that block is the only thing it sends:
/// on 2026-09-16 preprod received one block every ~35s for three days.
/// Nothing advances then, and `advanced` is how the worker counts that.
#[derive(Debug)]
pub struct Broadcast {
    pub published_to: Vec<String>,
    pub advanced: bool,
}

pub struct Broadcaster {
    pub destinations: Vec<Destination>,
    pub kinesis: KinesisClient,
    pub dynamo: DynamoClient,
    pub table: String,
    pub deadline: Receiver<u64>,
    pub put_timeout: Duration,
}

/// A sequence of blocks to undo, followed by one block to advance
/// Messages are structured this way to make sequences of undo's atomic
/// so that we can repair after a crash much easier
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BroadcastMessage {
    pub undo: Vec<BlockRef>,
    pub advance: BlockRef,
}

/// Points are the same block when slot and hash match. Height and timestamp
/// are not compared: points loaded from the destination table carry neither.
fn same_point(a: &BlockRef, b: &BlockRef) -> bool {
    a.slot == b.slot && a.hash == b.hash
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
            put_timeout: KINESIS_PUT_TIMEOUT,
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
            if destination.last_seen_point.slot > message.advance.slot
                || same_point(&destination.last_seen_point, &message.advance)
            {
                // This destination has already seen this message.
                // The upstream can send the block at our last point again
                // (after a restart, or at the end of a catch-up); publishing
                // it again would hand consumers the same block twice.
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

    /// Refuse a message that does not continue what this destination has
    /// already published. Consumers rely on the stream being a chain: every
    /// message advances one block from the last, and a rollback undoes the
    /// last blocks in order, newest first, then advances from the lowest of
    /// them. Publishing anything else leaves every consumer to detect and
    /// repair it; stopping here is the lesser harm.
    ///
    /// Only unfiltered destinations are checked: a filtered stream skips
    /// blocks by design. Heights are compared only when both are known;
    /// points stored before heights were stored have height 0.
    fn check_continuity(destination: &Destination, message: &BroadcastMessage) -> Result<()> {
        if destination.filter.is_some() {
            return Ok(());
        }
        let published = &destination.published_points;
        let advance = &message.advance;
        let heights_known = |a: &BlockRef, b: &BlockRef| a.height != 0 && b.height != 0;

        // The undone blocks are the last published blocks, newest first.
        // A rollback can reach past the points we track; those are not checked.
        for (undone, expected) in message.undo.iter().zip(published.iter().rev()) {
            if !same_point(undone, expected) {
                bail!(
                    "message undoes {}, but the published block there is {}",
                    point_to_string(undone),
                    point_to_string(expected)
                );
            }
        }
        for pair in message.undo.windows(2) {
            if heights_known(&pair[0], &pair[1]) && pair[0].height != pair[1].height + 1 {
                bail!(
                    "message undoes {} and then {}, which are not consecutive blocks",
                    point_to_string(&pair[0]),
                    point_to_string(&pair[1])
                );
            }
        }
        if let Some(lowest) = message.undo.last() {
            if heights_known(lowest, advance) && advance.height != lowest.height {
                bail!(
                    "message undoes down to {}, but advances to {}",
                    point_to_string(lowest),
                    point_to_string(advance)
                );
            }
        }

        // The block the message advances from: the last published block left
        // once the undos are applied.
        let parent = published
            .len()
            .checked_sub(message.undo.len() + 1)
            .map(|index| &published[index]);
        if let Some(parent) = parent {
            if advance.slot <= parent.slot {
                bail!(
                    "message advances to {}, which is not after the published block {}",
                    point_to_string(advance),
                    point_to_string(parent)
                );
            }
            if heights_known(parent, advance) && advance.height != parent.height + 1 {
                bail!(
                    "message advances to {}, which is not the block after the published block {}",
                    point_to_string(advance),
                    point_to_string(parent)
                );
            }
        }
        Ok(())
    }

    pub async fn broadcast(
        &mut self,
        block: Block,
        message: BroadcastMessage,
    ) -> Result<Broadcast> {
        let mut destinations = vec![];
        let mut advanced = false;
        // For each destination
        for destination in &mut self.destinations {
            let matches_filter = destination
                .filter
                .as_ref()
                .is_none_or(|f| f.applies_block(&block));

            let broadcast_message = match Self::next_action(destination, &message, matches_filter) {
                BroadcastAction::BroadcastAndCommit(message) => Some(message),
                BroadcastAction::Commit => None,
                BroadcastAction::Ignore => continue,
            };
            advanced = true;
            if let Some(message) = broadcast_message {
                Self::check_continuity(destination, &message).context(format!(
                    "refusing to publish to destination {}",
                    destination.pk
                ))?;
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
                // The write is bounded, so that one started before the deadline
                // cannot reach the stream after the lock has expired.
                let result = timeout(
                    self.put_timeout,
                    self.kinesis
                        .put_record()
                        .partition_key("sundae-sync-v2")
                        .data(Blob::new(message_bytes.clone()))
                        .stream_arn(&destination.stream_arn)
                        .send(),
                )
                .await
                .context(format!(
                    "timed out writing to kinesis after {:?}",
                    self.put_timeout
                ))?
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

    #[test]
    fn next_action_should_ignore_the_last_seen_point_sent_again() {
        let destination = test_destination(make_point(101), vec![make_point(100), make_point(101)]);
        let mut again = make_point(101);
        // The upstream block carries a height; the stored point does not.
        again.height = 7;
        let message = BroadcastMessage {
            undo: vec![],
            advance: again,
        };
        for matches_filter in [true, false] {
            assert_eq!(
                Broadcaster::next_action(&destination, &message, matches_filter),
                BroadcastAction::Ignore,
            );
        }
    }

    #[test]
    fn next_action_should_ignore_an_unpublished_last_seen_point_sent_again() {
        let destination = test_destination(make_point(103), vec![make_point(100), make_point(101)]);
        let message = BroadcastMessage {
            undo: vec![],
            advance: make_point(103),
        };
        assert_eq!(
            Broadcaster::next_action(&destination, &message, true),
            BroadcastAction::Ignore,
        );
    }

    fn make_block(height: u64) -> BlockRef {
        make_block_with_hash(height, &format!("hash-{}", height))
    }

    // A block at a height, 20 slots after the one before it
    fn make_block_with_hash(height: u64, hash: &str) -> BlockRef {
        BlockRef {
            slot: height * 20,
            hash: bytes::Bytes::from(hash.as_bytes().to_vec()),
            timestamp: 0,
            height,
        }
    }

    fn without_height(mut point: BlockRef) -> BlockRef {
        point.height = 0;
        point
    }

    fn published(heights: std::ops::RangeInclusive<u64>) -> Destination {
        let points: Vec<_> = heights.map(make_block).collect();
        test_destination(points.last().unwrap().clone(), points)
    }

    fn continuity(destination: &Destination, advance: BlockRef, undo: Vec<BlockRef>) -> Result<()> {
        Broadcaster::check_continuity(destination, &BroadcastMessage { undo, advance })
    }

    #[test]
    fn check_continuity_accepts_a_chain() {
        let destination = published(98..=100);
        // the next block
        assert!(continuity(&destination, make_block(101), vec![]).is_ok());
        // a rollback of the last block
        assert!(continuity(
            &destination,
            make_block_with_hash(100, "other 100"),
            vec![make_block(100)]
        )
        .is_ok());
        // a rollback of two blocks
        assert!(continuity(
            &destination,
            make_block_with_hash(99, "other 99"),
            vec![make_block(100), make_block(99)]
        )
        .is_ok());
        // a rollback past the points we track
        assert!(continuity(
            &destination,
            make_block_with_hash(96, "other 96"),
            vec![
                make_block(100),
                make_block(99),
                make_block(98),
                make_block(97),
                make_block(96)
            ]
        )
        .is_ok());
        // the first message of a destination
        let empty = test_destination(make_block(100), vec![]);
        assert!(continuity(&empty, make_block(500), vec![]).is_ok());
    }

    #[test]
    fn check_continuity_refuses_a_broken_chain() {
        let destination = published(98..=100);
        let cases = vec![
            ("the same block again", make_block(100), vec![]),
            (
                "a block replaced without an undo",
                make_block_with_hash(100, "other 100"),
                vec![],
            ),
            ("a skipped block", make_block(102), vec![]),
            ("an older block", make_block(99), vec![]),
            (
                "an undo of a block that was not published",
                make_block_with_hash(100, "other 100"),
                vec![make_block_with_hash(100, "never published")],
            ),
            (
                "an undo that does not start at the last block",
                make_block_with_hash(99, "other 99"),
                vec![make_block(99)],
            ),
            (
                "an undo with a hole",
                make_block_with_hash(96, "other 96"),
                vec![
                    make_block(100),
                    make_block(99),
                    make_block(98),
                    make_block(96),
                ],
            ),
            (
                "an advance above the lowest undone block",
                make_block_with_hash(100, "other 100"),
                vec![make_block(100), make_block(99)],
            ),
            (
                "an advance below the lowest undone block",
                make_block_with_hash(99, "other 99"),
                vec![make_block(100)],
            ),
        ];
        for (name, advance, undo) in cases {
            assert!(
                continuity(&destination, advance, undo).is_err(),
                "accepted {name}"
            );
        }
    }

    #[test]
    fn check_continuity_compares_only_known_heights() {
        // Points stored before heights were stored load with height 0.
        let points: Vec<_> = (98..=100).map(|h| without_height(make_block(h))).collect();
        let destination = test_destination(points[2].clone(), points.clone());
        assert!(continuity(&destination, make_block(101), vec![]).is_ok());
        assert!(continuity(
            &destination,
            make_block_with_hash(100, "other 100"),
            vec![points[2].clone()]
        )
        .is_ok());
        // Slots and hashes are still compared.
        assert!(continuity(&destination, make_block(100), vec![]).is_err());
        assert!(continuity(
            &destination,
            make_block_with_hash(100, "other 100"),
            vec![points[1].clone()]
        )
        .is_err());
    }

    #[test]
    fn check_continuity_skips_filtered_destinations() {
        let mut destination = published(98..=100);
        destination.filter = Some(crate::broadcast::filter::FilterConfig::Any(vec![]));
        assert!(continuity(&destination, make_block(105), vec![]).is_ok());
    }
}
