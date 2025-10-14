use crate::broadcast::BroadcastMessage;

use super::filter::FilterConfig;
use anyhow::{Context, Result};
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use aws_sdk_kinesis::{types::ShardIteratorType, Client as KinesisClient};
use bytes::Bytes;
use hex::ToHex;
use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use tracing::{info, warn};
use utxorpc::spec::sync::BlockRef;

#[derive(Serialize, Deserialize, Debug)]
pub struct Destination {
    pub pk: String,
    pub stream_arn: String,
    pub shard_id: String,
    pub filter: Option<FilterConfig>,
    pub sequence_number: Option<String>,
    #[serde(
        serialize_with = "serialize_point",
        deserialize_with = "deserialize_point"
    )]
    pub last_seen_point: BlockRef,
    #[serde(
        serialize_with = "serialize_points",
        deserialize_with = "deserialize_points"
    )]
    pub recovery_points: Vec<BlockRef>,
    pub enabled: bool,
    #[serde(default)]
    pub skip_repair: bool,
}

impl Destination {
    /// Commit a new point / sequence number back to the destinations table
    pub async fn commit(
        &mut self,
        dynamo: &DynamoClient,
        table: &String,
        point: BlockRef,
        seq_num: Option<String>,
    ) -> Result<()> {
        // Rotate the point into the list of 15 rollback points
        // TODO: make this more sophisticated, so that we stagger points further and further back
        // rather than just using the most recent 15
        let previous_point = self.last_seen_point.clone();
        self.last_seen_point = point.clone();
        self.recovery_points.push(point.clone());
        if self.recovery_points.len() > 15 {
            self.recovery_points.remove(0);
        }
        let recovery_points = self
            .recovery_points
            .iter()
            .map(|p| AttributeValue::S(point_to_string(p)))
            .collect();
        // Update the destination (assuming someone else hasn't already updated it) to
        // - set the last seen point
        // - update the list of recovery points, for finding an intersect if we restart
        // - set the kinesis sequence number so we can inspect the queue on restart
        dynamo
            .update_item()
            .table_name(table)
            .key("pk", AttributeValue::S(self.pk.clone()))
            .condition_expression("last_seen_point = :last_point")
            .update_expression(
                "SET last_seen_point = :new_point, sequence_number = :seq, recovery_points = :rotated_points",
            )
            .expression_attribute_values(
                ":last_point",
                AttributeValue::S(point_to_string(&previous_point)),
            )
            .expression_attribute_values(":seq", seq_num.map_or(AttributeValue::Null(true), AttributeValue::S))
            .expression_attribute_values(":new_point", AttributeValue::S(point_to_string(&point)))
            .expression_attribute_values(":rotated_points", AttributeValue::L(recovery_points))
            .send()
            .await
            .map_err(|e| {
                // Check if this is a conditional check failure, which indicates another worker
                // has taken over (normal during failover)
                if let Some(service_err) = e.as_service_error() {
                    if service_err.is_conditional_check_failed_exception() {
                        return anyhow::anyhow!(
                            "Another worker has updated destination {} (likely failover occurred). \
                             Expected point: {}, but destination has advanced. \
                             This is normal behavior when a new worker takes over the lock.",
                            self.pk,
                            point_to_string(&previous_point)
                        );
                    }
                }
                anyhow::anyhow!("failed to update destination {}: {}", self.pk, e)
            })?;
        Ok(())
    }

    pub async fn repair(
        &mut self,
        kinesis: KinesisClient,
        dynamo: DynamoClient,
        table: String,
    ) -> Result<BlockRef> {
        // Skip the repair step, and force using the last seen point
        if self.skip_repair {
            return Ok(self.last_seen_point.clone());
        }

        // Repair fills gaps where Kinesis broadcast succeeded but DynamoDB commit failed
        // by reading messages from Kinesis (source of truth) and attempting to commit them.
        // The conditional check in commit() prevents races with other workers.

        let mut shard_request = kinesis
            .get_shard_iterator()
            .stream_arn(&self.stream_arn)
            .shard_id(&self.shard_id);
        if let Some(seq_no) = &self.sequence_number {
            shard_request = shard_request
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .starting_sequence_number(seq_no);
        } else {
            warn!(
                "No sequence number for destination {}, starting from trim horizon",
                self.pk
            );
            shard_request = shard_request.shard_iterator_type(ShardIteratorType::TrimHorizon)
        }
        let mut iterator = shard_request
            .send()
            .await
            .context("failed to request shard iterator")?
            .shard_iterator
            .context("shard iterator is none")?;

        info!("Repairing destination {}", self.pk);

        // Limit repair iterations to prevent infinite loops
        const MAX_REPAIR_ITERATIONS: usize = 1000;
        for iteration in 0..MAX_REPAIR_ITERATIONS {
            let records = kinesis
                .get_records()
                .stream_arn(&self.stream_arn)
                .shard_iterator(&iterator)
                .send()
                .await
                .context(format!(
                    "failed to fetch records from stream {}",
                    self.stream_arn
                ))?;

            iterator = records
                .next_shard_iterator
                .context("next shard iterator is none")?;

            let millis_behind_latest = records.millis_behind_latest.unwrap_or(0);

            if !records.records.is_empty() {
                info!(
                    "Repair iteration {}: {} records, {:?}ms behind tip",
                    iteration,
                    records.records.len(),
                    millis_behind_latest
                );

                let last_record = records.records.into_iter().last().unwrap();
                let seq_no = last_record.sequence_number;
                let data = last_record.data.into_inner();
                let message: BroadcastMessage =
                    serde_json::from_slice(data.as_slice()).context("failed to parse data")?;
                let point = message.advance;

                // Try to commit with conditional check - this prevents zombie worker races
                match self
                    .commit(&dynamo, &table, point.clone(), Some(seq_no))
                    .await
                {
                    Ok(_) => {
                        info!("Repaired destination {} to point {}", self.pk, point.index);
                    }
                    Err(e) => {
                        // Check if this is a conditional check failure
                        let err_msg = e.to_string();
                        if err_msg.contains("Another worker has updated destination") {
                            // Another worker already advanced past this point - we're caught up
                            info!(
                                "Destination {} already advanced past repair point {}, repair complete",
                                self.pk, point.index
                            );
                            break;
                        }
                        // Other errors should propagate
                        return Err(e);
                    }
                }
            }

            if millis_behind_latest == 0 {
                info!(
                    "Repair complete for destination {}, caught up to tip",
                    self.pk
                );
                break;
            }

            if iteration == MAX_REPAIR_ITERATIONS - 1 {
                warn!(
                    "Repair hit max iterations ({}) for destination {}, still {}ms behind",
                    MAX_REPAIR_ITERATIONS, self.pk, millis_behind_latest
                );
            }
        }

        Ok(self.last_seen_point.clone())
    }
}

pub fn point_to_string(point: &BlockRef) -> String {
    format!("{}/{}", point.index, point.hash.encode_hex::<String>())
}
pub fn serialize_point<S>(point: &BlockRef, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(point_to_string(point).as_str())
}
pub fn serialize_points<S>(
    points: &Vec<BlockRef>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut s = serializer.serialize_seq(Some(points.len()))?;
    for point in points {
        s.serialize_element(point_to_string(point).as_str())?;
    }
    s.end()
}

pub fn string_to_point(s: String) -> Result<BlockRef> {
    let parts: Vec<_> = s.split('/').collect();
    let index = parts[0].parse()?;
    let hash = Bytes::from_iter(hex::decode(parts[1])?);
    Ok(BlockRef { index, hash })
}
pub fn deserialize_point<'de, D>(deserializer: D) -> std::result::Result<BlockRef, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).and_then(|string| {
        string_to_point(string).map_err(|err| {
            serde::de::Error::custom(format!("failed to deserialize point: {}", err))
        })
    })
}
pub fn deserialize_points<'de, D>(deserializer: D) -> std::result::Result<Vec<BlockRef>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer).and_then(|strings| {
        strings
            .into_iter()
            .map(|s| {
                string_to_point(s).map_err(|err| {
                    serde::de::Error::custom(format!("failed to deserialize point: {}", err))
                })
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_to_string() {
        let point = BlockRef {
            index: 12345,
            hash: bytes::Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
        };
        let result = point_to_string(&point);
        assert_eq!(result, "12345/deadbeef");
    }

    #[test]
    fn test_string_to_point() {
        let input = "12345/deadbeef".to_string();
        let result = string_to_point(input).unwrap();
        assert_eq!(result.index, 12345);
        assert_eq!(
            result.hash,
            bytes::Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])
        );
    }

    #[test]
    fn test_point_roundtrip() {
        let original = BlockRef {
            index: 98765,
            hash: bytes::Bytes::from(vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]),
        };
        let serialized = point_to_string(&original);
        let deserialized = string_to_point(serialized).unwrap();
        assert_eq!(original.index, deserialized.index);
        assert_eq!(original.hash, deserialized.hash);
    }

    #[test]
    fn test_string_to_point_invalid_format() {
        let result = string_to_point("invalid".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_string_to_point_invalid_hex() {
        let result = string_to_point("12345/notvalidhex".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_string_to_point_invalid_index() {
        let result = string_to_point("notanumber/deadbeef".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_destination_serde_roundtrip() {
        let dest = Destination {
            pk: "test-dest".to_string(),
            stream_arn: "arn:aws:kinesis:us-east-1:123456789:stream/test".to_string(),
            shard_id: "shard-0".to_string(),
            filter: None,
            sequence_number: Some("12345".to_string()),
            last_seen_point: BlockRef {
                index: 100,
                hash: bytes::Bytes::from(vec![0xaa, 0xbb, 0xcc, 0xdd]),
            },
            recovery_points: vec![
                BlockRef {
                    index: 90,
                    hash: bytes::Bytes::from(vec![0x11, 0x22, 0x33, 0x44]),
                },
                BlockRef {
                    index: 95,
                    hash: bytes::Bytes::from(vec![0x55, 0x66, 0x77, 0x88]),
                },
            ],
            enabled: true,
            skip_repair: false,
        };

        let json = serde_json::to_string(&dest).unwrap();
        let deserialized: Destination = serde_json::from_str(&json).unwrap();

        assert_eq!(dest.pk, deserialized.pk);
        assert_eq!(
            dest.last_seen_point.index,
            deserialized.last_seen_point.index
        );
        assert_eq!(dest.last_seen_point.hash, deserialized.last_seen_point.hash);
        assert_eq!(
            dest.recovery_points.len(),
            deserialized.recovery_points.len()
        );
        assert_eq!(
            dest.recovery_points[0].index,
            deserialized.recovery_points[0].index
        );
    }
}
