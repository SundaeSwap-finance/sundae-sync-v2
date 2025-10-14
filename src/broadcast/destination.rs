use crate::broadcast::BroadcastMessage;

use super::filter::FilterConfig;
use anyhow::{Context, Result};
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use aws_sdk_kinesis::{types::ShardIteratorType, Client as KinesisClient};
use bytes::Bytes;
use hex::ToHex;
use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use tracing::{info, trace, warn};
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
        // read from kinesis
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
        loop {
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

            info!(
                "Received {} records, {:?}ms behind tip",
                records.records.len(),
                records.millis_behind_latest
            );
            let millis_behind_latest = records.millis_behind_latest.unwrap_or(0);
            if !records.records.is_empty() {
                let last_record = records.records.into_iter().last().unwrap();
                let seq_no = last_record.sequence_number;
                let data = last_record.data.into_inner();
                let message: BroadcastMessage =
                    serde_json::from_slice(data.as_slice()).context("failed to parse data")?;
                let advance = message.advance;
                self.commit(&dynamo, &table, advance, Some(seq_no))
                    .await
                    .context("failed to commit while repairing")?;
            }

            if millis_behind_latest == 0 {
                break;
            }
        }
        trace!("Repaired destination {} sequence number", self.pk);

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
