// use aws_sdk_dynamodb::Client as DynamoClient;
// use aws_sdk_kinesis::Client as KinesisClient;
// use utxorpc::spec::sync::BlockRef;

use std::time::{SystemTime, UNIX_EPOCH};

// use crate::filter::FilterConfig;
use anyhow::Result;
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_s3::primitives::Blob;
use bytes::Bytes;
use hex::ToHex;
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use tokio::sync::watch::Receiver;
use tracing::info;
use utxorpc::spec::{cardano::Block, sync::BlockRef};

use crate::filter::FilterConfig;

#[derive(Serialize, Deserialize, Debug)]
pub struct Destination {
    pub pk: String,
    pub sk: String,
    pub stream_arn: String,
    pub filter: Option<FilterConfig>,
    pub sequence_number: Option<String>,
    pub last_seen_point: String,
    pub recovery_points: Vec<String>,
    pub enabled: bool,
}

pub struct Broadcaster {
    pub destinations: Vec<Destination>,
    pub kinesis: KinesisClient,
    pub dynamo: DynamoClient,
    pub table: String,
    pub deadline: Receiver<u64>,
}

#[derive(Serialize, Deserialize)]
pub enum BroadcastMessage {
    Roll(BlockRef),
    Undo(BlockRef),
}

impl BroadcastMessage {
    fn point(&self) -> BlockRef {
        match self {
            BroadcastMessage::Roll(point) => point,
            BroadcastMessage::Undo(point) => point,
        }
        .clone()
    }
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
            .filter_expression("pk = :key and enabled = :enabled")
            .expression_attribute_values(
                ":key",
                AttributeValue::S("sundae-sync-v2-destination".into()),
            )
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

    pub async fn broadcast(&mut self, block: Block, message: BroadcastMessage) -> Result<()> {
        let message_bytes = serde_json::to_vec(&message)?;
        for destination in &self.destinations {
            let applies = destination
                .filter
                .as_ref()
                .map_or(true, |f| f.applies_block(&block));
            if applies {
                self.deadline
                    .wait_for(|deadline| {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("time went backwards")
                            .as_millis() as u64;
                        now < *deadline
                    })
                    .await?;

                let result = self
                    .kinesis
                    .put_record()
                    .partition_key("partition")
                    .data(Blob::new(message_bytes.clone()))
                    .stream_arn(&destination.stream_arn)
                    .send()
                    .await?;

                self.commit(message.point(), result.sequence_number, &destination)
                    .await?;
            }
        }
        // TODO: move into commit?
        let point = point_to_string(message.point());
        for destination in &mut self.destinations {
            destination.last_seen_point = point.clone();
            destination.recovery_points.push(point.clone());
            if destination.recovery_points.len() > 15 {
                destination.recovery_points = destination.recovery_points[1..].to_vec();
            }
        }
        Ok(())
    }

    pub async fn commit(
        &self,
        point: BlockRef,
        seq_num: String,
        destination: &Destination,
    ) -> Result<()> {
        let mut new_recovery = destination.recovery_points.clone();
        let point = point_to_string(point);
        new_recovery.push(point.clone());
        if new_recovery.len() > 15 {
            new_recovery = new_recovery[1..].to_vec();
        }
        let new_recovery = new_recovery
            .into_iter()
            .map(|p| AttributeValue::S(p))
            .collect();
        self.dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(destination.pk.clone()))
            .key("sk", AttributeValue::S(destination.sk.clone()))
            .condition_expression("last_seen_point = :last_point")
            .update_expression(
                "SET last_seen_point = :new_point, sequence_number = :seq, recovery_points = :rotated_points",
            )
            .expression_attribute_values(
                ":last_point",
                AttributeValue::S(destination.last_seen_point.clone()),
            )
            .expression_attribute_values(":seq", AttributeValue::S(seq_num))
            .expression_attribute_values(":new_point", AttributeValue::S(point.clone()))
            .expression_attribute_values(":rotated_points", AttributeValue::L(new_recovery))
            .send()
            .await?;
        Ok(())
    }
}

pub fn point_to_string(point: BlockRef) -> String {
    format!("{}/{}", point.index, point.hash.encode_hex::<String>())
}

pub fn string_to_point(s: String) -> Result<BlockRef> {
    let parts: Vec<_> = s.split('/').collect();
    let index = parts[0].parse()?;
    let hash = Bytes::from_iter(hex::decode(parts[1])?.into_iter());
    Ok(BlockRef { index, hash })
}
