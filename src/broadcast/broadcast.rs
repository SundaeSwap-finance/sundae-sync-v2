use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
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
        // For each destination
        for destination in &mut self.destinations {
            let point = message.point();
            // Ignore this destination if we're further back in the chain
            if destination.last_seen_point.index > point.index {
                continue;
            }
            // Check if we *should* send to this destination,
            // based on whether any of the transactions match the criteria
            let applies = destination
                .filter
                .as_ref()
                .map_or(true, |f| f.applies_block(&block));
            // If so
            if applies {
                // Check/wait for us to be comfortably within the lock expiration deadline
                self.deadline
                    .wait_for(|deadline| {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("time went backwards")
                            .as_millis() as u64;
                        now < *deadline
                    })
                    .await?;

                // then send to kinesis, and save the point/seq number back to the destination
                let result = self
                    .kinesis
                    .put_record()
                    .partition_key("partition")
                    .data(Blob::new(message_bytes.clone()))
                    .stream_arn(&destination.stream_arn)
                    .send()
                    .await?;
                destination
                    .commit(
                        &self.dynamo,
                        &self.table,
                        point,
                        Some(result.sequence_number),
                    )
                    .await?;
            } else {
                // If the block doesn't apply, we still advance the point
                // with the same sequence number
                // so that we don't replay excessively if a filter makes
                // hits rare
                destination
                    .commit(
                        &self.dynamo,
                        &self.table,
                        point,
                        destination.sequence_number.clone(),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
