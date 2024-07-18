// use aws_sdk_dynamodb::Client as DynamoClient;
// use aws_sdk_kinesis::Client as KinesisClient;
// use utxorpc::spec::sync::BlockRef;

// use crate::filter::FilterConfig;
use anyhow::Result;
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_1::from_items;
use utxorpc::spec::sync::BlockRef;

use crate::filter::FilterConfig;

#[derive(Serialize, Deserialize, Debug)]
pub struct Destination {
    pub pk: String,
    pub sk: String,
    pub stream_arn: String,
    pub filter: Option<FilterConfig>,
    pub point: BlockRef,
    pub enabled: bool,
}

pub async fn load_destinations(dynamo: DynamoClient) -> Result<Vec<Destination>> {
    let resp = dynamo
        .scan()
        .consistent_read(true)
        .filter_expression("pk = :key and enabled = :enabled")
        .expression_attribute_values(
            ":key",
            AttributeValue::S("sundae-sync-v2-destination".into()),
        )
        .expression_attribute_values(":enabled", AttributeValue::Bool(true))
        .table_name("sundae-sync-v2-test-table")
        .send()
        .await?;

    Ok(from_items(resp.items().to_vec())?)
}

// pub struct Broadcaster {
//     point: BlockRef,
//     pub destinations: Vec<Destination>,
//     pub kinesis: KinesisClient,
//     pub dynamo: DynamoClient,
// }
/*
impl Broadcaster {
    pub fn new() -> Self {
        Self {

        }
    }

    pub async fn broadcast(&self, message_type: MessageType, block: Block) -> Result<()> {
        let header = block.header.clone().unwrap();
        let message = BroadcastMessage {
            message_type,
            block_hash: header.hash.encode_hex(),
            block_height: header.height,
            slot: header.slot,
        };
        let message_bytes = serde_json::to_vec(&message)?;
        if let Some(body) = block.body {
            'outer: for (label, stream, rule) in &self.destinations {
                for tx in &body.tx {
                    if rule.applies(&tx)? {
                        info!(
                            "Sending block {} (height={}, slot={}) to {}",
                            message.block_hash, message.block_height, message.slot, label
                        );
                        self.kinesis
                            .put_record()
                            .stream_arn(stream)
                            .data(Blob::new(message_bytes.clone()))
                            .send()
                            .await?;
                        continue 'outer;
                    }
                }
            }
        }
        Ok(())
    }
}
*/
