// use aws_sdk_dynamodb::Client as DynamoClient;
// use aws_sdk_kinesis::Client as KinesisClient;
// use utxorpc::spec::sync::BlockRef;

// use crate::filter::FilterConfig;

// pub struct Destination {
//     label: String,
//     stream_arn: String,
//     filter: FilterConfig,
//     point: BlockRef,
// }

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
