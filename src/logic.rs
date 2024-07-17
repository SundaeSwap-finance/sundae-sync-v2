use anyhow::*;
use aws_sdk_s3::Client as S3Client;
use hex::ToHex;
use tracing::{instrument, trace};
use utxorpc::spec::cardano::Block;

pub struct SundaeSyncLogic {
    pub s3: S3Client,

    pub bucket: String,
}

impl SundaeSyncLogic {
    #[instrument(name = "handle_block", skip_all, fields(block_hash))]
    pub async fn handle_block(&self, cbor: Vec<u8>, block: Block) -> Result<()> {
        let header = block.header.clone().unwrap();
        let hash = header.hash.encode_hex::<String>();
        tracing::Span::current().record("block_hash", hash.clone());

        self.archive(cbor, block).await?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn archive(&self, cbor: Vec<u8>, block: Block) -> Result<()> {
        let header = block.header.unwrap();
        let hash = header.hash.encode_hex::<String>();

        let (prefix, rest) = hash.split_at(2);
        let key = format!("blocks/by-hash/{}/{}{}.cbor", prefix, prefix, rest);
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(cbor.into())
            .send()
            .await?;
        trace!("Block saved to S3");

        let prefix = header.height / 100_000;
        let key = format!("blocks/by-height/{}/block-{}.ptr", prefix, header.height);
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(vec![].into())
            .metadata("x-sundae-block-hash", hash)
            .send()
            .await?;
        trace!("Pointer saved to S3");

        Ok(())
    }
    /*
        async fn broadcast(&self, message: KinesisMessage) -> Result<()> {
            let data = serde_json::to_vec(&message)?;
            /*self.kinesis
            .put_record()
            .stream_arn(&self.stream)
            .data(Blob::new(data))
            .send()
            .await?;*/
            trace!("Broadcast to Kinesis");
            Ok(())
        }
    */
    pub async fn handle_undo(&self, _cbor: Vec<u8>, block: Block) -> Result<()> {
        println!("undo: {}", block.header.unwrap().slot);
        Ok(())
    }
}
