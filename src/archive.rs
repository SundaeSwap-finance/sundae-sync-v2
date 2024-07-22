use std::time::SystemTime;

use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use hex::ToHex;
use pallas::interop::utxorpc::{LedgerContext, Mapper};
use tracing::trace;
use utxorpc::spec::cardano::Block;

use crate::utils::elapsed;

#[derive(Clone)]
pub struct Archive {
    pub s3: S3Client,
    pub bucket: String,
}

fn block_hash_key(hash: impl ToHex) -> String {
    let hash: String = hash.encode_hex();
    let (prefix, rest) = hash.split_at(2);
    format!("blocks/by-hash/{}/{}{}.cbor", prefix, prefix, rest)
}

#[derive(Clone)]
struct NoContext;
impl LedgerContext for NoContext {
    fn get_utxos(
        &self,
        _refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        None
    }
}

impl Archive {
    pub async fn save(&self, block: &Block, bytes: Vec<u8>) -> Result<()> {
        let start = SystemTime::now();
        let header = block.header.as_ref().expect("must have header");

        // Save the raw bytes of the block, indexed by its hash
        self.save_raw_block(&header.hash, bytes).await?;

        trace!("Finished saving block (elapsed={:?})", elapsed(start));
        Ok(())
    }

    pub async fn read_by_hash(&self, hash: impl ToHex) -> Result<Block> {
        let response = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(block_hash_key(hash))
            .send()
            .await?;
        let bytes = response.body.collect().await?;

        let mapper = Mapper::new(NoContext);
        let block = mapper.map_block_cbor(bytes.to_vec().as_slice());
        Ok(block)
    }

    pub async fn unsave(&self, _block: &Block) -> Result<()> {
        // Might add something here later
        Ok(())
    }

    async fn save_raw_block(&self, hash: impl ToHex, bytes: Vec<u8>) -> Result<()> {
        let start = SystemTime::now();
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(block_hash_key(hash))
            .body(bytes.into())
            .send()
            .await?;
        trace!("Finished uploading block (elapsed={:?})", elapsed(start));
        Ok(())
    }
}
