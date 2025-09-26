use std::time::SystemTime;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::{
    types::{TransactWriteItem, Update},
    Client as DynamoClient,
};
use aws_sdk_s3::Client as S3Client;
use futures::future::try_join_all;
use hex::ToHex;
use pallas::interop::utxorpc::{LedgerContext, Mapper};
use serde::{Deserialize, Serialize};
use serde_bytes_base64::Bytes;
use serde_dynamo::{to_attribute_value, to_item};
use tracing::trace;
use utxorpc::spec::cardano::{Block, Datum as utxorpcDatum, Multiasset, Script};

use crate::utils::elapsed;

#[derive(Clone)]
pub struct Archive {
    pub s3: S3Client,
    pub bucket: String,
    pub dynamo: DynamoClient,
    pub table_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct HeightRef {
    pub pk: String,
    pub sk: String,
    pub hash: String,
    pub location: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Datum {
    Structured(utxorpcDatum),
    Raw(Bytes),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TxOutput {
    /// Address receiving the output.
    pub address: Bytes,
    /// Amount of ADA in the output.
    pub coin: u64,
    /// Additional native (non-ADA) assets in the output.
    pub assets: Vec<Multiasset>,
    /// Plutus data associated with the output.
    pub datum: Option<Datum>,
    /// Script associated with the output.
    pub script: Option<Script>,
}

impl From<utxorpc::spec::cardano::TxOutput> for TxOutput {
    fn from(value: utxorpc::spec::cardano::TxOutput) -> Self {
        TxOutput {
            address: value.address.to_vec().into(),
            coin: value.coin,
            assets: value.assets.to_vec(),
            datum: value
                .datum
                .map(|d| Datum::Raw(d.original_cbor.to_vec().into())),
            script: value.script,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TxRef {
    pub pk: String,
    pub sk: String,
    pub block: String,
    pub location: String,
    pub in_chain: bool,
    pub successful: bool,
    pub utxos: Vec<TxOutput>,
    pub collateral_out: Option<TxOutput>,
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
        self.save_raw_block(&header.hash, bytes)
            .await
            .context(format!(
                "failed to save raw block {}",
                hex::encode(&header.hash)
            ))?;

        // Then, save various lookups in dynamodb
        let location = block_hash_key(&header.hash);
        let mut tasks = vec![];
        let height_ref = HeightRef {
            pk: format!("height:{}", header.height),
            sk: "height".to_string(),
            hash: header.hash.encode_hex(),
            location: location.clone(),
        };
        tasks.push(
            self.dynamo
                .put_item()
                .table_name(self.table_name.clone())
                .set_item(Some(to_item(height_ref)?))
                .send(),
        );
        let body = block
            .body
            .clone()
            .context("expected block to have a body")?;
        for tx in body.tx {
            let tx_ref = TxRef {
                pk: format!("tx:{}", tx.hash.encode_hex::<String>()),
                sk: "tx".to_string(),
                block: header.hash.encode_hex(),
                location: location.clone(),
                in_chain: true,
                utxos: tx.outputs.into_iter().map(|o| o.into()).collect(),
                successful: tx.successful,
                collateral_out: tx
                    .collateral
                    .and_then(|c| c.collateral_return.map(|o| o.into())),
            };
            tasks.push(
                self.dynamo
                    .put_item()
                    .table_name(self.table_name.clone())
                    .set_item(Some(to_item(tx_ref)?))
                    .send(),
            );
        }

        try_join_all(tasks)
            .await
            .context("failed to save pointers to dynamodb")?;
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

    pub async fn unsave(&self, block: &Block) -> Result<()> {
        let block = block.body.clone().context("expected block body")?;
        if !block.tx.is_empty() {
            let mut ddb_tx = self.dynamo.transact_write_items();
            for tx in block.tx {
                let tx_update = Update::builder()
                    .table_name(self.table_name.clone())
                    .key(
                        "pk",
                        to_attribute_value(format!("tx:{}", tx.hash.encode_hex::<String>()))?,
                    )
                    .key("sk", to_attribute_value("tx")?)
                    .update_expression("SET in_chain = :in_chain")
                    .expression_attribute_values(":in_chain", to_attribute_value(false)?)
                    .build()?;
                let write_item = TransactWriteItem::builder().update(tx_update).build();
                ddb_tx = ddb_tx.transact_items(write_item);
            }
            ddb_tx
                .send()
                .await
                .context("failed to mark txs as off-chain")?;
        }
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
