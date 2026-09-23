use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use aws_sdk_dynamodb::{
    types::{PutRequest, TransactWriteItem, Update, WriteRequest},
    Client as DynamoClient,
};
use aws_sdk_s3::Client as S3Client;
use futures::{stream, StreamExt, TryStreamExt};
use hex::ToHex;
use pallas::interop::utxorpc::{LedgerContext, Mapper};
use serde::{Deserialize, Serialize};
use serde_bytes_base64::Bytes;
use serde_dynamo::{to_attribute_value, to_item};
use tracing::{trace, warn};
use utxorpc::spec::cardano::{asset::Quantity, Block, Datum as utxorpcDatum, Redeemer, Script};

use crate::utils::{bigint_to_string, bigint_to_u64, elapsed};

// Keep both request count and payload size bounded for large blocks.
const MAX_CONCURRENT_POINTER_BATCHES: usize = 4;
const MAX_POINTER_BATCH_ITEMS: usize = 25;
// DynamoDB permits 16 MiB per request. Reserve room for the request envelope
// and differences between the SDK and serde_json string escaping.
const MAX_POINTER_BATCH_BYTES: usize = 15 * 1024 * 1024;
const MAX_UNPROCESSED_RETRIES: u32 = 8;

fn pointer_batches(items: Vec<serde_dynamo::Item>) -> Result<Vec<Vec<WriteRequest>>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    for item in items {
        let bytes = serde_json::to_vec(&item)?.len() + 32; // PutRequest/Item envelope
        if !batch.is_empty()
            && (batch.len() == MAX_POINTER_BATCH_ITEMS
                || batch_bytes + bytes > MAX_POINTER_BATCH_BYTES)
        {
            batches.push(std::mem::take(&mut batch));
            batch_bytes = 0;
        }
        anyhow::ensure!(
            bytes <= MAX_POINTER_BATCH_BYTES,
            "pointer exceeds batch request size limit"
        );
        batch.push(
            WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(item.into())).build()?)
                .build(),
        );
        batch_bytes += bytes;
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    Ok(batches)
}

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
pub struct Multiasset {
    pub policy_id: Bytes,
    pub assets: Vec<Asset>,
    pub redeemer: Option<Redeemer>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Asset {
    pub name: Bytes,
    pub output_coin: String,
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
            coin: value
                .coin
                .as_ref()
                .and_then(bigint_to_u64)
                .expect("value did not fit in bigint"),
            assets: value
                .assets
                .into_iter()
                .map(|m| Multiasset {
                    policy_id: m.policy_id.to_vec().into(),
                    redeemer: m.redeemer,
                    assets: m
                        .assets
                        .into_iter()
                        .map(|a| Asset {
                            name: a.name.to_vec().into(),
                            output_coin: match a.quantity {
                                Some(Quantity::OutputCoin(o)) => bigint_to_string(&o),
                                _ => "0".to_string(),
                            },
                        })
                        .collect(),
                })
                .collect(),
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
pub struct NoContext;
impl LedgerContext for NoContext {
    fn get_utxos(
        &self,
        _refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        None
    }

    fn get_slot_timestamp(&self, _slot: u64) -> Option<u64> {
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
        let mut items = vec![];
        let height_ref = HeightRef {
            pk: format!("height:{}", header.height),
            sk: "height".to_string(),
            hash: header.hash.encode_hex(),
            location: location.clone(),
        };
        items.push(to_item(height_ref)?);
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
            items.push(to_item(tx_ref)?);
        }

        let batches = pointer_batches(items)?;
        stream::iter(
            batches
                .into_iter()
                .map(|batch| self.save_pointer_batch(batch)),
        )
        .buffer_unordered(MAX_CONCURRENT_POINTER_BATCHES)
        .try_for_each(|_| async { Ok(()) })
        .await
        .context("failed to save pointers to dynamodb")?;
        trace!("Finished saving block (elapsed={:?})", elapsed(start));
        Ok(())
    }

    async fn save_pointer_batch(&self, mut pending: Vec<WriteRequest>) -> Result<()> {
        for retry in 0..=MAX_UNPROCESSED_RETRIES {
            let mut response = self
                .dynamo
                .batch_write_item()
                .request_items(&self.table_name, pending)
                .send()
                .await?;
            // HTTP 200 can still mean only part of the batch was written.
            pending = response
                .unprocessed_items
                .take()
                .and_then(|mut tables| tables.remove(&self.table_name))
                .unwrap_or_default();
            if pending.is_empty() {
                return Ok(());
            }
            if retry == MAX_UNPROCESSED_RETRIES {
                bail!(
                    "{} pointers remain unprocessed after {} retries",
                    pending.len(),
                    retry
                );
            }
            let cap_ms = (100_u64 << retry).min(5_000);
            let delay_ms = cap_ms / 2 + (uuid::Uuid::new_v4().as_u128() as u64 % (cap_ms / 2 + 1));
            warn!(
                unprocessed_count = pending.len(),
                retry = retry + 1,
                delay_ms,
                "Retrying unprocessed DynamoDB pointers"
            );
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        unreachable!("final retry returns success or an error")
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
        // DynamoDB allows at most 100 actions per transaction.
        for chunk in block.tx.chunks(100) {
            let mut ddb_tx = self.dynamo.transact_write_items();
            for tx in chunk {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_account_for_json_expansion_as_well_as_item_count() {
        // Each string is below DynamoDB's item limit, but JSON escaping makes
        // eight items exceed the request limit despite being fewer than 25.
        let items = (0..8)
            .map(|i| {
                to_item(serde_json::json!({
                    "pk": format!("tx:{i}"), "sk": "tx", "payload": "\u{1}".repeat(350_000)
                }))
                .unwrap()
            })
            .collect();
        let batches = pointer_batches(items).unwrap();
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), vec![7, 1]);
        for batch in batches {
            let writes: Vec<_> = batch
                .into_iter()
                .map(|r| {
                    let item: serde_dynamo::Item = r.put_request.unwrap().item.into();
                    serde_json::json!({"PutRequest":{"Item":item}})
                })
                .collect();
            let request = serde_json::json!({"RequestItems":{"test-lookup":writes}});
            assert!(serde_json::to_vec(&request).unwrap().len() < 16 * 1024 * 1024);
        }
    }
}
