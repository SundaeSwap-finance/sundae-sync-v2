use anyhow::Result;
use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoClient};
use aws_sdk_s3::{primitives::Blob, Client as S3Client};
use bytes::Bytes;
use futures::{join, stream::FuturesUnordered, StreamExt};
use hex::ToHex;
use pallas::interop::utxorpc::{LedgerContext, Mapper};
use prost::Message;
use serde::{Deserialize, Serialize};
use utxorpc::spec::cardano::{Block, TxInput, TxOutput};

#[derive(Clone)]
pub struct Archive {
    pub s3: S3Client,
    pub dynamo: DynamoClient,
    pub bucket: String,
    pub table: String,
}

fn block_hash_key(hash: impl ToHex) -> String {
    let hash: String = hash.encode_hex();
    let (prefix, rest) = hash.split_at(2);
    format!("blocks/by-hash/{}/{}{}.cbor", prefix, prefix, rest)
}

fn block_height_key(height: u64) -> String {
    let prefix = height / 100_000;
    format!("blocks/by-height/{}/{}.ptr", prefix, height)
}

fn utxo_key(hash: impl ToHex, idx: usize) -> String {
    format!("{}#{}", hash.encode_hex::<String>(), idx)
}

fn av_bytes(bytes: impl Into<Vec<u8>> + Clone) -> AttributeValue {
    AttributeValue::B(Blob::new(bytes.clone()))
}

/// A UTXO stored in dynamodb
#[derive(Serialize, Deserialize)]
pub struct DynamoUTXO {
    pk: String,
    slot: u64,
    ttl: u64,
    // protobuf serialized txout
    proto: Vec<u8>,
    // Addresses and credentials for indexing
    address: Vec<u8>,
    payment: Vec<u8>,
    staking: Option<Vec<u8>>,
    // The tx where this UTXO got spent
    // These fields are carefully structured to achieve a few goals:
    //  - We always have a record of a UTXO in case we need to look it up
    //  - We can index the table to get unspent utxos
    //  - We can handle rollbacks
    //  - Downstream consumers can query UTXOs and determine the state even if sundae-sync is ahead
    // +-------------+----------------------+----------------------------------------------------------------+
    // |   Unspent   |   SpentSlot/TxHash   |   Status                                                       |
    // +-------------+----------------------+----------------------------------------------------------------+
    // |   set       |   None               | UTXO has been seen, and is part of the ledger                  |
    // |   unset     |   Some(..)           | UTXO has been seen and spent, and is part of the history       |
    // |   unset     |   None               | UTXO was seen, but rolled back, and is not part of the history |
    // |   set       |   Some(..)           | Invalid State                                                  |
    // +-------------+----------------------+----------------------------------------------------------------+
    // unspent_slot: Option<u64>,
    // spent_slot: Option<u64>,
    // spent_tx_hash: Option<Vec<u8>>,
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
        let header = block.header.as_ref().expect("must have header");

        // We'll build up a list of different futures, and wait on
        // them all in parallel
        let save_utxos = FuturesUnordered::new();
        let spend_utxos = FuturesUnordered::new();

        // Save the raw bytes of the block, indexed by its hash
        let save_block = self.save_raw_block(&header.hash, bytes);
        // For each transaction, spend the inputs and save the outputs
        for tx in &block.body.as_ref().expect("must have body").tx {
            for input in &tx.inputs {
                spend_utxos.push(self.spend_utxo(&tx.hash, input));
            }
            for (idx, output) in tx.outputs.iter().enumerate() {
                save_utxos.push(self.save_raw_utxo(header.slot, &tx.hash, idx, output));
            }
        }

        // Wait for all the futures to finish, in any order
        let (save_block, save_utxos, spend_utxos) = join![
            save_block,
            save_utxos.collect::<Vec<_>>(),
            spend_utxos.collect::<Vec<_>>(),
        ];
        // Then ? all the results
        save_block?;
        for result in save_utxos {
            result?;
        }
        for result in spend_utxos {
            result?;
        }

        // Save the pointer only after the above futures have finished,
        // to avoid something trying to fetch by header height, and
        // finding a block that hasn't saved yet
        self.save_block_ptr(header.height, &header.hash).await?;

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
        let save_utxos = FuturesUnordered::new();
        let spend_utxos = FuturesUnordered::new();
        // "unspend" the utxos that were spent, and "unsave" the utxos that were created
        for tx in &block.body.as_ref().expect("must have body").tx {
            for input in &tx.inputs {
                spend_utxos.push(self.unspend_utxo(input));
            }
            for idx in 0..tx.outputs.len() {
                save_utxos.push(self.unsave_raw_utxo(&tx.hash, idx));
            }
        }

        // Wait for the futures to finish
        // // Wait for all the futures to finish, in any order
        let (save_utxos, spend_utxos) = join![
            save_utxos.collect::<Vec<_>>(),
            spend_utxos.collect::<Vec<_>>(),
        ];
        // Then ? all the results
        for result in save_utxos {
            result?;
        }
        for result in spend_utxos {
            result?;
        }
        Ok(())
    }

    async fn save_raw_block(&self, hash: impl ToHex, bytes: Vec<u8>) -> Result<()> {
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(block_hash_key(hash))
            .body(bytes.into())
            .send()
            .await?;
        Ok(())
    }

    async fn save_block_ptr(&self, height: u64, hash: &Bytes) -> Result<()> {
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(block_height_key(height))
            .body(hash.to_vec().into())
            .send()
            .await?;
        Ok(())
    }

    async fn save_raw_utxo(
        &self,
        slot: u64,
        hash: impl ToHex,
        idx: usize,
        output: &TxOutput,
    ) -> Result<()> {
        let key = utxo_key(hash, idx);
        let mut bytes: Vec<u8> = vec![];
        let address = av_bytes(output.address.clone());
        let payment = av_bytes(output.address[1..29].to_vec());
        let staking = if output.address.len() > 29 {
            av_bytes(output.address[29..].to_vec())
        } else {
            AttributeValue::Null(true)
        };
        output.encode(&mut bytes)?;
        self.dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(key))
            .key("sk", AttributeValue::S("utxo".to_string())) // TODO
            .update_expression(
                r#"SET proto   = :proto,
                       slot    = :slot,
                       address = :address,
                       payment = :payment,
                       staking = :staking,
                       unspent = :unspent
                "#,
            )
            .expression_attribute_values(":proto", av_bytes(bytes))
            .expression_attribute_values(":slot", AttributeValue::N(slot.to_string()))
            .expression_attribute_values(":address", address)
            .expression_attribute_values(":payment", payment)
            .expression_attribute_values(":staking", staking)
            .expression_attribute_values(":unspent", AttributeValue::Bool(true))
            .send()
            .await?;
        Ok(())
    }

    async fn unsave_raw_utxo(&self, _hash: impl ToHex, _idx: usize) -> Result<()> {
        // TODO
        // let key = utxo_key(hash, idx);
        // self.dynamo
        //     .update_item()
        //     .table_name(&self.table)
        //     .key("pk", AttributeValue::S(key))
        //     .key("sk", AttributeValue::S("utxo".to_string()))
        //     .update_expression("DELETE unspent, spent_tx_hash")
        //     .send()
        //     .await?;
        Ok(())
    }

    async fn spend_utxo(&self, _tx_hash: &Bytes, _input: &TxInput) -> Result<()> {
        // TODO
        // let key = utxo_key(&input.tx_hash, input.output_index as usize);
        // self.dynamo
        //     .update_item()
        //     .table_name(&self.table)
        //     .key("pk", AttributeValue::S(key))
        //     .key("sk", AttributeValue::S("utxo".to_string())) // TODO
        //     .update_expression("SET spent_tx_hash = :tx_hash, DELETE unspent")
        //     .expression_attribute_values(":tx_hash", av_bytes(tx_hash.clone()))
        //     .send()
        //     .await?;
        Ok(())
    }

    async fn unspend_utxo(&self, _input: &TxInput) -> Result<()> {
        // TODO
        // let key = utxo_key(&input.tx_hash, input.output_index as usize);
        // self.dynamo
        //     .update_item()
        //     .table_name(&self.table)
        //     .key("pk", AttributeValue::S(key))
        //     .key("sk", AttributeValue::S("utxo".to_string())) // TODO
        //     .update_expression("SET unspent = :unspent, DELETE spent_tx_hash")
        //     .expression_attribute_values(":unspent", AttributeValue::Bool(true))
        //     .send()
        //     .await?;
        Ok(())
    }
}
