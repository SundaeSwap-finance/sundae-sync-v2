use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::{retry::RetryConfig, Credentials, Region};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use serde_json::{json, Value};
use sundae_sync_v2::{
    archive::{Archive, NoContext},
    utils::u64_to_bigint,
};
use utxorpc_spec::utxorpc::v1beta::cardano::{
    Block, BlockBody, BlockHeader, Collateral, Tx, TxOutput,
};

fn block(tx_count: u32) -> Block {
    Block {
        body: Some(BlockBody {
            tx: (0..tx_count)
                .map(|index| Tx {
                    hash: index.to_be_bytes().repeat(8).into(),
                    ..Default::default()
                })
                .collect(),
        }),
        ..Default::default()
    }
}

fn archive(statuses: &[u16]) -> (Archive, StaticReplayClient) {
    let http = StaticReplayClient::new(
        statuses
            .iter()
            .map(|status| {
                let body = if *status == 200 {
                    "{}"
                } else {
                    r#"{"__type":"ValidationException","message":"simulated batch failure"}"#
                };
                ReplayEvent::new(
                    http::Request::new(SdkBody::empty()),
                    http::Response::builder()
                        .status(*status)
                        .header("content-type", "application/x-amz-json-1.0")
                        .body(SdkBody::from(body))
                        .unwrap(),
                )
            })
            .collect(),
    );
    // Explicit credentials and an in-memory transport keep tests independent of AWS and Docker.
    let config = aws_config::SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::SharedCredentialsProvider::new(
            Credentials::new("test", "test", None, None, "test"),
        ))
        .http_client(http.clone())
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .build();
    (
        Archive {
            dynamo: aws_sdk_dynamodb::Client::new(&config),
            s3: aws_sdk_s3::Client::new(&config),
            bucket: "unused".into(),
            table_name: "test-lookup".into(),
        },
        http,
    )
}

fn captured_batches(http: &StaticReplayClient) -> Vec<Vec<Value>> {
    http.actual_requests()
        .map(|request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.headers().get("x-amz-target"),
                Some("DynamoDB_20120810.TransactWriteItems")
            );
            let body: Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
            body["TransactItems"].as_array().unwrap().clone()
        })
        .collect()
}

#[tokio::test]
async fn rollback_batches_respect_action_limit_and_include_every_transaction() {
    for (count, sizes) in [
        (1, vec![1]),
        (100, vec![100]),
        (101, vec![100, 1]),
        (201, vec![100, 100, 1]),
    ] {
        let (archive, http) = archive(&vec![200; sizes.len()]);
        let block = block(count);
        archive.unsave(&block).await.unwrap();
        let batches = captured_batches(&http);
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), sizes);
        let expected: Vec<_> = block.body.unwrap().tx.iter().map(|tx| json!({
            "Update": {
                "TableName": "test-lookup",
                "Key": {"pk": {"S": format!("tx:{}", hex::encode(&tx.hash))}, "sk": {"S": "tx"}},
                "UpdateExpression": "SET in_chain = :in_chain",
                "ExpressionAttributeValues": {":in_chain": {"BOOL": false}}
            }
        })).collect();
        assert_eq!(batches.into_iter().flatten().collect::<Vec<_>>(), expected);
    }
}

#[tokio::test]
async fn rollback_empty_block_sends_no_requests() {
    let (archive, http) = archive(&[200]);
    archive.unsave(&block(0)).await.unwrap();
    assert!(captured_batches(&http).is_empty());
}

#[tokio::test]
async fn rollback_stops_and_propagates_failed_batch() {
    // Supply a third response so an erroneous attempt to continue is recorded too.
    let (archive, http) = archive(&[200, 400, 200]);
    let error = archive.unsave(&block(201)).await.unwrap_err();
    assert_eq!(error.to_string(), "failed to mark txs as off-chain");
    let sdk_error = error
        .downcast_ref::<aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError,
        >>()
        .unwrap();
    assert_eq!(
        sdk_error.as_service_error().unwrap().meta().message(),
        Some("simulated batch failure")
    );
    let batches = captured_batches(&http);
    assert_eq!(
        batches.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![100, 100]
    );
}

fn transaction(id: u32) -> Tx {
    Tx {
        hash: id.to_be_bytes().repeat(8).into(),
        successful: true,
        outputs: vec![TxOutput {
            address: vec![id as u8; 29].into(),
            coin: Some(u64_to_bigint(u64::from(id) + 1_000_000)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn save_block(transactions: Vec<Tx>) -> Block {
    Block {
        header: Some(BlockHeader {
            hash: vec![0xab; 32].into(),
            height: 42,
            slot: 100,
        }),
        body: Some(BlockBody { tx: transactions }),
        ..Default::default()
    }
}

fn requests_for(http: &StaticReplayClient, operation: &str) -> Vec<Value> {
    http.actual_requests()
        .filter(|request| request.headers().get("x-amz-target") == Some(operation))
        .map(|request| serde_json::from_slice(request.body().bytes().unwrap()).unwrap())
        .collect()
}

// Read the persisted representation independently of the writer's Rust types.
// In particular, the Go lookup client consumes addresses as base64 strings.
#[derive(serde::Serialize, serde::Deserialize)]
struct SavedOutput {
    address: String,
    coin: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SavedRow {
    pk: String,
    sk: String,
    block: String,
    location: String,
    in_chain: bool,
    successful: bool,
    utxos: Vec<SavedOutput>,
    collateral_out: Option<SavedOutput>,
}

fn saved_rows(http: &StaticReplayClient) -> Vec<SavedRow> {
    requests_for(http, "DynamoDB_20120810.PutItem")
        .into_iter()
        .filter(|request| request["Item"]["sk"]["S"] == "tx")
        .map(|request| {
            let item: serde_dynamo::Item = serde_json::from_value(request["Item"].clone()).unwrap();
            serde_dynamo::from_item(item).unwrap()
        })
        .collect()
}

#[tokio::test]
async fn save_indexes_each_subtransaction_with_its_own_outputs_and_parent_block_location() {
    let mut parent = transaction(1);
    parent.sub_transactions = vec![transaction(2), transaction(3)];
    let block = save_block(vec![parent.clone(), transaction(4)]);
    // S3, height pointer, parent, two sub-txs, ordinary tx.
    let (archive, http) = archive(&[200; 6]);
    archive.save(&block, vec![1, 2, 3]).await.unwrap();
    let rows = saved_rows(&http);
    assert_eq!(rows.len(), 4);
    for expected in [
        &parent,
        &parent.sub_transactions[0],
        &parent.sub_transactions[1],
        &transaction(4),
    ] {
        let row = rows
            .iter()
            .find(|row| row.pk == format!("tx:{}", hex::encode(&expected.hash)))
            .unwrap();
        assert_eq!(row.sk, "tx");
        assert_eq!(row.block, "ab".repeat(32));
        assert_eq!(
            row.location,
            format!("blocks/by-hash/ab/{}.cbor", "ab".repeat(32))
        );
        assert!(row.in_chain);
        assert!(row.successful);
        assert_eq!(row.utxos.len(), 1);
        assert_eq!(
            row.utxos[0].coin,
            expected.outputs[0]
                .coin
                .as_ref()
                .and_then(sundae_sync_v2::utils::bigint_to_u64)
                .unwrap()
        );
        assert_eq!(
            serde_json::to_value(&row.utxos[0].address).unwrap(),
            serde_json::to_value(serde_bytes_base64::Bytes::from(
                expected.outputs[0].address.to_vec()
            ))
            .unwrap()
        );
        assert!(row.collateral_out.is_none());
    }
    assert_eq!(requests_for(&http, "DynamoDB_20120810.PutItem").len(), 5);
}

#[tokio::test]
async fn failed_parent_retains_collateral_but_never_writes_or_rolls_back_subtransactions() {
    let mut parent = transaction(1);
    parent.successful = false;
    // Deliberately leave the child true: parent validity is the deciding rule.
    parent.sub_transactions = vec![transaction(2)];
    parent.collateral = Some(Collateral {
        collateral_return: Some(transaction(3).outputs.remove(0)),
        ..Default::default()
    });
    let block = save_block(vec![parent]);
    let (archive, http) = archive(&[200; 4]);
    archive.save(&block, vec![]).await.unwrap();
    archive.unsave(&block).await.unwrap();
    let rows = saved_rows(&http);
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].successful);
    assert_eq!(rows[0].utxos[0].coin, 1_000_001);
    assert_eq!(rows[0].collateral_out.as_ref().unwrap().coin, 1_000_003);
    let undo = requests_for(&http, "DynamoDB_20120810.TransactWriteItems");
    assert_eq!(undo.len(), 1);
    assert_eq!(undo[0]["TransactItems"].as_array().unwrap().len(), 1);
    assert_eq!(
        undo[0]["TransactItems"][0]["Update"]["Key"]["pk"]["S"],
        rows[0].pk
    );
}

#[tokio::test]
async fn rollback_splits_parent_and_subtransactions_at_the_row_limit() {
    for ordinary_count in [0, 94, 95, 97, 198] {
        let mut parents: Vec<_> = (0..ordinary_count).map(transaction).collect();
        let mut parent = transaction(ordinary_count);
        parent.sub_transactions = (ordinary_count + 1..ordinary_count + 6)
            .map(transaction)
            .collect();
        parents.push(parent);
        let block = save_block(parents);
        let total = ordinary_count as usize + 6;
        let (archive, http) = archive(&vec![200; total.div_ceil(100)]);
        archive.unsave(&block).await.unwrap();
        let batches = captured_batches(&http);
        assert_eq!(batches.len(), total.div_ceil(100));
        assert!(batches.iter().all(|batch| batch.len() <= 100));
        if ordinary_count == 97 {
            assert_eq!(
                batches.iter().map(Vec::len).collect::<Vec<_>>(),
                vec![100, 3]
            );
        }
        let actions: Vec<_> = batches.into_iter().flatten().collect();
        assert_eq!(actions.len(), total);
        for (id, action) in actions.iter().enumerate() {
            assert_eq!(
                action["Update"]["Key"]["pk"]["S"],
                format!("tx:{}", hex::encode((id as u32).to_be_bytes().repeat(8)))
            );
            assert_eq!(
                action["Update"]["ExpressionAttributeValues"][":in_chain"]["BOOL"],
                false
            );
        }
    }
}

#[tokio::test]
async fn duplicate_ids_are_written_and_rolled_back_once() {
    let mut parent = transaction(1);
    parent.sub_transactions = vec![transaction(2), transaction(2)];
    // Also exercise a repeated parent and a sub-tx repeated as a top-level ID.
    let block = save_block(vec![parent.clone(), parent, transaction(2)]);
    let (archive, http) = archive(&[200; 5]);
    archive.save(&block, vec![]).await.unwrap();
    archive.unsave(&block).await.unwrap();
    let rows = saved_rows(&http);
    assert_eq!(rows.len(), 2);
    let undo = requests_for(&http, "DynamoDB_20120810.TransactWriteItems");
    assert_eq!(undo.len(), 1);
    assert_eq!(undo[0]["TransactItems"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn rollback_failure_after_subtransaction_expansion_stops_and_propagates() {
    let mut parent = transaction(0);
    parent.sub_transactions = (1..201).map(transaction).collect();
    let (archive, http) = archive(&[200, 400, 200]);
    let error = archive.unsave(&save_block(vec![parent])).await.unwrap_err();
    assert_eq!(error.to_string(), "failed to mark txs as off-chain");
    assert_eq!(
        captured_batches(&http)
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        vec![100, 100]
    );
}

#[tokio::test]
async fn musashi_transaction_fixture_maps_and_survives_apply_undo_reapply() {
    use pallas::{
        interop::utxorpc::v1beta::Mapper,
        ledger::traverse::{Era, MultiEraTx},
    };
    const PARENT: &str = "74e2116ca6e0c809f156aa062a9d4ee8b156322618846f1bdea5d9ad7a206a12";
    const SUB: &str = "63ac433eefaed71cf0fa2c7134f82ea38f3c6c338b3d5129c686c47b44162691";
    let bytes = hex::decode(include_str!("fixtures/dijkstra-subtx.tx").trim()).unwrap();
    let tx = MultiEraTx::decode_for_era(Era::Dijkstra, &bytes).unwrap();
    let mapped = Mapper::new(NoContext).map_tx(&tx);
    assert_eq!(hex::encode(&mapped.hash), PARENT);
    assert!(mapped.successful);
    assert_eq!(mapped.sub_transactions.len(), 1);
    assert_eq!(hex::encode(&mapped.sub_transactions[0].hash), SUB);
    assert!(mapped.sub_transactions[0].successful);
    let block = save_block(vec![mapped]);
    let (archive, http) = archive(&[200; 9]);
    archive.save(&block, vec![]).await.unwrap();
    archive.unsave(&block).await.unwrap();
    archive.save(&block, vec![]).await.unwrap();

    let rows = saved_rows(&http);
    assert_eq!(rows.len(), 4);
    for (id, coin) in [(PARENT, 1_903_631), (SUB, 3_000_000)] {
        let matching: Vec<_> = rows
            .iter()
            .filter(|row| row.pk == format!("tx:{id}"))
            .collect();
        assert_eq!(matching.len(), 2);
        assert_eq!(
            serde_json::to_value(matching[0]).unwrap(),
            serde_json::to_value(matching[1]).unwrap()
        );
        assert!(matching[1].in_chain);
        assert!(matching[1].successful);
        assert_eq!(matching[1].utxos.len(), 1);
        assert_eq!(matching[1].utxos[0].coin, coin);
    }
    let undo = requests_for(&http, "DynamoDB_20120810.TransactWriteItems");
    assert_eq!(undo.len(), 1);
    let actions = undo[0]["TransactItems"].as_array().unwrap();
    assert_eq!(actions.len(), 2);
    for (action, id) in actions.iter().zip([PARENT, SUB]) {
        assert_eq!(action["Update"]["Key"]["pk"]["S"], format!("tx:{id}"));
        assert_eq!(
            action["Update"]["ExpressionAttributeValues"][":in_chain"]["BOOL"],
            false
        );
    }
}

#[test]
fn v1beta_output_conversion_preserves_lookup_amounts_datum_and_plutus_script() {
    use utxorpc_spec::utxorpc::v1beta::cardano::{
        big_int, script, Asset, BigInt, Datum, Multiasset, Script,
    };
    let output = TxOutput {
        address: vec![1, 2, 3].into(),
        coin: Some(u64_to_bigint(42)),
        assets: vec![Multiasset {
            policy_id: vec![4].into(),
            assets: vec![Asset {
                name: vec![5].into(),
                quantity: Some(BigInt {
                    big_int: Some(big_int::BigInt::BigUInt(
                        vec![1, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                    )),
                }),
            }],
        }],
        datum: Some(Datum {
            original_cbor: Some(vec![0x18, 0x2a].into()),
            ..Default::default()
        }),
        script: Some(Script {
            script: Some(script::Script::PlutusV3(vec![6, 7].into())),
        }),
        ..Default::default()
    };
    let stored: sundae_sync_v2::archive::TxOutput = output.into();
    assert_eq!(
        serde_json::to_value(stored).unwrap(),
        json!({
            "address": "AQID", "coin": 42,
            "assets": [{"policy_id": "BA==", "redeemer": null,
                "assets": [{"name": "BQ==", "output_coin": "18446744073709551616"}]}],
            "datum": "GCo=", "script": {"plutusV3": "Bgc="}
        })
    );
}

#[test]
fn native_reference_scripts_keep_legacy_pubkey_keys_without_losing_dijkstra_guards() {
    use utxorpc_spec::utxorpc::v1beta::cardano::{
        native_script::NativeScript as Kind, script, NativeScript, NativeScriptList, Script,
        ScriptNOfK,
    };
    let pubkey = NativeScript {
        native_script: Some(Kind::ScriptPubkeyHash(vec![1, 2, 3].into())),
    };
    let native = NativeScript {
        native_script: Some(Kind::ScriptAll(NativeScriptList {
            items: vec![
                pubkey.clone(),
                NativeScript {
                    native_script: Some(Kind::ScriptAny(NativeScriptList {
                        items: vec![pubkey.clone()],
                    })),
                },
                NativeScript {
                    native_script: Some(Kind::ScriptNOfK(ScriptNOfK {
                        k: 1,
                        scripts: vec![pubkey],
                    })),
                },
                NativeScript {
                    native_script: Some(Kind::ScriptRequireGuard(Default::default())),
                },
            ],
        })),
    };
    let output = TxOutput {
        coin: Some(u64_to_bigint(1)),
        script: Some(Script {
            script: Some(script::Script::Native(native)),
        }),
        ..Default::default()
    };
    let stored: sundae_sync_v2::archive::TxOutput = output.into();
    let json = serde_json::to_value(&stored).unwrap();
    assert_eq!(
        json["script"],
        json!({"native": {"scriptAll": {"items": [
            {"scriptPubkey": "AQID"},
            {"scriptAny": {"items": [{"scriptPubkey": "AQID"}]}},
            {"scriptNOfK": {"k": 1, "scripts": [{"scriptPubkey": "AQID"}]}},
            {"scriptRequireGuard": {}}
        ]}}})
    );
    let text = serde_json::to_string(&stored).unwrap();
    let restored: sundae_sync_v2::archive::TxOutput = serde_json::from_str(&text).unwrap();
    assert_eq!(restored.script, stored.script);
}

#[test]
fn broadcast_block_references_keep_the_existing_json_contract() {
    use sundae_sync_v2::broadcast::BroadcastMessage;
    use utxorpc_spec::utxorpc::v1beta::sync::BlockRef;
    let message = BroadcastMessage {
        undo: vec![BlockRef {
            slot: 1,
            hash: vec![1, 2, 3].into(),
            height: 2,
            timestamp: 3,
        }],
        advance: BlockRef {
            slot: 4,
            hash: vec![4, 5, 6].into(),
            height: 5,
            timestamp: 0,
        },
    };
    let json = json!({
        "undo": [{"slot": "1", "hash": "AQID", "height": "2", "timestamp": "3"}],
        "advance": {"slot": "4", "hash": "BAUG", "height": "5"}
    });
    assert_eq!(serde_json::to_value(&message).unwrap(), json);
    let restored: BroadcastMessage = serde_json::from_value(json).unwrap();
    assert_eq!(restored.undo, message.undo);
    assert_eq!(restored.advance, message.advance);
}

#[tokio::test]
async fn musashi_issue_block_archives_subtransaction_outputs_and_rolls_back_every_row() {
    use pallas::interop::utxorpc::v1beta::Mapper;
    use std::collections::HashSet;

    const BLOCK: &str = "5ba7b23a04aadcd104db62d67a840fa208342fdad749ee138a4579236579a5d7";
    const PARENT: &str = "ca89430ca01c3202ce2cc8428c53f91a4f34d618421ca510dd39f3d76c540aca";
    const SUB: &str = "ede49f69c8e99d253d82b0b06d024be6435168cfeb1b960761370fe1bf5f8377";
    let raw = hex::decode(
        include_str!(
            "fixtures/5ba7b23a04aadcd104db62d67a840fa208342fdad749ee138a4579236579a5d7.cbor.hex"
        )
        .trim(),
    )
    .unwrap();
    let block = Mapper::new(NoContext).map_block_cbor(&raw);
    let header = block.header.as_ref().unwrap();
    assert_eq!(hex::encode(&header.hash), BLOCK);
    assert_eq!((header.height, header.slot), (112_280, 2_494_559));
    let parents = &block.body.as_ref().unwrap().tx;
    assert_eq!(parents.len(), 126);
    assert_eq!(
        parents
            .iter()
            .map(|tx| tx.sub_transactions.len())
            .sum::<usize>(),
        1
    );
    let parent = &parents[123];
    assert_eq!(hex::encode(&parent.hash), PARENT);
    assert!(parent.successful);
    assert_eq!(hex::encode(&parent.sub_transactions[0].hash), SUB);

    // One S3 upload, one height pointer, 127 unique tx rows, then two undo batches.
    let (archive, http) = archive(&[200; 131]);
    archive.save(&block, raw.clone()).await.unwrap();
    archive.unsave(&block).await.unwrap();
    let uploads: Vec<_> = http
        .actual_requests()
        .filter(|request| request.method() == "PUT")
        .collect();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].body().bytes().unwrap(), raw);

    let rows = saved_rows(&http);
    assert_eq!(rows.len(), 127);
    let expected_keys: HashSet<_> = parents
        .iter()
        .map(|tx| format!("tx:{}", hex::encode(&tx.hash)))
        .chain(std::iter::once(format!("tx:{SUB}")))
        .collect();
    let saved_keys: HashSet<_> = rows.iter().map(|row| row.pk.clone()).collect();
    assert_eq!(saved_keys, expected_keys);
    for row in &rows {
        assert_eq!(row.sk, "tx");
        assert_eq!(row.block, BLOCK);
        assert_eq!(row.location, format!("blocks/by-hash/5b/{BLOCK}.cbor"));
        assert!(row.in_chain);
    }
    // These IDs and output values were read independently from the original CBOR,
    // not derived from the mapper under test. Lookup uses the sub-tx ID, not PARENT.
    let sub = rows
        .iter()
        .find(|row| row.pk == format!("tx:{SUB}"))
        .unwrap();
    assert!(sub.successful);
    assert!(sub.collateral_out.is_none());
    assert_eq!(sub.utxos.len(), 2);
    for (output, (coin, address)) in sub.utxos.iter().zip([
        (
            8_000_000,
            "70c323b9545afa6826b4121c048634ec168d78fa6f2349198169ff76f7",
        ),
        (
            1_043_020,
            "60fc8b6e7cebcc4b49a05640f9a64f8a9f2c67109bddbd348ce41ba22a",
        ),
    ]) {
        assert_eq!(output.coin, coin);
        assert_eq!(
            json!(output.address),
            serde_json::to_value(serde_bytes_base64::Bytes::from(
                hex::decode(address).unwrap()
            ))
            .unwrap()
        );
    }
    let undo = requests_for(&http, "DynamoDB_20120810.TransactWriteItems");
    assert_eq!(
        undo.iter()
            .map(|request| request["TransactItems"].as_array().unwrap().len())
            .collect::<Vec<_>>(),
        vec![100, 27]
    );
    let undone_keys: HashSet<_> = undo
        .iter()
        .flat_map(|request| request["TransactItems"].as_array().unwrap())
        .map(|action| {
            assert_eq!(
                action["Update"]["ExpressionAttributeValues"][":in_chain"]["BOOL"],
                false
            );
            action["Update"]["Key"]["pk"]["S"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(undone_keys, expected_keys);
}
