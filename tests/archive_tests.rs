use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::{retry::RetryConfig, Credentials, Region};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use serde_json::{json, Value};
use sundae_sync_v2::{archive::Archive, utils::u64_to_bigint};
use utxorpc_spec::utxorpc::v1beta::cardano::{Block, BlockBody, Tx, TxOutput};

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
