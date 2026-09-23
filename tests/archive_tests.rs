use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::{retry::RetryConfig, Credentials, Region};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use serde_json::{json, Value};
use sundae_sync_v2::archive::Archive;
use utxorpc::spec::cardano::{Block, BlockBody, Tx};

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
    let config = test_config(http.clone());
    (
        Archive {
            dynamo: aws_sdk_dynamodb::Client::new(&config),
            s3: aws_sdk_s3::Client::new(&config),
            bucket: "test-archive".into(),
            table_name: "test-lookup".into(),
        },
        http,
    )
}

fn test_config(http: StaticReplayClient) -> aws_config::SdkConfig {
    // Explicit credentials and an in-memory transport keep tests independent of AWS and Docker.
    aws_config::SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::SharedCredentialsProvider::new(
            Credentials::new("test", "test", None, None, "test"),
        ))
        .http_client(http)
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .build()
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

fn save_block(tx_count: u32) -> Block {
    Block {
        header: Some(utxorpc::spec::cardano::BlockHeader {
            hash: vec![0xab; 32].into(),
            height: 42,
            slot: 100,
        }),
        ..block(tx_count)
    }
}

fn save_archive(responses: Vec<(u16, Value)>) -> (Archive, StaticReplayClient, StaticReplayClient) {
    let ddb = StaticReplayClient::new(
        responses
            .into_iter()
            .map(|(status, body)| {
                ReplayEvent::new(
                    http::Request::new(SdkBody::empty()),
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/x-amz-json-1.0")
                        .body(SdkBody::from(body.to_string()))
                        .unwrap(),
                )
            })
            .collect(),
    );
    let s3 = StaticReplayClient::new(vec![ReplayEvent::new(
        http::Request::new(SdkBody::empty()),
        http::Response::builder()
            .status(200)
            .body(SdkBody::empty())
            .unwrap(),
    )]);
    let (mut archive, _) = archive(&[]);
    archive.dynamo = aws_sdk_dynamodb::Client::new(&test_config(ddb.clone()));
    archive.s3 = aws_sdk_s3::Client::new(&test_config(s3.clone()));
    (archive, ddb, s3)
}

fn captured_put_batches(http: &StaticReplayClient) -> Vec<Vec<Value>> {
    http.actual_requests()
        .map(|request| {
            assert_eq!(
                request.headers().get("x-amz-target"),
                Some("DynamoDB_20120810.BatchWriteItem")
            );
            assert!(request.body().bytes().unwrap().len() < 16 * 1024 * 1024);
            let body: Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
            body["RequestItems"]["test-lookup"]
                .as_array()
                .unwrap()
                .clone()
        })
        .collect()
}

fn tx_pointer(index: u32) -> Value {
    let hash = "ab".repeat(32);
    json!({"PutRequest":{"Item":{
        "pk":{"S":format!("tx:{}", hex::encode(index.to_be_bytes().repeat(8)))},
        "sk":{"S":"tx"}, "block":{"S":hash},
        "location":{"S":format!("blocks/by-hash/ab/{hash}.cbor")},
        "in_chain":{"BOOL":true}, "successful":{"BOOL":false},
        "utxos":{"L":[]}, "collateral_out":{"NULL":true}
    }}})
}

#[tokio::test]
async fn save_batches_include_height_and_every_transaction_with_unchanged_fields() {
    for count in [0, 1, 24, 25, 26, 99, 100] {
        let expected_batches = ((count + 1) as usize).div_ceil(25);
        let (archive, ddb, s3) = save_archive(vec![(200, json!({})); expected_batches]);
        archive
            .save(&save_block(count), vec![1, 2, 3])
            .await
            .unwrap();
        assert_eq!(s3.actual_requests().count(), 1);
        let batches = captured_put_batches(&ddb);
        assert_eq!(batches.len(), expected_batches);
        assert!(batches.iter().all(|b| !b.is_empty() && b.len() <= 25));
        let items: Vec<_> = batches.into_iter().flatten().collect();
        assert_eq!(items.len(), count as usize + 1);
        let hash = "ab".repeat(32);
        assert!(items.contains(&json!({"PutRequest":{"Item":{
            "pk":{"S":"height:42"}, "sk":{"S":"height"}, "hash":{"S":hash},
            "location":{"S":format!("blocks/by-hash/ab/{hash}.cbor")}
        }}})));
        for index in 0..count {
            assert!(items.contains(&tx_pointer(index)));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn save_retries_only_unprocessed_items_and_waits_before_success() {
    let pending = tx_pointer(0);
    let (archive, ddb, _) = save_archive(vec![
        (
            200,
            json!({"UnprocessedItems":{"test-lookup":[pending.clone()]}}),
        ),
        (200, json!({})),
    ]);
    let start = tokio::time::Instant::now();
    archive.save(&save_block(2), vec![]).await.unwrap();
    assert!(start.elapsed() >= std::time::Duration::from_millis(50));
    let batches = captured_put_batches(&ddb);
    assert_eq!(batches[0].len(), 3);
    assert_eq!(batches[1], vec![pending]);
}

#[tokio::test(start_paused = true)]
async fn save_fails_when_unprocessed_items_exhaust_retries() {
    let pending = tx_pointer(0);
    let (archive, ddb, _) = save_archive(vec![
        (
            200,
            json!({"UnprocessedItems":{"test-lookup":[pending.clone()]}})
        );
        9
    ]);
    let error = archive.save(&save_block(1), vec![]).await.unwrap_err();
    assert_eq!(error.to_string(), "failed to save pointers to dynamodb");
    assert!(format!("{error:#}").contains("1 pointers remain unprocessed after 8 retries"));
    let batches = captured_put_batches(&ddb);
    assert_eq!(batches.len(), 9);
    assert!(batches[1..].iter().all(|b| b == &vec![pending.clone()]));
}

#[tokio::test(start_paused = true)]
async fn save_propagates_failure_after_partial_success_and_can_be_replayed() {
    let failure = json!({"__type":"ValidationException","message":"simulated write failure"});
    let (archive, ddb, _) = save_archive(vec![(200, json!({})), (400, failure)]);
    let block = save_block(25);
    let error = archive.save(&block, vec![]).await.unwrap_err();
    assert!(format!("{error:#}").contains("failed to save pointers to dynamodb"));
    let first_attempt = captured_put_batches(&ddb);
    assert_eq!(first_attempt.len(), 2);
    let (archive, ddb, _) = save_archive(vec![(200, json!({})); 2]);
    archive.save(&block, vec![]).await.unwrap();
    // Replaying uses exactly the same keys and values, including the already-written batch.
    assert_eq!(captured_put_batches(&ddb), first_attempt);
}

#[tokio::test(start_paused = true)]
async fn save_limits_in_flight_batches_and_does_not_finish_with_pending_writes() {
    use aws_smithy_http_client::test_util::NeverClient;
    let (mut archive, _, _) = save_archive(vec![]);
    let http = NeverClient::new();
    let config = archive
        .dynamo
        .config()
        .to_builder()
        .http_client(http.clone())
        .build();
    archive.dynamo = aws_sdk_dynamodb::Client::from_conf(config);
    // This requires 11 batches, but only four may be in flight at a time.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        archive.save(&save_block(250), vec![]),
    )
    .await;
    assert!(
        result.is_err(),
        "save must wait for every batch to complete"
    );
    assert_eq!(http.num_calls(), 4);
}

#[tokio::test]
async fn save_preserves_outputs_and_collateral() {
    use utxorpc::spec::cardano::{big_int, BigInt, Collateral, TxOutput};
    let mut block = save_block(1);
    let tx = &mut block.body.as_mut().unwrap().tx[0];
    tx.successful = true;
    let output = TxOutput {
        address: vec![1, 2, 3].into(),
        coin: Some(BigInt {
            big_int: Some(big_int::BigInt::Int(123)),
        }),
        ..Default::default()
    };
    tx.outputs.push(output.clone());
    tx.collateral = Some(Collateral {
        collateral_return: Some(output),
        ..Default::default()
    });
    let (archive, http, _) = save_archive(vec![(200, json!({}))]);
    archive.save(&block, vec![]).await.unwrap();
    let batches = captured_put_batches(&http);
    let actual = &batches[0][1]["PutRequest"]["Item"];
    assert_eq!(actual["successful"], json!({"BOOL":true}));
    let expected = json!({"M":{
        "address":{"S":"AQID"}, "coin":{"N":"123"},
        "assets":{"L":[]}, "datum":{"NULL":true}, "script":{"NULL":true}
    }});
    assert_eq!(actual["utxos"], json!({"L":[expected.clone()]}));
    assert_eq!(actual["collateral_out"], expected);
}

#[tokio::test(start_paused = true)]
async fn save_propagates_request_error_while_retrying_unprocessed_items() {
    let (archive, http, _) = save_archive(vec![
        (
            200,
            json!({"UnprocessedItems":{"test-lookup":[tx_pointer(0)]}}),
        ),
        (
            400,
            json!({"__type":"ValidationException","message":"retry failed"}),
        ),
    ]);
    let error = archive.save(&save_block(1), vec![]).await.unwrap_err();
    assert_eq!(error.to_string(), "failed to save pointers to dynamodb");
    assert_eq!(captured_put_batches(&http).len(), 2);
}
