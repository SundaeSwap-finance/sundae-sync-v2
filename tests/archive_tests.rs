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

fn saved_block(hashes: &[u32]) -> Block {
    Block {
        header: Some(utxorpc::spec::cardano::BlockHeader {
            slot: 10,
            hash: vec![0xab; 32].into(),
            height: 7,
        }),
        body: Some(BlockBody {
            tx: hashes
                .iter()
                .map(|index| Tx {
                    hash: index.to_be_bytes().repeat(8).into(),
                    ..Default::default()
                })
                .collect(),
        }),
        ..Default::default()
    }
}

/// The pointer batches of a save: every request after the S3 upload, as the
/// list of item pks each BatchWriteItem carried.
fn saved_batches(http: &StaticReplayClient) -> Vec<Vec<String>> {
    http.actual_requests()
        .filter(|request| {
            request.headers().get("x-amz-target") == Some("DynamoDB_20120810.BatchWriteItem")
        })
        .map(|request| {
            let body: Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
            body["RequestItems"]["test-lookup"]
                .as_array()
                .unwrap()
                .iter()
                .map(|w| w["PutRequest"]["Item"]["pk"]["S"].as_str().unwrap().to_string())
                .collect()
        })
        .collect()
}

fn tx_pk(index: u32) -> String {
    format!("tx:{}", hex::encode(index.to_be_bytes().repeat(8)))
}

#[tokio::test]
async fn save_batches_pointers_and_includes_every_transaction() {
    let hashes: Vec<u32> = (0..60).collect();
    // One S3 upload, then 61 pointers (height + 60 txs) in batches of 25, 25, 11.
    let (archive, http) = archive(&[200; 4]);
    archive.save(&saved_block(&hashes), vec![1, 2, 3]).await.unwrap();

    let batches = saved_batches(&http);
    let mut sizes: Vec<_> = batches.iter().map(Vec::len).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![11, 25, 25]);

    let mut written: Vec<_> = batches.into_iter().flatten().collect();
    written.sort();
    let mut expected: Vec<_> = hashes.iter().map(|i| tx_pk(*i)).collect();
    expected.push("height:7".to_string());
    expected.sort();
    assert_eq!(written, expected);
}

#[tokio::test]
async fn save_skips_repeated_transactions() {
    // Leios endorser blocks can repeat a transaction; one batch must not hold a key twice.
    let (archive, http) = archive(&[200; 2]);
    archive.save(&saved_block(&[1, 2, 1]), vec![]).await.unwrap();
    let batches = saved_batches(&http);
    assert_eq!(batches, vec![vec!["height:7".to_string(), tx_pk(1), tx_pk(2)]]);
}

#[tokio::test]
async fn save_retries_unprocessed_pointers() {
    let unprocessed = json!({
        "UnprocessedItems": {"test-lookup": [{"PutRequest": {"Item": {
            "pk": {"S": tx_pk(2)}, "sk": {"S": "tx"}
        }}}]}
    })
    .to_string();
    let responses = [(200, "{}".to_string()), (200, unprocessed), (200, "{}".to_string())];
    let http = StaticReplayClient::new(
        responses
            .iter()
            .map(|(status, body)| {
                ReplayEvent::new(
                    http::Request::new(SdkBody::empty()),
                    http::Response::builder()
                        .status(*status)
                        .header("content-type", "application/x-amz-json-1.0")
                        .body(SdkBody::from(body.clone()))
                        .unwrap(),
                )
            })
            .collect(),
    );
    let (mut archive, _) = archive(&[]);
    let config = aws_config::SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::SharedCredentialsProvider::new(
            Credentials::new("test", "test", None, None, "test"),
        ))
        .http_client(http.clone())
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .build();
    archive.dynamo = aws_sdk_dynamodb::Client::new(&config);
    archive.s3 = aws_sdk_s3::Client::new(&config);

    archive.save(&saved_block(&[1, 2]), vec![]).await.unwrap();
    let batches = saved_batches(&http);
    assert_eq!(batches.len(), 2, "the unprocessed item is sent again");
    assert_eq!(batches[1], vec![tx_pk(2)]);
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
