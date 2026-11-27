use std::time::{Duration, Instant};

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::{retry::RetryConfig, Credentials, Region};
use aws_smithy_http_client::test_util::NeverClient;
use sundae_sync_v2::broadcast::{BroadcastMessage, Broadcaster, Destination};
use utxorpc::spec::{cardano::Block, sync::BlockRef};

fn point(slot: u64) -> BlockRef {
    BlockRef {
        slot,
        hash: bytes::Bytes::from(slot.to_be_bytes().to_vec()),
        timestamp: 0,
        // one block per slot, to keep the test points a chain
        height: slot,
    }
}

/// A broadcaster with one unfiltered destination, whose AWS transport never answers.
fn broadcaster(put_timeout: Duration) -> Broadcaster {
    let config = aws_config::SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::SharedCredentialsProvider::new(
            Credentials::new("test", "test", None, None, "test"),
        ))
        .http_client(NeverClient::new())
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .build();
    // The deadline never passes: only the write timeout can stop the broadcast.
    let (_, deadline) = tokio::sync::watch::channel(u64::MAX);
    Broadcaster {
        destinations: vec![Destination {
            pk: "test".into(),
            stream_arn: "arn:aws:kinesis:us-east-1:000000000000:stream/test".into(),
            shard_id: "shardId-000000000000".into(),
            filter: None,
            sequence_number: None,
            last_seen_point: point(100),
            recovery_points: vec![point(100)],
            published_points: vec![point(100)],
            enabled: true,
            skip_repair: true,
        }],
        kinesis: aws_sdk_kinesis::Client::new(&config),
        dynamo: aws_sdk_dynamodb::Client::new(&config),
        table: "unused".into(),
        deadline,
        put_timeout,
    }
}

#[tokio::test]
async fn broadcast_fails_when_the_kinesis_write_does_not_end_in_time() {
    let mut broadcaster = broadcaster(Duration::from_millis(100));
    let start = Instant::now();
    let result = broadcaster
        .broadcast(
            Block::default(),
            BroadcastMessage {
                undo: vec![],
                advance: point(101),
            },
        )
        .await;
    let err = result.expect_err("a write that never ends must fail the broadcast");
    assert!(
        format!("{err:#}").contains("timed out writing to kinesis"),
        "unexpected error: {err:#}"
    );
    assert!(start.elapsed() < Duration::from_secs(5));
    // Nothing was committed: the destination still holds its last point.
    assert_eq!(broadcaster.destinations[0].last_seen_point, point(100));
}

#[tokio::test]
async fn broadcast_does_not_write_the_last_seen_point_again() {
    // The transport never answers, so this returns only if nothing is sent.
    let mut broadcaster = broadcaster(Duration::from_secs(60));
    let published = tokio::time::timeout(
        Duration::from_secs(5),
        broadcaster.broadcast(
            Block::default(),
            BroadcastMessage {
                undo: vec![],
                advance: point(100),
            },
        ),
    )
    .await
    .expect("the broadcast must not wait on AWS")
    .expect("broadcast failed");
    assert!(published.published_to.is_empty());
    assert!(!published.advanced);
}

#[tokio::test]
async fn broadcast_refuses_a_block_that_does_not_continue_the_published_chain() {
    // The transport never answers, so this returns only if nothing is sent.
    let mut broadcaster = broadcaster(Duration::from_secs(60));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        broadcaster.broadcast(
            Block::default(),
            BroadcastMessage {
                undo: vec![],
                // the published chain ends at block 100; 101 is missing
                advance: point(102),
            },
        ),
    )
    .await
    .expect("the broadcast must not wait on AWS");
    let err = result.expect_err("a skipped block must fail the broadcast");
    assert!(
        format!("{err:#}").contains("refusing to publish to destination test"),
        "unexpected error: {err:#}"
    );
    assert_eq!(broadcaster.destinations[0].last_seen_point, point(100));
}
