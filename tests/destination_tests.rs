use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client as DynamoClient,
};
use bytes::Bytes;
use serde_dynamo::to_item;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::dynamodb_local::DynamoDb;
use utxorpc::spec::sync::BlockRef;

use sundae_sync_v2::broadcast::destination::Destination;

/// Helper to set up DynamoDB Local for destination tests
async fn setup_dynamodb() -> Result<(testcontainers::ContainerAsync<DynamoDb>, DynamoClient, String)> {
    let container = DynamoDb::default().start().await?;

    let port = container.get_host_port_ipv4(8000).await?;
    let endpoint = format!("http://127.0.0.1:{}", port);

    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .region("us-east-1")
        .load()
        .await;

    let client = DynamoClient::new(&config);
    let table_name = format!("dest-table-{}", uuid::Uuid::new_v4());

    // Create destinations table
    client
        .create_table()
        .table_name(&table_name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()?,
        )
        .send()
        .await?;

    Ok((container, client, table_name))
}

/// Helper to create a test destination and insert it into DynamoDB
async fn create_test_destination(
    dynamo: &DynamoClient,
    table: &str,
    pk: &str,
    initial_point: BlockRef,
) -> Result<Destination> {
    let dest = Destination {
        pk: pk.to_string(),
        stream_arn: "arn:aws:kinesis:us-east-1:123456789:stream/test".to_string(),
        shard_id: "shard-0".to_string(),
        filter: None,
        sequence_number: Some("seq-0".to_string()),
        last_seen_point: initial_point.clone(),
        recovery_points: vec![initial_point],
        enabled: true,
        skip_repair: true,
    };

    // Insert into DynamoDB
    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(to_item(&dest)?))
        .send()
        .await?;

    Ok(dest)
}

fn make_point(index: u64) -> BlockRef {
    BlockRef {
        index,
        hash: Bytes::from(format!("hash-{}", index).as_bytes().to_vec()),
    }
}

#[tokio::test]
async fn test_destination_commit_succeeds_on_correct_sequence() -> Result<()> {
    let (_container, dynamo, table) = setup_dynamodb().await?;

    let mut dest = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;

    // Commit the next point in sequence
    dest.commit(&dynamo, &table, make_point(101), Some("seq-101".to_string()))
        .await?;

    // Verify it updated
    assert_eq!(dest.last_seen_point.index, 101);
    assert_eq!(dest.sequence_number, Some("seq-101".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_destination_commit_prevents_split_brain() -> Result<()> {
    let (_container, dynamo, table) = setup_dynamodb().await?;

    let mut dest1 = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;
    let mut dest2 = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;

    // Worker 1 commits point 101
    let result1 = dest1
        .commit(&dynamo, &table, make_point(101), Some("seq-101".to_string()))
        .await;

    // Worker 2 tries to commit point 102 (different point, starting from same base)
    let result2 = dest2
        .commit(&dynamo, &table, make_point(102), Some("seq-102".to_string()))
        .await;

    // Exactly one should succeed
    assert!(
        result1.is_ok() != result2.is_ok(),
        "Exactly one worker should succeed, got result1={:?}, result2={:?}",
        result1.is_ok(),
        result2.is_ok()
    );

    // The failed one should have the helpful error message
    if let Err(e) = result1.as_ref() {
        assert!(
            e.to_string().contains("Another worker has updated destination"),
            "Error should mention failover: {}",
            e
        );
    }
    if let Err(e) = result2.as_ref() {
        assert!(
            e.to_string().contains("Another worker has updated destination"),
            "Error should mention failover: {}",
            e
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_destination_commit_detects_zombie_worker() -> Result<()> {
    let (_container, dynamo, table) = setup_dynamodb().await?;

    // Current worker at point 100
    let mut current_worker = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;

    // Current worker advances to 101
    current_worker
        .commit(&dynamo, &table, make_point(101), Some("seq-101".to_string()))
        .await?;

    // Zombie worker still thinks we're at 100, tries to commit 101
    let mut zombie_worker = Destination {
        pk: "dest-1".to_string(),
        last_seen_point: make_point(100), // Stale!
        recovery_points: vec![make_point(100)],
        stream_arn: "arn:aws:kinesis:us-east-1:123456789:stream/test".to_string(),
        shard_id: "shard-0".to_string(),
        filter: None,
        sequence_number: Some("seq-100".to_string()),
        enabled: true,
        skip_repair: true,
    };

    let result = zombie_worker
        .commit(&dynamo, &table, make_point(101), Some("seq-zombie".to_string()))
        .await;

    // Should fail with conditional check
    assert!(result.is_err(), "Zombie worker should fail to commit");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Another worker has updated destination"),
        "Should have helpful error message: {}",
        err_msg
    );
    assert!(
        err_msg.contains("likely failover occurred"),
        "Should mention failover: {}",
        err_msg
    );

    Ok(())
}

#[tokio::test]
async fn test_destination_recovery_points_rotation() -> Result<()> {
    let (_container, dynamo, table) = setup_dynamodb().await?;

    let mut dest = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;

    // Commit 20 points
    for i in 101..=120 {
        dest.commit(&dynamo, &table, make_point(i), Some(format!("seq-{}", i)))
            .await?;
    }

    // Should have exactly 15 recovery points
    assert_eq!(
        dest.recovery_points.len(),
        15,
        "Should maintain exactly 15 recovery points"
    );

    // Should have the most recent 15 (106-120)
    assert_eq!(dest.recovery_points[0].index, 106, "Oldest should be 106");
    assert_eq!(dest.recovery_points[14].index, 120, "Newest should be 120");

    // Current point should be 120
    assert_eq!(dest.last_seen_point.index, 120);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_destination_commits() -> Result<()> {
    let (_container, dynamo, table) = setup_dynamodb().await?;

    let _initial = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;

    // Spawn 10 workers trying to commit sequentially from point 100
    let mut handles = vec![];

    for i in 101..=110 {
        let dynamo = dynamo.clone();
        let table = table.clone();

        handles.push(tokio::spawn(async move {
            // Each worker starts from 100 and tries to commit their point
            let mut dest = Destination {
                pk: "dest-1".to_string(),
                last_seen_point: make_point(100),
                recovery_points: vec![make_point(100)],
                stream_arn: "arn:aws:kinesis:us-east-1:123456789:stream/test".to_string(),
                shard_id: "shard-0".to_string(),
                filter: None,
                sequence_number: Some("seq-100".to_string()),
                enabled: true,
                skip_repair: true,
            };

            dest.commit(&dynamo, &table, make_point(i), Some(format!("seq-{}", i)))
                .await
                .ok()
                .map(|_| i)
        }));
    }

    let results: Vec<Option<u64>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r: Result<Option<u64>, _>| r.ok())
        .collect();

    // Exactly one should succeed (the first one to write)
    let successful_count = results.iter().filter(|r: &&Option<u64>| r.is_some()).count();

    assert_eq!(
        successful_count, 1,
        "Exactly one worker should succeed in committing, got {} successes",
        successful_count
    );

    // Verify final state in DynamoDB
    let item = dynamo
        .get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S("dest-1".to_string()))
        .send()
        .await?;

    let item = item.item.expect("Destination should exist");
    let last_point = item.get("last_seen_point").unwrap();

    // Should be one of 101-110, not 100
    if let AttributeValue::S(point_str) = last_point {
        let index: u64 = point_str.split('/').next().unwrap().parse()?;
        assert!(index >= 101 && index <= 110, "Final point should be 101-110, got {}", index);
    } else {
        panic!("last_seen_point should be a string");
    }

    Ok(())
}

#[tokio::test]
async fn test_destination_commit_updates_sequence_number() -> Result<()> {
    let (_container, dynamo, table) = setup_dynamodb().await?;

    let mut dest = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;

    // Commit with new sequence number
    dest.commit(&dynamo, &table, make_point(101), Some("new-seq-101".to_string()))
        .await?;

    assert_eq!(dest.sequence_number, Some("new-seq-101".to_string()));

    // Commit with None sequence number
    dest.commit(&dynamo, &table, make_point(102), None).await?;

    assert_eq!(dest.sequence_number, None);

    Ok(())
}

#[tokio::test]
async fn test_destination_commit_adds_to_recovery_points() -> Result<()> {
    let (_container, dynamo, table) = setup_dynamodb().await?;

    let mut dest = create_test_destination(&dynamo, &table, "dest-1", make_point(100)).await?;

    let initial_recovery_count = dest.recovery_points.len();

    // Commit a new point
    dest.commit(&dynamo, &table, make_point(101), Some("seq-101".to_string()))
        .await?;

    // Recovery points should have grown by 1
    assert_eq!(dest.recovery_points.len(), initial_recovery_count + 1);

    // Last recovery point should be the newly committed point
    assert_eq!(dest.recovery_points.last().unwrap().index, 101);

    Ok(())
}
