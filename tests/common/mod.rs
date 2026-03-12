use std::sync::OnceLock;

use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        TimeToLiveSpecification,
    },
    Client as DynamoClient,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::dynamodb_local::DynamoDb;
use uuid::Uuid;

#[allow(dead_code)]
pub enum TableType {
    Lock,
    Destination,
}

/// One DynamoDB Local container shared across all tests in this binary.
/// Lives in a dedicated background thread so it's never accidentally dropped.
static DYNAMO_PORT: OnceLock<u16> = OnceLock::new();

fn get_dynamo_port() -> u16 {
    *DYNAMO_PORT.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let container = DynamoDb::default().start().await.unwrap();
                let port = container.get_host_port_ipv4(8000).await.unwrap();
                tx.send(port).unwrap();
                // Hold the container alive until the process exits.
                std::future::pending::<()>().await;
            });
        });
        rx.recv().unwrap()
    })
}

/// Sets up a uniquely-named table in the shared DynamoDB Local container.
/// Each call produces an isolated table, so tests can run in parallel safely.
pub async fn setup_dynamodb(table_type: TableType) -> Result<(DynamoClient, String)> {
    // Initialise (or reuse) the shared container on a blocking thread so we
    // don't stall the async executor during the one-time Docker startup.
    let port = tokio::task::spawn_blocking(get_dynamo_port).await?;
    let endpoint = format!("http://127.0.0.1:{}", port);

    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = DynamoClient::new(&config);

    let (table_prefix, ttl_attribute) = match table_type {
        TableType::Lock => ("test-locks", "expires_at"),
        TableType::Destination => ("test-destinations", "ttl"),
    };

    // Unique name so parallel tests never share state.
    let table_name = format!("{}-{}", table_prefix, Uuid::new_v4());

    client
        .create_table()
        .table_name(&table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await?;

    client
        .update_time_to_live()
        .table_name(&table_name)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name(ttl_attribute)
                .build()?,
        )
        .send()
        .await?;

    Ok((client, table_name))
}
