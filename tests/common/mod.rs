use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        TimeToLiveSpecification,
    },
    Client as DynamoClient,
};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::dynamodb_local::DynamoDb;

#[allow(dead_code)]
pub enum TableType {
    Lock,
    Destination,
}

/// Sets up a DynamoDB Local container for testing
/// Returns (container, client, table_name)
pub async fn setup_dynamodb(
    table_type: TableType,
) -> Result<(ContainerAsync<DynamoDb>, DynamoClient, String)> {
    let container = DynamoDb::default().start().await?;
    let port = container.get_host_port_ipv4(8000).await?;
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

    let (table_name, ttl_attribute) = match table_type {
        TableType::Lock => ("test-locks".to_string(), "expires_at"),
        TableType::Destination => ("test-destinations".to_string(), "ttl"),
    };

    // Create table with common schema (pk hash key)
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

    // Enable TTL with the appropriate attribute name
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

    Ok((container, client, table_name))
}
