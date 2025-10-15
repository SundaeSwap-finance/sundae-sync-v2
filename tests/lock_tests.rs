mod common;

use std::time::Duration;

use anyhow::Result;

use sundae_sync_v2::lock::Lock;

#[tokio::test]
async fn test_lock_acquisition_success() -> Result<()> {
    let (_container, dynamo, table) = common::setup_dynamodb(common::TableType::Lock).await?;

    let lock: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(60), table.clone()).await?;
    assert!(lock.is_some(), "Lock acquisition should succeed on empty table");

    Ok(())
}

#[tokio::test]
async fn test_lock_acquisition_fails_when_held() -> Result<()> {
    let (_container, dynamo, table) = common::setup_dynamodb(common::TableType::Lock).await?;

    let lock1: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(60), table.clone()).await?;
    assert!(lock1.is_some(), "First lock acquisition should succeed");

    let lock2: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(60), table.clone()).await?;
    assert!(lock2.is_none(), "Second lock acquisition should fail while first lock is held");

    Ok(())
}

#[tokio::test]
async fn test_lock_release_allows_reacquisition() -> Result<()> {
    let (_container, dynamo, table) = common::setup_dynamodb(common::TableType::Lock).await?;

    let lock1: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(60), table.clone()).await?;
    lock1.unwrap().release().await?;

    let lock2: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(60), table.clone()).await?;
    assert!(lock2.is_some(), "Lock should be acquirable after release");

    Ok(())
}

#[tokio::test]
async fn test_lock_renewal_extends_expiration() -> Result<()> {
    let (_container, dynamo, table) = common::setup_dynamodb(common::TableType::Lock).await?;

    let lock: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(5), table.clone()).await?;
    let lock = lock.unwrap();
    let initial_expiration = lock.record.expiration;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let renewed_lock: Option<Lock> = lock.renew(Duration::from_secs(5)).await?;
    let renewed_lock = renewed_lock.unwrap();
    let new_expiration = renewed_lock.record.expiration;

    assert!(
        new_expiration > initial_expiration,
        "Renewed lock should have later expiration: {} vs {}",
        new_expiration,
        initial_expiration
    );

    Ok(())
}

#[tokio::test]
async fn test_lock_expiration_allows_takeover() -> Result<()> {
    let (_container, dynamo, table) = common::setup_dynamodb(common::TableType::Lock).await?;

    let lock1: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(2), table.clone()).await?;
    assert!(lock1.is_some(), "First lock acquisition should succeed");

    let lock2: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(2), table.clone()).await?;
    assert!(lock2.is_none(), "Second worker should fail while lock is held");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let lock3: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(2), table.clone()).await?;
    assert!(lock3.is_some(), "Second worker should acquire lock after first lock expires");

    Ok(())
}

#[tokio::test]
async fn test_same_instance_can_renew_own_lock() -> Result<()> {
    let (_container, dynamo, table) = common::setup_dynamodb(common::TableType::Lock).await?;

    let lock_opt: Option<Lock> = Lock::acquire(dynamo.clone(), Duration::from_secs(10), table.clone()).await?;
    let lock = lock_opt.unwrap();
    let instance_id = lock.record.instance_id.clone();

    let renewed: Option<Lock> = lock.renew(Duration::from_secs(10)).await?;

    assert!(renewed.is_some(), "Same instance should be able to renew its own lock");
    assert_eq!(
        renewed.unwrap().record.instance_id,
        instance_id,
        "Instance ID should remain the same"
    );

    Ok(())
}

#[tokio::test]
async fn test_concurrent_lock_acquisition() -> Result<()> {
    let (_container, dynamo, table) = common::setup_dynamodb(common::TableType::Lock).await?;

    let mut handles = vec![];

    for i in 0..5 {
        let dynamo = dynamo.clone();
        let table = table.clone();

        handles.push(tokio::spawn(async move {
            match Lock::acquire(dynamo, Duration::from_secs(10), table).await {
                Ok(Some(_lock)) => Some(i),
                _ => None,
            }
        }));
    }

    let results: Vec<Option<i32>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r: Result<Option<i32>, _>| r.ok())
        .collect();

    let successful_count = results.iter().filter(|r: &&Option<i32>| r.is_some()).count();

    assert_eq!(
        successful_count, 1,
        "Exactly one worker should acquire the lock, got {} successes",
        successful_count
    );

    Ok(())
}
