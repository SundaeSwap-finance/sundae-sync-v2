use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use anyhow::{anyhow, bail};
use aws_sdk_dynamodb::{error::SdkError, types::AttributeValue, Client as DynamoClient};
use serde::{Deserialize, Serialize};
use serde_dynamo::aws_sdk_dynamodb_1::to_item;
use std::fmt::Debug;
use tracing::{info, trace};
use uuid::Uuid;

/// A dynamodb record for holding the lock
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct LockRecord {
    pub pk: String,
    pub sk: String,
    pub instance_id: String,
    pub expiration: u64,
}

/// A lock RAII guard
pub struct Lock {
    /// The details about the lock stored in dynamodb
    pub record: LockRecord,
    /// Whether the lock is currently held or not, mostly used to avoid stack explosions in drop
    locked: bool,
    /// Optionally, a dynamodb client, set if we have the lock
    dynamo: DynamoClient,
}

impl Debug for Lock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.record.fmt(f)
    }
}

impl Lock {
    /// Acquire a new lock from scratch
    pub async fn acquire(dynamo: DynamoClient, duration: Duration) -> Result<Option<Lock>> {
        trace!("Acquiring lock");
        // We do this by constructing the lock, and then trying to renew it
        let instance_id = Uuid::new_v4().to_string();
        let lock_record = LockRecord {
            pk: "sundae-sync-v2-lock".to_string(),
            sk: "sundae-sync-v2-lock".to_string(),
            instance_id: instance_id.clone(),
            expiration: 0,
        };
        let lock = Lock {
            locked: false,
            dynamo: dynamo.clone(),
            record: lock_record,
        };
        lock.renew(duration).await
    }

    /// Renew an existing lock, setting the expiration to now + duration
    pub async fn renew(mut self, duration: Duration) -> Result<Option<Lock>> {
        trace!("Renewing lock");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards");

        self.record.expiration = (now + duration).as_millis() as u64;
        // Insert the item into dynamo, with a condition so it succeeds
        // only if one of the following is true:
        // - The lock doesn't exist at all
        // - The lock is already held by this instance
        // - The lock expired in the past
        let result = self
            .dynamo
            .put_item()
            .table_name("sundae-sync-v2-test-table")
            .set_item(Some(to_item(&self.record)?))
            .condition_expression(
                "attribute_not_exists(pk) OR instance_id = :instance_id OR expiration < :now",
            )
            .expression_attribute_values(
                ":instance_id",
                AttributeValue::S(self.record.instance_id.clone()),
            )
            .expression_attribute_values(":now", AttributeValue::N(now.as_millis().to_string()))
            .send()
            .await;

        match result {
            Ok(_) => {
                // If we succeeded in acquiring the lock, record it and return self
                self.locked = true;
                Ok(Some(self))
            }
            Err(SdkError::ServiceError(err)) => {
                // If we failed just because of a conditional check
                // then we just didn't acquire the lock; so report None instead of an error
                let err = err.into_err();
                if err.is_conditional_check_failed_exception() {
                    return Ok(None);
                }
                // Otherwise, for some other kind of error, return that error
                bail!("error acquiring lock: {:#?}", err);
            }
            err => {
                bail!("failed to acquire lock: {:#?}", err);
            }
        }
    }

    /// Release the lock
    pub async fn release(mut self) -> Result<()> {
        if self.locked {
            self.locked = false;
            info!("Releasing lock");
            // Delete from dynamodb, but only if:
            // the lock doesn't exist, or the lock is held by us
            let result = self
                .dynamo
                .delete_item()
                .table_name("sundae-sync-v2-test-table")
                .key("pk", AttributeValue::S("sundae-sync-v2-lock".to_string()))
                .key("sk", AttributeValue::S("sundae-sync-v2-lock".to_string()))
                .condition_expression("attribute_not_exists(pk) OR instance_id = :instance_id")
                .expression_attribute_values(
                    ":instance_id",
                    AttributeValue::S(self.record.instance_id.clone()),
                )
                .send()
                .await;

            match result {
                Ok(_) => Ok(()),
                Err(SdkError::ServiceError(err)) => {
                    let err = err.into_err();
                    if err.is_conditional_check_failed_exception() {
                        Ok(())
                    } else {
                        Err(anyhow!("failed to release lock: {:?}", err))
                    }
                }
                err => Err(anyhow!("failed to release lock: {:?}", err)),
            }
        } else {
            Ok(())
        }
    }
}
