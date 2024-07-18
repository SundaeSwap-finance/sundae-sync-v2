use std::time::Duration;

use crate::Worker;

use super::lock::Lock;
use anyhow::Result;
use aws_sdk_dynamodb::Client as DynamoClient;
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// A lock thread is responsible for acquiring an exclusive
/// lock, and then spawning a given worker thread while that
/// lock is held, renewing at some regular interval
pub struct LockThread {
    pub lock_duration: Duration,
    pub lock_acquire_freq: Duration,
    pub lock_renew_freq: Duration,
    pub lock_stall_window: Duration,

    pub dynamo: DynamoClient,
}

impl LockThread {
    /// Loops until cancelled, acquiring / renewing a lock, and calling worker_thread while it's open
    pub async fn maintain_lock(self, cancel: CancellationToken, worker: Worker) -> Result<()> {
        info!("Starting lock thread");

        // Make sure there's a safety zone before expiration before we renew
        // In case we stall out on renewing the lock for some reason
        // If we acquire the lock for 20s, and renew every 10s, then we don't
        // want to do any work if we haven't renewed the lock by 15s in, for example
        let deadline_offset =
            (self.lock_duration - self.lock_renew_freq - self.lock_stall_window).as_millis() as u64;

        'outer: loop {
            // If we've been cancelled, don't try to acquire the lock
            if cancel.is_cancelled() {
                info!("Lock thread cancelled");
                break;
            }
            // Acquire the lock for a specific duration
            let lock = Lock::acquire(self.dynamo.clone(), self.lock_duration).await?;
            info!("Done");
            match lock {
                None => {
                    // We failed to acquire the lock, so sleep for some time (or until cancellation)
                    info!("Failed to acquire lock");
                    let cancelled = select! {
                        _ = cancel.cancelled() => { true }
                        _ = sleep(self.lock_acquire_freq) => { false }
                    };
                    if cancelled {
                        info!("Lock thread cancelled without lock");
                        break;
                    }
                }
                Some(inner) => {
                    // We *did* acquire a lock, so now we can spawn the worker
                    // thread, and then continually renew the lock
                    let lock_id = inner.record.instance_id.clone();
                    info!("Lock {} acquired", lock_id);

                    // Create a `watch` so that the worker thread always knows if we've renewed the lock
                    let (deadline_sender, dr) =
                        tokio::sync::watch::channel(inner.record.expiration - deadline_offset);

                    // Store the lock in an option, so we can mutate this variable each time we renew
                    let mut lock = Some(inner);
                    // Construct the future described by the worker thread
                    // Doesn't actually run anything until we select below
                    // Pin it so it survives async memory reshuffles
                    // Pass it the lock_id, mainly for log messages, and the deadline receiver,
                    // so it knows we're keeping the lock renewed
                    let mut task = std::pin::pin!(worker.worker_thread(lock_id.clone(), dr));

                    // Then, we can sit in a loop
                    loop {
                        // Wait until either
                        select! {
                            // We're cancelled, and we need to abort the loop
                            _ = cancel.cancelled() => {
                                // We break out of the *outer* loop to actually exit everything
                                info!("Lock {} thread cancelled with lock", lock_id);
                                lock.expect("not holding lock??").release().await?;
                                break 'outer;
                            }
                            // it's time to renew the lock
                            _ = sleep(self.lock_renew_freq) => {
                                info!("Renewing Lock {}", lock_id);
                                lock = lock.expect("not holding lock??").renew(self.lock_duration).await?;
                                // we may fail to renew the lock!
                                match &lock {
                                    Some(inner) => {
                                        // If we did renew the lock, extend out the deadline for the worker thread
                                        deadline_sender.send(inner.record.expiration - deadline_offset)?;
                                    },
                                    None => {
                                        // Otherwise, we break out of the loop so that we
                                        // drop the worker future, and re-acquire the lock
                                        info!("Lost lock {}", lock_id);
                                        break;
                                    },
                                }
                            }
                            // or, the worker future fails for some reason;
                            // for example, maybe the node hasn't produced blocks in 5 minutes
                            // this actually runs the worker future
                            result = &mut task => {
                                info!("Lock {} Worker failed: {:?}", lock_id, result);
                                // Release the lock, then sleep for twice as long as our acquire frequency
                                // to give someone else a good chance of picking up the lock
                                lock.unwrap().release().await?;
                                sleep(2 * self.lock_acquire_freq).await;
                                break;
                            }
                        }
                    }
                }
            }
        }
        info!("Shutting down lock thread");
        Ok(())
    }
}
