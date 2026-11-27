//! The one number an operator needs from the producer: did it publish a NEW
//! block lately. `BlocksAdvanced` counts blocks that moved a destination's
//! cursor forward, flushed once a minute — including zeros, so a producer that
//! is alive but not advancing (an upstream node stuck on one block) reads as
//! zero rather than as missing data. The stream-level metrics cannot tell
//! that case from health: a stuck producer still emits records.

use anyhow::Result;
use aws_sdk_cloudwatch::{
    types::{Dimension, MetricDatum, StandardUnit},
    Client as CloudWatchClient,
};
use tracing::warn;

pub const NAMESPACE: &str = "SundaeSync";
pub const BLOCKS_ADVANCED: &str = "BlocksAdvanced";

/// Counts advances between flushes. Pure, so the arithmetic is testable
/// without CloudWatch.
#[derive(Debug, Default)]
pub struct AdvanceCounter {
    advanced: u64,
}

impl AdvanceCounter {
    pub fn record(&mut self, advanced: bool) {
        if advanced {
            self.advanced += 1;
        }
    }

    /// The count since the last take, which resets it.
    pub fn take(&mut self) -> u64 {
        std::mem::take(&mut self.advanced)
    }
}

pub struct Metrics {
    pub client: CloudWatchClient,
    /// The destination table name, unique per environment, as the dimension.
    pub table: String,
    pub counter: AdvanceCounter,
}

impl Metrics {
    /// Publishes the count since the last flush. A failure is logged and
    /// dropped: the metric must never take the producer down.
    pub async fn flush(&mut self) {
        let advanced = self.counter.take();
        if let Err(e) = self.put(advanced).await {
            warn!("failed to publish {BLOCKS_ADVANCED}={advanced}: {e:#}");
        }
    }

    async fn put(&self, advanced: u64) -> Result<()> {
        let datum = MetricDatum::builder()
            .metric_name(BLOCKS_ADVANCED)
            .dimensions(
                Dimension::builder()
                    .name("Table")
                    .value(&self.table)
                    .build(),
            )
            .unit(StandardUnit::Count)
            .value(advanced as f64)
            .build();
        self.client
            .put_metric_data()
            .namespace(NAMESPACE)
            .metric_data(datum)
            .send()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::AdvanceCounter;

    #[test]
    fn counts_only_real_advances_and_resets_on_take() {
        let mut c = AdvanceCounter::default();
        c.record(true);
        c.record(false);
        c.record(true);
        assert_eq!(c.take(), 2);
        // A quiet minute reads as zero, not as nothing.
        assert_eq!(c.take(), 0);
    }
}
