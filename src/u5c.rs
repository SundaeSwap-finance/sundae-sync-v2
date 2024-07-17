use anyhow::*;
use pallas::network::miniprotocols::Point;
use utxorpc::{spec::sync::BlockRef, CardanoSyncClient, ClientBuilder, TipEvent};

use crate::logic::SundaeSyncLogic;

pub async fn sync_with_utxorpc(
    logic: SundaeSyncLogic,
    uri: impl ToString,
    points: Vec<Point>,
) -> Result<()> {
    let mut client = ClientBuilder::new()
        .uri(uri)?
        .build::<CardanoSyncClient>()
        .await;

    let points: Vec<BlockRef> = points
        .iter()
        .map(|p| match p {
            Point::Origin => BlockRef::default(),
            Point::Specific(index, hash) => BlockRef {
                index: *index,
                hash: hash.clone().into(),
            },
        })
        .collect();

    let mut tip = client
        .follow_tip(points)
        .await
        .context("failed to follow tip")?;

    // TODO: https://github.com/txpipe/dolos/issues/294
    let mut first = true;

    while let std::result::Result::Ok(event) = tip.event().await {
        match event {
            TipEvent::Apply(block) => {
                if first {
                    first = false;
                } else {
                    // TODO: Raw Client
                    logic.handle_block(vec![], block).await?;
                };
            }
            TipEvent::Undo(block) => logic.handle_undo(vec![], block).await?,
            TipEvent::Reset(point) => println!("reset: {}", point.index),
        }
    }
    Ok(())
}
