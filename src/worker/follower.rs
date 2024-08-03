use bytes::Bytes;
use utxorpc::{
    spec::{
        cardano::{Block, BlockHeader},
        sync::BlockRef,
    },
    Cardano, CardanoSyncClient, ClientBuilder, LiveTip, TipEvent,
};

use anyhow::{Context, Result};

pub struct Follower {
    tip: LiveTip<Cardano>,
}

impl Follower {
    pub async fn new(uri: &String, points: Vec<BlockRef>) -> Result<Self> {
        let mut client = ClientBuilder::new()
            .uri(uri)?
            .build::<CardanoSyncClient>()
            .await;
        Ok(Self {
            tip: client.follow_tip(points).await?,
        })
    }

    pub async fn next_event(&mut self) -> Result<(bool, Bytes, Block, BlockHeader)> {
        let event = self.tip.event().await.context("failed to grab tip")?;
        let (bytes, block) = match &event {
            TipEvent::Apply(block) | TipEvent::Undo(block) => {
                let bytes = block.native.clone();
                let block = block.parsed.clone().expect("must include block");

                (bytes, block)
            }
            TipEvent::Reset(_) => todo!(),
        };
        let header = block.header.as_ref().expect("must include header").clone();

        Ok((matches!(event, TipEvent::Apply(_)), bytes, block, header))
    }
}
