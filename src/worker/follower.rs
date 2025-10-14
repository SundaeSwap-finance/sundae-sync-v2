use anyhow::Result;
use bytes::Bytes;
use tracing::warn;
use utxorpc::{
    spec::{
        cardano::{Block, BlockHeader},
        sync::BlockRef,
    },
    Cardano, CardanoSyncClient, ClientBuilder, LiveTip, TipEvent,
};

pub struct Follower {
    tip: LiveTip<Cardano>,
}

impl Follower {
    pub async fn new(
        uri: &String,
        api_key: &Option<String>,
        points: Vec<BlockRef>,
    ) -> Result<Self> {
        let mut client_builder = ClientBuilder::new().uri(uri)?;
        if let Some(api_key) = api_key {
            client_builder = client_builder.metadata("dmtr-api-key", api_key)?;
        }
        let mut client = client_builder.build::<CardanoSyncClient>().await;
        Ok(Self {
            tip: client.follow_tip(points).await?,
        })
    }

    pub async fn next_event(&mut self) -> Result<(bool, Bytes, Block, BlockHeader)> {
        let (bytes, block, roll_forward) = loop {
            let event = self.tip.event().await?;

            match &event {
                Some(TipEvent::Apply(block)) | Some(TipEvent::Undo(block)) => {
                    let bytes = block.native.clone();
                    let block = block.parsed.clone().expect("must include block");

                    break (bytes, block, matches!(event, Some(TipEvent::Apply(_))));
                }
                Some(TipEvent::Reset(b)) => {
                    warn!("Upstream requested reset to {:?}", b);
                    continue;
                }
                None => return Err(anyhow::anyhow!("Stream closed")),
            };
        };
        let header = block.header.as_ref().expect("must include header").clone();

        Ok((roll_forward, bytes, block, header))
    }
}
