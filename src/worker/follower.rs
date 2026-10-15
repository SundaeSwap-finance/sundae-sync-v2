use anyhow::{Context, Result};
use bytes::Bytes;
use tonic::{
    transport::{ClientTlsConfig, Endpoint},
    Request, Streaming,
};
use tracing::warn;
use utxorpc_spec::utxorpc::v1::{
    cardano::{Block, BlockHeader},
    sync::{
        any_chain_block::Chain, follow_tip_response::Action,
        sync_service_client::SyncServiceClient, BlockRef, FollowTipRequest, FollowTipResponse,
    },
};

pub struct Follower {
    tip: Streaming<FollowTipResponse>,
}

impl Follower {
    pub async fn new(uri: &str, api_key: &Option<String>, points: Vec<BlockRef>) -> Result<Self> {
        let endpoint = Endpoint::from_shared(uri.to_owned())?
            .tls_config(ClientTlsConfig::new().with_enabled_roots())?;
        let mut client =
            SyncServiceClient::new(endpoint.connect_lazy()).max_decoding_message_size(usize::MAX);
        let mut request = Request::new(FollowTipRequest {
            intersect: points,
            field_mask: None,
        });
        if let Some(api_key) = api_key {
            request.metadata_mut().insert(
                "dmtr-api-key",
                api_key
                    .parse()
                    .context("invalid UTxO RPC API key metadata")?,
            );
        }
        Ok(Self {
            tip: client.follow_tip(request).await?.into_inner(),
        })
    }

    pub async fn next_event(&mut self) -> Result<(bool, Bytes, Block, BlockHeader)> {
        let (bytes, block, roll_forward) = loop {
            let event = self.tip.message().await?.context("Stream closed")?;
            let action = event.action.context("missing follow-tip action")?;
            let roll_forward = matches!(action, Action::Apply(_));
            match action {
                Action::Apply(block) | Action::Undo(block) => {
                    let Some(Chain::Cardano(parsed)) = block.chain else {
                        anyhow::bail!("expected parsed Cardano block");
                    };
                    break (block.native_bytes, parsed, roll_forward);
                }
                Action::Reset(b) => {
                    warn!("Upstream requested reset to {:?}", b);
                    continue;
                }
            };
        };
        let header = block
            .header
            .as_ref()
            .context("must include header")?
            .clone();

        Ok((roll_forward, bytes, block, header))
    }
}
