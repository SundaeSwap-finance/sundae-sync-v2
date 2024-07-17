use anyhow::*;
use pallas::{
    interop::utxorpc::{LedgerContext, Mapper, TxoRef, UtxoMap},
    network::{
        facades::NodeClient,
        miniprotocols::{chainsync::NextResponse, Point},
    },
};
use std::{collections::VecDeque, path::Path};
use tracing::warn;

use crate::logic::SundaeSyncLogic;

#[derive(Clone)]
struct NoContext;
impl LedgerContext for NoContext {
    fn get_utxos(&self, _refs: &[TxoRef]) -> Option<UtxoMap> {
        None
    }
}

pub async fn sync_with_socket(
    logic: SundaeSyncLogic,
    socket_path: impl AsRef<Path>,
    magic: u64,
    points: Vec<Point>,
) -> Result<()> {
    let mut client = NodeClient::connect(socket_path, magic).await?;
    let (point, _) = client.chainsync().find_intersect(points).await?;

    if point.is_none() {
        warn!("syncing from origin")
    }

    let mut recent_blocks = VecDeque::new();
    let mapper = Mapper::new(NoContext {});

    loop {
        let next = client.chainsync().request_or_await_next().await?;
        match next {
            NextResponse::RollForward(bytes, _) => {
                let bytes = bytes.to_vec();
                recent_blocks.push_front(bytes.clone());
                while recent_blocks.len() > 2160 {
                    recent_blocks.pop_back();
                }
                let block = mapper.map_block_cbor(&bytes);

                logic.handle_block(bytes, block).await?;
            }
            // Since we're just scraping data until we catch up, we don't need to handle rollbacks
            NextResponse::RollBackward(point, _) => {
                while recent_blocks.len() > 0 {
                    let bytes = recent_blocks.pop_front().unwrap();
                    let block = mapper.map_block_cbor(&bytes);
                    if block.header.as_ref().unwrap().slot == point.slot_or_default() {
                        recent_blocks.push_front(bytes.to_vec());
                        break;
                    } else {
                        logic.handle_undo(bytes, block).await?;
                    }
                }
            }
            NextResponse::Await => continue,
        }
    }
}
