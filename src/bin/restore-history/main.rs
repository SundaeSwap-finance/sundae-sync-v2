use anyhow::{bail, Context, Result};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use pallas::{
    codec::Fragment,
    interop::utxorpc::Mapper,
    ledger::traverse::MultiEraHeader,
    network::{
        facades::{NodeClient, PeerClient},
        miniprotocols::{
            chainsync::{self, Message, NextResponse},
            Point,
        },
    },
};
use sundae_sync_v2::archive::{Archive, NoContext};
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::args::Args;

mod args;

enum ClientImpl {
    Local(NodeClient),
    Remote(PeerClient),
}

struct Client {
    inner: ClientImpl,
    synced_from: Point,
}

impl Client {
    async fn connect(args: &Args) -> Result<Self> {
        let magic = args.network_magic.unwrap_or(764824073);
        let inner = if let Some(path) = args.socket_path.as_ref() {
            let client = NodeClient::connect(path, magic).await?;
            ClientImpl::Local(client)
        } else if let Some(address) = args.peer_address.as_ref() {
            let client = PeerClient::connect(address, magic).await?;
            ClientImpl::Remote(client)
        } else {
            bail!("must provide --socket-path or --peer-address")
        };
        Ok(Self {
            inner,
            synced_from: Point::Origin,
        })
    }

    async fn find_intersect(&mut self, point: Point) -> Result<()> {
        async fn find_intersect<D>(client: &mut chainsync::Client<D>, point: Point) -> Result<()>
        where
            Message<D>: Fragment,
        {
            let (Some(p), _) = client.find_intersect(vec![point.clone()]).await? else {
                bail!("intersect not found");
            };
            if p != point {
                bail!("unexpected intersect {p:?}");
            } else {
                Ok(())
            }
        }
        self.synced_from = point.clone();
        match &mut self.inner {
            ClientImpl::Local(client) => find_intersect(client.chainsync(), point).await,
            ClientImpl::Remote(client) => find_intersect(client.chainsync(), point).await,
        }
    }

    async fn next_block(&mut self) -> Result<Vec<u8>> {
        async fn next_block<D>(client: &mut chainsync::Client<D>, synced_from: &Point) -> Result<D>
        where
            Message<D>: Fragment,
        {
            loop {
                match client.request_or_await_next().await? {
                    NextResponse::Await => continue,
                    NextResponse::RollBackward(roll_back_to, _) => {
                        if synced_from == &roll_back_to {
                            continue;
                        } else {
                            bail!("unexpected rollback");
                        }
                    }
                    NextResponse::RollForward(d, _) => return Ok(d),
                }
            }
        }
        match &mut self.inner {
            ClientImpl::Local(client) => {
                let block = next_block(client.chainsync(), &self.synced_from).await?;
                Ok(block.0)
            }
            ClientImpl::Remote(client) => {
                let header = next_block(client.chainsync(), &self.synced_from).await?;
                let hdr_tag = header.byron_prefix.map(|p| p.0);
                let hdr_variant = header.variant;
                let hdr = MultiEraHeader::decode(hdr_variant, hdr_tag, &header.cbor)?;
                let point = Point::Specific(hdr.slot(), hdr.hash().to_vec());
                let block = client.blockfetch().fetch_single(point).await?;
                Ok(block)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    info!("Starting sundae-sync-v2 restore history");

    let archive = construct_archive(&args).await?;

    let mut client = Client::connect(&args).await?;

    let sync_from = decode_point(&args.sync_from)?;
    let sync_to = decode_point(&args.sync_to)?;
    info!("Restoring from {sync_from:?} to {sync_to:?}");
    client.find_intersect(sync_from.clone()).await?;
    let mut point = sync_from;
    restore_history(&mut client, archive, sync_to, &mut point)
        .await
        .inspect(|()| info!("Successfully restored to {point:?}"))
        .with_context(|| format!("Failed to restore past {point:?}"))
}

async fn restore_history(
    client: &mut Client,
    archive: Archive,
    target: Point,
    synced_to: &mut Point,
) -> Result<()> {
    let mapper = Mapper::new(NoContext);
    loop {
        let raw_block = client.next_block().await?;
        let block = mapper.map_block_cbor(&raw_block);
        let Some(header) = block.header.as_ref() else {
            bail!("Block without a header")
        };
        let point = Point::Specific(header.slot, header.hash.to_vec());
        let height = header.height;
        let mut attempt = 0u32;
        loop {
            match archive.save(&block, raw_block.clone()).await {
                Ok(()) => break,
                Err(e) => {
                    attempt += 1;
                    if attempt > 10 {
                        return Err(e).context("archive.save failed after 10 retries");
                    }
                    let delay = Duration::from_millis(100 * 2u64.pow(attempt.min(7)));
                    warn!(
                        "archive.save failed (attempt {attempt}/10, retrying in {delay:?}): {e:#}"
                    );
                    sleep(delay).await;
                }
            }
        }
        if height.is_multiple_of(100) {
            info!("Restored up to {point:?}");
        }
        *synced_to = point;
        if synced_to == &target {
            return Ok(());
        }
    }
}

async fn construct_archive(args: &Args) -> Result<Archive> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let s3_client = S3Client::new(&config);
    let dynamo_client = DynamoClient::new(&config);
    Ok(Archive {
        s3: s3_client,
        bucket: args.archive_bucket.clone(),
        dynamo: dynamo_client,
        table_name: args.lookup_table.clone(),
    })
}

fn decode_point(desc: &str) -> Result<Point> {
    if desc == "origin" {
        return Ok(Point::Origin);
    }
    let Some((slot, hash)) = desc.split_once(":") else {
        bail!("invalid point");
    };
    let slot = slot.parse()?;
    let hash = hex::decode(hash)?;
    Ok(Point::Specific(slot, hash))
}
