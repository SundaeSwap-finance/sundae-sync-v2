use anyhow::{bail, Context, Result};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use pallas::{
    interop::utxorpc::Mapper,
    network::{
        facades::NodeClient,
        miniprotocols::{
            chainsync::{self, NextResponse},
            Point,
        },
    },
};
use sundae_sync_v2::archive::{Archive, NoContext};
use tracing::info;

use crate::args::Args;

mod args;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let archive = construct_archive(&args).await?;

    let magic = args.network_magic.unwrap_or(764824073);
    let mut client = NodeClient::connect(args.socket_path, magic).await?;
    let cs = client.chainsync();

    let sync_from = decode_point(&args.sync_from)?;
    let sync_to = decode_point(&args.sync_to)?;
    let (Some(_), _) = cs.find_intersect(vec![sync_from.clone()]).await? else {
        bail!("sync_from not found")
    };
    let mut point = sync_from;
    restore_history(cs, archive, sync_to, &mut point)
        .await
        .inspect(|()| info!("Successfully restored to {point:?}"))
        .with_context(|| format!("Failed to restore past {point:?}"))
}

async fn restore_history(
    cs: &mut chainsync::N2CClient,
    archive: Archive,
    target: Point,
    synced_to: &mut Point,
) -> Result<()> {
    let mapper = Mapper::new(NoContext);
    loop {
        match cs.request_or_await_next().await? {
            NextResponse::RollBackward(_, _) => bail!("unexpected rollback"),
            NextResponse::Await => continue,
            NextResponse::RollForward(content, _) => {
                let block = mapper.map_block_cbor(&content.0);
                let Some(header) = block.header.as_ref() else {
                    bail!("Block without a header")
                };
                let point = Point::Specific(header.slot, header.hash.to_vec());
                archive.save(&block, content.0).await?;
                *synced_to = point;
                if synced_to == &target {
                    return Ok(());
                }
            }
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
