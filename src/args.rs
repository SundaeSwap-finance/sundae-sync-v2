use std::path::PathBuf;

use clap::{command, Parser};
use pallas::network::miniprotocols::Point;

/// A small utility to crawl the Cardano blockchain and save sample data
#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Args {
    /// The path to the node.sock file to connect to a local node
    #[arg(
        short,
        long,
        env("CARDANO_NODE_SOCKET_PATH"),
        requires = "network_magic"
    )]
    pub socket_path: Option<String>,
    /// The network magic used to handshake with that node; defaults to mainnet
    #[arg(short, long, env("CARDANO_NETWORK_MAGIC"))]
    pub network_magic: Option<u64>,

    #[arg(short, long, env("UTXO_RPC_URL"), conflicts_with = "socket_path")]
    pub utxo_rpc_url: Option<String>,

    /// A list of points to use when trying to decide a startpoint; defaults to origin
    #[arg(short, long, value_parser = parse_point)]
    pub point: Vec<Point>,
    /// Download only the first block found that matches this criteria
    #[arg(long)]
    pub one: bool,
    /// The directory to save the files into
    #[arg(short, long, default_value = "out")]
    pub out: PathBuf,
}

pub fn parse_point(s: &str) -> Result<Point, Box<dyn std::error::Error + Send + Sync + 'static>> {
    if s == "origin" {
        return std::result::Result::Ok(Point::Origin);
    }
    let parts: Vec<_> = s.split('/').collect();
    let slot = parts[0].parse()?;
    let hash = hex::decode(parts[1])?;
    std::result::Result::Ok(Point::Specific(slot, hash))
}
