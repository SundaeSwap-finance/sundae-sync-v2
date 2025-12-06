use clap::Parser;

/// A small utility to crawl the Cardano blockchain and save sample data
#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Args {
    /// The path to the node.sock file to connect to a local node
    #[arg(long, env("CARDANO_NODE_SOCKET_PATH"), conflicts_with = "peer_address")]
    pub socket_path: Option<String>,
    #[arg(long, env("CARDANO_NODE_PEER_ADDRESS"), conflicts_with = "socket_path")]
    pub peer_address: Option<String>,
    /// The network magic used to handshake with that node; defaults to mainnet
    #[arg(long, env("CARDANO_NETWORK_MAGIC"))]
    pub network_magic: Option<u64>,

    #[arg(long, env("ARCHIVE_BUCKET"))]
    pub archive_bucket: String,
    #[arg(long, env("LOOKUP_TABLE"))]
    pub lookup_table: String,

    #[arg(long, env("SYNC_FROM"))]
    pub sync_from: String,
    #[arg(long, env("SYNC_TO"))]
    pub sync_to: String,
}
