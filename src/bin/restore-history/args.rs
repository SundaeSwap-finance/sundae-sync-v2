use clap::Parser;

/// A small utility to crawl the Cardano blockchain and save sample data
#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Args {
    /// The path to the node.sock file to connect to a local node
    #[arg(long, env("CARDANO_NODE_SOCKET_PATH"), requires = "network_magic")]
    pub socket_path: String,
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
