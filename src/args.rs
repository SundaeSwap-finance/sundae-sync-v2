use clap::{command, Parser};

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

    /// The URL of the utxorpc server to connect to
    #[arg(short, long, env("UTXO_RPC_URL"), conflicts_with = "socket_path")]
    pub utxo_rpc_url: Option<String>,

    #[arg(short, long, env("ARCHIVE_BUCKET"))]
    pub archive_bucket: String,
    #[arg(short, long, env("LOCK_TABLE"))]
    pub lock_table: String,
    #[arg(short, long, env("DESTINATION_TABLE"))]
    pub destination_table: String,
    #[arg(short, long, env("UTXO_TABLE"))]
    pub utxo_table: String,
}
