// src/cli.rs
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "questdb-cli", version, about = "A CLI for QuestDB")]
pub struct Cli {
    /// Hostname or IP address of the QuestDB server (default: localhost)
    #[arg(short = 'H', long)]
    pub host: Option<String>,

    /// Port number of the QuestDB server (default: 8812)
    #[arg(short = 'p', long)]
    pub port: Option<u16>,

    /// Use TLS for the connection (default: false)
    #[arg(short = 's', long)]
    pub use_tls: bool,

    /// Accept invalid TLS certificates (self-signed)
    #[arg(short = 'k', long)]
    pub allow_invalid_cert: bool,

    /// Database user (default: admin)
    #[arg(short = 'u', long)]
    pub user: Option<String>,

    /// Database password (default: quest)
    #[arg(short = 'w', long)]
    pub password: Option<String>,

    /// Database name (default: qdb)
    #[arg(short = 'd', long)]
    pub dbname: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Execute a SQL query directly
    Query {
        /// The SQL query to execute
        sql: String,
    },
}

