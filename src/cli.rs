// src/cli.rs

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(name = "QuestDB CLI", version, about, long_about = None)]
pub struct Cli {
    /// Host address of QuestDB
    #[clap(short = 'H', long)]
    pub host: Option<String>,

    /// Port number of QuestDB
    #[clap(short = 'p', long)]
    pub port: Option<u16>,

    /// Username for authentication
    #[clap(short = 'u', long)]
    pub user: Option<String>,

    /// Password for authentication
    #[clap(short = 'P', long)]
    pub password: Option<String>,

    /// Database name
    #[clap(short = 'd', long)]
    pub dbname: Option<String>,

    /// Use TLS for the connection
    #[clap(long)]
    pub use_tls: bool,

    /// Allow invalid TLS certificates
    #[clap(long)]
    pub allow_invalid_cert: bool,

    /// Output format (table, csv, json, vertical)
    #[clap(short = 'f', long, default_value = "table")]
    pub format: String,

    /// Command history file
    #[clap(short = 'c', long, default_value = "history.txt")]
    pub history_file: String,

    /// Subcommands
    #[clap(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Execute a single SQL query
    Exec {
        /// SQL query to execute
        sql: String,
    },
    /// Execute SQL queries from a script file
    ExecFrom {
        /// Path to the SQL script file
        source: String,
    },
}
