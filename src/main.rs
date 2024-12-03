// src/main.rs

mod cli;
mod commands;
mod completer;
mod db;
mod output;
mod repl;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use db::connect_to_db;
use repl::start_repl;
use commands::{execute_script, execute_query_command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let host = cli.host.unwrap_or_else(|| "localhost".to_string());
    let port = cli.port.unwrap_or(8812);
    let use_tls = cli.use_tls;

    let user = cli.user.as_deref().unwrap_or("admin");
    let password = cli.password.as_deref().unwrap_or("quest");
    let dbname = cli.dbname.as_deref().unwrap_or("qdb");

    match connect_to_db(
        &host,
        port,
        user,
        password,
        dbname,
        use_tls,
        cli.allow_invalid_cert,
    )
    .await
    {
        Ok(client) => {
            match cli.command {
                Some(Commands::Exec { sql }) => {
                    if let Err(e) = execute_query_command(&client, &sql, &cli.format).await {
                        eprintln!("Error executing query: {}", e);
                    }
                }
                Some(Commands::ExecFrom { source }) => {
                    if let Err(e) = execute_script(&client, &source, &cli.format).await {
                        eprintln!("Error executing script: {}", e);
                    }
                }
                _ => {
                    println!("Connected to QuestDB at {}:{}.", host, port);
                    if let Err(e) = start_repl(client, &cli.format, &cli.history_file).await {
                        eprintln!("Error in REPL: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
        }
    }

    Ok(())
}
