// src/main.rs
mod cli;
mod commands;
mod completer;
mod db;
mod repl;

use anyhow::Result;
use clap::Parser; // Import the Parser trait
use cli::{Cli, Commands};
use db::connect_to_db;
use repl::start_repl;

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
            println!("Connected to QuestDB at {}:{}.", host, port);
            if let Some(Commands::Query { sql }) = cli.command {
                commands::execute_query(&client, &sql).await;
            } else {
                start_repl(client).await;
            }
        }
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
        }
    }

    Ok(())
}

