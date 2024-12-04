use crate::output::print_query_results;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use tokio_postgres::{Client, types::ToSql};

pub async fn execute_query_command(
    client: &Client,
    query: &str,
    format: &str,
) -> Result<()> {
    let params: &[&(dyn ToSql + Sync)] = &[];

    let stream = client.query_raw(query, params.iter().map(|p| *p)).await?;
    futures::pin_mut!(stream);

    while let Some(row_result) = stream.next().await {
        match row_result {
            Ok(row) => {
                print_query_results(vec![row], format).await?;
            }
            Err(e) => {
                eprintln!("Error processing row: {}", e);
                break;
            }
        }
    }

    Ok(())
}

pub async fn handle_meta_command(
    client: &Client,
    command: &str,
    completer: &mut crate::completer::SQLCompleter,
    format: &mut String,
) {
    match command {
        "\\help" => {
            println!("Meta commands:");
            println!("  \\q                 Quit");
            println!("  \\help              Show this help message");
            println!("  \\dt                List all tables");
            println!("  \\dwal              List all WAL tables");
            println!("  \\dstorage <table>  Show storage details for a table");
            println!("  \\refresh           Refresh metadata");
            println!("  \\format [format]   Set output format (table, csv, json, vertical)");
        }
        "\\dt" => {
            if let Err(e) = execute_query_command(client, "SELECT * FROM tables()", format).await {
                eprintln!("Error executing \\dt: {}", e);
            }
        }
        "\\dwal" => {
            if let Err(e) = execute_query_command(client, "SELECT * FROM wal_tables()", format).await {
                eprintln!("Error executing \\dwal: {}", e);
            }
        }
        cmd if cmd.starts_with("\\dstorage") => {
            let table = cmd.trim_start_matches("\\dstorage").trim();
            if table.is_empty() {
                eprintln!("Usage: \\dstorage <table>");
            } else {
                let query = format!("SELECT * FROM table_storage('{}')", table);
                if let Err(e) = execute_query_command(client, &query, format).await {
                    eprintln!("Error executing \\dstorage: {}", e);
                }
            }
        }
        "\\refresh" => {
            println!("Refreshing metadata...");
            if let Err(e) = completer.update_tables(client).await {
                eprintln!("Failed to refresh metadata: {}", e);
            } else {
                println!("Metadata refreshed.");
            }
        }
        cmd if cmd.starts_with("\\format") => {
            let args = cmd.trim_start_matches("\\format").trim();
            if args.is_empty() {
                println!("Current format: {}", format);
                println!("Available formats: table, csv, json, vertical");
            } else {
                *format = args.to_string();
                println!("Output format set to '{}'", format);
            }
        }
        _ => println!("Unknown meta command: {}", command),
    }
}

pub async fn execute_script(client: &Client, source: &str, format: &str) -> Result<()> {
    let content = if source.starts_with("http://") || source.starts_with("https://") {
        let response = reqwest::Client::new().get(source).send().await?;
        response.text().await?
    } else {
        std::fs::read_to_string(source)?
    };

    let dialect = sqlparser::dialect::GenericDialect {};
    let statements = sqlparser::parser::Parser::parse_sql(&dialect, &content)
        .map_err(|e| anyhow!("Failed to parse SQL: {}", e))?;

    for statement in statements {
        let query = statement.to_string();
        println!("Executing: {}", query);
        if let Err(e) = execute_query_command(client, &query, format).await {
            eprintln!("Error executing query in script: {}", e);
        }
    }

    Ok(())
}
