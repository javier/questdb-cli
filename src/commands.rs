// src/commands.rs
use crate::completer::SQLCompleter;
use tokio_postgres::Client;

pub async fn execute_query(client: &Client, query: &str) {
    match client.simple_query(query).await {
        Ok(result) => {
            for message in result {
                match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => {
                        let columns = row.columns();
                        for (i, col) in columns.iter().enumerate() {
                            print!("{}: {}", col.name(), row.get(i).unwrap_or("NULL"));
                            if i < columns.len() - 1 {
                                print!(", ");
                            }
                        }
                        println!();
                    }
                    tokio_postgres::SimpleQueryMessage::CommandComplete(count) => {
                        println!("Command completed: {}", count);
                    }
                    _ => {}
                }
            }
        }
        Err(e) => eprintln!("Query failed: {}", e),
    }
}

pub async fn handle_meta_command(
    client: &Client,
    command: &str,
    completer: &mut SQLCompleter,
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
        }
        "\\dt" => {
            execute_query(client, "SELECT * FROM tables()").await;
        }
        "\\dwal" => {
            execute_query(client, "SELECT * FROM wal_tables()").await;
        }
        cmd if cmd.starts_with("\\dstorage") => {
            let table = cmd.trim_start_matches("\\dstorage").trim();
            if table.is_empty() {
                eprintln!("Usage: \\dstorage <table>");
            } else {
                let query = format!("SELECT * FROM table_storage('{}')", table);
                execute_query(client, &query).await;
            }
        }
        "\\refresh" => {
            // Refresh metadata
            println!("Refreshing metadata...");
            if let Err(e) = completer.update_tables(client).await {
                eprintln!("Failed to refresh metadata: {}", e);
            } else {
                println!("Metadata refreshed.");
            }
        }
        _ => println!("Unknown meta command: {}", command),
    }
}

