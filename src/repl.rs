// src/repl.rs
use crate::commands::{execute_query, handle_meta_command};
use crate::completer::SQLCompleter;
use rustyline::Editor;
use tokio_postgres::Client;

pub async fn start_repl(client: Client) {
    let mut completer = SQLCompleter::new();

    // Fetch tables before starting the REPL
    if let Err(e) = completer.update_tables(&client).await {
        eprintln!("Failed to fetch table names: {}", e);
    }

    // Create an Editor with the helper
    let mut rl = Editor::new();
    rl.set_helper(Some(completer));

    println!("Type '\\q' to quit.");

    loop {
        match rl.readline("questdb> ") {
            Ok(line) => {
                let trimmed = line.trim();
                rl.add_history_entry(trimmed);

                if trimmed == "\\q" {
                    println!("Goodbye!");
                    break;
                } else if trimmed.starts_with('\\') {
                    if let Some(helper) = rl.helper_mut() {
                        handle_meta_command(&client, trimmed, helper).await;
                    } else {
                        eprintln!("Helper not available.");
                    }
                } else if !trimmed.is_empty() {
                    execute_query(&client, trimmed).await;
                }
            }
            Err(_) => {
                println!("Error reading input. Exiting...");
                break;
            }
        }
    }
}

