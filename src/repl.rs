// src/repl.rs

use crate::commands::{execute_query_command, handle_meta_command};
use crate::completer::SQLCompleter;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::signal;
use tokio_postgres::{Client, NoTls};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as TokioMutex;
use anyhow::Result;
use shellexpand::tilde;
use std::io::Write;

pub async fn start_repl(client: Client, default_format: &str, history_file: &str) -> Result<()> {
    let mut completer = SQLCompleter::new();

    // Update table names for auto-completion
    if let Err(e) = completer.update_tables(&client).await {
        eprintln!("Failed to fetch table names: {}", e);
    }

    // Wrap the completer in an Arc and Tokio Mutex for shared access across tasks
    let completer = Arc::new(TokioMutex::new(completer));

    // Initialize the rustyline editor with the completer
    let mut rl = Editor::<SQLCompleter>::new();
    {
        let completer_lock = completer.lock().await;
        rl.set_helper(Some(completer_lock.clone()));
    }

    // Wrap the rl in an Arc<StdMutex<Editor>>
    let rl = Arc::new(StdMutex::new(rl));

    // Load command history from the specified history file
    let history_path = tilde(history_file).to_string();
    {
        let mut rl_guard = rl.lock().unwrap();
        if rl_guard.load_history(&history_path).is_err() {
            println!("No previous history.");
        }
    }

    println!("Connected to QuestDB. Type '\\q' to quit.");

    // Wrap the client in an Arc for shared ownership
    let client = Arc::new(client);

    // Initialize the output format, wrapped in an Arc and Tokio Mutex for thread-safe access
    let format = Arc::new(TokioMutex::new(default_format.to_string()));

    loop {
        tokio::select! {
            // Listen for incoming SIGINT (Ctrl+C)
            _ = signal::ctrl_c() => {
                println!("\nReceived Ctrl+C. Cancelling ongoing query if any.");
                let cancel_token = client.cancel_token();
                let _ = cancel_token.cancel_query(NoTls).await;
            },
            // Listen for user input in the REPL
            maybe_line = tokio::task::spawn_blocking({
                let rl = Arc::clone(&rl);
                move || {
                    let mut rl = rl.lock().unwrap();
                    rl.readline("questdb> ")
                }
            }) => {
                match maybe_line {
                    Ok(line_result) => {
                        match line_result {
                            Ok(line) => {
                                let mut rl_guard = rl.lock().unwrap();
                                rl_guard.add_history_entry(line.as_str());

                                let trimmed = line.trim();
                                if trimmed == "\\q" {
                                    println!("Goodbye!");
                                    break;
                                } else if trimmed.starts_with('\\') {
                                    // Handle meta commands (e.g., \help, \format)
                                    let mut completer_lock = completer.lock().await;
                                    let mut format_lock = format.lock().await;
                                    handle_meta_command(&client, trimmed, &mut *completer_lock, &mut *format_lock).await;
                                } else if !trimmed.is_empty() {
                                    // Execute SQL query
                                    let query = trimmed.to_string();
                                    let client_clone = Arc::clone(&client);
                                    let format_clone = Arc::clone(&format);

                                    let query_task = tokio::spawn(async move {
                                        execute_query_command(&client_clone, &query, &format_clone.lock().await).await
                                    });

                                    tokio::select! {
                                        result = query_task => {
                                            if let Err(e) = result {
                                                eprintln!("Query execution error: {:?}", e);
                                            }
                                        },
                                        _ = signal::ctrl_c() => {
                                            eprintln!("\nQuery canceled.");
                                            let cancel_token = client.cancel_token();
                                            let _ = cancel_token.cancel_query(NoTls).await;
                                        }
                                    }

                                    // Ensure the prompt reappears
                                    print!("questdb> ");
                                    std::io::stdout().flush().unwrap();
                                }
                            },
                            Err(ReadlineError::Interrupted) => {
                                println!("Use \\q to quit.");
                            },
                            Err(ReadlineError::Eof) => {
                                println!("Exiting...");
                                break;
                            },
                            Err(err) => {
                                println!("Error: {:?}", err);
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        println!("Failed to read line: {:?}", e);
                        break;
                    }
                }
            }
        }
    }

    // Save command history to the specified history file
    {
        let mut rl_guard = rl.lock().unwrap();
        if let Err(e) = rl_guard.save_history(&history_path) {
            eprintln!("Failed to save history: {}", e);
        }
    }

    Ok(())
}
