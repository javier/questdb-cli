use clap::{Parser, Subcommand};
use tokio_postgres::{Client, Config, NoTls};
use postgres_native_tls::MakeTlsConnector;
use native_tls::TlsConnector;
use rustyline::Editor;
use anyhow::{anyhow, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let host = cli.host.unwrap_or_else(|| "localhost".to_string());
    let port = cli.port.unwrap_or(8812);
    let use_tls = cli.use_tls;

    match connect_to_db(
        &host,
        port,
        cli.user.as_deref(),
        cli.password.as_deref(),
        cli.dbname.as_deref(),
        use_tls,
        cli.allow_invalid_cert,
    )
    .await
    {
        Ok(client) => {
            println!("Connected to QuestDB at {}:{}.", host, port);
            if let Some(Commands::Query { sql }) = cli.command {
                execute_query(&client, &sql).await;
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

#[derive(Parser)]
#[command(name = "questdb-cli", version, about = "A CLI for QuestDB")]
struct Cli {
    /// Hostname or IP address of the QuestDB server (default: localhost)
    #[arg(short = 'H', long)]
    host: Option<String>,

    /// Port number of the QuestDB server (default: 8812)
    #[arg(short = 'p', long)]
    port: Option<u16>,

    /// Use TLS for the connection (default: false)
    #[arg(short = 's', long)]
    use_tls: bool,

    /// Accept invalid TLS certificates (self-signed)
    #[arg(short = 'k', long)]
    allow_invalid_cert: bool,

    /// Database user (default: admin)
    #[arg(short = 'u', long)]
    user: Option<String>,

    /// Database password (default: quest)
    #[arg(short = 'w', long)]
    password: Option<String>,

    /// Database name (default: qdb)
    #[arg(short = 'd', long)]
    dbname: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a SQL query directly
    Query {
        /// The SQL query to execute
        sql: String,
    },
}

async fn connect_to_db(
    host: &str,
    port: u16,
    user: Option<&str>,
    password: Option<&str>,
    dbname: Option<&str>,
    use_tls: bool,
    allow_invalid_cert: bool,
) -> Result<Client> {
    // Set default user, password, and database if not provided
    let user = user.unwrap_or("admin");
    let password = password.unwrap_or("quest");
    let dbname = dbname.unwrap_or("qdb");

    // Print connection details for debugging
    println!(
        "Connecting to PostgreSQL at {}:{} with user '{}', TLS: {}",
        host, port, user, use_tls
    );

    // Construct the PostgreSQL configuration
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.user(user);
    config.password(password);
    config.dbname(dbname);

    if use_tls {
        // TLS connection
        let mut builder = TlsConnector::builder();
        if allow_invalid_cert {
            builder.danger_accept_invalid_certs(true);
        }
        let connector = builder
            .build()
            .map_err(|e| anyhow!("Failed to build TLS connector: {}", e))?;
        let tls_connector = MakeTlsConnector::new(connector);

        let (client, connection) = config.connect(tls_connector).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        Ok(client)
    } else {
        // Non-TLS connection
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        Ok(client)
    }
}

async fn start_repl(client: Client) {
    let mut rl = Editor::<()>::new();
    println!("Type '\\q' to quit.");

    loop {
        match rl.readline("questdb> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed == "\\q" {
                    println!("Goodbye!");
                    break;
                } else if trimmed.starts_with('\\') {
                    handle_meta_command(&client, trimmed).await;
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

async fn execute_query(client: &Client, query: &str) {
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

async fn handle_meta_command(client: &Client, command: &str) {
    match command {
        "\\help" => {
            println!("Meta commands:");
            println!("  \\q                 Quit");
            println!("  \\help              Show this help message");
            println!("  \\dt                List all tables");
            println!("  \\dwal              List all WAL tables");
            println!("  \\dstorage <table>  Show storage details for a table");
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
        _ => println!("Unknown meta command: {}", command),
    }
}

