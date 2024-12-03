// src/db.rs

use tokio_postgres::{Client, NoTls, Error};
use tokio_postgres::config::SslMode;

/// Connects to the QuestDB database with the given parameters.
pub async fn connect_to_db(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
    use_tls: bool,
    _allow_invalid_cert: bool, // Prefixed with underscore to suppress unused variable warning
) -> Result<Client, Error> {
    let mut config = tokio_postgres::Config::new();
    config.host(host)
          .port(port)
          .user(user)
          .password(password)
          .dbname(dbname);

    if use_tls {
        config.ssl_mode(SslMode::Require);
        // Additional TLS configuration can be added here if needed
    } else {
        config.ssl_mode(SslMode::Disable);
    }

    let (client, connection) = config.connect(NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}
