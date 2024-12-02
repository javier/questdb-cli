// src/db.rs
use anyhow::{anyhow, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, Config, NoTls};

pub async fn connect_to_db(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
    use_tls: bool,
    allow_invalid_cert: bool,
) -> Result<Client> {
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

