//! APRS-IS TCP client connection.
//!
//! Connects to an APRS-IS server (e.g., aprs.glidernet.org:14580),
//! authenticates, applies a traffic filter, and streams packets.

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config;

/// Messages sent from the APRS connection to the main processing loop
#[derive(Debug)]
pub enum AprsEvent {
    /// A raw APRS packet line
    Packet(String),
    /// Server comment/keepalive line
    ServerMessage(String),
    /// Connection lost or errored
    Disconnected(String),
}

pub struct AprsConnection {
    /// Held to keep the shutdown channel alive; dropping `AprsConnection`
    /// closes the sender, which signals the connection task to stop.
    #[allow(dead_code)]
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl AprsConnection {
    /// Create a new APRS connection that streams events to the provided channel.
    ///
    /// Spawns a tokio task that handles the TCP connection, reconnection,
    /// and keepalive. Returns a handle that can be used to shut down.
    pub fn start(
        event_tx: mpsc::Sender<AprsEvent>,
        git_version: String,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        tokio::spawn(connection_loop(event_tx, shutdown_rx, git_version));
        AprsConnection {
            shutdown_tx: Some(shutdown_tx),
        }
    }

}

async fn connection_loop(
    event_tx: mpsc::Sender<AprsEvent>,
    mut shutdown_rx: mpsc::Receiver<()>,
    git_version: String,
) {
    let callsign = "OGNRANGE";
    let siteurl = &*config::NEXT_PUBLIC_SITEURL;
    let filter = &*config::APRS_TRAFFIC_FILTER;
    let keepalive_ms = *config::APRS_KEEPALIVE_PERIOD_MS;

    loop {
        let server_addr = &*config::APRS_SERVER;
        info!("Connecting to APRS server: {}", server_addr);

        match connect_and_stream(
            server_addr,
            callsign,
            siteurl,
            filter,
            &git_version,
            keepalive_ms,
            &event_tx,
            &mut shutdown_rx,
        )
        .await
        {
            Ok(()) => {
                // Clean shutdown requested
                info!("APRS connection shut down cleanly");
                return;
            }
            Err(e) => {
                warn!("APRS connection error: {}, reconnecting in 5s...", e);
                let _ = event_tx.send(AprsEvent::Disconnected(e.to_string())).await;

                // Wait before reconnecting, but honour shutdown
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown during reconnect wait");
                        return;
                    }
                }
            }
        }
    }
}

async fn connect_and_stream(
    server_addr: &str,
    callsign: &str,
    siteurl: &str,
    filter: &str,
    git_version: &str,
    keepalive_ms: u64,
    event_tx: &mpsc::Sender<AprsEvent>,
    shutdown_rx: &mut mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let stream = TcpStream::connect(server_addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Send login
    let login = format!(
        "user {} pass -1 vers ognrange {} filter {}\r\n",
        callsign, git_version, filter
    );
    writer.write_all(login.as_bytes()).await?;

    // Send identification comment
    let ident = format!("# ognrange {} {}\r\n", siteurl, git_version);
    writer.write_all(ident.as_bytes()).await?;

    info!("APRS login sent as {}", callsign);

    let mut keepalive_interval = tokio::time::interval(Duration::from_millis(keepalive_ms));
    keepalive_interval.tick().await; // consume immediate first tick
    let mut had_traffic = false;
    let mut raw_buf = Vec::new();

    loop {
        tokio::select! {
            // Incoming APRS data — read raw bytes to handle non-UTF-8
            read_result = reader.read_until(b'\n', &mut raw_buf) => {
                match read_result {
                    Ok(0) => {
                        // Connection closed
                        return Err("APRS server closed connection".into());
                    }
                    Ok(_) => {
                        had_traffic = true;
                        let line = match std::str::from_utf8(&raw_buf) {
                            Ok(s) => s.trim().to_string(),
                            Err(_) => {
                                // Log with non-UTF-8 bytes shown as \xHH
                                let escaped: String = raw_buf.iter().map(|&b| {
                                    if b.is_ascii_graphic() || b == b' ' {
                                        (b as char).to_string()
                                    } else {
                                        format!("\\x{:02x}", b)
                                    }
                                }).collect();
                                warn!("APRS non-UTF-8 line: {}", escaped);
                                String::from_utf8_lossy(&raw_buf).trim().to_string()
                            }
                        };
                        raw_buf.clear();
                        if line.starts_with('#') || line.starts_with("user") {
                            let _ = event_tx.send(AprsEvent::ServerMessage(line)).await;
                        } else if !line.is_empty() {
                            let _ = event_tx.send(AprsEvent::Packet(line)).await;
                        }
                    }
                    Err(e) => {
                        return Err(format!("APRS read error: {}", e).into());
                    }
                }
            }

            // Keepalive timer
            _ = keepalive_interval.tick() => {
                if !had_traffic {
                    return Err("No traffic received since last keepalive".into());
                }
                had_traffic = false;

                let keepalive = format!("# {} {}\r\n", siteurl, git_version);
                if let Err(e) = writer.write_all(keepalive.as_bytes()).await {
                    return Err(format!("Keepalive write error: {}", e).into());
                }
            }

            // Shutdown signal
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received, closing APRS connection");
                return Ok(());
            }
        }
    }
}
