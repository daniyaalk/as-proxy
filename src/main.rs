mod utils;

use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};

use crate::utils::parser::{AerospikePacket, AerospikePacketBody, ParseError};
use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    io,
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "as-proxy", version, about = "Simple multi-port TCP proxy")]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
}

#[derive(Debug)]
enum TransformDecision {
    /// Forward (optionally modified) bytes to the other party
    Forward(Vec<u8>),
    /// Do not send any data to the other party
    Drop,
    /// Do not send to the other party; send these bytes back to the origin instead
    Respond(Vec<u8>),
}

#[derive(Debug, Deserialize)]
struct Config {
    mappings: HashMap<String, String>,
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    let cfg: Config = toml::from_str(&contents).context("failed to parse TOML config")?;
    Ok(cfg)
}

fn transform_client_to_server(mut bytes: Vec<u8>) -> TransformDecision {
    // Modify or inspect bytes from client to server here
    debug!("Client to server:");
    utils::packet_printer::print_packet(&bytes);

    let packet = AerospikePacket::parse(bytes.as_slice());
    match packet {
        Ok(packet) => {
            if let AerospikePacketBody::Info(_) = &packet.body {
                // Do nothing
            } else {
                debug!("{:?}", packet);
                match packet.body {
                    AerospikePacketBody::Message(m) => {
                        info!("{:?}", m.is_read());

                        m.fields.iter().for_each(|f| {
                            info!("{}", String::from_utf8_lossy(&f.data))
                        });
                        m.operations.iter().for_each(|op| {
                            info!("{}", String::from_utf8_lossy(&op.data))
                        });
                    },
                    _ => {}
                }
            }
        },
        Err(e)=> {
            match e {
                ParseError::ErrorWhileParsingField(e) => {error!("{:?}", e)}
                ParseError::ErrorWhileParsingMessage(e) => {error!("{:?}", e)}
                _ => {error!("{:?}", e)}
            }

        }
    }
    // Example: by default, forward as-is
    TransformDecision::Forward(bytes)
}

fn transform_server_to_client(mut bytes: Vec<u8>) -> TransformDecision {
    // Modify or inspect bytes from server to client here
    debug!("Server to client:");
    utils::packet_printer::print_packet(&bytes);

    let packet = AerospikePacket::parse(bytes.as_slice());
    match packet {
        Ok(packet) => {
            if let AerospikePacketBody::Info(_) = &packet.body {
                // Do nothing
            } else {
                debug!("{:?}", packet);
                match packet.body {
                    AerospikePacketBody::Message(m) => {
                        info!("{:?}", m.is_read());

                        m.fields.iter().for_each(|f| {
                            info!("{}", String::from_utf8_lossy(&f.data))
                        });
                        m.operations.iter().for_each(|op| {
                            info!("{}", String::from_utf8_lossy(&op.data))
                        });
                    }
                    _ => {}
                }
            }
        },
        Err(e)=> {
            match e {
                ParseError::ErrorWhileParsingField(e) => {error!("{:?}", e)}
                ParseError::ErrorWhileParsingMessage(e) => {error!("{:?}", e)}
                _ => {error!("{:?}", e)}
            }

        }
    }
    // Example: by default, forward as-is
    TransformDecision::Forward(bytes)
}

async fn proxy_with_transform(mut inbound: TcpStream, mut outbound: TcpStream) -> io::Result<()> {
    let (mut ri, wi) = inbound.split();
    let (mut ro, wo) = outbound.split();

    let wi = Arc::new(tokio::sync::Mutex::new(wi));
    let wo = Arc::new(tokio::sync::Mutex::new(wo));

    let wi_for_c2s = wi.clone();
    let wo_for_c2s = wo.clone();
    let client_to_server = async {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = ri.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let chunk = buf[..n].to_vec();
            match transform_client_to_server(chunk) {
                TransformDecision::Forward(bytes) => {
                    let mut w = wo_for_c2s.lock().await;
                    w.write_all(&bytes).await?;
                }
                TransformDecision::Drop => {
                    // do nothing
                }
                TransformDecision::Respond(bytes) => {
                    // send response back to client (origin)
                    let mut w = wi_for_c2s.lock().await;
                    w.write_all(&bytes).await?;
                }
            }
        }
        let mut w = wo_for_c2s.lock().await;
        w.shutdown().await
    };

    let wi_for_s2c = wi.clone();
    let wo_for_s2c = wo.clone();
    let server_to_client = async {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = ro.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let chunk = buf[..n].to_vec();
            match transform_server_to_client(chunk) {
                TransformDecision::Forward(bytes) => {
                    let mut w = wi_for_s2c.lock().await;
                    w.write_all(&bytes).await?;
                }
                TransformDecision::Drop => {
                    // do nothing
                }
                TransformDecision::Respond(bytes) => {
                    // send response back to server (origin)
                    let mut w = wo_for_s2c.lock().await;
                    w.write_all(&bytes).await?;
                }
            }
        }
        let mut w = wi_for_s2c.lock().await;
        w.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;
    Ok(())
}

async fn run_listener(listen_port: u16, target: String) -> Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", listen_port))
        .await
        .with_context(|| format!("failed to bind on 0.0.0.0:{}", listen_port))?;
    info!(port = listen_port, target = %target, "listening and proxying");

    loop {
        let (inbound, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => {
                warn!(%err, "accept failed");
                continue;
            }
        };

        let target_clone = target.clone();
        tokio::spawn(async move {
            match TcpStream::connect(target_clone.as_str()).await {
                Ok(outbound) => match proxy_with_transform(inbound, outbound).await {
                    Ok(()) => {
                        // info!(client = %peer_addr, "connection closed");
                    }
                    Err(err) => {
                        warn!(client = %peer_addr, %err, "proxying error");
                    }
                },
                Err(err) => {
                    warn!(client = %peer_addr, target = %target_clone, %err, "connect failed");
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".into()),
        )
        .init();

    let args = Args::parse();
    let config = load_config(&args.config)?;

    if config.mappings.is_empty() {
        warn!("no mappings configured");
    }

    for (listen_port, target) in &config.mappings {
        let listen_port = listen_port.clone();
        let target = target.clone();
        tokio::spawn(async move {
            if let Err(err) = run_listener(listen_port.parse().unwrap(), target).await {
                error!(port = listen_port, %err, "listener terminated");
            }
        });
    }

    info!("press Ctrl+C to stop");
    tokio::signal::ctrl_c()
        .await
        .context("failed to install Ctrl+C handler")?;
    info!("shutting down");
    Ok(())
}
