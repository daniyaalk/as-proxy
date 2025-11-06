mod handler;
mod utils;

use crate::handler::{
    ReplayRecord, TransformDecision, transform_client_to_server, transform_server_to_client,
};
use crate::utils::parser::{
    AerospikeKey, AerospikeOperation, AerospikePacket, AerospikePacketBody, INFO1_ALLOWED_MASK,
    INFO2_ALLOWED_MASK, INFO3_ALLOWED_MASK, INFO4_ALLOWED_MASK, ParseError,
};
use anyhow::{Context, Result, anyhow};
use clap::Parser;
use kafka::producer::{Producer, RequiredAcks};
use serde::Deserialize;
use std::sync::{Mutex, RwLock};
use std::time::Duration;
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    io,
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, warn};
use ttl_cache::TtlCache;

#[derive(Parser, Debug)]
#[command(name = "as-proxy", version, about = "Simple multi-port TCP proxy")]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
struct Config {
    mappings: HashMap<String, String>,
    intercept_writes: Option<bool>,
    #[serde(default)]
    diff_ttl: u64,

    #[cfg(feature = "replay")]
    kafka_config: Option<KafkaConfig>,
}

#[cfg(feature = "replay")]
#[derive(Debug, Deserialize, Clone, PartialEq)]
enum KafkaMode {
    Produce,
    Consume,
}

#[cfg(feature = "replay")]
#[derive(Debug, Deserialize, Clone)]
struct KafkaConfig {
    hosts: String,
    topic: String,
    mode: KafkaMode,
    prioritize_local_cache: Option<bool>,
}

struct AppState {
    config: Config,
    diff_map: Arc<RwLock<TtlCache<AerospikeKey, Vec<AerospikeOperation>>>>,
    #[cfg(feature = "replay")]
    kafka_producer: Option<Arc<Mutex<Producer>>>,
}

impl AppState {
    pub fn is_write_intercept_enabled(&self) -> bool {
        self.config.intercept_writes.unwrap_or(false)
    }

    #[cfg(feature = "replay")]
    pub fn is_kafka_consumer_enabled(&self) -> bool {
        self.config
            .kafka_config
            .as_ref()
            .is_some_and(|kc| kc.mode == KafkaMode::Consume)
    }

    pub fn intercept_messages(&self) -> bool {
        #[cfg(feature = "replay")]
        {
            self.is_write_intercept_enabled() || self.is_kafka_consumer_enabled()
        }

        #[cfg(not(feature = "replay"))]
        {
            self.is_write_intercept_enabled()
        }
    }
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    let cfg: Config = toml::from_str(&contents).context("failed to parse TOML config")?;
    Ok(cfg)
}

async fn proxy_with_transform(
    mut inbound: TcpStream,
    mut outbound: TcpStream,
    state: Arc<AppState>,
) -> io::Result<()> {
    let (mut ri, wi) = inbound.split();
    let (mut ro, wo) = outbound.split();

    let wi = Arc::new(tokio::sync::Mutex::new(wi));
    let wo = Arc::new(tokio::sync::Mutex::new(wo));

    let wi_for_c2s = wi.clone();
    let wo_for_c2s = wo.clone();

    let state_clone = state.clone();

    #[cfg(feature = "replay")]
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AerospikeKey>();

    let client_to_server = async {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = ri.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let chunk = buf[..n].to_vec();
            match transform_client_to_server(
                chunk,
                &*state_clone,
                #[cfg(feature = "replay")]
                &tx,
            )
            .await
            {
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
            match transform_server_to_client(chunk, &*state, &mut rx).await {
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

async fn run_listener(listen_port: u16, target: String, state: Arc<AppState>) -> Result<()> {
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
        let state_clone = state.clone();
        tokio::spawn(async move {
            match TcpStream::connect(target_clone.as_str()).await {
                Ok(outbound) => match proxy_with_transform(inbound, outbound, state_clone).await {
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

    let diff_map: Arc<RwLock<TtlCache<AerospikeKey, Vec<AerospikeOperation>>>> =
        Arc::new(RwLock::new(TtlCache::new(20)));

    #[cfg(feature = "replay")]
    if config
        .kafka_config
        .as_ref()
        .is_some_and(|kc| kc.mode == KafkaMode::Consume)
    {
        match spawn_kafka_receiver(diff_map.clone(), &config.kafka_config, config.diff_ttl) {
            Ok(_) => {}
            Err(err) => {
                panic!("Unable to start kafka! err: {}", err)
            }
        }
    }

    let state = Arc::new(AppState {
        config: config.clone(),
        diff_map,
        #[cfg(feature = "replay")]
        kafka_producer: get_kafka_producer(&config),
    });

    for (listen_port, target) in &config.mappings {
        let listen_port = listen_port.clone();
        let target = target.clone();
        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_listener(listen_port.parse().unwrap(), target, state_clone).await
            {
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

fn spawn_kafka_receiver(
    cache: Arc<RwLock<TtlCache<AerospikeKey, Vec<AerospikeOperation>>>>,
    kafka_config: &Option<KafkaConfig>,
    diff_ttl: u64,
) -> Result<()> {
    let kafka_config = match kafka_config {
        Some(kafka_config) => kafka_config,
        None => return Err(anyhow!("kafka config not found")),
    };

    let mut consumer = kafka::consumer::Consumer::from_hosts(
        kafka_config.hosts.split(",").map(String::from).collect(),
    )
    .with_topic(kafka_config.topic.clone())
    .with_fetch_max_bytes_per_partition(16 * 1024 * 1024)
    .create()?;

    let prioritize_local_cache = kafka_config.prioritize_local_cache.clone();

    let _ = tokio::spawn(async move {
        loop {
            match consumer.poll() {
                Ok(poll) => {
                    for ms in poll.iter() {
                        for message in ms.messages() {
                            let message_string = String::from_utf8(message.value.to_vec()).unwrap();

                            match serde_json::from_str::<ReplayRecord>(&message_string) {
                                Ok(record) => {
                                    let mut cache = cache.write().unwrap();

                                    if prioritize_local_cache.is_some_and(|x| x) {
                                        // If prioritize_local_cache is enabled and the key for current record already exists, don't override with replay response.
                                        if cache.contains_key(&record.key) {
                                            continue;
                                        }
                                    }

                                    cache.insert(
                                        record.key,
                                        record.operations,
                                        Duration::from_secs(diff_ttl),
                                    );
                                }
                                Err(_) => {}
                            }
                        }
                    }
                }
                Err(_) => {}
            };
        }
    });
    Ok(())
}

#[cfg(feature = "replay")]
fn get_kafka_producer(config: &Config) -> Option<Arc<Mutex<Producer>>> {
    if let Some(kc) = &config.kafka_config {
        Some(Arc::new(Mutex::new(
            Producer::from_hosts(kc.hosts.split(',').map(|s| s.to_string()).collect())
                .with_ack_timeout(Duration::from_secs(1))
                .with_required_acks(RequiredAcks::None)
                .create()
                .unwrap(),
        )))
    } else {
        None
    }
}
