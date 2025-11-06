use crate::utils::parse_to_as_packet;
use crate::utils::parser::{
    AerospikeField, AerospikeKey, AerospikeMessage, AerospikeOperation, AerospikePacket,
    AerospikePacketBody,
};
use crate::{AppState, Config, KafkaMode, utils};
#[cfg(feature = "replay")]
use kafka::producer::Record;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tracing::field::debug;
use tracing::{debug, error};

#[derive(Debug)]
pub enum TransformDecision {
    /// Forward (optionally modified) bytes to the other party
    Forward(Vec<u8>),
    /// Do not send any data to the other party
    Drop,
    /// Do not send to the other party; send these bytes back to the origin instead
    Respond(Vec<u8>),
}

pub async fn transform_client_to_server(
    mut bytes: Vec<u8>,
    state: &AppState,
    #[cfg(feature = "replay")] tx: &UnboundedSender<AerospikeKey>,
) -> TransformDecision {
    // Modify or inspect bytes from client to server here
    debug!("Client to server:");
    utils::packet_printer::print_packet(&bytes);

    let packet = match parse_to_as_packet(bytes.as_slice(), state) {
        Ok(packet) => packet,
        Err(_) => return TransformDecision::Drop,
    };

    if let AerospikePacketBody::Message(message) = packet.body {
        if state.intercept_messages()
            || (cfg!(feature = "replay") && state.is_kafka_consumer_enabled())
        {
            if message.is_write_op() {
                let key = AerospikeKey::parse(&message.fields);

                if key.is_some() {
                    let mut diff_map = state.diff_map.write().unwrap();
                    diff_map.insert(
                        key.unwrap(),
                        message.operations,
                        Duration::from_secs(state.config.diff_ttl),
                    );
                }
                return TransformDecision::Respond(AerospikeMessage::get_successful_write_packet(
                    message.record_ttl,
                    message.transaction_ttl,
                ));
            } else if message.is_read_op() {
                let key = AerospikeKey::parse(&message.fields);

                if key.is_some() {
                    let mut diff_map = state.diff_map.write().unwrap();
                    let key = key.unwrap();
                    if let Some(operations) = diff_map.get(&key) {
                        let response_packet = AerospikePacket::new(AerospikePacketBody::Message(
                            AerospikeMessage::new(
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                vec![],
                                operations.clone(),
                            ),
                        ));

                        return TransformDecision::Respond(response_packet.to_bytes());
                    }
                }
            }
        }

        #[cfg(feature = "replay")]
        if state
            .config
            .kafka_config
            .as_ref()
            .is_some_and(|x| x.mode == KafkaMode::Produce)
        {
            if let Some(key) = AerospikeKey::parse(&message.fields) {
                let _ = tx.send(key);
            }
        }
    }

    // Example: by default, forward as-is
    TransformDecision::Forward(bytes)
}

pub async fn transform_server_to_client(
    mut bytes: Vec<u8>,
    state: &AppState,
    rx: &mut UnboundedReceiver<AerospikeKey>,
) -> TransformDecision {
    // Modify or inspect bytes from server to client here
    debug!("Server to client:");
    utils::packet_printer::print_packet(&bytes);

    let packet = match parse_to_as_packet(bytes.as_slice(), state) {
        Ok(packet) => packet,
        Err(_) => return TransformDecision::Drop,
    };

    debug!("packet type: {:?}", packet.body);

    #[cfg(feature = "replay")]
    if let AerospikePacketBody::Message(message) = packet.body {
        if let Some(kafka_producer) = &state.kafka_producer {
            // get the key first
            if let Some(key) = rx.recv().await {
                let mut handle = kafka_producer.lock().unwrap();

                let r = ReplayRecord {
                    key,
                    operations: message.operations,
                };

                match handle.send(&Record::from_value(
                    &state.config.kafka_config.as_ref().unwrap().topic,
                    serde_json::to_string(&r).unwrap(),
                )) {
                    Ok(_) => (),
                    Err(err) => {
                        error!("Unable to produce message to kafka, {}", err)
                    }
                }
            }
        }
    }

    // Example: by default, forward as-is
    TransformDecision::Forward(bytes)
}
#[cfg(feature = "replay")]
#[derive(Serialize, Deserialize, Debug)]
pub struct ReplayRecord {
    pub key: AerospikeKey,
    pub operations: Vec<AerospikeOperation>,
}
