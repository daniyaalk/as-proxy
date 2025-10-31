use std::sync::Arc;
use std::time::Duration;
use tracing::debug;
use crate::{utils, AppState, Config};
use crate::utils::parse_to_as_packet;
use crate::utils::parser::{AerospikeKey, AerospikeMessage, AerospikePacket, AerospikePacketBody};

#[derive(Debug)]
pub enum TransformDecision {
    /// Forward (optionally modified) bytes to the other party
    Forward(Vec<u8>),
    /// Do not send any data to the other party
    Drop,
    /// Do not send to the other party; send these bytes back to the origin instead
    Respond(Vec<u8>),
}

pub fn transform_client_to_server(mut bytes: Vec<u8>, state: &AppState) -> TransformDecision {
    // Modify or inspect bytes from client to server here
    debug!("Client to server:");
    utils::packet_printer::print_packet(&bytes);

    let packet = match parse_to_as_packet(bytes.as_slice(), state){
        Ok(packet) => packet,
        Err(_) => return TransformDecision::Drop,
    };

    if let AerospikePacketBody::Message(message) = packet.body {

        if state.config.intercept_writes.is_some_and(|x| x==true) {
            if message.is_write_op() {
                let key = AerospikeKey::parse(message.fields);

                if key.is_some() {
                    let mut diff_map = state.diff_map.write().unwrap();
                    diff_map.insert(key.unwrap(), message.operations, Duration::from_secs(state.config.diff_ttl));
                }
                return TransformDecision::Respond(AerospikeMessage::get_successful_write_packet(message.record_ttl, message.transaction_ttl));
            } else if message.is_read_op() {
                let key = AerospikeKey::parse(message.fields);

                if key.is_some() {
                    let mut diff_map = state.diff_map.write().unwrap();
                    let key = key.unwrap();
                    if let Some(operations) = diff_map.get(&key) {
                        let response_packet = AerospikePacket::new(
                            AerospikePacketBody::Message(AerospikeMessage::new(
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                vec![],
                                operations.clone())
                            ));

                        return TransformDecision::Respond(response_packet.to_bytes())
                    }


                }
            }
        }



    }

    // Example: by default, forward as-is
    TransformDecision::Forward(bytes)
}

pub fn transform_server_to_client(mut bytes: Vec<u8>, state: &AppState) -> TransformDecision {
    // Modify or inspect bytes from server to client here
    debug!("Server to client:");
    utils::packet_printer::print_packet(&bytes);


    let packet = match parse_to_as_packet(bytes.as_slice(), state){
        Ok(packet) => packet,
        Err(_) => return TransformDecision::Drop,
    };

    // Example: by default, forward as-is
    TransformDecision::Forward(bytes)
}