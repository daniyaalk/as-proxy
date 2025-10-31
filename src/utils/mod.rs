use crate::utils::parser::{
    AerospikePacket, AerospikePacketBody, INFO1_ALLOWED_MASK, INFO2_ALLOWED_MASK,
    INFO3_ALLOWED_MASK, INFO4_ALLOWED_MASK, ParseError,
};
use crate::{AppState, Config, TransformDecision, utils};
use tracing::{debug, error, info};

mod cache;
pub mod packet_printer;
pub mod parser;

pub fn verify_supported_feature(packet: &AerospikePacket, state: &AppState) {
    // No need to check feature support if as-proxy is acting purely as a network proxy.
    if state.config.intercept_writes.is_none_or(|iw| iw == false) {
        return;
    }

    match &packet.body {
        AerospikePacketBody::Info(_) => {}
        AerospikePacketBody::Message(m) => {
            if (m.info1 & (!INFO1_ALLOWED_MASK) != 0)
                || (m.info2 & (!INFO2_ALLOWED_MASK) != 0)
                || (m.info3 & (!INFO3_ALLOWED_MASK) != 0)
                || (m.info4 & (!INFO4_ALLOWED_MASK) != 0)
            {
                panic!("Unsupported action, shutting down {:?}", packet);
            }
        }
    }
}

pub fn parse_to_as_packet(bytes: &[u8], state: &AppState) -> Result<AerospikePacket, ParseError> {
    let packet = match AerospikePacket::from_bytes(bytes) {
        Ok(packet) => {
            if let AerospikePacketBody::Info(_) = &packet.body {
                // Do nothing
            } else {
                debug!("{:?}", packet);
                verify_supported_feature(&packet, state);
                match &packet.body {
                    AerospikePacketBody::Message(m) => {
                        m.fields.iter().for_each(|f| {
                            info!(
                                "field: {:?} {}",
                                f.field_type,
                                String::from_utf8_lossy(&f.data)
                            )
                        });
                        m.operations
                            .iter()
                            .for_each(|op| info!("op: {}", String::from_utf8_lossy(&op.data)));
                    }
                    _ => {}
                }
            }
            packet
        }
        Err(e) => {
            error!("{:?}", e);
            return Err(e);
        }
    };
    Ok(packet)
}
