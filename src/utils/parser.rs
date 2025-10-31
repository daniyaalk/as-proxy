use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io;
use std::io::{Cursor, Read};
use std::string::FromUtf8Error;
use tracing::warn;


// READ, GET_ALL
const INFO1_ALLOWED_MASK: u8 = (1 << 0) | (1 << 1);

// WRITE, DELETE, CREATE_ONLY
const INFO2_ALLOWED_MASK: u8 = (1 << 0) | (1 << 1) | (1 << 5);
const INFO3_ALLOWED_MASK: u8 = 0;
const INFO4_ALLOWED_MASK: u8 = 0;

#[derive(Debug)]
pub struct AerospikePacket {
    version: u8,
    message_type: u8,
    size: u64,
    pub(crate) body: AerospikePacketBody,
}

impl AerospikePacket {
    pub fn parse(data: &[u8]) -> Result<AerospikePacket, ParseError> {
        if data.len() < 8 {
            return Err(ParseError::PacketTooShort);
        }

        let version = data[0];
        let message_type = data[1];

        // Next 6 bytes are size in big-endian (most significant first)
        let mut size_bytes = [0u8; 8];
        size_bytes[2..].copy_from_slice(&data[2..8]); // pad the first two bytes with zeros
        let size = u64::from_be_bytes(size_bytes);

        Ok(AerospikePacket {
            version,
            message_type,
            size,
            body: AerospikePacketBody::parse(message_type, size as usize, &data[8..])?,
        })
    }
}

#[derive(Debug)]
pub struct AerospikeMessage {
    header_sz: u8,
    info1: u8,
    info2: u8,
    info3: u8,
    info4: u8,
    result_code: u8,
    generation: u32,
    record_ttl: u32,
    transaction_ttl: u32,
    n_fields: u16,
    n_ops: u16,
    pub fields: Vec<AerospikeField>,
    pub operations: Vec<AerospikeOperation>,
}

impl AerospikeMessage {
    pub fn parse(data: &[u8]) -> Result<AerospikeMessage, ParseError> {
        if data.len() == 0 {
            return Err(ParseError::HeaderSizeNotPresent);
        }

        if data.len() < data[0] as usize {
            return Err(ParseError::MessageSizeMismatch);
        }

        let mut cursor = Cursor::new(data);

        let header_sz=cursor.read_u8()?;
        let info1=cursor.read_u8()?;
        let info2=cursor.read_u8()?;
        let info3=cursor.read_u8()?;
        let info4=cursor.read_u8()?;
        let result_code=cursor.read_u8()?;
        let generation=cursor.read_u32::<BigEndian>()?;
        let record_ttl=cursor.read_u32::<BigEndian>()?;
        let transaction_ttl=cursor.read_u32::<BigEndian>()?;
        let n_fields = cursor.read_u16::<BigEndian>()?;
        let n_ops = cursor.read_u16::<BigEndian>()?;
        let fields=AerospikeField::parse(n_fields as usize, &mut cursor)?;
        let operations=AerospikeOperation::parse(n_ops as usize, &mut cursor)?;

        if cursor.position() as usize != data.len() {
            panic!("Cursor at {} while data length is {}", cursor.position(), data.len());
        }

        Ok(AerospikeMessage {
            header_sz,
            info1,
            info2,
            info3,
            info4,
            result_code,
            generation,
            record_ttl,
            transaction_ttl,
            n_fields,
            n_ops,
            fields,
            operations
        })
    }

    pub fn is_read(&self) -> bool {
        self.info1 & (1<<0) != 0
    }

    pub fn get_all_bins(&self) -> bool {
        self.info1 & (1<<1) != 0
    }

    pub fn is_batch_query(&self) -> bool {
        self.info1 & (1<<3) != 0

    }

    pub fn is_write(&self) -> bool {
        self.info2 & (1<<0) != 0
    }

    pub fn is_delete(&self) -> bool {
        self.info2 & (1<<0) != 0
    }

    pub fn is_create_ony(&self) -> bool {
        self.info2 & (1<<5) != 0
    }
}

#[derive(Debug)]
pub enum ParseError {
    PacketTooShort,
    HeaderSizeNotPresent,
    MessageSizeMismatch,
    ErrorWhileParsingField(io::Error),
    ErrorWhileParsingMessage(FromUtf8Error),
    UnsupportedMessageType,
}

impl From<io::Error> for ParseError {
    fn from(err: io::Error) -> Self {
        ParseError::ErrorWhileParsingField(err)
    }
}

#[derive(Debug)]
pub enum AerospikePacketBody {
    Info(AerospikeInfo),
    Message(AerospikeMessage),
}

impl AerospikePacketBody {
    pub fn parse(msg_type: u8, len: usize, data: &[u8]) -> Result<AerospikePacketBody, ParseError> {
        match msg_type {
            0x01 => Self::parse_info_packet(data),
            0x03 => Ok(AerospikePacketBody::Message(AerospikeMessage::parse(data)?)),
            _ => Err(ParseError::UnsupportedMessageType),
        }
    }

    fn parse_info_packet(data: &[u8]) -> Result<AerospikePacketBody, ParseError> {
        let string_repr = String::from_utf8(data.to_vec());

        if let Err(err) = string_repr {
            return Err(ParseError::ErrorWhileParsingMessage(err));
        }

        let string = string_repr.unwrap();
        let mut map = AerospikeInfo::new();

        for line in string.lines() {
            let (key, value_opt) = match line.split_once('\t') {
                Some((key, value)) => (
                    key.to_string(),
                    match value.is_empty() {
                        true => None,
                        false => Some(value.to_string()),
                    },
                ),
                None => continue,
            };
            map.insert(key, value_opt);
        }

        Ok(AerospikePacketBody::Info(map))
    }
}

pub type AerospikeInfo = HashMap<String, Option<String>>;

#[derive(Debug)]
pub enum AerospikeFieldType {
    Namespace=0,
    Set=1,
    Key=2,
    RecordVersion =3,
    DigestRipe =4,
    MrtId =5,
    MrtDeadline =6,
    TRID=7,
    SocketTimeout =9,
    RecsPerSec =10,
    PidArray =11,
    DigestArray =12,
    SampleMax =13,
    LUT=14,
    BvalArray =15,
    IndexName =21,
    IndexRange =22,
    IndexContext =23,
    IndexType =26,
    UdfFilename =30,
    UdfFunction =31,
    UdfArglist =32,
    UdfOp =33,
    QueryBinlist =40,
    Batch=41,
    BatchWithSet =42,
    PredExp=43,
}
impl TryFrom<u8> for AerospikeFieldType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use AerospikeFieldType::*;
        Ok(match value {
            0 => Namespace,
            1 => Set,
            2 => Key,
            3 => RecordVersion,
            4 => DigestRipe,
            5 => MrtId,
            6 => MrtDeadline,
            7 => TRID,
            9 => SocketTimeout,
            10 => RecsPerSec,
            11 => PidArray,
            12 => DigestArray,
            13 => SampleMax,
            14 => LUT,
            15 => BvalArray,
            21 => IndexName,
            22 => IndexRange,
            23 => IndexContext,
            26 => IndexType,
            30 => UdfFilename,
            31 => UdfFunction,
            32 => UdfArglist,
            33 => UdfOp,
            40 => QueryBinlist,
            41 => Batch,
            42 => BatchWithSet,
            43 => PredExp,
            _ => return Err(()),
        })
    }
}

#[derive(Debug)]
pub struct AerospikeField {
    size: u32,
    field_type: AerospikeFieldType,
    pub data: Vec<u8>,
}

impl AerospikeField {

    fn parse(count: usize, cursor: &mut Cursor<&[u8]>) -> Result<Vec<AerospikeField>, ParseError> {
        let mut fields: Vec<AerospikeField> = Vec::with_capacity(count);

        while fields.len() < count {
            fields.push(Self::parse_one(cursor)?);
        }

        Ok(fields)
    }

    fn parse_one(cursor: &mut Cursor<&[u8]>) -> Result<AerospikeField, ParseError> {
        let size= cursor.read_u32::<BigEndian>()?;
        let field_type = AerospikeFieldType::try_from(cursor.read_u8()?);

        if let Err(_) = field_type {
            return Err(ParseError::UnsupportedMessageType);
        }
        let field_type = field_type.unwrap();

        let data = {
            let mut buf = vec![0u8; size as usize - 1]; // 1 byte is used by the field_type
            cursor.read_exact(&mut buf)?;
            buf
        };
        Ok(AerospikeField{
            size,
            field_type,
            data,
        })
    }
}

#[derive(Debug)]
pub struct AerospikeOperation {
    pub op_sz: u32,
    pub op: u8,
    pub particle_type: u8,
    pub bin_version: u8,
    pub bin_name_length: u8,
    pub bin_name: String,
    pub data: Vec<u8>,
}

impl AerospikeOperation {
    fn parse(count: usize, cursor: &mut Cursor<&[u8]>) -> Result<Vec<AerospikeOperation>, ParseError> {
        let mut fields: Vec<AerospikeOperation> = Vec::with_capacity(count);

        while fields.len() < count {
            fields.push(Self::parse_one(cursor)?);
        }

        Ok(fields)
    }

    fn parse_one(cursor: &mut Cursor<&[u8]>) -> Result<AerospikeOperation, ParseError> {
        let op_sz= cursor.read_u32::<BigEndian>()?;
        let op = cursor.read_u8()?;
        let particle_type= cursor.read_u8()?;
        let bin_version = cursor.read_u8()?;
        let bin_name_length = cursor.read_u8()?;
        let bin_name = String::from_utf8({
            let mut buf = vec![0u8; bin_name_length as usize]; // 1 byte is used by the field_type
            cursor.read_exact(&mut buf)?;
            buf
        }).map_err(|e| ParseError::ErrorWhileParsingMessage(e))?;

        let data = {
            let mut buf = vec![0u8; (op_sz - 4 - (bin_name_length as u32)) as usize];
            (cursor).read_exact(&mut buf)?;
            buf
        };
        let data_as_str = String::from_utf8_lossy(&data);
        Ok(AerospikeOperation{
            op_sz,
            op,
            particle_type,
            bin_version,
            bin_name_length,
            bin_name,
            data,
        })
    }
}