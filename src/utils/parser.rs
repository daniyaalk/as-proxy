use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io;
use std::io::{Cursor, Read};
use std::string::FromUtf8Error;
use toml::macros::push_toml;
use tracing::warn;


// READ, GET_ALL
pub const INFO1_ALLOWED_MASK: u8 = (1 << 0) | (1 << 1);

// WRITE, DELETE, CREATE_ONLY
pub const INFO2_ALLOWED_MASK: u8 = (1 << 0) | (1 << 1) | (1 << 5);
pub const INFO3_ALLOWED_MASK: u8 = 0;
pub const INFO4_ALLOWED_MASK: u8 = 0;

#[derive(Debug)]
pub struct AerospikePacket {
    version: u8,
    message_type: u8,
    size: u64,
    pub(crate) body: AerospikePacketBody,
}

impl AerospikePacket {
    pub fn from_bytes(data: &[u8]) -> Result<AerospikePacket, ParseError> {
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
            body: AerospikePacketBody::from_bytes(message_type, size as usize, &data[8..])?,
        })
    }

    pub fn new(body: AerospikePacketBody) -> AerospikePacket {

        let message_type;
        let size;
        match &body {
            AerospikePacketBody::Message(m) => {
                message_type = 0x03;
                size = m.get_raw_byte_size();
            },
            _ => unimplemented!()
        };
        AerospikePacket {
            version: 0x02,
            size: size as u64, // Add size of type
            message_type,
            body,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::with_capacity(self.size as usize + 10);

        ret.push(self.version);
        ret.push(self.message_type);
        ret.extend(&(self.size).to_be_bytes()[2..]);
            ret.extend(self.body.to_bytes());
        ret
    }
}

#[derive(Debug)]
pub struct AerospikeMessage {
    header_sz: u8,
    pub info1: u8,
    pub info2: u8,
    pub info3: u8,
    pub info4: u8,
    result_code: u8,
    generation: u32,
    pub record_ttl: u32,
    pub transaction_ttl: u32,
    n_fields: u16,
    n_ops: u16,
    pub fields: Vec<AerospikeField>,
    pub operations: Vec<AerospikeOperation>,
}

impl AerospikeMessage {
    pub fn from_bytes(data: &[u8]) -> Result<AerospikeMessage, ParseError> {
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

    pub fn get_raw_byte_size(&self) -> usize {
        let mut size = 22;

        for field in &self.fields {
            size += field.get_raw_byte_size();
        }

        for operation in &self.operations {
            size += operation.get_raw_byte_size();
        }

        size
    }
    pub fn to_bytes(self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::with_capacity(self.header_sz as usize);
        ret.push(self.header_sz);
        ret.push(self.info1);
        ret.push(self.info2);
        ret.push(self.info3);
        ret.push(self.info4);
        ret.push(self.result_code);
        ret.extend(self.generation.to_be_bytes());
        ret.extend(self.record_ttl.to_be_bytes());
        ret.extend(self.transaction_ttl.to_be_bytes());
        ret.extend(self.n_fields.to_be_bytes());
        ret.extend(self.n_ops.to_be_bytes());

        for field in self.fields {
            ret.extend(field.to_bytes());
        }

        for operation in self.operations {
            ret.extend(operation.to_bytes());
        }


        ret
    }

    pub fn new(info1: u8, info2: u8, info3: u8, info4: u8, result_code: u8, generation: u32, record_ttl: u32, transaction_ttl: u32, fields: Vec<AerospikeField>, operations: Vec<AerospikeOperation>) -> Self {
        AerospikeMessage {
            header_sz: 0x16,
            info1,
            info2,
            info3,
            info4,
            result_code,
            generation,
            record_ttl,
            transaction_ttl,
            n_fields: fields.len() as u16,
            n_ops: operations.len() as u16,
            fields,
            operations,
        }
    }
    pub fn get_successful_write_packet(record_ttl: u32, transaction_ttl: u32) -> Vec<u8> {

        let mut ret = Vec::with_capacity(22);
        ret.push(0x02); // version

        ret.push(0x03); // message_type

        // size
        ret.push(0x00);
        ret.push(0x00);
        ret.push(0x00);
        ret.push(0x00);
        ret.push(0x00);
        ret.push(0x16);

        ret.push(0x16); // header_sz
        ret.push(0); // info1
        ret.push(0); // info2
        ret.push(0); // info3
        ret.push(0); // info 4
        ret.push(0); // result_code

        for i in 0..4 {
            ret.push(0x00); // generation
        }

        record_ttl.to_be_bytes().iter().for_each(|&x| ret.push(x));
        transaction_ttl.to_be_bytes().iter().for_each(|&x| ret.push(x));


        // n_fields
        ret.push(0x00);
        ret.push(0x00);

        // n_ops
        ret.push(0x00);
        ret.push(0x00);

        ret
    }

    pub fn is_read_op(&self) -> bool {
        self.info1 & (1<<0) != 0
    }

    pub fn get_all_bins(&self) -> bool {
        self.info1 & (1<<1) != 0
    }

    pub fn is_batch_query(&self) -> bool {
        self.info1 & (1<<3) != 0

    }

    pub fn is_write_op(&self) -> bool {
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
    pub fn from_bytes(msg_type: u8, len: usize, data: &[u8]) -> Result<AerospikePacketBody, ParseError> {
        match msg_type {
            0x01 => Self::parse_info_packet(data),
            0x03 => Ok(AerospikePacketBody::Message(AerospikeMessage::from_bytes(data)?)),
            _ => Err(ParseError::UnsupportedMessageType),
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {

        match self {
            AerospikePacketBody::Message(m) => {
                m.to_bytes()
            },
            AerospikePacketBody::Info(_) => todo!(),
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
#[derive(PartialEq)]
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
    pub field_type: AerospikeFieldType,
    pub data: Vec<u8>,
}

impl AerospikeField {
    pub(crate) fn get_raw_byte_size(&self) -> usize {
        todo!()
    }
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

    fn to_bytes(&self) -> Vec<u8> {
        todo!()
    }
}

#[derive(Debug,Clone)]
pub struct AerospikeOperation {
    op_sz: u32,
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

    fn to_bytes(mut self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::with_capacity(4 + self.op_sz as usize);
        ret.extend(self.op_sz.to_be_bytes());
        ret.push(self.op);
        ret.push(self.particle_type);
        ret.push(self.bin_version);
        ret.push(self.bin_name_length);
        ret.append(&mut self.bin_name.as_bytes().to_vec());
        ret.append(&mut self.data);
        ret
    }

    pub fn new(op: u8, particle_type: u8, bin_version: u8, bin_name: &str, data: Vec<u8>) -> Self {
        let op_sz: u32 = (4 + bin_name.len() + data.len()) as u32;
        AerospikeOperation {
            op_sz,
            op,
            particle_type,
            bin_version,
            bin_name_length: bin_name.len() as u8,
            bin_name: bin_name.into(),
            data
        }
    }
    fn get_raw_byte_size(&self) -> usize {
        4 + self.op_sz as usize
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AerospikeKey {
    pub namespace: String,
    pub set: String,
    pub digest: Vec<u8>
}


impl AerospikeKey {
    pub fn parse(fields: Vec<AerospikeField>) -> Option<Self> {
        if fields.len() != 3 { return None;}

        let namespace = fields.iter().filter(|x| x.field_type == AerospikeFieldType::Namespace).last()?;
        let set = fields.iter().filter(|x| x.field_type == AerospikeFieldType::Set).last()?;
        let digest = fields.iter().filter(|x| x.field_type == AerospikeFieldType::Namespace).last()?;

        Some(AerospikeKey {
            namespace: String::from_utf8(namespace.data.clone()).ok()?,
            set: String::from_utf8(set.data.clone()).ok()?,
            digest: digest.data.to_vec()
        })
    }
}