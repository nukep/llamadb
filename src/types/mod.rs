use identifier::Identifier;

mod variant;
pub use self::variant::Variant;

use std::borrow::Cow;

#[derive(Debug, Copy)]
pub enum DbType {
    /// A type that only contains the NULL value.
    Null,
    /// byte[]: dynamic length byte array
    ByteDynamic,
    /// byte[N]: fixed length byte array
    ByteFixed(u64),

    /// integer with N/8 bytes
    Integer {
        signed: bool,
        bytes: u8
    },
    /// f64: floating point number, double precision
    F64,
    /// string: utf-8 string
    String,
}

impl DbType {
    pub fn from_identifier(ident: &Identifier, array_size: Option<Option<u64>>) -> Option<DbType> {
        match (ident.as_slice(), array_size) {
            ("byte", None) => Some(DbType::Integer { signed: false, bytes: 1 }),
            ("byte", Some(None)) => Some(DbType::ByteDynamic),
            ("byte", Some(Some(v))) => Some(DbType::ByteFixed(v)),
            ("f64", None) | ("double", None) => Some(DbType::F64),
            ("string", None) | ("varchar", None) => Some(DbType::String),
            ("int", None) | ("integer", None) => Some(DbType::Integer { signed: true, bytes: 4 }),
            (ident, None) => {
                if ident.len() >= 2 {
                    let bits: u8 = match ident[1..].parse() {
                        Ok(v) => v,
                        Err(_) => return None
                    };

                    if bits < 8 || bits > 64 {
                        return None;
                    }

                    let bytes = match bits % 8 {
                        0 => bits / 8,
                        _ => return None
                    };

                    match ident.char_at(0) {
                        'u' => Some(DbType::Integer { signed: false, bytes: bytes }),
                        'i' => Some(DbType::Integer { signed: true, bytes: bytes }),
                        _ => None
                    }
                } else {
                    None
                }
            },
            (_, Some(_)) => None
        }
    }

    pub fn get_default(&self) -> Cow<'static, [u8]> {
        use std::iter::repeat;
        use std::borrow::Cow::*;

        static EMPTY: &'static [u8; 0] = &[];
        static ZERO: &'static [u8; 1] = &[0];
        static F64_ZERO: &'static [u8; 8] = &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        match self {
            &DbType::Null => Borrowed(EMPTY),
            // Empty byte array
            &DbType::ByteDynamic => Borrowed(EMPTY),
            // Byte array with all values set to zero
            &DbType::ByteFixed(bytes) => Owned(repeat(0).take(bytes as usize).collect()),
            // Zero
            &DbType::Integer { bytes, .. } => Owned(repeat(0).take(bytes as usize).collect()),
            // Positive zero
            &DbType::F64 => Borrowed(F64_ZERO),
            // Empty string
            &DbType::String => Borrowed(ZERO)
        }
    }

    pub fn is_valid_length(&self, length: u64) -> bool {
        match self {
            &DbType::Null => length == 0,
            &DbType::ByteDynamic => true,
            &DbType::ByteFixed(bytes) => length == bytes,
            &DbType::Integer { bytes, .. } => length == bytes as u64,
            &DbType::F64 => length == 8,
            &DbType::String => true
        }
    }

    pub fn is_variable_length(&self) -> bool {
        match self {
            &DbType::Null => false,
            &DbType::ByteDynamic => true,
            &DbType::ByteFixed(_) => false,
            &DbType::Integer {..} => false,
            &DbType::F64 => false,
            &DbType::String => true
        }
    }
}
