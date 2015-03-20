use identifier::Identifier;

use std::borrow::Cow;

#[derive(Debug, Copy)]
pub enum DbType {
    /// byte: 8-bit octet
    Byte,
    /// byte[]: dynamic length byte array
    ByteDynamic,
    /// byte[N]: fixed length byte array
    ByteFixed(u64),

    /// uN: unsigned integer with N/8 bytes
    Unsigned(u8),
    /// iN: signed integer with N/8 bytes
    Signed(u8),
    /// f32: floating point number, single precision
    F32,
    /// string: utf-8 string
    String,
}

impl DbType {
    pub fn from_identifier(ident: &Identifier, array_size: Option<Option<u64>>) -> Option<DbType> {
        match (ident.as_slice(), array_size) {
            ("byte", None) => Some(DbType::Byte),
            ("byte", Some(None)) => Some(DbType::ByteDynamic),
            ("byte", Some(Some(v))) => Some(DbType::ByteFixed(v)),
            ("f32", None) | ("float", None) => Some(DbType::F32),
            ("string", None) | ("varchar", None) => Some(DbType::String),
            ("int", None) | ("integer", None) => Some(DbType::Signed(32)),
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
                        'u' => Some(DbType::Unsigned(bytes)),
                        'i' => Some(DbType::Signed(bytes)),
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
        use std::iter::{IntoIterator, repeat};
        use std::borrow::Cow::*;

        static EMPTY: &'static [u8; 0] = &[];
        static ZERO: &'static [u8; 1] = &[0];
        static F32_ZERO: &'static [u8; 4] = &[0x80, 0x00, 0x00, 0x00];

        match self {
            // Zero
            &DbType::Byte => Borrowed(ZERO),
            // Empty byte array
            &DbType::ByteDynamic => Borrowed(EMPTY),
            // Byte array with all values set to zero
            &DbType::ByteFixed(bytes) => Owned(repeat(0).take(bytes as usize).collect()),
            // Zero
            &DbType::Unsigned(bytes) => Owned(repeat(0).take(bytes as usize).collect()),
            // Zero
            &DbType::Signed(bytes) => Owned(repeat(0).take(bytes as usize).collect()),
            // Positive zero
            &DbType::F32 => Borrowed(F32_ZERO),
            // Empty string
            &DbType::String => Borrowed(ZERO)
        }
    }

    pub fn is_valid_length(&self, length: u64) -> bool {
        match self {
            &DbType::Byte => length == 1,
            &DbType::ByteDynamic => true,
            &DbType::ByteFixed(bytes) => length == bytes,
            &DbType::Unsigned(bytes) => length == bytes as u64,
            &DbType::Signed(bytes) => length == bytes as u64,
            &DbType::F32 => length == 4,
            &DbType::String => true
        }
    }

    pub fn is_variable_length(&self) -> bool {
        match self {
            &DbType::Byte => false,
            &DbType::ByteDynamic => true,
            &DbType::ByteFixed(_) => false,
            &DbType::Unsigned(_) => false,
            &DbType::Signed(_) => false,
            &DbType::F32 => false,
            &DbType::String => true
        }
    }
}
