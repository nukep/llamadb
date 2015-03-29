use byteutils;
use columnvalueops::ColumnValueOps;
use types::DbType;
use std::borrow::Cow;
use std::fmt;

#[derive(Clone)]
pub enum Variant {
    Null,
    Bytes(Vec<u8>),
    StringLiteral(String),
    SignedInteger(i64),
    UnsignedInteger(u64),
    Float(f64)
}

impl fmt::Display for Variant {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &Variant::Null => write!(f, "NULL"),
            &Variant::Bytes(ref v) => write!(f, "\"{:?}\"", v),
            &Variant::StringLiteral(ref s) => write!(f, "\"{}\"", s),
            &Variant::SignedInteger(n) => write!(f, "{}", n),
            &Variant::UnsignedInteger(n) => write!(f, "{}", n),
            &Variant::Float(n) => write!(f, "{}", n),
        }
    }
}

fn from_bool(value: bool) -> Variant {
    Variant::UnsignedInteger(if value { 1 } else { 0 })
}

impl ColumnValueOps for Variant {
    fn from_string_literal(s: Cow<str>) -> Result<Variant, Cow<str>> {
        Ok(Variant::StringLiteral(s.into_owned()))
    }

    fn from_number_literal(s: Cow<str>) -> Result<Variant, Cow<str>> {
        if let Ok(number) = s.parse() {
            Ok(Variant::UnsignedInteger(number))
        } else if let Ok(number) = s.parse() {
            Ok(Variant::SignedInteger(number))
        } else if let Ok(number) = s.parse() {
            Ok(Variant::Float(number))
        } else {
            Err(s)
        }
    }

    fn from_bytes(dbtype: DbType, bytes: Cow<[u8]>) -> Result<Variant, ()> {
        match dbtype {
            DbType::Null => Ok(Variant::Null),
            DbType::ByteDynamic => Ok(Variant::Bytes(bytes.into_owned())),
            DbType::ByteFixed(n) => {
                if bytes.as_slice().len() as u64 != n {
                    Err(())
                } else {
                    Ok(Variant::Bytes(bytes.into_owned()))
                }
            },
            DbType::Integer { signed, bytes: n } => {
                if bytes.as_slice().len() != n as usize {
                    Err(())
                } else {
                    if signed {
                        Ok(Variant::SignedInteger(byteutils::read_sdbinteger(&bytes)))
                    } else {
                        Ok(Variant::UnsignedInteger(byteutils::read_udbinteger(&bytes)))
                    }
                }
            },
            DbType::F64 => {
                Ok(Variant::Float(byteutils::read_dbfloat(&bytes)))
            },
            DbType::String => {
                let len = bytes.len();
                if len > 0 && bytes[len - 1] == 0 {
                    let s = String::from_utf8_lossy(&bytes[0..len - 1]);
                    Ok(Variant::StringLiteral(s.into_owned()))
                } else {
                    Err(())
                }
            }
        }
    }

    fn to_bytes(self, dbtype: DbType) -> Result<Box<[u8]>, ()> {
        match (self.cast(dbtype), dbtype) {
            (Variant::Null, DbType::Null) => {
                // NULL has no data.
                Err(())
            },
            (Variant::Bytes(v), DbType::ByteDynamic) => {
                Ok(v.into_boxed_slice())
            },
            (Variant::StringLiteral(s), DbType::String) => {
                Ok((s + "\0").into_bytes().into_boxed_slice())
            },
            (Variant::SignedInteger(v), DbType::Integer { signed: true, bytes }) => {
                let mut buf = vec![0; bytes as usize];
                byteutils::write_sdbinteger(v, &mut buf);
                Ok(buf.into_boxed_slice())
            },
            (Variant::UnsignedInteger(v), DbType::Integer { signed: false, bytes }) => {
                let mut buf = vec![0; bytes as usize];
                byteutils::write_udbinteger(v, &mut buf);
                Ok(buf.into_boxed_slice())
            },
            (Variant::Float(v), DbType::F64) => {
                let mut buf = [0; 8];
                byteutils::write_dbfloat(v, &mut buf);
                Ok(Box::new(buf))
            },
            _ => {
                Err(())
            }
        }
    }

    fn get_dbtype(&self) -> DbType {
        match self {
            &Variant::Null => DbType::Null,
            &Variant::Bytes(ref bytes) => DbType::ByteFixed(bytes.len() as u64),
            &Variant::StringLiteral(..) => DbType::String,
            &Variant::SignedInteger(..) => DbType::Integer { signed: true, bytes: 8 },
            &Variant::UnsignedInteger(..) => DbType::Integer { signed: false, bytes: 8 },
            &Variant::Float(..) => DbType::F64
        }
    }

    fn tests_true(&self) -> bool {
        match self {
            &Variant::Null => false,
            &Variant::Bytes(ref bytes) => !bytes.is_empty(),
            &Variant::StringLiteral(ref s) => !s.is_empty(),
            &Variant::SignedInteger(n) => n != 0,
            &Variant::UnsignedInteger(n) => n != 0,
            &Variant::Float(n) => n != 0.0
        }
    }

    fn cast(self, dbtype: DbType) -> Self {
        // TODO
        self
    }

    fn equals(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (&Variant::Null, _) | (_, &Variant::Null) => {
                // NULL does not compare.
                Variant::Null
            },
            (&Variant::StringLiteral(ref l), &Variant::StringLiteral(ref r)) => {
                from_bool(l == r)
            },
            (&Variant::UnsignedInteger(l), &Variant::UnsignedInteger(r)) => {
                from_bool(l == r)
            },
            _ => from_bool(false)
        }
    }

    fn not_equals(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (&Variant::Null, _) | (_, &Variant::Null) => {
                // NULL does not compare.
                Variant::Null
            },
            (&Variant::StringLiteral(ref l), &Variant::StringLiteral(ref r)) => {
                from_bool(l != r)
            },
            (&Variant::UnsignedInteger(l), &Variant::UnsignedInteger(r)) => {
                from_bool(l != r)
            },
            _ => from_bool(true)
        }
    }

    fn and(&self, rhs: &Self) -> Self {
        from_bool(self.tests_true() && rhs.tests_true())
    }

    fn or(&self, rhs: &Self) -> Self {
        from_bool(self.tests_true() || rhs.tests_true())
    }

    fn concat(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (&Variant::StringLiteral(ref l), &Variant::StringLiteral(ref r)) => {
                Variant::StringLiteral(format!("{}{}", l, r))
            },
            (e, _) => e.clone()
        }
    }
}
