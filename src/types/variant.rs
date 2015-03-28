use columnvalueops::ColumnValueOps;
use std::borrow::Cow;
use std::fmt;

#[derive(Clone)]
pub enum Variant {
    Null,
    StringLiteral(String),
    Number(u64)
}

impl fmt::Display for Variant {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &Variant::Null => write!(f, "NULL"),
            &Variant::StringLiteral(ref s) => write!(f, "\"{}\"", s),
            &Variant::Number(n) => write!(f, "{}", n)
        }
    }
}

fn from_bool(value: bool) -> Variant {
    Variant::Number(if value { 1 } else { 0 })
}

impl ColumnValueOps for Variant {
    fn from_string_literal(s: Cow<str>) -> Result<Variant, Cow<str>> {
        Ok(Variant::StringLiteral(s.into_owned()))
    }

    fn from_number_literal(s: Cow<str>) -> Result<Variant, Cow<str>> {
        match s.parse() {
            Ok(number) => Ok(Variant::Number(number)),
            Err(_) => Err(s)
        }
    }

    fn tests_true(&self) -> bool {
        match self {
            &Variant::Null => false,
            &Variant::StringLiteral(ref s) => !s.is_empty(),
            &Variant::Number(n) => n != 0
        }
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
            (&Variant::Number(l), &Variant::Number(r)) => {
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
            (&Variant::Number(l), &Variant::Number(r)) => {
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
