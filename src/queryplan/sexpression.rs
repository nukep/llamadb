use databaseinfo::{DatabaseInfo, TableInfo};
use identifier::Identifier;

use std::fmt;

pub enum SExpression<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    Scan {
        table: &'a <DB as DatabaseInfo>::Table,
        source_id: u32,
        yield_fn: Box<SExpression<'a, DB>>
    },
    Map {
        source_id: u32,
        yield_in_fn: Box<SExpression<'a, DB>>,
        yield_out_fn: Box<SExpression<'a, DB>>
    },
    Yield {
        fields: Vec<SExpression<'a, DB>>
    },
    ColumnField {
        source_id: u32,
        column_offset: u32
    },
    If {
        predicate: Box<SExpression<'a, DB>>,
        yield_fn: Box<SExpression<'a, DB>>
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<SExpression<'a, DB>>,
        rhs: Box<SExpression<'a, DB>>
    },
    Value(<DB as DatabaseInfo>::ColumnValue)
}

impl<'a, DB: DatabaseInfo> fmt::Display for SExpression<'a, DB>
where <DB as DatabaseInfo>::Table: 'a
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.format(f, 0)
    }
}

impl<'a, DB: DatabaseInfo> SExpression<'a, DB>
where <DB as DatabaseInfo>::Table: 'a
{
    fn format(&self, f: &mut fmt::Formatter, indent: usize) -> Result<(), fmt::Error> {
        for _ in 0..indent {
            try!(write!(f, "  "));
        }

        match self {
            &SExpression::Scan { table, source_id, ref yield_fn } => {
                try!(writeln!(f, "(scan `{}` :source_id {}", table.get_name(), source_id));
                try!(yield_fn.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::Map { source_id, ref yield_in_fn, ref yield_out_fn } => {
                try!(writeln!(f, "(map :source_id {}", source_id));
                try!(yield_in_fn.format(f, indent + 1));
                try!(writeln!(f, ""));
                try!(yield_out_fn.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::Yield { ref fields } => {
                try!(writeln!(f, "(yield "));
                for (i, field) in fields.iter().enumerate() {
                    try!(field.format(f, indent + 1));
                    if i != fields.len() - 1 {
                        try!(writeln!(f, ""));
                    }
                }
                write!(f, ")")
            },
            &SExpression::ColumnField { source_id, column_offset } => {
                write!(f, "(column-field :source_id {} :column_offset {})", source_id, column_offset)
            },
            &SExpression::If { ref predicate, ref yield_fn } => {
                try!(writeln!(f, "(if "));
                try!(predicate.format(f, indent + 1));
                try!(writeln!(f, ""));
                try!(yield_fn.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::BinaryOp { ref op, ref lhs, ref rhs } => {
                try!(writeln!(f, "({} ", op.sigil()));
                try!(lhs.format(f, indent + 1));
                try!(writeln!(f, ""));
                try!(rhs.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::Value(ref v) => {
                write!(f, "{}", v)
            }
        }
    }
}

pub enum BinaryOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    BitAnd,
    BitOr,
    Concatenate,
}

impl BinaryOp {
    fn sigil(&self) -> &'static str {
        use self::BinaryOp::*;

        match self {
            &Equal => "=",
            &NotEqual => "<>",
            &LessThan => "<",
            &LessThanOrEqual => "<=",
            &GreaterThan => ">",
            &GreaterThanOrEqual => ">=",
            &And => "and",
            &Or => "or",
            &Add => "+",
            &Subtract => "-",
            &Multiply => "*",
            &BitAnd => "&",
            &BitOr => "|",
            &Concatenate => "concat"
        }
    }
}
