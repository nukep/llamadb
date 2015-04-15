use databaseinfo::{DatabaseInfo, TableInfo};

use std::fmt;

#[derive(Clone)]
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
    TempGroupBy {
        source_id: u32,
        yield_in_fn: Box<SExpression<'a, DB>>,
        group_by_values: Vec<SExpression<'a, DB>>,
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
    AggregateOp {
        op: AggregateOp,
        source_id: u32,
        value: Box<SExpression<'a, DB>>
    },
    CountAll {
        source_id: u32
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
        macro_rules! write_indent {
            ($i:expr) => (
                for _ in 0..$i {
                    try!(write!(f, "  "));
                }
            )
        }

        write_indent!(indent);

        match self {
            &SExpression::Scan { table, source_id, ref yield_fn } => {
                try!(writeln!(f, "(scan `{}` :source-id {}", table.get_name(), source_id));
                try!(yield_fn.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::Map { source_id, ref yield_in_fn, ref yield_out_fn } => {
                try!(writeln!(f, "(map :source-id {}", source_id));
                try!(yield_in_fn.format(f, indent + 1));
                try!(writeln!(f, ""));
                try!(yield_out_fn.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::TempGroupBy { source_id, ref yield_in_fn, ref group_by_values, ref yield_out_fn } => {
                try!(writeln!(f, "(temp-group-by :source-id {}", source_id));
                try!(yield_in_fn.format(f, indent + 1));
                try!(writeln!(f, ""));
                write_indent!(indent+1);
                try!(write!(f, "(group-by-values"));
                for group_by_value in group_by_values {
                    try!(writeln!(f, ""));
                    try!(group_by_value.format(f, indent + 2));
                }
                try!(writeln!(f, ")"));
                try!(yield_out_fn.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::Yield { ref fields } => {
                try!(write!(f, "(yield"));
                for field in fields {
                    try!(writeln!(f, ""));
                    try!(field.format(f, indent + 1));
                }
                write!(f, ")")
            },
            &SExpression::ColumnField { source_id, column_offset } => {
                write!(f, "(column-field :source-id {} :column-offset {})", source_id, column_offset)
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
            &SExpression::AggregateOp { ref op, source_id, ref value } => {
                try!(writeln!(f, "({} :source-id {} ", op.name(), source_id));
                try!(value.format(f, indent + 1));
                write!(f, ")")
            },
            &SExpression::CountAll { source_id } => {
                write!(f, "(count-all :source-id {})", source_id)
            },
            &SExpression::Value(ref v) => {
                write!(f, "{}", v)
            }
        }
    }
}

#[derive(Copy, Clone)]
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
    Divide,
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
            &Divide => "/",
            &BitAnd => "&",
            &BitOr => "|",
            &Concatenate => "concat"
        }
    }
}

#[derive(Copy, Clone)]
pub enum AggregateOp {
    Count,
    First,
    Avg,
    Sum,
    Min,
    Max
}

impl AggregateOp {
    fn name(&self) -> &'static str {
        use self::AggregateOp::*;

        match self {
            &Count => "count",
            &First => "first",
            &Avg => "avg",
            &Sum => "sum",
            &Min => "min",
            &Max => "max"
        }
    }
}
