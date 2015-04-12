use columnvalueops::ColumnValueOps;
use databaseinfo::{DatabaseInfo, TableInfo};
use identifier::Identifier;
use sqlsyntax::ast;

use std::fmt;
use std::collections::HashMap;

mod execute;
mod sexpression;
mod source;
pub use self::execute::*;
pub use self::sexpression::*;
use self::source::*;

pub enum QueryPlanCompileError {
    TableDoesNotExist(Identifier),
    /// ambiguous column name; two or more tables have a column of the same name
    AmbiguousColumnName(Identifier),
    BadIdentifier(String),
    BadStringLiteral(String),
    BadNumberLiteral(String),
    UnknownFunctionName(Identifier),
    AggregateFunctionRequiresOneArgument
}

impl fmt::Display for QueryPlanCompileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use self::QueryPlanCompileError::*;

        match self {
            &TableDoesNotExist(ref name) => {
                write!(f, "table does not exist: {}", name)
            },
            &AmbiguousColumnName(ref name) => {
                write!(f, "ambiguous column name: {}", name)
            },
            &BadIdentifier(ref name) => {
                write!(f, "bad identifier: {}", name)
            },
            &BadStringLiteral(ref s) => {
                write!(f, "bad string literal: {}", s)
            },
            &BadNumberLiteral(ref s) => {
                write!(f, "bad number literal: {}", s)
            },
            &UnknownFunctionName(ref s) => {
                write!(f, "unknown function name: {}", s)
            },
            &AggregateFunctionRequiresOneArgument => {
                write!(f, "aggregate function requires exactly one argument")
            },
        }
    }
}

pub struct QueryPlan<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    pub expr: SExpression<'a, DB>,
    pub out_column_names: Vec<Identifier>
}

fn new_identifier(value: &str) -> Result<Identifier, QueryPlanCompileError> {
    Identifier::new(value).ok_or(QueryPlanCompileError::BadIdentifier(value.to_string()))
}

impl<'a, DB: DatabaseInfo> QueryPlan<'a, DB>
where <DB as DatabaseInfo>::Table: 'a
{
    pub fn compile_select(db: &'a DB, stmt: ast::SelectStatement)
    -> Result<QueryPlan<'a, DB>, QueryPlanCompileError>
    {
        let scope = SourceScope::new(None, Vec::new(), Vec::new());

        let mut source_id_to_query_id = HashMap::new();
        let mut next_source_id = 0;
        let mut next_query_id = 1;

        let mut groups_info = GroupsInfo::new();

        let compiler = QueryCompiler {
            query_id: 0,
            db: db,
            source_id_to_query_id: &mut source_id_to_query_id,
            next_source_id: &mut next_source_id,
            next_query_id: &mut next_query_id
        };

        compiler.compile(stmt, &scope, &mut groups_info)
    }
}

struct GroupsInfo {
    innermost_nonaggregated_query: Option<u32>
}

impl GroupsInfo {
    fn new() -> GroupsInfo {
        GroupsInfo {
            innermost_nonaggregated_query: None
        }
    }

    fn add_query_id(&mut self, query_id: u32) {
        // The innermost query of any two queries is the one with the highest ID

        if self.innermost_nonaggregated_query.is_none() {
            self.innermost_nonaggregated_query = Some(query_id);
        } else {
            if query_id > self.innermost_nonaggregated_query.unwrap() {
                self.innermost_nonaggregated_query = Some(query_id);
            }
        }
    }
}

struct QueryCompiler<'a, 'z, DB: DatabaseInfo>
where DB: 'a, <DB as DatabaseInfo>::Table: 'a
{
    query_id: u32,
    db: &'a DB,
    source_id_to_query_id: &'z mut HashMap<u32, u32>,
    next_source_id: &'z mut u32,
    next_query_id: &'z mut u32
}

struct FromWhere<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    tables: Vec<FromWhereTableOrSubquery<'a, DB>>,
    where_expr: Option<SExpression<'a, DB>>
}

enum FromWhereTableOrSubquery<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    Table {
        source_id: u32,
        table: &'a <DB as DatabaseInfo>::Table
    },
    Subquery {
        source_id: u32,
        expr: SExpression<'a, DB>
    }
}

impl<'a, DB: DatabaseInfo> FromWhere<'a, DB>
where <DB as DatabaseInfo>::Table: 'a
{
    fn evaluate(self, inner_expr: SExpression<'a, DB>) -> SExpression<'a, DB> {
        let core_expr = if let Some(where_expr) = self.where_expr {
            SExpression::If {
                predicate: Box::new(where_expr),
                yield_fn: Box::new(inner_expr)
            }
        } else {
            inner_expr
        };

        self.tables.into_iter().fold(core_expr, |nested_expr, x| {
            match x {
                FromWhereTableOrSubquery::Subquery { source_id, expr } => {
                    SExpression::Map {
                        source_id: source_id,
                        yield_in_fn: Box::new(expr),
                        yield_out_fn: Box::new(nested_expr)
                    }
                },
                FromWhereTableOrSubquery::Table { source_id, table } => {
                    SExpression::Scan {
                        source_id: source_id,
                        table: table,
                        yield_fn: Box::new(nested_expr)
                    }
                }
            }
        })
    }
}

impl<'a, 'z, DB: DatabaseInfo> QueryCompiler<'a, 'z, DB>
where DB: 'a, <DB as DatabaseInfo>::Table: 'a
{
    fn new_source_id(&mut self) -> u32 {
        let old_source_id = *self.next_source_id;

        assert!(self.source_id_to_query_id.insert(old_source_id, self.query_id).is_none());

        *self.next_source_id += 1;
        old_source_id
    }

    fn new_query_id(&mut self) -> u32 {
        let old_query_id = *self.next_query_id;
        *self.next_query_id += 1;
        old_query_id
    }

    fn get_query_id_from_source_id(&self, source_id: u32) -> u32 {
        *self.source_id_to_query_id.get(&source_id).unwrap()
    }

    fn compile<'b>(mut self, stmt: ast::SelectStatement, outer_scope: &'b SourceScope<'b>, groups_info: &mut GroupsInfo)
    -> Result<QueryPlan<'a, DB>, QueryPlanCompileError>
    {
        // Unimplemented syntaxes: GROUP BY, HAVING, ORDER BY
        // TODO - implement them!
        if !stmt.group_by.is_empty() { unimplemented!() }
        if stmt.having.is_some() { unimplemented!() }
        if !stmt.order_by.is_empty() { unimplemented!() }

        // FROM and WHERE are compiled together.
        // This makes sense for INNER and OUTER joins, which also
        // contain ON (conditional) expressions.

        let (new_scope, from_where) = try!(self.from_where(stmt.from, stmt.where_expr, outer_scope, groups_info));

        let (column_names, select_exprs) = try!(self.select(stmt.result_columns, &new_scope, groups_info));

        let expr = from_where.evaluate(SExpression::Yield { fields: select_exprs });

        Ok(QueryPlan {
            expr: expr,
            out_column_names: column_names
        })
    }

    fn from_where<'b>(&mut self, from: ast::From, where_expr: Option<ast::Expression>, scope: &'b SourceScope<'b>, groups_info: &mut GroupsInfo)
    -> Result<(SourceScope<'b>, FromWhere<'a, DB>), QueryPlanCompileError>
    {
        // TODO - avoid naive nested scans when indices are available

        // All FROM subqueries are nested, never correlated.
        let ast_cross_tables = match from {
            ast::From::Cross(v) => v,
            ast::From::Join {..} => unimplemented!()
        };

        let a: Vec<_> = try!(ast_cross_tables.into_iter().map(|ast_table_or_subquery| {
            match ast_table_or_subquery {
                ast::TableOrSubquery::Subquery { subquery, alias } => {
                    let plan = {
                        let compiler = QueryCompiler {
                            query_id: self.new_query_id(),
                            db: self.db,
                            source_id_to_query_id: self.source_id_to_query_id,
                            next_source_id: self.next_source_id,
                            next_query_id: self.next_query_id
                        };

                        try!(compiler.compile(*subquery, scope, groups_info))
                    };
                    let alias_identifier = try!(new_identifier(&alias));

                    let source_id = self.new_source_id();

                    let s = TableOrSubquery {
                        source_id: source_id,
                        out_column_names: plan.out_column_names
                    };

                    let t = FromWhereTableOrSubquery::Subquery {
                        source_id: source_id,
                        expr: plan.expr
                    };

                    Ok(((s, t), alias_identifier))
                },
                ast::TableOrSubquery::Table { table, alias } => {
                    let table_name_identifier = try!(new_identifier(&table.table_name));
                    let table = match self.db.find_table_by_name(&table_name_identifier) {
                        Some(table) => table,
                        None => return Err(QueryPlanCompileError::TableDoesNotExist(table_name_identifier))
                    };

                    let alias_identifier = if let Some(alias) = alias {
                        try!(new_identifier(&alias))
                    } else {
                        table_name_identifier
                    };

                    let source_id = self.new_source_id();

                    let s = TableOrSubquery {
                        source_id: source_id,
                        out_column_names: table.get_column_names()
                    };

                    let t = FromWhereTableOrSubquery::Table {
                        source_id: source_id,
                        table: table
                    };

                    Ok(((s, t), alias_identifier))
                }
            }
        }).collect());

        let (tables, table_aliases): (Vec<_>, _) = a.into_iter().unzip();

        let (source_tables, fromwhere_tables) = tables.into_iter().unzip();

        let new_scope = SourceScope::new(Some(scope), source_tables, table_aliases);

        let where_expr = if let Some(where_expr) = where_expr {
            Some(try!(self.ast_expression_to_sexpression(where_expr, &new_scope, groups_info)))
        } else {
            None
        };

        Ok((new_scope, FromWhere {
            tables: fromwhere_tables,
            where_expr: where_expr
        }))
    }

    fn select<'b>(&mut self, result_columns: Vec<ast::SelectColumn>, scope: &'b SourceScope<'b>, groups_info: &mut GroupsInfo)
    -> Result<(Vec<Identifier>, Vec<SExpression<'a, DB>>), QueryPlanCompileError>
    {
        let mut arbitrary_column_count = 0;

        let mut arbitrary_column_name = || {
            let s = format!("_{}", arbitrary_column_count);
            arbitrary_column_count += 1;

            Identifier::new(&s).unwrap()
        };

        let mut a: Vec<_> = Vec::new();

        for c in result_columns {
            match c {
                ast::SelectColumn::AllColumns => {
                    a.extend(scope.tables().iter().flat_map(|table| {
                        let source_id = table.source_id;

                        table.out_column_names.iter().enumerate().map(move |(i, name)| {
                            (name.clone(), SExpression::ColumnField {
                                source_id: source_id,
                                column_offset: i as u32
                            })
                        })
                    }));
                },
                ast::SelectColumn::Expr { expr, alias } => {
                    let column_name = if let Some(alias) = alias {
                        try!(new_identifier(&alias))
                    } else {
                        // if the expression is a simple identifier, make that
                        // the column name. else, assign an arbitrary name.
                        if let &ast::Expression::Ident(ref n) = &expr {
                            try!(new_identifier(n))
                        } else {
                            arbitrary_column_name()
                        }
                    };

                    let e = try!(self.ast_expression_to_sexpression(expr, &scope, groups_info));
                    a.push((column_name, e));
                }
            }
        }

        Ok(a.into_iter().unzip())
    }

    fn ast_expression_to_sexpression<'b>(&mut self, ast: ast::Expression, scope: &'b SourceScope<'b>,
        groups_info: &mut GroupsInfo)
    -> Result<SExpression<'a, DB>, QueryPlanCompileError>
    {
        use std::borrow::IntoCow;

        match ast {
            ast::Expression::Ident(s) => {
                let column_identifier = try!(new_identifier(&s));

                let (source_id, column_offset) = match scope.get_column_offset(&column_identifier) {
                    Some(v) => v,
                    None => return Err(QueryPlanCompileError::AmbiguousColumnName(column_identifier))
                };

                groups_info.add_query_id(self.get_query_id_from_source_id(source_id));

                Ok(SExpression::ColumnField {
                    source_id: source_id,
                    column_offset: column_offset
                })
            },
            ast::Expression::IdentMember(s1, s2) => {
                let table_identifier = try!(new_identifier(&s1));
                let column_identifier = try!(new_identifier(&s2));

                let (source_id, column_offset) = match scope.get_table_column_offset(&table_identifier, &column_identifier) {
                    Some(v) => v,
                    None => return Err(QueryPlanCompileError::AmbiguousColumnName(column_identifier))
                };

                groups_info.add_query_id(self.get_query_id_from_source_id(source_id));

                Ok(SExpression::ColumnField {
                    source_id: source_id,
                    column_offset: column_offset
                })
            },
            ast::Expression::BinaryOp { lhs, rhs, op } => {
                let l = try!(self.ast_expression_to_sexpression(*lhs, scope, groups_info));
                let r = try!(self.ast_expression_to_sexpression(*rhs, scope, groups_info));

                Ok(SExpression::BinaryOp {
                    op: ast_binaryop_to_sexpression_binaryop(op),
                    lhs: Box::new(l),
                    rhs: Box::new(r)
                })
            },
            ast::Expression::StringLiteral(s) => {
                match DB::ColumnValue::from_string_literal(s.into_cow()) {
                    Ok(value) => Ok(SExpression::Value(value)),
                    Err(s) => Err(QueryPlanCompileError::BadStringLiteral(s.into_owned()))
                }
            },
            ast::Expression::Number(s) => {
                match DB::ColumnValue::from_number_literal(s.into_cow()) {
                    Ok(value) => Ok(SExpression::Value(value)),
                    Err(s) => Err(QueryPlanCompileError::BadNumberLiteral(s.into_owned()))
                }
            },
            ast::Expression::Subquery(subquery) => {
                let source_id = self.new_source_id();

                let compiler = QueryCompiler {
                    query_id: self.new_query_id(),
                    db: self.db,
                    source_id_to_query_id: self.source_id_to_query_id,
                    next_source_id: self.next_source_id,
                    next_query_id: self.next_query_id
                };

                let plan = try!(compiler.compile(*subquery, scope, groups_info));

                Ok(SExpression::Map {
                    source_id: source_id,
                    yield_in_fn: Box::new(plan.expr),
                    yield_out_fn: Box::new(SExpression::ColumnField {
                        source_id: source_id,
                        column_offset: 0
                    })
                })
            },
            ast::Expression::FunctionCall { name, arguments } => {
                let ident = try!(new_identifier(&name));

                macro_rules! aggregate {
                    ($op:expr) => (
                        if arguments.len() != 1 {
                            Err(QueryPlanCompileError::AggregateFunctionRequiresOneArgument)
                        } else {
                            let arg = arguments.into_iter().nth(0).unwrap();

                            let mut g = GroupsInfo::new();

                            let value = try!(self.ast_expression_to_sexpression(arg, scope, &mut g));

                            let new_groups = groups_info.queries_used_as_groups.union(&g.queries_used_as_groups).cloned().collect();
                            groups_info.queries_used_as_groups = new_groups;

                            let source_id = unimplemented!();
                            Ok(SExpression::AggregateOp {
                                op: $op,
                                source_id: source_id,
                                value: Box::new(value)
                            })
                        }
                    )
                }

                match &ident as &str {
                    "count" => aggregate!(AggregateOp::Count),
                    "avg" => aggregate!(AggregateOp::Avg),
                    _ => Err(QueryPlanCompileError::UnknownFunctionName(ident))
                }
            },
            e => panic!("unimplemented: {:?}", e)
        }
    }
}

fn ast_binaryop_to_sexpression_binaryop(ast: ast::BinaryOp) -> BinaryOp {
    match ast {
        ast::BinaryOp::Equal => BinaryOp::Equal,
        ast::BinaryOp::NotEqual => BinaryOp::NotEqual,
        ast::BinaryOp::LessThan => BinaryOp::LessThan,
        ast::BinaryOp::LessThanOrEqual => BinaryOp::LessThanOrEqual,
        ast::BinaryOp::GreaterThan => BinaryOp::GreaterThan,
        ast::BinaryOp::GreaterThanOrEqual => BinaryOp::GreaterThanOrEqual,
        ast::BinaryOp::And => BinaryOp::And,
        ast::BinaryOp::Or => BinaryOp::Or,
        ast::BinaryOp::Add => BinaryOp::Add,
        ast::BinaryOp::Subtract => BinaryOp::Subtract,
        ast::BinaryOp::Multiply => BinaryOp::Multiply,
        ast::BinaryOp::BitAnd => BinaryOp::BitAnd,
        ast::BinaryOp::BitOr => BinaryOp::BitOr,
        ast::BinaryOp::Concatenate => BinaryOp::Concatenate,
    }
}

impl<'a, DB: DatabaseInfo> fmt::Display for QueryPlan<'a, DB>
where <DB as DatabaseInfo>::Table: 'a
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let cn: Vec<_> = self.out_column_names.iter().map(|n| format!("`{}`", n)).collect();

        try!(writeln!(f, "query plan"));
        try!(writeln!(f, "column names: ({})", cn.connect(", ")));
        self.expr.fmt(f)
    }
}
