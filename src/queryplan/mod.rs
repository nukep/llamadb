use databaseinfo::{DatabaseInfo, TableInfo, ColumnValueOps};
use identifier::Identifier;
use sqlsyntax::ast;

use std::fmt;

mod columnnames;
mod execute;
mod sexpression;
mod source;
pub use self::columnnames::*;
pub use self::execute::*;
pub use self::sexpression::*;
use self::source::*;

pub enum QueryPlanCompileError {
    TableDoesNotExist(Identifier),
    /// ambiguous column name; two or more tables have a column of the same name
    AmbiguousColumnName(Identifier),
    BadIdentifier(String),
    BadStringLiteral(String),
    BadNumberLiteral(String)
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
        }
    }
}

pub struct QueryPlan<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    pub expr: SExpression<'a, DB>,
    pub out_column_names: ColumnNames
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
        let scope = SourceScope {
            parent: None,
            tables: Vec::new(),
            table_aliases: Vec::new()
        };

        let mut source_id: u32 = 0;

        let mut source_id_fn = || {
            let old_source_id = source_id;
            source_id += 1;
            old_source_id
        };

        QueryPlan::compile_select_recurse(db, stmt, &scope, &mut source_id_fn)
    }

    fn compile_select_recurse<'b, F>(db: &'a DB, stmt: ast::SelectStatement, scope: &'b SourceScope<'a, 'b, DB>, new_source_id: &mut F)
    -> Result<QueryPlan<'a, DB>, QueryPlanCompileError>
    where F: FnMut() -> u32
    {
        // TODO - avoid naive nested scans when indices are available

        // Unimplemented syntaxes: GROUP BY, HAVING, ORDER BY
        // TODO - implement them!
        if !stmt.group_by.is_empty() { unimplemented!() }
        if stmt.having.is_some() { unimplemented!() }
        if !stmt.order_by.is_empty() { unimplemented!() }

        let mut arbitrary_column_count = 0;

        let mut arbitrary_column_name = || {
            let s = format!("_{}", arbitrary_column_count);
            arbitrary_column_count += 1;

            Identifier::new(s).unwrap()
        };

        // let mut sources = SourceCollection::new();

        // All FROM subqueries are nested, never correlated.
        let ast_cross_tables = match stmt.from {
            ast::From::Cross(v) => v,
            ast::From::Join {..} => unimplemented!()
        };

        let new_scope = {
            let a: Vec<_> = try!(ast_cross_tables.into_iter().map(|ast_table_or_subquery| {
                match ast_table_or_subquery {
                    ast::TableOrSubquery::Subquery { subquery, alias } => {
                        let plan = try!(QueryPlan::compile_select_recurse(db, *subquery, scope, new_source_id));
                        let alias_identifier = try!(new_identifier(&alias));

                        let s = TableOrSubquery::Subquery {
                            source_id: new_source_id(),
                            expr: plan.expr,
                            out_column_names: plan.out_column_names
                        };

                        Ok((s, alias_identifier))
                    },
                    ast::TableOrSubquery::Table { table, alias } => {
                        let table_name_identifier = try!(new_identifier(&table.table_name));
                        let table = match db.find_table_by_name(&table_name_identifier) {
                            Some(table) => table,
                            None => return Err(QueryPlanCompileError::TableDoesNotExist(table_name_identifier))
                        };

                        let alias_identifier = if let Some(alias) = alias {
                            try!(new_identifier(&alias))
                        } else {
                            table_name_identifier
                        };

                        let s = TableOrSubquery::Table {
                            source_id: new_source_id(),
                            table: table
                        };

                        Ok((s, alias_identifier))
                    }
                }
            }).collect());

            let (tables, table_aliases) = a.into_iter().unzip();

            SourceScope {
                parent: Some(scope),
                tables: tables,
                table_aliases: table_aliases
            }
        };

        // prevent accidental use of the old scope
        drop(scope);

        let where_expr = if let Some(where_expr) = stmt.where_expr {
            Some(try!(QueryPlan::ast_expression_to_sexpression(where_expr, db, &new_scope, new_source_id)))
        } else {
            None
        };

        // TODO: refactor this terrible mess.
        let (column_names, select_exprs) = {
            let mut a: Vec<_> = Vec::new();
            for c in stmt.result_columns {
                match c {
                    ast::SelectColumn::AllColumns => {
                        for (i, table) in new_scope.tables.iter().enumerate() {
                            match table {
                                &TableOrSubquery::Table { source_id, table} => {
                                    let it = table.get_column_names().into_iter().enumerate().map(|(i, name)| {
                                        (name, SExpression::ColumnField {
                                            source_id: source_id,
                                            column_offset: i as u32
                                        })
                                    });

                                    a.extend(it);
                                },
                                &TableOrSubquery::Subquery { source_id, ref out_column_names, .. } => {
                                    let it = out_column_names.iter().enumerate().map(|(i, name)| {
                                        (name.clone(), SExpression::ColumnField {
                                            source_id: source_id,
                                            column_offset: i as u32
                                        })
                                    });

                                    a.extend(it);
                                }
                            }
                        }
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

                        let e = try!(QueryPlan::ast_expression_to_sexpression(expr, db, &new_scope, new_source_id));
                        a.push((column_name, e));
                    }
                }
            }

            let (column_names, select_exprs) = a.into_iter().unzip();

            (ColumnNames::new(column_names), select_exprs)
        };

        let core_expr = if let Some(where_expr) = where_expr {
            SExpression::If {
                predicate: Box::new(where_expr),
                yield_fn: Box::new(SExpression::Yield { fields: select_exprs })
            }
        } else {
            SExpression::Yield { fields: select_exprs }
        };

        // table references and source ids need to be known at this point
        let expr = new_scope.tables.into_iter().fold(core_expr, |nested_expr, x| {
            match x {
                TableOrSubquery::Subquery { source_id, expr, .. } => {
                    SExpression::Map {
                        source_id: source_id,
                        yield_in_fn: Box::new(expr),
                        yield_out_fn: Box::new(nested_expr)
                    }
                },
                TableOrSubquery::Table { source_id, table } => {
                    SExpression::Scan {
                        source_id: source_id,
                        table: table,
                        yield_fn: Box::new(nested_expr)
                    }
                }
            }
        });

        Ok(QueryPlan {
            expr: expr,
            out_column_names: column_names
        })
    }

    fn ast_expression_to_sexpression<'b, F>(ast: ast::Expression, db: &'a DB, scope: &'b SourceScope<'a, 'b, DB>, new_source_id: &mut F)
    -> Result<SExpression<'a, DB>, QueryPlanCompileError>
    where F: FnMut() -> u32
    {
        use std::borrow::IntoCow;

        match ast {
            ast::Expression::Ident(s) => {
                let column_identifier = try!(new_identifier(&s));

                let (source_id, column_offset) = match scope.get_column_offset(&column_identifier) {
                    Some(v) => v,
                    None => return Err(QueryPlanCompileError::AmbiguousColumnName(column_identifier))
                };

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

                Ok(SExpression::ColumnField {
                    source_id: source_id,
                    column_offset: column_offset
                })
            },
            ast::Expression::BinaryOp { lhs, rhs, op } => {
                let l = try!(QueryPlan::ast_expression_to_sexpression(*lhs, db, scope, new_source_id));
                let r = try!(QueryPlan::ast_expression_to_sexpression(*rhs, db, scope, new_source_id));

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
                let source_id = new_source_id();

                let plan = try!(QueryPlan::compile_select_recurse(db, *subquery, scope, new_source_id));

                Ok(SExpression::Map {
                    source_id: source_id,
                    yield_in_fn: Box::new(plan.expr),
                    yield_out_fn: Box::new(SExpression::ColumnField {
                        source_id: source_id,
                        column_offset: 0
                    })
                })
            }
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
        try!(writeln!(f, "column names: ({})", cn.as_slice().connect(", ")));
        self.expr.fmt(f)
    }
}
