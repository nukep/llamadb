use columnvalueops::{ColumnValueOps, ColumnValueOpsExt};
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
    ColumnDoesNotExist(Identifier),
    AmbiguousColumnName(Identifier),
    BadIdentifier(String),
    BadStringLiteral(String),
    BadNumberLiteral(String),
    UnknownFunctionName(Identifier),
    AggregateFunctionRequiresOneArgument,
    AggregateFunctionHasNoQueryToAggregate,
    AggregateAllMustBeCount(Identifier)
}

impl fmt::Display for QueryPlanCompileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use self::QueryPlanCompileError::*;

        match self {
            &TableDoesNotExist(ref name) => {
                write!(f, "table does not exist: {}", name)
            },
            &ColumnDoesNotExist(ref name) => {
                write!(f, "column does not exist: {}", name)
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
            &AggregateFunctionHasNoQueryToAggregate => {
                write!(f, "aggregate function contains no query to aggregate")
            },
            &AggregateAllMustBeCount(ref name) => {
                write!(f, "aggregate (*) function must be `count` (found {})", name)
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
        let mut query_to_aggregated_source_id = HashMap::new();
        let mut next_source_id = 0;
        let mut next_query_id = 1;

        let mut groups_info = GroupsInfo::new();

        let plan = {
            let compiler = QueryCompiler {
                query_id: 0,
                db: db,
                source_id_to_query_id: &mut source_id_to_query_id,
                query_to_aggregated_source_id: &mut query_to_aggregated_source_id,
                next_source_id: &mut next_source_id,
                next_query_id: &mut next_query_id
            };

            compiler.compile(stmt, &scope, &mut groups_info)
        };

        debug!("source id to query id; {:?}", source_id_to_query_id);
        debug!("query to aggregated source id; {:?}", query_to_aggregated_source_id);

        plan
    }
}

pub fn compile_ast_expression<'a, DB: DatabaseInfo>(db: &'a DB, expr: ast::Expression)
-> Result<SExpression<'a, DB>, QueryPlanCompileError>
where <DB as DatabaseInfo>::Table: 'a
{
    let scope = SourceScope::new(None, Vec::new(), Vec::new());

    let mut source_id_to_query_id = HashMap::new();
    let mut query_to_aggregated_source_id = HashMap::new();
    let mut next_source_id = 0;
    let mut next_query_id = 1;

    let mut groups_info = GroupsInfo::new();

    let mut compiler = QueryCompiler {
        query_id: 0,
        db: db,
        source_id_to_query_id: &mut source_id_to_query_id,
        query_to_aggregated_source_id: &mut query_to_aggregated_source_id,
        next_source_id: &mut next_source_id,
        next_query_id: &mut next_query_id
    };

    compiler.ast_expression_to_sexpression(expr, &scope, &mut groups_info)
}

#[derive(Debug)]
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
    query_to_aggregated_source_id: &'z mut HashMap<u32, u32>,
    next_source_id: &'z mut u32,
    next_query_id: &'z mut u32
}

enum FromWhere<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    Cross {
        tables: Vec<FromWhereTableOrSubquery<'a, DB>>,
        where_expr: Option<SExpression<'a, DB>>
    },
    Join {
        outer_table: FromWhereTableOrSubquery<'a, DB>,
        joins: Vec<FromWhereJoin<'a, DB>>,
        where_expr: Option<SExpression<'a, DB>>
    }
}

enum FromWhereJoin<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    Inner {
        table: FromWhereTableOrSubquery<'a, DB>,
        on: SExpression<'a, DB>
    },
    Left {
        source_id: u32,
        table: SExpression<'a, DB>,
        on: SExpression<'a, DB>,
        right_rows_if_none: Vec<<DB as DatabaseInfo>::ColumnValue>
    }
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
    pub fn evaluate(self, inner_expr: SExpression<'a, DB>) -> SExpression<'a, DB> {
        let core_expr = |where_expr| {
            if let Some(where_expr) = where_expr {
                SExpression::If {
                    chains: vec![IfChain {
                        predicate: where_expr,
                        yield_fn: inner_expr
                    }],
                    else_: None
                }
            } else {
                inner_expr
            }
        };

        match self {
            FromWhere::Cross { tables, where_expr } => {
                tables.into_iter().rev().fold(core_expr(where_expr), |nested_expr, x| {
                    x.into_sexpr(nested_expr)
                })
            },
            FromWhere::Join { outer_table, joins, where_expr } => {
                let s = joins.into_iter().rev().fold(core_expr(where_expr), |nested_expr, join| {
                    match join {
                        FromWhereJoin::Inner { table, on } => {
                            table.into_sexpr(SExpression::If {
                                chains: vec![IfChain {
                                    predicate: on,
                                    yield_fn: nested_expr
                                }],
                                else_: None
                            })
                        },
                        FromWhereJoin::Left { source_id, table, on, right_rows_if_none } => {
                            SExpression::LeftJoin {
                                source_id: source_id,
                                yield_in_fn: Box::new(table),
                                predicate: Box::new(on),
                                yield_out_fn: Box::new(nested_expr),
                                right_rows_if_none: right_rows_if_none
                            }
                        }
                    }
                });

                outer_table.into_sexpr(s)
            }
        }
    }
}

impl<'a, DB: DatabaseInfo> FromWhereTableOrSubquery<'a, DB>
where <DB as DatabaseInfo>::Table: 'a
{
    fn into_sexpr(self, nested_expr: SExpression<'a, DB>) -> SExpression<'a, DB> {
        match self {
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
    }

    fn source_id(&self) -> u32 {
        match self {
            &FromWhereTableOrSubquery::Table { source_id, .. } => source_id,
            &FromWhereTableOrSubquery::Subquery { source_id, .. } => source_id,
        }
    }

    fn yield_all_columns(self, column_count: u32) -> SExpression<'a, DB> {
        // TODO: remove column_count parameter, put that information in the type
        let source_id = self.source_id();

        self.into_sexpr(SExpression::Yield {
            fields: (0..column_count).map(|column_offset| {
                SExpression::ColumnField {
                    source_id: source_id,
                    column_offset: column_offset
                }
            }).collect()
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

    fn new_aggregated_source_id(&mut self, aggregated_query_id: u32) -> u32 {
        if let Some(&source_id) = self.query_to_aggregated_source_id.get(&aggregated_query_id) {
            source_id
        } else {
            let old_source_id = *self.next_source_id;

            assert!(self.source_id_to_query_id.insert(old_source_id, aggregated_query_id).is_none());
            assert!(self.query_to_aggregated_source_id.insert(aggregated_query_id, old_source_id).is_none());

            *self.next_source_id += 1;
            old_source_id
        }
    }

    fn get_query_id_from_source_id(&self, source_id: u32) -> u32 {
        *self.source_id_to_query_id.get(&source_id).unwrap()
    }

    fn compile<'b>(mut self, stmt: ast::SelectStatement, outer_scope: &'b SourceScope<'b>, groups_info: &mut GroupsInfo)
    -> Result<QueryPlan<'a, DB>, QueryPlanCompileError>
    {
        // Unimplemented syntaxes: ORDER BY
        // TODO - implement them!
        if !stmt.order_by.is_empty() { unimplemented!() }

        // FROM and WHERE are compiled together.
        // This makes sense for INNER and OUTER joins, which also
        // contain ON (conditional) expressions.

        let (new_scope, from_where) = try!(self.from_where(stmt.from, stmt.where_expr, outer_scope, groups_info));

        let (mut group_by_values, having_predicate) = if !stmt.group_by.is_empty() {
            let query_id = self.query_id;
            self.new_aggregated_source_id(query_id);

            let group_by_values = try!(stmt.group_by.into_iter().map(|expr| {
                self.ast_expression_to_sexpression(expr, &new_scope, groups_info)
            }).collect());

            let having_predicate = if let Some(having) = stmt.having {
                Some(try!(self.ast_expression_to_sexpression(having, &new_scope, groups_info)))
            } else {
                None
            };

            (group_by_values, having_predicate)
        } else {
            (vec![], None)
        };

        let (column_names, select_exprs) = try!(self.select(stmt.result_columns, &new_scope, groups_info));

        let grouped_source_id = self.query_to_aggregated_source_id.get(&self.query_id).cloned();

        let expr = if let Some(source_id) = grouped_source_id {
            let yield_every_column = SExpression::Yield {
                fields: new_scope.tables().iter().flat_map(|table| {
                    let source_id = table.source_id;

                    (0..table.out_column_names.len() as u32).map(move |column_offset| {
                        SExpression::ColumnField {
                            source_id: source_id,
                            column_offset: column_offset
                        }
                    })
                }).collect()
            };

            let yield_in_fn = from_where.evaluate(yield_every_column);

            let mapping = {
                let mut c = 0;

                new_scope.tables().iter().map(|table| {
                    let m = Mapping {
                        source_id: source_id,
                        column_offset: c
                    };

                    c += table.out_column_names.len() as u32;

                    (table.source_id, m)
                }).collect()
            };

            let mut yield_out_fn = SExpression::Yield { fields: select_exprs };

            for expr in &mut group_by_values {
                remap_columns_in_sexpression(expr, &mapping);
            }

            if let Some(having_predicate) = having_predicate {
                yield_out_fn = SExpression::If {
                    chains: vec![IfChain {
                        predicate: having_predicate,
                        yield_fn: yield_out_fn
                    }],
                    else_: None
                }
            }

            remap_columns_in_sexpression(&mut yield_out_fn, &mapping);

            SExpression::TempGroupBy {
                source_id: source_id,
                yield_in_fn: Box::new(yield_in_fn),
                group_by_values: group_by_values,
                yield_out_fn: Box::new(yield_out_fn)
            }
        } else {
            from_where.evaluate(SExpression::Yield { fields: select_exprs })
        };

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
        match from {
            ast::From::Cross(v) => self.from_where_cross(v, where_expr, scope, groups_info),
            ast::From::Join { table, joins } => self.from_where_join(table, joins, where_expr, scope, groups_info)
        }
    }

    fn ast_table_or_subquery_to<'b>(&mut self, ast_table_or_subquery: ast::TableOrSubquery, scope: &'b SourceScope<'b>, groups_info: &mut GroupsInfo)
    -> Result<((TableOrSubquery, FromWhereTableOrSubquery<'a, DB>), Identifier), QueryPlanCompileError>
    {
        match ast_table_or_subquery {
            ast::TableOrSubquery::Subquery { subquery, alias } => {
                let plan = {
                    let compiler = QueryCompiler {
                        query_id: self.new_query_id(),
                        db: self.db,
                        source_id_to_query_id: self.source_id_to_query_id,
                        query_to_aggregated_source_id: self.query_to_aggregated_source_id,
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
    }

    fn from_where_cross<'b>(&mut self, ast_cross_tables: Vec<ast::TableOrSubquery>, where_expr: Option<ast::Expression>, scope: &'b SourceScope<'b>, groups_info: &mut GroupsInfo)
    -> Result<(SourceScope<'b>, FromWhere<'a, DB>), QueryPlanCompileError>
    {
        let a: Vec<_> = try!(ast_cross_tables.into_iter().map(|ast_table_or_subquery| {
            self.ast_table_or_subquery_to(ast_table_or_subquery, scope, groups_info)
        }).collect());

        let (tables, table_aliases): (Vec<_>, _) = a.into_iter().unzip();

        let (source_tables, fromwhere_tables) = tables.into_iter().unzip();

        let new_scope = SourceScope::new(Some(scope), source_tables, table_aliases);

        let where_expr = if let Some(where_expr) = where_expr {
            Some(try!(self.ast_expression_to_sexpression(where_expr, &new_scope, groups_info)))
        } else {
            None
        };

        Ok((new_scope, FromWhere::Cross {
            tables: fromwhere_tables,
            where_expr: where_expr
        }))
    }

    fn from_where_join<'b>(&mut self, table: ast::TableOrSubquery, joins: Vec<ast::Join>, where_expr: Option<ast::Expression>, scope: &'b SourceScope<'b>, groups_info: &mut GroupsInfo)
    -> Result<(SourceScope<'b>, FromWhere<'a, DB>), QueryPlanCompileError>
    {
        let ((source_table, fromwhere_table), alias) = try!(self.ast_table_or_subquery_to(table, scope, groups_info));

        let mut new_scope = SourceScope::new(Some(scope), vec![source_table], vec![alias]);

        let j = try!(joins.into_iter().map(|join| {
            let ((source_table, fromwhere_table), alias) = try!(self.ast_table_or_subquery_to(join.table, scope, groups_info));

            match join.operator {
                ast::JoinOperator::Inner => {
                    new_scope.tables.push(source_table);
                    new_scope.table_aliases.push(alias);

                    let on = try!(self.ast_expression_to_sexpression(join.on, &new_scope, groups_info));
                    Ok(FromWhereJoin::Inner {
                        table: fromwhere_table,
                        on: on
                    })
                },
                ast::JoinOperator::Left => {
                    let source_id = self.new_source_id();

                    let left_join_source_table = TableOrSubquery {
                        source_id: source_id,
                        out_column_names: source_table.out_column_names
                    };

                    let column_count = left_join_source_table.out_column_names.len() as u32;

                    new_scope.tables.push(left_join_source_table);
                    new_scope.table_aliases.push(alias);

                    let on = try!(self.ast_expression_to_sexpression(join.on, &new_scope, groups_info));
                    Ok(FromWhereJoin::Left {
                        source_id: source_id,
                        table: fromwhere_table.yield_all_columns(column_count),
                        on: on,
                        right_rows_if_none: (0..column_count).map(|_| ColumnValueOpsExt::null()).collect()
                    })
                }
            }
        }).collect());

        let where_expr = if let Some(where_expr) = where_expr {
            Some(try!(self.ast_expression_to_sexpression(where_expr, &new_scope, groups_info)))
        } else {
            None
        };

        Ok((new_scope, FromWhere::Join {
            outer_table: fromwhere_table,
            joins: j,
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
                    groups_info.add_query_id(self.query_id);

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
        use std::borrow::Cow;

        match ast {
            ast::Expression::Ident(s) => {
                let column_identifier = try!(new_identifier(&s));

                let (source_id, column_offset) = match scope.get_column_offset(&column_identifier) {
                    GetColumnOffsetResult::One(v) => v,
                    GetColumnOffsetResult::None => return Err(QueryPlanCompileError::ColumnDoesNotExist(column_identifier)),
                    GetColumnOffsetResult::Ambiguous(..) => return Err(QueryPlanCompileError::AmbiguousColumnName(column_identifier))
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
                    GetColumnOffsetResult::One(v) => v,
                    GetColumnOffsetResult::None => return Err(QueryPlanCompileError::ColumnDoesNotExist(column_identifier)),
                    GetColumnOffsetResult::Ambiguous(..) => return Err(QueryPlanCompileError::AmbiguousColumnName(column_identifier))
                };

                groups_info.add_query_id(self.get_query_id_from_source_id(source_id));

                Ok(SExpression::ColumnField {
                    source_id: source_id,
                    column_offset: column_offset
                })
            },
            ast::Expression::UnaryOp { expr, op } => {
                let e = try!(self.ast_expression_to_sexpression(*expr, scope, groups_info));

                Ok(SExpression::UnaryOp {
                    op: ast_unaryop_to_sexpression_unaryop(op),
                    expr: Box::new(e)
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
                match DB::ColumnValue::from_string_literal(Cow::Borrowed(&s)) {
                    Ok(value) => Ok(SExpression::Value(value)),
                    Err(s) => Err(QueryPlanCompileError::BadStringLiteral(s.into_owned()))
                }
            },
            ast::Expression::Number(s) => {
                match DB::ColumnValue::from_number_literal(Cow::Borrowed(&s)) {
                    Ok(value) => Ok(SExpression::Value(value)),
                    Err(s) => Err(QueryPlanCompileError::BadNumberLiteral(s.into_owned()))
                }
            },
            ast::Expression::Null => {
                Ok(SExpression::Value(ColumnValueOpsExt::null()))
            },
            ast::Expression::Subquery(subquery) => {
                let source_id = self.new_source_id();

                let compiler = QueryCompiler {
                    query_id: self.new_query_id(),
                    db: self.db,
                    source_id_to_query_id: self.source_id_to_query_id,
                    query_to_aggregated_source_id: self.query_to_aggregated_source_id,
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

                            if let Some(aggregated_query) = g.innermost_nonaggregated_query {
                                if aggregated_query <= self.query_id {
                                    // aggregated query is outside of the expression

                                    let source_id = self.new_aggregated_source_id(aggregated_query);

                                    Ok(SExpression::AggregateOp {
                                        op: $op,
                                        source_id: source_id,
                                        value: Box::new(value)
                                    })
                                } else {
                                    // cannot aggregate over query defined inside the expression
                                    // TODO: investigate. this might actually be impossible.
                                    Err(QueryPlanCompileError::AggregateFunctionHasNoQueryToAggregate)
                                }
                            } else {
                                Err(QueryPlanCompileError::AggregateFunctionHasNoQueryToAggregate)
                            }
                        }
                    )
                }

                match &ident as &str {
                    "count" => aggregate!(AggregateOp::Count),
                    "avg" => aggregate!(AggregateOp::Avg),
                    "sum" => aggregate!(AggregateOp::Sum),
                    "min" => aggregate!(AggregateOp::Min),
                    "max" => aggregate!(AggregateOp::Max),
                    _ => Err(QueryPlanCompileError::UnknownFunctionName(ident))
                }
            },
            ast::Expression::FunctionCallAggregateAll { name } => {
                let ident = try!(new_identifier(&name));

                match &ident as &str {
                    "count" => {
                        let query_id = self.query_id;
                        let source_id = self.new_aggregated_source_id(query_id);

                        Ok(SExpression::CountAll {
                            source_id: source_id
                        })
                    }
                    _ => Err(QueryPlanCompileError::AggregateAllMustBeCount(ident))
                }
            }
        }
    }
}

#[derive(Debug)]
struct Mapping {
    source_id: u32,
    column_offset: u32
}

fn remap_columns_in_sexpression<'a, DB>(expr: &mut SExpression<'a, DB>, mapping: &HashMap<u32, Mapping>)
where DB: DatabaseInfo + 'a, <DB as DatabaseInfo>::Table: 'a
{
    match expr {
        &mut SExpression::ColumnField { ref mut source_id, ref mut column_offset } => {
            if let Some(m) = mapping.get(source_id) {
                *source_id = m.source_id;
                *column_offset += m.column_offset;
            }
        },
        _ => {
            iter_mut_expressions_in_expression(expr, |e| remap_columns_in_sexpression(e, mapping));
        }
    }
}

fn iter_mut_expressions_in_expression<'a, DB, F>(expr: &mut SExpression<'a, DB>, mut cb: F)
where DB: DatabaseInfo + 'a, <DB as DatabaseInfo>::Table: 'a, F: FnMut(&mut SExpression<'a, DB>)
{
    match expr {
        &mut SExpression::Scan { ref mut yield_fn, .. } => {
            cb(yield_fn);
        },
        &mut SExpression::Map { ref mut yield_in_fn, ref mut yield_out_fn, .. } => {
            cb(yield_in_fn);
            cb(yield_out_fn);
        },
        &mut SExpression::TempGroupBy { ref mut yield_in_fn, ref mut group_by_values, ref mut yield_out_fn, .. } => {
            cb(yield_in_fn);
            for v in group_by_values {
                cb(v);
            }
            cb(yield_out_fn);
        },
        &mut SExpression::Yield { ref mut fields } => {
            for v in fields {
                cb(v);
            }
        },
        &mut SExpression::If {
            ref mut chains,
            ref mut else_
        } => {
            for chain in chains {
                cb(&mut chain.predicate);
                cb(&mut chain.yield_fn);
            }
            if let Some(e) = else_.as_mut() {
                cb(e);
            }
        },
        &mut SExpression::BinaryOp {
            ref mut lhs,
            ref mut rhs, ..
        } => {
            cb(lhs);
            cb(rhs);
        },
        &mut SExpression::AggregateOp {
            ref mut value, ..
        } => {
            cb(value);
        },
        _ => ()
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
        ast::BinaryOp::Divide => BinaryOp::Divide,
        ast::BinaryOp::BitAnd => BinaryOp::BitAnd,
        ast::BinaryOp::BitOr => BinaryOp::BitOr,
        ast::BinaryOp::Concatenate => BinaryOp::Concatenate,
    }
}

fn ast_unaryop_to_sexpression_unaryop(ast: ast::UnaryOp) -> UnaryOp {
    match ast {
        ast::UnaryOp::Negate => UnaryOp::Negate,
    }
}

impl<'a, DB: DatabaseInfo> fmt::Display for QueryPlan<'a, DB>
where <DB as DatabaseInfo>::Table: 'a
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let cn: Vec<_> = self.out_column_names.iter().map(|n| format!("`{}`", n)).collect();

        try!(writeln!(f, "query plan"));
        try!(writeln!(f, "column names: ({})", cn.join(", ")));
        self.expr.fmt(f)
    }
}
