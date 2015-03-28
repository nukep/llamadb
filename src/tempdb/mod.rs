//! A temporary in-memory database meant to hold us over until the pager and
//! B+Tree modules are finalized.
//!
//! This module will be removed once the pager and B+Tree are functional.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;

use databaseinfo::{DatabaseInfo, TableInfo, ColumnInfo, ColumnValueOps};
use identifier::Identifier;
use types::DbType;
use sqlsyntax::ast;

mod table;
use self::table::Table;

pub struct TempDb {
    tables: Vec<Table>
}

pub enum ExecuteStatementResponse<'a> {
    Created,
    Inserted,
    Select(ResultSet<'a>)
}

pub struct ResultSet<'a> {
    db: &'a mut TempDb
}

pub type ExecuteStatementResult<'a> = Result<ExecuteStatementResponse<'a>, String>;


#[derive(Clone)]
pub enum ColumnValue {
    StringLiteral(String),
    Number(u64)
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &ColumnValue::StringLiteral(ref s) => write!(f, "\"{}\"", s),
            &ColumnValue::Number(n) => write!(f, "{}", n)
        }
    }
}

impl ColumnValueOps for ColumnValue {
    fn from_string_literal(s: Cow<str>) -> Result<ColumnValue, Cow<str>> {
        Ok(ColumnValue::StringLiteral(s.into_owned()))
    }

    fn from_number_literal(s: Cow<str>) -> Result<ColumnValue, Cow<str>> {
        match s.parse() {
            Ok(number) => Ok(ColumnValue::Number(number)),
            Err(_) => Err(s)
        }
    }

    fn tests_true(&self) -> bool {
        match self {
            &ColumnValue::StringLiteral(ref s) => !s.is_empty(),
            &ColumnValue::Number(n) => n != 0
        }
    }
}

impl DatabaseInfo for TempDb {
    type Table = Table;
    type ColumnValue = ColumnValue;

    fn find_table_by_name(&self, name: &Identifier) -> Option<&Table> {
        self.tables.iter().find(|t| &t.name == name)
    }
}

impl TempDb {
    pub fn new() -> TempDb {
        TempDb {
            tables: Vec::new()
        }
    }

    pub fn execute_statement(&mut self, stmt: ast::Statement) -> ExecuteStatementResult {
        match stmt {
            ast::Statement::Create(create_stmt) => {
                match create_stmt {
                    ast::CreateStatement::Table(s) => self.create_table(s)
                }
            },
            ast::Statement::Insert(insert_stmt) => self.insert_into(insert_stmt),
            ast::Statement::Select(select_stmt) => self.select(select_stmt)
        }
    }

    fn create_table(&mut self, stmt: ast::CreateTableStatement) -> ExecuteStatementResult {
        if stmt.table.database_name.is_some() {
            unimplemented!()
        }

        let table_name = Identifier::new(stmt.table.table_name).unwrap();

        let columns = try!(stmt.columns.into_iter().enumerate().map(|(i, column)| {
            let name = Identifier::new(column.column_name).unwrap();
            let type_name = Identifier::new(column.type_name).unwrap();
            let type_array_size = match column.type_array_size {
                Some(Some(s)) => {
                    let v = try!(self.parse_number_as_u64(s));
                    Some(Some(v))
                },
                Some(None) => Some(None),
                None => None
            };

            let dbtype = try!(DbType::from_identifier(&type_name, type_array_size).ok_or(format!("{} is not a valid column type", type_name)));

            Ok(table::Column {
                offset: i as u32,
                name: name,
                dbtype: dbtype
            })
        }).collect());

        try!(self.add_table(Table {
            name: table_name,
            columns: columns,
            next_rowid: 1,
            rowid_index: BTreeSet::new()
        }));

        Ok(ExecuteStatementResponse::Created)
    }

    fn insert_into(&mut self, stmt: ast::InsertStatement) -> ExecuteStatementResult {
        use std::collections::VecMap;

        trace!("inserting row: {:?}", stmt);

        let mut table = try!(self.get_table_mut(stmt.table));

        let column_types: Vec<DbType> = table.get_columns().iter().map(|c| {
            c.dbtype
        }).collect();

        let ast_index_to_column_index: Vec<u32> = match stmt.into_columns {
            // Column names listed; map specified columns
            Some(v) => try!(v.iter().map(|column_name| {
                let ident = Identifier::new(column_name.as_slice()).unwrap();
                match table.find_column_by_name(&ident) {
                    Some(column) => Ok(column.get_offset()),
                    None => Err(format!("column {} not in table", column_name))
                }
            }).collect()),
            // No column names are listed; map all columns
            None => (0..table.get_column_count()).collect()
        };

        trace!("ast_index_to_column_index: {:?}", ast_index_to_column_index);

        let column_index_to_ast_index: VecMap<usize> = ast_index_to_column_index.iter().enumerate().map(|(ast_index, column_index)| {
            ((*column_index) as usize, ast_index)
        }).collect();

        trace!("column_index_to_ast_index: {:?}", column_index_to_ast_index);

        match stmt.source {
            ast::InsertSource::Values(rows) => {
                for row in rows {
                    if ast_index_to_column_index.len() != row.len() {
                        return Err(format!("INSERT value contains wrong amount of columns"));
                    }

                    let iter = column_types.iter().enumerate().map(|(i, dbtype)| {
                        let ast_index = column_index_to_ast_index.get(&i);

                        match ast_index {
                            Some(ast_index) => {
                                // TODO - allocate buffer outside of loop
                                let mut buf = Vec::new();
                                let expr = &row[*ast_index];
                                ast_expression_to_data(expr, dbtype, &mut buf);
                                Cow::Owned(buf)
                            },
                            None => {
                                // use default value for column type
                                dbtype.get_default()
                            }
                        }
                    });

                    try!(table.insert_row(iter).map_err(|e| e.to_string()));
                }

                Ok(ExecuteStatementResponse::Inserted)
            },
            ast::InsertSource::Select(s) => unimplemented!()
        }
    }

    fn select(&self, stmt: ast::SelectStatement) -> ExecuteStatementResult {
        use queryplan::QueryPlan;

        let plan = try!(QueryPlan::compile_select(self, stmt).map_err(|e| format!("{}", e)));

        debug!("{}", plan);

        unimplemented!()
    }

    fn add_table(&mut self, table: Table) -> Result<(), String> {
        if self.tables.iter().any(|t| t.name == table.name) {
            Err(format!("Table {} already exists", table.name))
        } else {
            debug!("adding table: {:?}", table);
            self.tables.push(table);

            Ok(())
        }
    }

    fn get_table_mut(&mut self, table: ast::Table) -> Result<&mut Table, String> {
        if table.database_name.is_some() {
            unimplemented!()
        }

        let table_name = try!(Identifier::new(table.table_name.as_slice()).ok_or(format!("Bad table name: {}", table.table_name)));

        match self.tables.iter_mut().find(|t| t.name == table_name) {
            Some(s) => Ok(s),
            None => Err(format!("Could not find table named {}", table_name))
        }
    }

    fn parse_number_as_u64(&self, number: String) -> Result<u64, String> {
        number.parse().map_err(|_| format!("{} is not a valid number", number))
    }
}

fn ast_expression_to_data(expr: &ast::Expression, column_type: &DbType, buf: &mut Vec<u8>) {
    use sqlsyntax::ast::Expression::*;

    // XXX: VERY TEMPORARY.

    match (expr, column_type) {
        (&StringLiteral(ref s), &DbType::String) => {
            buf.push_all(s.as_bytes());
            buf.push(0);
        },
        (&Number(ref n), &DbType::Unsigned(bytes)) => {
            let value: u64 = n.parse().unwrap();
            buf.extend((0..bytes).map(|i| {
                let j = (bytes-1)-i;
                ((value & (0xFF << (j*8))) >> (j*8)) as u8
            }));
        },
        _ => unimplemented!()
    }
}
