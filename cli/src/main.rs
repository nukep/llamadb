#[macro_use]
extern crate log;

extern crate env_logger;

extern crate linenoise;
extern crate llamadb;

use std::io::Write;
use std::time::Instant;

mod prettyselect;
use prettyselect::pretty_select;

fn main() {
    env_logger::init().unwrap();

    let mut lexer = llamadb::sqlsyntax::lexer::Lexer::new();

    let mut db = llamadb::tempdb::TempDb::new();

    let mut out = std::io::stdout();

    loop {
        let prompt = if lexer.tokens.is_empty() && lexer.is_no_state() {
            "llamadb> "
        } else {
            "    ...> "
        };

        let val = linenoise::input(prompt);

        match val {
            None => break,
            Some(input) => {
                if input == "testdata" {
                    let mut sink = std::io::sink();

                    match load_testdata(&mut sink, &mut db) {
                        Ok(()) => println!("Test data loaded."),
                        Err(message) => println!("{}", message)
                    };
                    continue;
                }

                lexer.feed_characters(input.chars());
                lexer.feed_character(Some('\n'));

                if !input.is_empty() && !lexer.tokens.is_empty() {
                    linenoise::history_add(&input);
                }

                while let Some(i) = lexer.tokens.iter().position(|token| token == &llamadb::sqlsyntax::lexer::Token::Semicolon) {
                    match execute(&mut out, &mut db, &lexer.tokens[0..i+1]) {
                        Ok(()) => (),
                        Err(message) => println!("{}", message)
                    };

                    let right = lexer.tokens.split_off(i+1);
                    lexer.tokens = right;
                }
            }
        }
    }
}

fn execute(out: &mut Write, db: &mut llamadb::tempdb::TempDb, tokens: &[llamadb::sqlsyntax::lexer::Token])
-> Result<(), String>
{
    let statement = match llamadb::sqlsyntax::parser::parse_statement(tokens) {
        Ok(stmt) => stmt,
        Err(e) => return Err(format!("syntax error: {}", e))
    };

    execute_statement(out, db, statement)
}

fn execute_statement(out: &mut Write, db: &mut llamadb::tempdb::TempDb, statement: llamadb::sqlsyntax::ast::Statement)
-> Result<(), String>
{
    use llamadb::tempdb::ExecuteStatementResponse;

    let now = Instant::now();
    let execute_result = Some(db.execute_statement(statement));
    let duration = now.elapsed();

    let seconds = duration.as_secs() as f32 + (duration.subsec_nanos() as f32 * 1.0e-9);

    let duration_string = format!("{:.3}s", seconds);

    let result = match execute_result.unwrap() {
        Ok(r) => r,
        Err(e) => return Err(format!("execution error: {}", e))
    };

    let write_result = match result {
        ExecuteStatementResponse::Created => {
            writeln!(out, "Created ({}).", duration_string)
        },
        ExecuteStatementResponse::Inserted(rows) => {
            writeln!(out, "{} rows inserted ({}).", rows, duration_string)
        },
        ExecuteStatementResponse::Select { column_names, rows } => {
            pretty_select(out, &column_names, rows, 32).and_then(|row_count| {
                writeln!(out, "{} rows selected ({}).", row_count, duration_string)
            })
        },
        ExecuteStatementResponse::Explain(plan) => {
            writeln!(out, "{}", plan)
        },
    };

    write_result.unwrap();

    Ok(())
}

fn load_testdata(out: &mut Write, db: &mut llamadb::tempdb::TempDb) -> Result<(), String> {
    let test_data = include_str!("testdata.sql");

    let statements = llamadb::sqlsyntax::parse_statements(test_data);

    for statement in statements {
        try!(execute_statement(out, db, statement));
    }

    Ok(())
}
