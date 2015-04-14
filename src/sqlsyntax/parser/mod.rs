/// The parser is a recursive descent parser.

use std::marker::{PhantomData, Sized};
use std::fmt;

use super::lexer::Token;
use super::ast::*;

mod tokens;
use self::tokens::Tokens;

pub enum RuleError {
    ExpectingFirst(&'static str, Option<Token>),
    Expecting(&'static str, Option<Token>)
}

impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use self::RuleError::*;

        match self {
            &ExpectingFirst(s, Some(ref token)) => write!(f, "Expected {}; got {:?}", s, token),
            &Expecting(s, Some(ref token)) => write!(f, "Expected {}; got {:?}", s, token),
            &ExpectingFirst(s, None) => write!(f, "Expected {}; got no more tokens", s),
            &Expecting(s, None) => write!(f, "Expected {}; got no more tokens", s)
        }
    }
}

impl fmt::Debug for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

type RuleResult<T> = Result<T, RuleError>;

fn rule_result_not_first<T>(rule_result: RuleResult<T>) -> RuleResult<T> {
    use self::RuleError::*;

    match rule_result {
        Err(ExpectingFirst(s, t)) => Err(Expecting(s, t)),
        value => value
    }
}

macro_rules! try_notfirst {
    ($r:expr) => {
        try!(rule_result_not_first($r))
    }
}

trait Rule: Sized {
    type Output: Sized = Self;

    fn parse(tokens: &mut Tokens) -> RuleResult<Self::Output>;
}

trait RuleExt: Rule {
    /// Attempts to parse a rule. If the rule is "wrong", None is returned.
    /// The parser will backtrack if the rule doesn't match or there's an error.
    ///
    /// This parses a rule with a lookahead of 1.
    /// If the error from parse is ExpectingFirst, it's converted to None.
    /// All other errors are unmodified.
    fn parse_lookahead<'a>(tokens: &mut Tokens<'a>) -> RuleResult<Option<Self::Output>> {
        let mut tokens_copy: Tokens<'a> = *tokens;

        match Self::parse(&mut tokens_copy) {
            Ok(v) => {
                *tokens = tokens_copy;
                Ok(Some(v))
            },
            Err(RuleError::ExpectingFirst(..)) => {
                Ok(None)
            },
            Err(e) => Err(e)
        }
    }

    fn parse_comma_delimited(tokens: &mut Tokens) -> RuleResult<Vec<Self::Output>> {
        CommaDelimitedRule::<Self>::parse(tokens)
    }

    /// Match zero or more consecutive occurances of the rule
    fn parse_series_star<'a>(tokens: &mut Tokens<'a>) -> RuleResult<Vec<Self::Output>> {
        let mut v = Vec::new();

        while let Some(value) = try!(Self::parse_lookahead(tokens)) {
            v.push(value);
        }

        Ok(v)
    }
}

struct CommaDelimitedRule<R: Rule> {
    _marker: PhantomData<R>
}

impl<R: Rule> Rule for CommaDelimitedRule<R> {
    type Output = Vec<R::Output>;

    fn parse(tokens: &mut Tokens) -> RuleResult<Vec<R::Output>> {
        let mut v = Vec::new();

        let value = try!(R::parse(tokens));
        v.push(value);

        // loop until no comma
        while tokens.pop_if_token(&Token::Comma) {
            // After the first item, ExpectingFirst gets converted to Expecting.
            let value = try!(rule_result_not_first(R::parse(tokens)));
            v.push(value);
        }

        Ok(v)
    }
}

struct ParensSurroundRule<R: Rule> {
    _marker: PhantomData<R>
}

impl<R: Rule> Rule for ParensSurroundRule<R> {
    type Output = R::Output;

    fn parse(tokens: &mut Tokens) -> RuleResult<R::Output> {
        try!(tokens.pop_token_expecting(&Token::LeftParen, "("));
        let p = try_notfirst!(R::parse(tokens));
        try_notfirst!(tokens.pop_token_expecting(&Token::RightParen, ")"));
        Ok(p)
    }
}

/// (R,R,R,...)
type ParensCommaDelimitedRule<R> = ParensSurroundRule<CommaDelimitedRule<R>>;

impl<R> RuleExt for R where R: Rule {}

struct Ident;

impl Rule for Ident {
    type Output = String;

    fn parse(tokens: &mut Tokens) -> RuleResult<String> {
        tokens.pop_ident_expecting("identifier")
    }
}

impl Rule for BinaryOp {
    type Output = BinaryOp;
    fn parse(tokens: &mut Tokens) -> RuleResult<BinaryOp> {
        match try!(tokens.pop_expecting("binary operator")) {
            &Token::Equal => Ok(BinaryOp::Equal),
            &Token::NotEqual => Ok(BinaryOp::NotEqual),
            &Token::LessThan => Ok(BinaryOp::LessThan),
            &Token::LessThanOrEqual => Ok(BinaryOp::LessThanOrEqual),
            &Token::GreaterThan => Ok(BinaryOp::GreaterThan),
            &Token::GreaterThanOrEqual => Ok(BinaryOp::GreaterThanOrEqual),
            &Token::And => Ok(BinaryOp::And),
            &Token::Or => Ok(BinaryOp::Or),
            &Token::Plus => Ok(BinaryOp::Add),
            &Token::Minus => Ok(BinaryOp::Subtract),
            &Token::Asterisk => Ok(BinaryOp::Multiply),
            &Token::ForwardSlash => Ok(BinaryOp::Divide),
            &Token::Ampersand => Ok(BinaryOp::BitAnd),
            &Token::Pipe => Ok(BinaryOp::BitOr),
            &Token::DoublePipe => Ok(BinaryOp::Concatenate),
            _ => Err(tokens.expecting("binary operator"))
        }
    }
}

impl UnaryOp {
    fn precedence(&self) -> u8 {
        use super::ast::UnaryOp::*;

        match self {
            &Negate => 6
        }
    }
}

impl BinaryOp {
    /// Operators with a higher precedence have a higher number.
    fn precedence(&self) -> u8 {
        use super::ast::BinaryOp::*;

        match self {
            &Multiply | &Divide => 5,
            &Add | &Subtract | &BitAnd | &BitOr | &Concatenate => 4,
            // comparison
            &Equal | &NotEqual | &LessThan | &LessThanOrEqual | &GreaterThan | &GreaterThanOrEqual => 3,
            // conjugation
            &And => 2,
            &Or => 1,
        }
    }
}

impl Rule for Expression {
    type Output = Expression;
    fn parse(tokens: &mut Tokens) -> RuleResult<Expression> {
        Expression::parse_precedence(tokens, 0)
    }
}

impl Expression {
    /// Expressions are parsed using an algorithm known as "precedence climbing".
    ///
    /// Precedence can be tricky to implement with recursive descent parsers,
    /// so this is simple a method that doesn't involve creating different
    /// rules for different precedence levels.
    fn parse_precedence(tokens: &mut Tokens, min_precedence: u8) -> RuleResult<Expression> {
        let mut expr = try!(Expression::parse_beginning(tokens));
        let mut prev_tokens = *tokens;

        // Test for after-expression tokens
        while let Some(binary_op) = try_notfirst!(BinaryOp::parse_lookahead(tokens)) {
            let binary_op_precedence = binary_op.precedence();

            if binary_op_precedence >= min_precedence {
                // Assuming left associative
                let q = binary_op_precedence + 1;
                let rhs = try_notfirst!(Expression::parse_precedence(tokens, q));

                let new_expr = Expression::BinaryOp {
                    lhs: Box::new(expr),
                    rhs: Box::new(rhs),
                    op: binary_op
                };

                expr = new_expr;

                prev_tokens = *tokens;
            } else {
                // Backtrack if the precedence is lower
                *tokens = prev_tokens;
                // Let the previous expression rule with the lower precedence (if any) take over
                break;
            }
        }

        Ok(expr)
    }

    fn parse_beginning(tokens: &mut Tokens) -> RuleResult<Expression> {
        if tokens.pop_if_token(&Token::Plus) {
            // Unary, positive

            // There's no point in making a Positive unary operator, so we'll "cheat" and use negate's precedence.
            Ok(try_notfirst!(Expression::parse_precedence(tokens, UnaryOp::Negate.precedence())))
        } else if tokens.pop_if_token(&Token::Minus) {
            // Unary, negation
            let e = try_notfirst!(Expression::parse_precedence(tokens, UnaryOp::Negate.precedence()));
            Ok(Expression::UnaryOp {
                expr: Box::new(e),
                op: UnaryOp::Negate
            })
        } else if tokens.pop_if_token(&Token::LeftParen) {
            if let Some(subquery) = try!(SelectStatement::parse_lookahead(tokens)) {
                // Expression is a subquery.
                try!(tokens.pop_token_expecting(&Token::RightParen, ") after subquery"));
                Ok(Expression::Subquery(Box::new(subquery)))
            } else if let Some(encased_expression) = try!(Expression::parse_lookahead(tokens)) {
                // Expression is surrounded in parens for precedence.
                try!(tokens.pop_token_expecting(&Token::RightParen, ") after expression"));
                Ok(encased_expression)
            } else {
                Err(tokens.expecting("expression or subquery after ("))
            }
        } else if tokens.pop_if_token(&Token::Null) {
            Ok(Expression::Null)
        } else if let Some(ident) = tokens.pop_if_ident() {
            if tokens.pop_if_token(&Token::LeftParen) {
                // Function call
                if tokens.pop_if_token(&Token::Asterisk) {
                    try_notfirst!(tokens.pop_token_expecting(&Token::RightParen, ") after aggregate asterisk. e.g. (*)"));

                    Ok(Expression::FunctionCallAggregateAll { name: ident })
                } else {
                    let arguments = try_notfirst!(Expression::parse_comma_delimited(tokens));

                    try_notfirst!(tokens.pop_token_expecting(&Token::RightParen, ") after function arguments"));

                    Ok(Expression::FunctionCall { name: ident, arguments: arguments })
                }
            } else if tokens.pop_if_token(&Token::Dot) {
                let ident2 = try_notfirst!(tokens.pop_ident_expecting("ident after ."));

                Ok(Expression::IdentMember(ident, ident2))
            } else {
                Ok(Expression::Ident(ident))
            }
        } else if let Some(string) = tokens.pop_if_string_literal() {
            Ok(Expression::StringLiteral(string))
        } else if let Some(number) = tokens.pop_if_number() {
            Ok(Expression::Number(number))
        } else {
            Err(tokens.expecting("identifier or number"))
        }
    }
}

#[allow(dead_code)]
struct AsAlias;

impl Rule for AsAlias {
    type Output = String;
    fn parse(tokens: &mut Tokens) -> RuleResult<String> {
        if tokens.pop_if_token(&Token::As) {
            // Expecting alias
            Ok(try_notfirst!(tokens.pop_ident_expecting("alias after `as` keyword")))
        } else {
            tokens.pop_ident_expecting("alias name or `as` keyword")
        }
    }
}

impl Rule for Table {
    type Output = Table;
    fn parse(tokens: &mut Tokens) -> RuleResult<Table> {
        let table_name = try!(tokens.pop_ident_expecting("table name"));

        Ok(Table {
            database_name: None,
            table_name: table_name
        })
    }
}

impl Rule for TableOrSubquery {
    type Output = TableOrSubquery;
    fn parse(tokens: &mut Tokens) -> RuleResult<TableOrSubquery> {
        if let Some(select) = try!(ParensSurroundRule::<SelectStatement>::parse_lookahead(tokens)) {
            // Subquery
            let alias = try_notfirst!(AsAlias::parse(tokens));

            Ok(TableOrSubquery::Subquery {
                subquery: Box::new(select),
                alias: alias
            })
        } else if let Some(table) = try!(Table::parse_lookahead(tokens)) {
            // Table
            let alias = try_notfirst!(AsAlias::parse_lookahead(tokens));

            Ok(TableOrSubquery::Table {
                table: table,
                alias: alias
            })
        } else {
            Err(tokens.expecting("subquery or table name"))
        }
    }
}

impl Rule for SelectColumn {
    type Output = SelectColumn;
    fn parse(tokens: &mut Tokens) -> RuleResult<SelectColumn> {
        if tokens.pop_if_token(&Token::Asterisk) {
            Ok(SelectColumn::AllColumns)
        } else if let Some(expr) = try!(Expression::parse_lookahead(tokens)) {
            let alias = try_notfirst!(AsAlias::parse_lookahead(tokens));

            Ok(SelectColumn::Expr {
                expr: expr,
                alias: alias
            })
        } else {
            Err(tokens.expecting("* or expression for SELECT column"))
        }
    }
}

impl Rule for SelectStatement {
    type Output = SelectStatement;
    fn parse(tokens: &mut Tokens) -> RuleResult<SelectStatement> {
        try!(tokens.pop_token_expecting(&Token::Select, "SELECT"));

        let result_columns: Vec<SelectColumn> = try_notfirst!(SelectColumn::parse_comma_delimited(tokens));

        let from = try_notfirst!(From::parse(tokens));

        let where_expr = if tokens.pop_if_token(&Token::Where) {
            Some(try_notfirst!(Expression::parse(tokens)))
        } else {
            None
        };

        let (group_by, having) = if tokens.pop_if_token(&Token::Group) {
            try_notfirst!(tokens.pop_token_expecting(&Token::By, "BY after GROUP"));

            let group_exprs = try_notfirst!(Expression::parse_comma_delimited(tokens));

            if tokens.pop_if_token(&Token::Having) {
                let having_expr = try_notfirst!(Expression::parse(tokens));
                (group_exprs, Some(having_expr))
            } else {
                (group_exprs, None)
            }
        } else {
            (Vec::new(), None)
        };

        let order_by = if tokens.pop_if_token(&Token::Order) {
            try_notfirst!(tokens.pop_token_expecting(&Token::By, "BY after ORDER"));

            try_notfirst!(OrderingTerm::parse_comma_delimited(tokens))
        } else {
            Vec::new()
        };

        Ok(SelectStatement {
            result_columns: result_columns,
            from: from,
            where_expr: where_expr,
            group_by: group_by,
            having: having,
            order_by: order_by
        })
    }
}

impl Rule for From {
    type Output = From;
    fn parse(tokens: &mut Tokens) -> RuleResult<From> {
        try!(tokens.pop_token_expecting(&Token::From, "FROM"));

        let tables = try_notfirst!(TableOrSubquery::parse_comma_delimited(tokens));

        if tables.len() == 1 {
            // Could add a JOIN clause
            let joins = try_notfirst!(Join::parse_series_star(tokens));

            if joins.len() > 0 {
                let table = tables.into_iter().nth(0).unwrap();

                Ok(From::Join {
                    table: table,
                    joins: joins
                })
            } else {
                Ok(From::Cross(tables))
            }
        } else {
            Ok(From::Cross(tables))
        }
    }
}

impl Rule for JoinOperator {
    type Output = JoinOperator;
    fn parse(tokens: &mut Tokens) -> RuleResult<JoinOperator> {
        if tokens.pop_if_token(&Token::Left) {
            // "Outer" is optional. Pop if it exists.
            tokens.pop_if_token(&Token::Outer);

            try_notfirst!(tokens.pop_token_expecting(&Token::Join, "JOIN after LEFT (OUTER)"));
            Ok(JoinOperator::Left)
        } else if tokens.pop_if_token(&Token::Inner) {
            try_notfirst!(tokens.pop_token_expecting(&Token::Join, "JOIN after INNER"));
            Ok(JoinOperator::Inner)
        } else {
            Err(tokens.expecting("Join operator (LEFT or INNER)"))
        }
    }
}

impl Rule for Join {
    type Output = Join;
    fn parse(tokens: &mut Tokens) -> RuleResult<Join> {
        let operator = try!(JoinOperator::parse(tokens));
        let table = try_notfirst!(TableOrSubquery::parse(tokens));
        try_notfirst!(tokens.pop_token_expecting(&Token::On, "ON"));
        let on = try_notfirst!(Expression::parse(tokens));

        Ok(Join {
            operator: operator,
            table: table,
            on: on
        })
    }
}

impl Rule for OrderingTerm {
    type Output = OrderingTerm;
    fn parse(tokens: &mut Tokens) -> RuleResult<OrderingTerm> {
        let expr = try!(Expression::parse(tokens));

        let order = if tokens.pop_if_token(&Token::Asc) {
            Order::Ascending
        } else if tokens.pop_if_token(&Token::Desc) {
            Order::Descending
        } else {
            // Ascending order by default
            Order::Ascending
        };

        Ok(OrderingTerm {
            expr: expr,
            order: order
        })
    }
}

impl Rule for InsertStatement {
    type Output = InsertStatement;
    fn parse(tokens: &mut Tokens) -> RuleResult<InsertStatement> {
        try!(tokens.pop_token_expecting(&Token::Insert, "INSERT"));
        try_notfirst!(tokens.pop_token_expecting(&Token::Into, "INTO"));

        let table = try_notfirst!(Table::parse(tokens));

        let into_columns = try_notfirst!(ParensCommaDelimitedRule::<Ident>::parse_lookahead(tokens));

        let source = try_notfirst!(InsertSource::parse(tokens));

        Ok(InsertStatement {
            table: table,
            into_columns: into_columns,
            source: source
        })
    }
}

impl Rule for InsertSource {
    type Output = InsertSource;
    fn parse(tokens: &mut Tokens) -> RuleResult<InsertSource> {
        if tokens.pop_if_token(&Token::Values) {
            let values = try_notfirst!(CommaDelimitedRule::<ParensCommaDelimitedRule<Expression>>::parse(tokens));
            Ok(InsertSource::Values(values))
        } else if let Some(select) = try!(SelectStatement::parse_lookahead(tokens)) {
            Ok(InsertSource::Select(Box::new(select)))
        } else {
            Err(tokens.expecting("VALUES or SELECT"))
        }
    }
}

impl Rule for CreateTableColumnConstraint {
    type Output = CreateTableColumnConstraint;
    fn parse(tokens: &mut Tokens) -> RuleResult<CreateTableColumnConstraint> {
        if tokens.pop_if_token(&Token::Constraint) {
            let name = try_notfirst!(tokens.pop_ident_expecting("constraint name after CONSTRAINT"));
            let constraint = try_notfirst!(CreateTableColumnConstraintType::parse(tokens));

            Ok(CreateTableColumnConstraint {
                name: Some(name),
                constraint: constraint
            })
        } else {
            let constraint = try!(CreateTableColumnConstraintType::parse(tokens));

            Ok(CreateTableColumnConstraint {
                name: None,
                constraint: constraint
            })
        }
    }
}

impl Rule for CreateTableColumnConstraintType {
    type Output = CreateTableColumnConstraintType;
    fn parse(tokens: &mut Tokens) -> RuleResult<CreateTableColumnConstraintType> {
        use super::ast::CreateTableColumnConstraintType::*;

        if tokens.pop_if_token(&Token::Primary) {
            try_notfirst!(tokens.pop_token_expecting(&Token::Key, "KEY after PRIMARY"));
            Ok(PrimaryKey)
        } else if tokens.pop_if_token(&Token::Unique) {
            Ok(Unique)
        } else if tokens.pop_if_token(&Token::Null) {
            Ok(Nullable)
        } else if tokens.pop_if_token(&Token::References) {
            let table = try_notfirst!(Table::parse(tokens));
            let columns = try_notfirst!(ParensCommaDelimitedRule::<Ident>::parse_lookahead(tokens));
            Ok(ForeignKey {
                table: table,
                columns: columns
            })
        } else {
            Err(tokens.expecting("column constraint"))
        }
    }
}

impl Rule for CreateTableColumn {
    type Output = CreateTableColumn;
    fn parse(tokens: &mut Tokens) -> RuleResult<CreateTableColumn> {
        let column_name = try!(tokens.pop_ident_expecting("column name"));
        let type_name = try_notfirst!(tokens.pop_ident_expecting("type name"));

        let type_size = if tokens.pop_if_token(&Token::LeftParen) {
            let x = try!(tokens.pop_number_expecting("column type size"));
            try!(tokens.pop_token_expecting(&Token::RightParen, ")"));
            Some(x)
        } else {
            None
        };

        let type_array_size = if tokens.pop_if_token(&Token::LeftBracket) {
            if tokens.pop_if_token(&Token::RightBracket) {
                // Dynamic array
                Some(None)
            } else {
                let x = try!(tokens.pop_number_expecting("column array size"));
                try!(tokens.pop_token_expecting(&Token::RightBracket, "]"));
                Some(Some(x))
            }
        } else {
            None
        };

        let constraints = try_notfirst!(CreateTableColumnConstraint::parse_series_star(tokens));

        Ok(CreateTableColumn {
            column_name: column_name,
            type_name: type_name,
            type_size: type_size,
            type_array_size: type_array_size,
            constraints: constraints
        })
    }
}

impl Rule for CreateTableStatement {
    type Output = CreateTableStatement;
    fn parse(tokens: &mut Tokens) -> RuleResult<CreateTableStatement> {
        try!(tokens.pop_token_expecting(&Token::Table, "TABLE"));

        let table = try_notfirst!(Table::parse(tokens));

        try_notfirst!(tokens.pop_token_expecting(&Token::LeftParen, "( after table name"));
        let columns = try_notfirst!(CreateTableColumn::parse_comma_delimited(tokens));
        try_notfirst!(tokens.pop_token_expecting(&Token::RightParen, ") after table columns and constraints"));

        Ok(CreateTableStatement {
            table: table,
            columns: columns
        })
    }
}

impl Rule for CreateStatement {
    type Output = CreateStatement;
    fn parse(tokens: &mut Tokens) -> RuleResult<CreateStatement> {
        try!(tokens.pop_token_expecting(&Token::Create, "CREATE"));

        if let Some(stmt) = try_notfirst!(CreateTableStatement::parse_lookahead(tokens)) {
            Ok(CreateStatement::Table(stmt))
        } else {
            Err(tokens.expecting("TABLE"))
        }
    }
}

impl Rule for ExplainStatement {
    type Output = ExplainStatement;
    fn parse(tokens: &mut Tokens) -> RuleResult<ExplainStatement> {
        try!(tokens.pop_token_expecting(&Token::Explain, "EXPLAIN"));

        if let Some(stmt) = try_notfirst!(SelectStatement::parse_lookahead(tokens)) {
            Ok(ExplainStatement::Select(stmt))
        } else {
            Err(tokens.expecting("SELECT statement"))
        }
    }
}

impl Rule for Statement {
    type Output = Statement;
    fn parse(tokens: &mut Tokens) -> RuleResult<Statement> {
        if let Some(select) = try!(SelectStatement::parse_lookahead(tokens)) {
            Ok(Statement::Select(select))
        } else if let Some(insert) = try!(InsertStatement::parse_lookahead(tokens)) {
            Ok(Statement::Insert(insert))
        } else if let Some(create) = try!(CreateStatement::parse_lookahead(tokens)) {
            Ok(Statement::Create(create))
        } else if let Some(explain) = try!(ExplainStatement::parse_lookahead(tokens)) {
            Ok(Statement::Explain(explain))
        } else {
            Err(tokens.expecting("SELECT, INSERT, CREATE, or EXPLAIN statement"))
        }
    }
}

#[allow(dead_code)]
struct Statements;

impl Rule for Statements {
    type Output = Vec<Statement>;
    fn parse(tokens: &mut Tokens) -> RuleResult<Vec<Statement>> {
        let mut statements = Vec::new();

        while let Some(stmt) = try!(Statement::parse_lookahead(tokens)) {
            statements.push(stmt);
            try!(tokens.pop_token_expecting(&Token::Semicolon, "semicolon"));
        }

        Ok(statements)
    }
}

pub fn parse_statement(tokens_slice: &[Token]) -> Result<Statement, RuleError> {
    let mut tokens = Tokens::new(tokens_slice);
    let statement = try!(Statement::parse(&mut tokens));

    // Pop a semicolon if it's there
    tokens.pop_if_token(&Token::Semicolon);

    try!(tokens.expect_no_more_tokens());
    Ok(statement)
}

/// Parses a series of statements separated by semicolons
pub fn parse_statements(tokens_slice: &[Token]) -> Result<Vec<Statement>, RuleError> {
    let mut tokens = Tokens::new(tokens_slice);
    let statements = try!(Statements::parse(&mut tokens));
    try!(tokens.expect_no_more_tokens());
    Ok(statements)
}
