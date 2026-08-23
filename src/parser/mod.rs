//! SQL text → query model.
//!
//! Clients speak SQL-shaped text; the engine speaks [`crate::model`]. This
//! module is the boundary. Parsing is schema-aware — the model embeds
//! [`DbTable`]s and splits primary-key values from row data, so every entry
//! point takes a [`Catalog`] to resolve tables, validate column names, and
//! know which columns form the primary key.
//!
//! ```text
//! query     := read | write
//! read      := SELECT '*' FROM ident [WHERE filter]
//!              [ORDER BY column [ASC | DESC]] [LIMIT integer]
//! write     := INSERT INTO ident '(' ident (',' ident)* ')' VALUES row
//!            | UPDATE ident SET assign (',' assign)* WHERE filter
//!            | DELETE FROM ident WHERE filter
//! row       := '(' value (',' value)* ')'
//! assign    := ident '=' value
//! filter    := conjunct (OR conjunct)*
//! conjunct  := primary (AND primary)*
//! primary   := '(' filter ')' | condition | TRUE | FALSE
//! condition := column ('=' | '!=' | '<>' | '>' | '>=' | '<' | '<=') value
//!            | column [NOT] IN '(' [value (',' value)*] ')'
//! column    := ident
//! value     := NULL | TRUE | FALSE | number | string
//!            | '[' [value (',' value)*] ']'
//! ```
//!
//! Keywords are case-insensitive; identifiers are bare words; strings are
//! single-quoted with `''` for an embedded quote; `TRUE` / `FALSE` as a
//! filter parse to the vacuous `AND(vec![])` / `OR(vec![])`.
//!
//! # v1 restrictions (each rejected with a clear error, never narrowed silently)
//!
//! - Single table only — `LEFT JOIN` is recognised and refused.
//! - Writes address **exactly one row by primary key**: the `WHERE` of an
//!   `UPDATE` / `DELETE` must be a conjunction of `pkey_column = value`
//!   conditions covering the whole primary key. Predicate writes ("set all
//!   OPEN tickets to DONE") arrive with the storage connector.
//! - `UPDATE` must `SET` every non-pkey column (the engine's full-row-image
//!   constraint, see [`crate::model::UpdateQuery`]); setting a pkey column
//!   is refused (that is a delete + insert, not an update).
//! - `INSERT` is single-row and must provide every pkey column; other
//!   columns may be omitted and then behave like `NULL` under predicates.
//! - Write values are checked against the catalog: each literal is coerced
//!   to its column's declared [`ValueType`] (numeric literals are typed by
//!   spelling, so `1.0` into an `Int` column becomes `Int(1)`); mismatches
//!   and `NULL` primary-key values are rejected. Row identity is
//!   variant-exact, so an uncoerced `Float(1.0)` key would silently miss
//!   the row stored under `Int(1)`.
//! - A `SELECT` without `ORDER BY` defaults to the first declared pkey
//!   column ascending, and without `LIMIT` to `u32::MAX` — the model makes
//!   both mandatory (see "Design notes" in the README).
//! - `(` / `[` nesting is capped at [`MAX_NESTING_DEPTH`] levels so
//!   pathological input returns an error instead of overflowing the stack.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::model::{
    ComparisonOperator, Condition, DbRecord, DbTable, DeleteQuery, InsertQuery, Order,
    OrderBy, ReadQuery, UpdateQuery, Value, ValueType, Where, WriteQuery,
};

/// Hard cap on `(` / `[` nesting depth. The parser descends recursively, so
/// without a bound a client-supplied string with thousands of nested parens
/// would overflow the stack and abort the whole process.
pub const MAX_NESTING_DEPTH: usize = 128;

/// The authoritative table schemas live in the model; re-exported here
/// because the parser is their primary consumer.
pub use crate::model::Catalog;

/// A successfully parsed statement — either side of the read/write split.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedQuery {
    Read(ReadQuery),
    Write(WriteQuery),
}

/// Parse one statement of either kind.
pub fn parse(sql: &str, catalog: &Catalog) -> Result<ParsedQuery, ParseError> {
    let mut parser = Parser {
        tokens: lex(sql)?,
        pos: 0,
        depth: 0,
    };
    let parsed = parser.query(catalog)?;
    if !matches!(parser.peek(), Tok::Eof) {
        return parser.err(format!("unexpected trailing input: {}", parser.describe()));
    }
    Ok(parsed)
}

/// Parse a statement that must be a `SELECT`.
pub fn parse_read(sql: &str, catalog: &Catalog) -> Result<ReadQuery, ParseError> {
    match parse(sql, catalog)? {
        ParsedQuery::Read(read) => Ok(read),
        ParsedQuery::Write(_) => Err(ParseError {
            message: "expected a SELECT, found a write statement".into(),
            position: 0,
        }),
    }
}

/// Parse a statement that must be an `INSERT` / `UPDATE` / `DELETE`.
pub fn parse_write(sql: &str, catalog: &Catalog) -> Result<WriteQuery, ParseError> {
    match parse(sql, catalog)? {
        ParsedQuery::Write(write) => Ok(write),
        ParsedQuery::Read(_) => Err(ParseError {
            message: "expected a write statement, found a SELECT".into(),
            position: 0,
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    /// Byte offset into the input where the offending token starts.
    pub position: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (at byte {})", self.message, self.position)
    }
}

impl std::error::Error for ParseError {}

/// The error, the input, and a caret under the token that caused it —
/// the rendering to show a human who typed the query.
pub fn point_at(source: &str, error: &ParseError) -> String {
    let position = error.position.min(source.len());
    let column = source[..position].chars().count();
    format!(
        "error: {}\n  {}\n  {}^",
        error.message,
        source,
        " ".repeat(column + 2)
    )
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// Identifiers and keywords alike; keywords match case-insensitively.
    Word(String),
    Str(String),
    /// Raw numeric text; the parser decides int vs float.
    Num(String),
    /// Operators and punctuation, normalised (`<>` lexes as `!=`).
    Sym(&'static str),
    Eof,
}

struct Spanned {
    tok: Tok,
    at: usize,
}

fn lex(source: &str) -> Result<Vec<Spanned>, ParseError> {
    let mut tokens = Vec::new();
    let mut chars = source.char_indices().peekable();

    while let Some(&(at, c)) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c.is_alphabetic() || c == '_' {
            let mut word = String::new();
            while let Some(&(_, c)) = chars.peek() {
                if c.is_alphanumeric() || c == '_' {
                    word.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            tokens.push(Spanned {
                tok: Tok::Word(word),
                at,
            });
        } else if c.is_ascii_digit() {
            let mut number = String::new();
            take_digits(&mut chars, &mut number);
            // A '.' only continues the number when a digit follows, so
            // `1.5` lexes as one number without capturing a stray dot.
            if peek_at_is(&chars, 0, '.') && next_is_digit(&chars, 1) {
                number.push('.');
                chars.next();
                take_digits(&mut chars, &mut number);
            }
            tokens.push(Spanned {
                tok: Tok::Num(number),
                at,
            });
        } else if c == '\'' {
            chars.next();
            let mut text = String::new();
            loop {
                match chars.next() {
                    // A doubled quote is an escaped one; a lone quote ends it.
                    Some((_, '\'')) => {
                        if peek_at_is(&chars, 0, '\'') {
                            text.push('\'');
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    Some((_, c)) => text.push(c),
                    None => {
                        return Err(ParseError {
                            message: "unterminated string literal".into(),
                            position: at,
                        })
                    }
                }
            }
            tokens.push(Spanned {
                tok: Tok::Str(text),
                at,
            });
        } else {
            chars.next();
            let sym: &'static str = match c {
                '(' => "(",
                ')' => ")",
                '[' => "[",
                ']' => "]",
                ',' => ",",
                '*' => "*",
                '-' => "-",
                '=' => "=",
                '>' => {
                    if peek_at_is(&chars, 0, '=') {
                        chars.next();
                        ">="
                    } else {
                        ">"
                    }
                }
                '<' => {
                    if peek_at_is(&chars, 0, '=') {
                        chars.next();
                        "<="
                    } else if peek_at_is(&chars, 0, '>') {
                        chars.next();
                        "!="
                    } else {
                        "<"
                    }
                }
                '!' => {
                    if peek_at_is(&chars, 0, '=') {
                        chars.next();
                        "!="
                    } else {
                        return Err(ParseError {
                            message: "expected `=` after `!`".into(),
                            position: at,
                        });
                    }
                }
                other => {
                    return Err(ParseError {
                        message: format!("unexpected character `{other}`"),
                        position: at,
                    })
                }
            };
            tokens.push(Spanned { tok: Tok::Sym(sym), at });
        }
    }

    tokens.push(Spanned {
        tok: Tok::Eof,
        at: source.len(),
    });
    Ok(tokens)
}

type Chars<'a> = std::iter::Peekable<std::str::CharIndices<'a>>;

fn take_digits(chars: &mut Chars<'_>, out: &mut String) {
    while let Some(&(_, c)) = chars.peek() {
        if c.is_ascii_digit() {
            out.push(c);
            chars.next();
        } else {
            break;
        }
    }
}

fn peek_at_is(chars: &Chars<'_>, ahead: usize, want: char) -> bool {
    chars.clone().nth(ahead).is_some_and(|(_, c)| c == want)
}

fn next_is_digit(chars: &Chars<'_>, ahead: usize) -> bool {
    chars
        .clone()
        .nth(ahead)
        .is_some_and(|(_, c)| c.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Spanned>,
    pos: usize,
    /// Current `(` / `[` nesting depth, bounded by [`MAX_NESTING_DEPTH`].
    depth: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.tokens[self.pos].tok
    }

    /// Enter one nesting level; errors past [`MAX_NESTING_DEPTH`]. Callers
    /// decrement `self.depth` on the success path; error paths abandon the
    /// parse entirely, so an unwound counter is irrelevant there.
    fn descend(&mut self) -> Result<(), ParseError> {
        self.depth += 1;
        if self.depth > MAX_NESTING_DEPTH {
            self.err(format!(
                "nesting deeper than {MAX_NESTING_DEPTH} levels is not supported"
            ))
        } else {
            Ok(())
        }
    }

    fn at(&self) -> usize {
        self.tokens[self.pos].at
    }

    fn bump(&mut self) {
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
    }

    fn err<T>(&self, message: impl Into<String>) -> Result<T, ParseError> {
        Err(ParseError {
            message: message.into(),
            position: self.at(),
        })
    }

    fn describe(&self) -> String {
        match self.peek() {
            Tok::Word(word) => format!("`{word}`"),
            Tok::Str(text) => format!("string `{text}`"),
            Tok::Num(number) => format!("number `{number}`"),
            Tok::Sym(symbol) => format!("`{symbol}`"),
            Tok::Eof => "end of input".into(),
        }
    }

    fn is_word(&self, word: &str) -> bool {
        matches!(self.peek(), Tok::Word(found) if found.eq_ignore_ascii_case(word))
    }

    fn eat_word(&mut self, word: &str) -> bool {
        let found = self.is_word(word);
        if found {
            self.bump();
        }
        found
    }

    fn expect_word(&mut self, word: &str) -> Result<(), ParseError> {
        if self.eat_word(word) {
            Ok(())
        } else {
            self.err(format!("expected `{word}`, found {}", self.describe()))
        }
    }

    fn is_sym(&self, symbol: &str) -> bool {
        matches!(self.peek(), Tok::Sym(found) if *found == symbol)
    }

    fn eat_sym(&mut self, symbol: &str) -> bool {
        let found = self.is_sym(symbol);
        if found {
            self.bump();
        }
        found
    }

    fn expect_sym(&mut self, symbol: &str) -> Result<(), ParseError> {
        if self.eat_sym(symbol) {
            Ok(())
        } else {
            self.err(format!("expected `{symbol}`, found {}", self.describe()))
        }
    }

    fn ident(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            Tok::Word(word) => {
                self.bump();
                Ok(word)
            }
            _ => self.err(format!("expected a name, found {}", self.describe())),
        }
    }

    /// A table name, resolved against the catalog.
    fn table<'c>(&mut self, catalog: &'c Catalog) -> Result<&'c DbTable, ParseError> {
        let at = self.at();
        let name = self.ident()?;
        catalog.table(&name).ok_or(ParseError {
            message: format!("unknown table `{name}`"),
            position: at,
        })
    }

    /// A column name, validated to exist on `table`.
    fn column_name(&mut self, table: &DbTable) -> Result<String, ParseError> {
        let at = self.at();
        let name = self.ident()?;
        if table.column(&name).is_some() {
            Ok(name)
        } else {
            Err(ParseError {
                message: format!("unknown column `{}` on table `{}`", name, table.name),
                position: at,
            })
        }
    }

    fn query(&mut self, catalog: &Catalog) -> Result<ParsedQuery, ParseError> {
        if self.is_word("SELECT") {
            Ok(ParsedQuery::Read(self.read(catalog)?))
        } else if self.is_word("INSERT") || self.is_word("UPDATE") || self.is_word("DELETE") {
            Ok(ParsedQuery::Write(self.write(catalog)?))
        } else {
            self.err(format!(
                "expected SELECT, INSERT, UPDATE or DELETE, found {}",
                self.describe()
            ))
        }
    }

    // -- reads -------------------------------------------------------------

    fn read(&mut self, catalog: &Catalog) -> Result<ReadQuery, ParseError> {
        self.expect_word("SELECT")?;
        self.expect_sym("*")?;
        self.expect_word("FROM")?;
        let table = self.table(catalog)?;

        if self.is_word("LEFT") || self.is_word("JOIN") {
            return self.err("joins are not supported yet — single-table queries only");
        }

        let filter = if self.eat_word("WHERE") {
            self.filter(table)?
        } else {
            // No WHERE: subscribe to the whole table (vacuously true).
            Where::AND(Vec::new())
        };

        let order_by = if self.eat_word("ORDER") {
            self.expect_word("BY")?;
            let at = self.at();
            let name = self.column_name(table)?;
            let column = table
                .column(&name)
                .expect("column_name validated existence")
                .clone();
            let direction = if self.eat_word("DESC") {
                Order::DESC
            } else {
                self.eat_word("ASC");
                Order::ASC
            };
            if self.is_sym(",") {
                return Err(ParseError {
                    message: "only one ORDER BY column is supported yet".into(),
                    position: at,
                });
            }
            OrderBy::new(column, direction)
        } else {
            default_order(table, self.at())?
        };

        let limit = if self.eat_word("LIMIT") {
            match self.peek().clone() {
                Tok::Num(number) => {
                    let at = self.at();
                    self.bump();
                    number.parse::<u32>().map_err(|_| ParseError {
                        message: format!("`{number}` is not a valid limit"),
                        position: at,
                    })?
                }
                _ => return self.err(format!("expected a limit, found {}", self.describe())),
            }
        } else {
            // The model makes `limit` mandatory; absent means "no bound".
            u32::MAX
        };

        Ok(ReadQuery::new(table.name.clone(), filter, order_by, limit))
    }

    // -- writes ------------------------------------------------------------

    fn write(&mut self, catalog: &Catalog) -> Result<WriteQuery, ParseError> {
        if self.eat_word("INSERT") {
            self.expect_word("INTO")?;
            let table = self.table(catalog)?;

            self.expect_sym("(")?;
            let columns_at = self.at();
            let mut columns = vec![self.column_name(table)?];
            while self.eat_sym(",") {
                columns.push(self.column_name(table)?);
            }
            self.expect_sym(")")?;
            ensure_no_duplicates(&columns, columns_at)?;

            self.expect_word("VALUES")?;
            let row_at = self.at();
            self.expect_sym("(")?;
            let mut values = Vec::new();
            loop {
                let value_at = self.at();
                let value = self.value()?;
                // Coerce while the value still lines up with a named column;
                // a count mismatch is reported after the whole row is read.
                values.push(match columns.get(values.len()) {
                    Some(column) => coerce_to_column_type(value, table, column, value_at)?,
                    None => value,
                });
                if !self.eat_sym(",") {
                    break;
                }
            }
            self.expect_sym(")")?;
            if self.is_sym(",") {
                return self.err("multi-row INSERT is not supported yet — one row per statement");
            }
            if values.len() != columns.len() {
                return Err(ParseError {
                    message: format!(
                        "row has {} values but {} columns were named",
                        values.len(),
                        columns.len()
                    ),
                    position: row_at,
                });
            }

            let assignments: Vec<(String, Value)> = columns.into_iter().zip(values).collect();
            let (pkey_value, data) = split_pkey(table, assignments);
            ensure_full_pkey(table, &pkey_value, "INSERT must provide", row_at)?;
            ensure_pkey_not_null(&pkey_value, row_at)?;

            Ok(WriteQuery::INSERT(InsertQuery {
                table: table.name.clone(),
                pkey_value: pkey_value.clone(),
                record: DbRecord {
                    table: table.name.clone(),
                    pkey_value,
                    data,
                },
            }))
        } else if self.eat_word("UPDATE") {
            let table = self.table(catalog)?;
            self.expect_word("SET")?;

            let set_at = self.at();
            let mut assignments: Vec<(String, Value)> = Vec::new();
            loop {
                let column = self.column_name(table)?;
                self.expect_sym("=")?;
                let value_at = self.at();
                let value = self.value()?;
                let value = coerce_to_column_type(value, table, &column, value_at)?;
                assignments.push((column, value));
                if !self.eat_sym(",") {
                    break;
                }
            }
            ensure_no_duplicates(
                &assignments
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect::<Vec<_>>(),
                set_at,
            )?;
            for (column, _) in &assignments {
                if table.is_pkey(column) {
                    return Err(ParseError {
                        message: format!(
                            "updating primary-key column `{column}` is not supported — \
                             that is a DELETE plus an INSERT"
                        ),
                        position: set_at,
                    });
                }
            }
            // The engine evaluates predicates against `record.data` as the
            // complete new row, so a partial SET would corrupt the views.
            let missing: Vec<&str> = table
                .columns
                .values()
                .filter(|column| !table.is_pkey(&column.name))
                .filter(|column| !assignments.iter().any(|(name, _)| name == &column.name))
                .map(|column| column.name.as_str())
                .collect();
            if !missing.is_empty() {
                let mut missing = missing;
                missing.sort_unstable();
                return Err(ParseError {
                    message: format!(
                        "UPDATE must SET every non-pkey column until partial row images are \
                         supported (missing: {})",
                        missing.join(", ")
                    ),
                    position: set_at,
                });
            }

            let pkey_value = self.write_pkey_filter(table)?;
            Ok(WriteQuery::UPDATE(UpdateQuery {
                table: table.name.clone(),
                pkey_value: pkey_value.clone(),
                record: DbRecord {
                    table: table.name.clone(),
                    pkey_value,
                    data: assignments.into_iter().collect(),
                },
            }))
        } else {
            self.expect_word("DELETE")?;
            self.expect_word("FROM")?;
            let table = self.table(catalog)?;
            let pkey_value = self.write_pkey_filter(table)?;
            Ok(WriteQuery::DELETE(DeleteQuery {
                table: table.name.clone(),
                pkey_value,
            }))
        }
    }

    /// The mandatory `WHERE` of an UPDATE / DELETE, restricted to a
    /// conjunction of `pkey_column = value` covering the full primary key.
    fn write_pkey_filter(
        &mut self,
        table: &DbTable,
    ) -> Result<HashMap<String, Value>, ParseError> {
        self.expect_word("WHERE")?;
        let at = self.at();
        let filter = self.filter(table)?;

        let mut pairs = Vec::new();
        collect_pkey_equalities(&filter, &mut pairs).map_err(|()| ParseError {
            message: "writes must address one row by primary key: WHERE must be a \
                      conjunction of `pkey_column = value` conditions"
                .into(),
            position: at,
        })?;

        let mut pkey_value = HashMap::new();
        for (column, value) in pairs {
            if !table.is_pkey(&column) {
                return Err(ParseError {
                    message: format!(
                        "`{column}` is not a primary-key column of `{}` — writes may only \
                         filter on the primary key yet",
                        table.name
                    ),
                    position: at,
                });
            }
            let value = coerce_to_column_type(value, table, &column, at)?;
            if pkey_value.insert(column.clone(), value).is_some() {
                return Err(ParseError {
                    message: format!("primary-key column `{column}` appears twice in WHERE"),
                    position: at,
                });
            }
        }
        ensure_full_pkey(table, &pkey_value, "WHERE must pin", at)?;
        ensure_pkey_not_null(&pkey_value, at)?;
        Ok(pkey_value)
    }

    // -- filters -----------------------------------------------------------

    /// `OR` is the loosest binder, so it sits at the top of the chain.
    fn filter(&mut self, table: &DbTable) -> Result<Where, ParseError> {
        let mut parts = vec![self.conjunct(table)?];
        while self.eat_word("OR") {
            parts.push(self.conjunct(table)?);
        }
        Ok(collapse(parts, Where::OR))
    }

    fn conjunct(&mut self, table: &DbTable) -> Result<Where, ParseError> {
        let mut parts = vec![self.primary(table)?];
        while self.eat_word("AND") {
            parts.push(self.primary(table)?);
        }
        Ok(collapse(parts, Where::AND))
    }

    fn primary(&mut self, table: &DbTable) -> Result<Where, ParseError> {
        if self.eat_sym("(") {
            self.descend()?;
            let filter = self.filter(table)?;
            self.expect_sym(")")?;
            self.depth -= 1;
            return Ok(filter);
        }
        // The identities: an empty AND is true, an empty OR is false.
        if self.eat_word("TRUE") {
            return Ok(Where::AND(Vec::new()));
        }
        if self.eat_word("FALSE") {
            return Ok(Where::OR(Vec::new()));
        }

        let column = self.column_name(table)?;
        let comparison_operator = if self.eat_sym("=") {
            ComparisonOperator::EQ
        } else if self.eat_sym("!=") {
            ComparisonOperator::NEQ
        } else if self.eat_sym(">=") {
            ComparisonOperator::GTE
        } else if self.eat_sym(">") {
            ComparisonOperator::GT
        } else if self.eat_sym("<=") {
            ComparisonOperator::LTE
        } else if self.eat_sym("<") {
            ComparisonOperator::LT
        } else if self.eat_word("IN") {
            return Ok(Where::Condition(Condition {
                column,
                comparison_operator: ComparisonOperator::IN,
                value: self.value_list()?,
            }));
        } else if self.eat_word("NOT") {
            self.expect_word("IN")?;
            return Ok(Where::Condition(Condition {
                column,
                comparison_operator: ComparisonOperator::NOT_IN,
                value: self.value_list()?,
            }));
        } else {
            return self.err(format!(
                "expected a comparison operator or [NOT] IN after a column, found {}",
                self.describe()
            ));
        };
        Ok(Where::Condition(Condition {
            column,
            comparison_operator,
            value: self.value()?,
        }))
    }

    /// The parenthesised value list of `IN` / `NOT IN`.
    fn value_list(&mut self) -> Result<Value, ParseError> {
        self.expect_sym("(")?;
        let mut items = Vec::new();
        if !self.is_sym(")") {
            items.push(self.value()?);
            while self.eat_sym(",") {
                items.push(self.value()?);
            }
        }
        self.expect_sym(")")?;
        Ok(Value::List(items))
    }

    fn value(&mut self) -> Result<Value, ParseError> {
        let at = self.at();

        if self.eat_sym("[") {
            self.descend()?;
            let mut items = Vec::new();
            if !self.is_sym("]") {
                items.push(self.value()?);
                while self.eat_sym(",") {
                    items.push(self.value()?);
                }
            }
            self.expect_sym("]")?;
            self.depth -= 1;
            return Ok(Value::List(items));
        }

        let negative = self.eat_sym("-");
        match self.peek().clone() {
            Tok::Num(number) => {
                self.bump();
                let text = if negative {
                    format!("-{number}")
                } else {
                    number
                };
                let parsed = if text.contains('.') {
                    text.parse::<f64>().map(Value::Float).ok()
                } else {
                    text.parse::<i32>().map(Value::Int).ok()
                };
                parsed.ok_or(ParseError {
                    message: format!("`{text}` is out of range"),
                    position: at,
                })
            }
            _ if negative => self.err(format!(
                "expected a number after `-`, found {}",
                self.describe()
            )),
            Tok::Str(text) => {
                self.bump();
                Ok(Value::String(text))
            }
            Tok::Word(word) if word.eq_ignore_ascii_case("NULL") => {
                self.bump();
                Ok(Value::Null)
            }
            Tok::Word(word) if word.eq_ignore_ascii_case("TRUE") => {
                self.bump();
                Ok(Value::Bool(true))
            }
            Tok::Word(word) if word.eq_ignore_ascii_case("FALSE") => {
                self.bump();
                Ok(Value::Bool(false))
            }
            _ => self.err(format!("expected a value, found {}", self.describe())),
        }
    }
}

// ---------------------------------------------------------------------------
// Semantic helpers
// ---------------------------------------------------------------------------

fn collapse(mut parts: Vec<Where>, group: fn(Vec<Where>) -> Where) -> Where {
    if parts.len() == 1 {
        parts.pop().expect("just checked the length")
    } else {
        group(parts)
    }
}

fn default_order(table: &DbTable, at: usize) -> Result<OrderBy, ParseError> {
    // First declared pkey column; a (degenerate) pkey-less table falls back
    // to the alphabetically first column name for determinism.
    let column = table
        .pkey_columns()
        .next()
        .or_else(|| table.columns.values().min_by_key(|column| &column.name))
        .ok_or(ParseError {
            message: format!(
                "table `{}` has no columns to default ORDER BY to — specify ORDER BY",
                table.name
            ),
            position: at,
        })?;
    Ok(OrderBy::new(column.clone(), Order::ASC))
}

/// Split `(column, value)` assignments into pkey values and row data.
fn split_pkey(
    table: &DbTable,
    assignments: Vec<(String, Value)>,
) -> (HashMap<String, Value>, HashMap<String, Value>) {
    let mut pkey_value = HashMap::new();
    let mut data = HashMap::new();
    for (column, value) in assignments {
        if table.is_pkey(&column) {
            pkey_value.insert(column, value);
        } else {
            data.insert(column, value);
        }
    }
    (pkey_value, data)
}

fn ensure_full_pkey(
    table: &DbTable,
    pkey_value: &HashMap<String, Value>,
    verb: &str,
    at: usize,
) -> Result<(), ParseError> {
    let mut missing: Vec<&str> = table
        .pkey
        .iter()
        .filter(|pk| !pkey_value.contains_key(*pk))
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        missing.sort_unstable();
        Err(ParseError {
            message: format!(
                "{verb} every primary-key column of `{}` (missing: {})",
                table.name,
                missing.join(", ")
            ),
            position: at,
        })
    }
}

fn ensure_pkey_not_null(
    pkey_value: &HashMap<String, Value>,
    at: usize,
) -> Result<(), ParseError> {
    let mut null_columns: Vec<&str> = pkey_value
        .iter()
        .filter(|(_, value)| value.is_null())
        .map(|(column, _)| column.as_str())
        .collect();
    if null_columns.is_empty() {
        Ok(())
    } else {
        null_columns.sort_unstable();
        Err(ParseError {
            message: format!(
                "primary-key column(s) {} cannot be NULL — a NULL-keyed row could never \
                 be matched or addressed again",
                null_columns.join(", ")
            ),
            position: at,
        })
    }
}

/// Fit a parsed write literal to `column`'s declared type. Numeric literals
/// are typed by spelling (`1` lexes as Int, `1.0` as Float), so Int/Float
/// cross-spellings coerce; every other mismatch is an error — storing it
/// would create a row whose identity and predicate behavior disagree with
/// the column (`Float(1.0)` is not the frame key `Int(1)`).
fn coerce_to_column_type(
    value: Value,
    table: &DbTable,
    column: &str,
    at: usize,
) -> Result<Value, ParseError> {
    let declared = table
        .column(column)
        .map(|candidate| &candidate.r#type)
        .expect("column names are validated against the table before coercion");
    coerce_to_type(value, declared, column, at)
}

fn coerce_to_type(
    value: Value,
    declared: &ValueType,
    column: &str,
    at: usize,
) -> Result<Value, ParseError> {
    Ok(match (value, declared) {
        // NULL fits any nullable column; pkey columns reject it separately.
        (Value::Null, _) => Value::Null,
        (value @ Value::Int(_), ValueType::Int) => value,
        (value @ Value::Float(_), ValueType::Float) => value,
        (value @ Value::String(_), ValueType::String) => value,
        (value @ Value::Bool(_), ValueType::Bool) => value,
        (Value::Int(int), ValueType::Float) => Value::Float(f64::from(int)),
        (Value::Float(float), ValueType::Int)
            if float.fract() == 0.0
                && (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&float) =>
        {
            Value::Int(float as i32)
        }
        (Value::List(items), ValueType::List(inner)) => Value::List(
            items
                .into_iter()
                .map(|item| coerce_to_type(item, inner, column, at))
                .collect::<Result<_, _>>()?,
        ),
        (Value::String(_), ValueType::Date | ValueType::Datetime) => {
            return Err(ParseError {
                message: format!(
                    "column `{column}` is declared {declared:?} — date/datetime literals \
                     are not supported yet"
                ),
                position: at,
            })
        }
        (value, _) => {
            return Err(ParseError {
                message: format!(
                    "column `{column}` is declared {declared:?} but the value is {value:?}"
                ),
                position: at,
            })
        }
    })
}

fn ensure_no_duplicates(columns: &[String], at: usize) -> Result<(), ParseError> {
    let mut seen = HashSet::new();
    for column in columns {
        if !seen.insert(column.as_str()) {
            return Err(ParseError {
                message: format!("column `{column}` appears twice"),
                position: at,
            });
        }
    }
    Ok(())
}

/// Flatten a filter into `column = value` pairs; `Err(())` if it contains
/// anything other than a conjunction of equality conditions.
fn collect_pkey_equalities(filter: &Where, out: &mut Vec<(String, Value)>) -> Result<(), ()> {
    match filter {
        Where::Condition(condition) => {
            if condition.comparison_operator == ComparisonOperator::EQ {
                out.push((condition.column.clone(), condition.value.clone()));
                Ok(())
            } else {
                Err(())
            }
        }
        Where::AND(children) => {
            for child in children {
                collect_pkey_equalities(child, out)?;
            }
            Ok(())
        }
        Where::OR(_) => Err(()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbColumn, ValueType};

    fn catalog() -> Catalog {
        Catalog::new(vec![DbTable::new(
            "tickets",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("status", ValueType::String),
                DbColumn::new("priority", ValueType::String),
                DbColumn::new("points", ValueType::Int),
            ],
        )])
    }

    fn read(sql: &str) -> ReadQuery {
        parse_read(sql, &catalog()).expect("should parse")
    }

    fn write(sql: &str) -> WriteQuery {
        parse_write(sql, &catalog()).expect("should parse")
    }

    fn read_err(sql: &str) -> ParseError {
        parse_read(sql, &catalog()).expect_err("should fail")
    }

    fn write_err(sql: &str) -> ParseError {
        parse_write(sql, &catalog()).expect_err("should fail")
    }

    #[test]
    fn select_with_all_clauses() {
        let q = read(
            "SELECT * FROM tickets WHERE status = 'OPEN' AND points >= 8 \
             ORDER BY points DESC LIMIT 10",
        );
        assert_eq!(q.table, "tickets");
        assert_eq!(
            q.filter,
            Where::AND(vec![
                Where::condition("status", ComparisonOperator::EQ, "OPEN"),
                Where::condition("points", ComparisonOperator::GTE, 8),
            ])
        );
        assert_eq!(q.order_by.column.name, "points");
        assert_eq!(q.order_by.direction, Order::DESC);
        assert_eq!(q.limit, 10);
    }

    #[test]
    fn select_defaults_no_filter_pkey_order_unbounded_limit() {
        let q = read("SELECT * FROM tickets");
        assert_eq!(q.filter, Where::AND(vec![]));
        assert_eq!(q.order_by.column.name, "id");
        assert_eq!(q.order_by.direction, Order::ASC);
        assert_eq!(q.limit, u32::MAX);
    }

    #[test]
    fn default_order_uses_first_declared_pkey_column() {
        // pkey declaration order (ts, actor) wins — not column order, not
        // alphabetical order.
        let catalog = Catalog::new(vec![DbTable::new(
            "events",
            ["ts", "actor"],
            vec![
                DbColumn::new("actor", ValueType::String),
                DbColumn::new("ts", ValueType::Int),
            ],
        )]);
        let q = parse_read("SELECT * FROM events", &catalog).expect("should parse");
        assert_eq!(q.order_by.column.name, "ts");
    }

    #[test]
    fn and_binds_tighter_than_or_and_parens_override() {
        let q = read("SELECT * FROM tickets WHERE status = 'a' AND points > 1 OR status = 'b'");
        assert!(matches!(&q.filter, Where::OR(parts) if parts.len() == 2));

        let q = read("SELECT * FROM tickets WHERE status = 'a' AND (points > 1 OR status = 'b')");
        let Where::AND(parts) = &q.filter else {
            panic!("expected a top-level AND");
        };
        assert!(matches!(parts.as_slice(), [_, Where::OR(_)]));
    }

    #[test]
    fn in_not_in_and_operator_spellings() {
        let q = read("SELECT * FROM tickets WHERE priority IN ('HIGH', 'URGENT')");
        assert_eq!(
            q.filter,
            Where::condition(
                "priority",
                ComparisonOperator::IN,
                Value::List(vec!["HIGH".into(), "URGENT".into()])
            )
        );

        let q = read("SELECT * FROM tickets WHERE priority NOT IN ('LOW')");
        assert!(matches!(
            &q.filter,
            Where::Condition(c) if c.comparison_operator == ComparisonOperator::NOT_IN
        ));

        // `<>` is the same operator as `!=`.
        assert_eq!(
            read("SELECT * FROM tickets WHERE points <> 1").filter,
            read("SELECT * FROM tickets WHERE points != 1").filter,
        );
    }

    #[test]
    fn true_false_filters_and_keyword_case() {
        assert_eq!(read("SELECT * FROM tickets WHERE TRUE").filter, Where::AND(vec![]));
        assert_eq!(read("SELECT * FROM tickets WHERE FALSE").filter, Where::OR(vec![]));
        assert_eq!(
            read("select * from tickets where status = 'x' limit 5"),
            read("SELECT * FROM tickets WHERE status = 'x' LIMIT 5"),
        );
    }

    #[test]
    fn values_parse_with_types() {
        let q = read("SELECT * FROM tickets WHERE points = -3");
        assert!(matches!(&q.filter, Where::Condition(c) if c.value == Value::Int(-3)));
        let q = read("SELECT * FROM tickets WHERE points > 1.5");
        assert!(matches!(&q.filter, Where::Condition(c) if c.value == Value::Float(1.5)));
        let q = read("SELECT * FROM tickets WHERE status = NULL");
        assert!(matches!(&q.filter, Where::Condition(c) if c.value == Value::Null));
        let q = read("SELECT * FROM tickets WHERE status = 'it''s'");
        assert!(matches!(&q.filter, Where::Condition(c) if c.value == Value::String("it's".into())));
    }

    #[test]
    fn insert_splits_pkey_from_data() {
        let WriteQuery::INSERT(insert) =
            write("INSERT INTO tickets (id, status, points) VALUES (7, 'OPEN', 3)")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(insert.pkey_value["id"], Value::Int(7));
        assert!(!insert.record.data.contains_key("id"));
        assert_eq!(insert.record.data["status"], Value::String("OPEN".into()));
        assert_eq!(insert.record.data["points"], Value::Int(3));
    }

    #[test]
    fn update_builds_full_row_image_addressed_by_pkey() {
        let WriteQuery::UPDATE(update) = write(
            "UPDATE tickets SET status = 'DONE', priority = 'LOW', points = 3 WHERE id = 7",
        ) else {
            panic!("expected an UPDATE");
        };
        assert_eq!(update.pkey_value["id"], Value::Int(7));
        assert_eq!(update.record.data.len(), 3);
    }

    #[test]
    fn delete_is_pkey_addressed() {
        let WriteQuery::DELETE(delete) = write("DELETE FROM tickets WHERE id = 7") else {
            panic!("expected a DELETE");
        };
        assert_eq!(delete.pkey_value["id"], Value::Int(7));
    }

    #[test]
    fn v1_restrictions_are_rejected_loudly() {
        let cases = [
            ("SELECT * FROM tickets LEFT JOIN tickets", "joins are not supported"),
            ("SELECT * FROM nope", "unknown table"),
            ("SELECT * FROM tickets WHERE ghost = 1", "unknown column"),
            ("SELECT * FROM tickets ORDER BY id, points", "one ORDER BY column"),
            (
                "INSERT INTO tickets (status) VALUES ('x')",
                "every primary-key column",
            ),
            (
                "INSERT INTO tickets (id, status) VALUES (1, 'x'), (2, 'y')",
                "multi-row INSERT",
            ),
            (
                "INSERT INTO tickets (id, id) VALUES (1, 2)",
                "appears twice",
            ),
            (
                "UPDATE tickets SET status = 'x' WHERE id = 1",
                "must SET every non-pkey column",
            ),
            (
                "UPDATE tickets SET id = 2, status = 'x', priority = 'y', points = 1 WHERE id = 1",
                "updating primary-key column",
            ),
            (
                "UPDATE tickets SET status = 'x', priority = 'y', points = 1 WHERE status = 'x'",
                "not a primary-key column",
            ),
            (
                "UPDATE tickets SET status = 'x', priority = 'y', points = 1 WHERE id > 1",
                "conjunction of `pkey_column = value`",
            ),
            ("DELETE FROM tickets WHERE id = 1 OR id = 2", "conjunction"),
            ("DELETE FROM tickets", "expected `WHERE`"),
        ];
        for (sql, expected) in cases {
            let error = parse(sql, &catalog()).expect_err(sql);
            assert!(
                error.message.contains(expected),
                "for {sql:?} expected {expected:?} in {:?}",
                error.message
            );
        }
    }

    #[test]
    fn write_values_coerce_to_declared_column_types() {
        // `1.0` is spelled as a float but the pkey column is Int.
        let WriteQuery::INSERT(insert) =
            write("INSERT INTO tickets (id, status) VALUES (1.0, 'OPEN')")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(insert.pkey_value["id"], Value::Int(1));

        let WriteQuery::UPDATE(update) = write(
            "UPDATE tickets SET status = 'DONE', priority = 'LOW', points = 3.0 WHERE id = 7.0",
        ) else {
            panic!("expected an UPDATE");
        };
        assert_eq!(update.pkey_value["id"], Value::Int(7));
        assert_eq!(update.record.data["points"], Value::Int(3));

        // An Int literal into a Float column widens.
        let float_catalog = Catalog::new(vec![DbTable::new(
            "metrics",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("score", ValueType::Float),
            ],
        )]);
        let WriteQuery::INSERT(insert) =
            parse_write("INSERT INTO metrics (id, score) VALUES (1, 2)", &float_catalog)
                .expect("should parse")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(insert.record.data["score"], Value::Float(2.0));
    }

    #[test]
    fn mistyped_or_null_write_values_are_rejected() {
        let cases = [
            // A string-spelled pkey would silently miss the Int-keyed row.
            ("DELETE FROM tickets WHERE id = '7'", "declared Int"),
            ("INSERT INTO tickets (id, points) VALUES (1, 'many')", "declared Int"),
            ("INSERT INTO tickets (id, status) VALUES (1.5, 'x')", "declared Int"),
            ("INSERT INTO tickets (id, status) VALUES (NULL, 'x')", "cannot be NULL"),
            (
                "UPDATE tickets SET status = 'x', priority = 'y', points = 1 WHERE id = NULL",
                "cannot be NULL",
            ),
        ];
        for (sql, expected) in cases {
            let error = write_err(sql);
            assert!(
                error.message.contains(expected),
                "for {sql:?} expected {expected:?} in {:?}",
                error.message
            );
        }
    }

    #[test]
    fn deep_nesting_errors_instead_of_overflowing_the_stack() {
        let parens = format!(
            "SELECT * FROM tickets WHERE {}status = 'x'{}",
            "(".repeat(10_000),
            ")".repeat(10_000)
        );
        let error = read_err(&parens);
        assert!(error.message.contains("nesting deeper than"));

        let brackets = format!(
            "SELECT * FROM tickets WHERE status IN ({}1{})",
            "[".repeat(10_000),
            "]".repeat(10_000)
        );
        let error = read_err(&brackets);
        assert!(error.message.contains("nesting deeper than"));

        // Reasonable nesting still parses.
        let shallow = format!(
            "SELECT * FROM tickets WHERE {}status = 'x'{}",
            "(".repeat(20),
            ")".repeat(20)
        );
        assert!(parse_read(&shallow, &catalog()).is_ok());
    }

    #[test]
    fn lex_errors_carry_positions() {
        let error = read_err("SELECT * FROM tickets WHERE status = 'oops");
        assert!(error.message.contains("unterminated string"));
        assert_eq!(error.position, 37);

        let error = read_err("SELECT * FROM tickets WHERE points ! 1");
        assert!(error.message.contains("expected `=` after `!`"));
    }

    #[test]
    fn wrong_kind_is_an_error() {
        assert!(read_err("DELETE FROM tickets WHERE id = 1")
            .message
            .contains("expected a SELECT"));
        assert!(write_err("SELECT * FROM tickets")
            .message
            .contains("expected a write statement"));
    }

    #[test]
    fn trailing_input_is_rejected() {
        let error = read_err("SELECT * FROM tickets LIMIT 1 garbage");
        assert!(error.message.contains("unexpected trailing input"));
        assert_eq!(error.position, 30);
    }
}
