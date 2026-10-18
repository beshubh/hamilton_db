//! Converts sqlparser's AST into [`Statement`]s.
//!
//! sqlparser's statements carry dozens of dialect-specific clauses. Rather
//! than check each field, every conversion rebuilds the only form it accepts
//! from the parts it extracted and compares that with sqlparser's rendering
//! of the whole statement. Any clause we didn't look at makes the two differ,
//! so it is rejected instead of silently ignored.

use common::{Column, DataType, Value};
use sqlparser::ast::{
    self as sql, CharacterLength, ColumnDef, ColumnOption, Expr, Ident, ObjectName, ObjectNamePart,
    SelectItem, SetExpr, TableFactor, TableObject, UnaryOperator,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::ast::{CreateTable, Insert, Select, Statement};
use crate::error::{ParseError, Result};

/// Parses one or more `;`-separated statements.
pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql)?
        .iter()
        .map(convert_statement)
        .collect()
}

fn convert_statement(stmt: &sql::Statement) -> Result<Statement> {
    match stmt {
        sql::Statement::CreateTable(create) => convert_create_table(stmt, create),
        sql::Statement::Insert(insert) => convert_insert(stmt, insert),
        sql::Statement::Query(query) => convert_query(stmt, query),
        _ => Err(unsupported(stmt)),
    }
}

fn convert_create_table(stmt: &sql::Statement, create: &sql::CreateTable) -> Result<Statement> {
    if create.if_not_exists {
        return Err(ParseError::Unsupported("CREATE TABLE IF NOT EXISTS".into()));
    }
    if create.query.is_some() {
        return Err(ParseError::Unsupported("CREATE TABLE ... AS".into()));
    }
    if !create.constraints.is_empty() {
        return Err(ParseError::Unsupported("table constraints".into()));
    }
    if create.columns.is_empty() {
        return Err(ParseError::Unsupported("a table with no columns".into()));
    }

    let canonical = format!(
        "CREATE TABLE {} ({})",
        create.name,
        join(create.columns.iter())
    );
    ensure_canonical(
        stmt,
        &canonical,
        "CREATE TABLE <name> (<column> <type>, ...)",
    )?;

    Ok(Statement::CreateTable(CreateTable {
        name: object_name(&create.name)?,
        columns: create
            .columns
            .iter()
            .map(convert_column)
            .collect::<Result<_>>()?,
    }))
}

fn convert_column(column: &ColumnDef) -> Result<Column> {
    let mut nullable = true;
    for option in &column.options {
        match &option.option {
            _ if option.name.is_some() => {
                return Err(ParseError::Unsupported(format!(
                    "named column constraint `{option}`"
                )));
            }
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            _ => {
                return Err(ParseError::Unsupported(format!("column option `{option}`")));
            }
        }
    }
    Ok(Column::new(
        ident(&column.name),
        convert_data_type(&column.data_type)?,
        nullable,
    ))
}

fn convert_data_type(data_type: &sql::DataType) -> Result<DataType> {
    use sql::DataType as T;
    match data_type {
        T::Int(None) | T::Integer(None) | T::Int4(None) => Ok(DataType::Int),
        T::BigInt(None) | T::Int8(None) => Ok(DataType::BigInt),
        T::Bool | T::Boolean => Ok(DataType::Bool),
        T::Varchar(Some(CharacterLength::IntegerLength { length, unit: None }))
        | T::CharacterVarying(Some(CharacterLength::IntegerLength { length, unit: None })) => {
            match u32::try_from(*length) {
                Ok(length) if length > 0 => Ok(DataType::Varchar(length)),
                _ => Err(ParseError::InvalidVarcharLength(*length)),
            }
        }
        _ => Err(ParseError::Unsupported(format!("column type {data_type}"))),
    }
}

fn convert_insert(stmt: &sql::Statement, insert: &sql::Insert) -> Result<Statement> {
    let TableObject::TableName(table) = &insert.table else {
        return Err(unsupported(stmt));
    };
    if !insert.columns.is_empty() {
        return Err(ParseError::Unsupported("INSERT with a column list".into()));
    }
    let Some(source) = &insert.source else {
        return Err(unsupported(stmt));
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(ParseError::Unsupported("INSERT ... SELECT".into()));
    };

    let canonical = format!("INSERT INTO {table} {values}");
    ensure_canonical(stmt, &canonical, "INSERT INTO <table> VALUES (...), ...")?;

    let rows = values
        .rows
        .iter()
        .map(|row| row.content.iter().map(convert_literal).collect())
        .collect::<Result<_>>()?;
    Ok(Statement::Insert(Insert {
        table: object_name(table)?,
        rows,
    }))
}

/// Converts a literal in a VALUES row. Integers become INT when they fit and
/// BIGINT otherwise; the binder will coerce them to the column type.
fn convert_literal(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(value) => match &value.value {
            sql::Value::Number(digits, _) => integer(digits),
            sql::Value::SingleQuotedString(s) => Ok(Value::Varchar(s.clone())),
            sql::Value::Boolean(b) => Ok(Value::Bool(*b)),
            sql::Value::Null => Ok(Value::Null),
            _ => Err(unsupported_literal(expr)),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            // Parse the sign together with the digits so i64::MIN fits.
            Expr::Value(value) => match &value.value {
                sql::Value::Number(digits, _) => integer(&format!("-{digits}")),
                _ => Err(unsupported_literal(expr)),
            },
            _ => Err(unsupported_literal(expr)),
        },
        _ => Err(unsupported_literal(expr)),
    }
}

fn integer(text: &str) -> Result<Value> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ParseError::Unsupported(format!("numeric literal {text}")));
    }
    let n: i64 = text
        .parse()
        .map_err(|_| ParseError::IntegerOutOfRange(text.to_string()))?;
    Ok(match i32::try_from(n) {
        Ok(n) => Value::Int(n),
        Err(_) => Value::BigInt(n),
    })
}

fn unsupported_literal(expr: &Expr) -> ParseError {
    ParseError::Unsupported(format!("`{expr}` in VALUES (only literals)"))
}

fn convert_query(stmt: &sql::Statement, query: &sql::Query) -> Result<Statement> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported(stmt));
    };
    if select.selection.is_some() {
        return Err(ParseError::Unsupported("WHERE".into()));
    }
    let [SelectItem::Wildcard(_)] = select.projection.as_slice() else {
        return Err(ParseError::Unsupported(
            "a column list (only SELECT *)".into(),
        ));
    };
    let [from] = select.from.as_slice() else {
        return Err(ParseError::Unsupported(
            "selecting from zero or several tables".into(),
        ));
    };
    if !from.joins.is_empty() {
        return Err(ParseError::Unsupported("JOIN".into()));
    }
    let TableFactor::Table { name, .. } = &from.relation else {
        return Err(unsupported(stmt));
    };

    let canonical = format!("SELECT * FROM {name}");
    ensure_canonical(stmt, &canonical, "SELECT * FROM <table>")?;

    Ok(Statement::Select(Select {
        table: object_name(name)?,
    }))
}

/// Rejects `stmt` unless it renders exactly as `canonical`, i.e. it has no
/// clauses beyond the ones the caller extracted.
fn ensure_canonical(stmt: &sql::Statement, canonical: &str, supported: &str) -> Result<()> {
    if stmt.to_string() == canonical {
        Ok(())
    } else {
        Err(ParseError::Unsupported(format!(
            "`{stmt}` (only `{supported}`)"
        )))
    }
}

fn unsupported(stmt: &sql::Statement) -> ParseError {
    ParseError::Unsupported(format!("`{stmt}`"))
}

/// A table name. Qualified names (`schema.table`) are not supported.
fn object_name(name: &ObjectName) -> Result<String> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(id)] => Ok(ident(id)),
        _ => Err(ParseError::Unsupported(format!("qualified name {name}"))),
    }
}

/// Unquoted identifiers are case-insensitive and fold to lower case, as in
/// Postgres; quoted ones keep their case.
fn ident(id: &Ident) -> String {
    match id.quote_style {
        None => id.value.to_lowercase(),
        Some(_) => id.value.clone(),
    }
}

fn join<T: std::fmt::Display>(items: impl Iterator<Item = T>) -> String {
    items
        .map(|item| item.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(sql: &str) -> Result<Statement> {
        let mut stmts = parse(sql)?;
        assert_eq!(stmts.len(), 1);
        Ok(stmts.remove(0))
    }

    fn assert_unsupported(sql: &str) {
        let err = parse_one(sql).unwrap_err();
        assert!(
            matches!(err, ParseError::Unsupported(_)),
            "{sql}: expected Unsupported, got {err:?}"
        );
    }

    #[test]
    fn create_table() {
        let stmt = parse_one(
            "CREATE TABLE t (id INT NOT NULL, name VARCHAR(32), big BIGINT, ok BOOLEAN NULL)",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable(CreateTable {
                name: "t".into(),
                columns: vec![
                    Column::new("id", DataType::Int, false),
                    Column::new("name", DataType::Varchar(32), true),
                    Column::new("big", DataType::BigInt, true),
                    Column::new("ok", DataType::Bool, true),
                ],
            })
        );
    }

    #[test]
    fn identifiers_fold_unless_quoted() {
        let stmt = parse_one("CREATE TABLE Users (\"Id\" INT, Name INT)").unwrap();
        let Statement::CreateTable(create) = stmt else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(create.name, "users");
        assert_eq!(create.columns[0].name, "Id");
        assert_eq!(create.columns[1].name, "name");
    }

    #[test]
    fn create_table_rejects_unsupported_parts() {
        assert_unsupported("CREATE TABLE IF NOT EXISTS t (id INT)");
        assert_unsupported("CREATE TABLE t (id INT PRIMARY KEY)");
        assert_unsupported("CREATE TABLE t (id INT DEFAULT 1)");
        assert_unsupported("CREATE TABLE t (id INT, PRIMARY KEY (id))");
        assert_unsupported("CREATE TABLE t (x TEXT)");
        assert_unsupported("CREATE TABLE t (x VARCHAR)");
        assert_unsupported("CREATE TEMPORARY TABLE t (id INT)");
        assert_unsupported("CREATE TABLE s.t (id INT)");
    }

    #[test]
    fn varchar_length_must_be_positive_and_fit() {
        assert!(matches!(
            parse_one("CREATE TABLE t (x VARCHAR(0))"),
            Err(ParseError::InvalidVarcharLength(0))
        ));
        assert!(matches!(
            parse_one("CREATE TABLE t (x VARCHAR(4294967296))"),
            Err(ParseError::InvalidVarcharLength(4294967296))
        ));
    }

    #[test]
    fn insert_values() {
        let stmt = parse_one(
            "INSERT INTO t VALUES (1, 'a', TRUE, NULL), (-2147483649, 'it''s', false, -9223372036854775808)",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::Insert(Insert {
                table: "t".into(),
                rows: vec![
                    vec![
                        Value::Int(1),
                        Value::Varchar("a".into()),
                        Value::Bool(true),
                        Value::Null,
                    ],
                    vec![
                        Value::BigInt(-2147483649),
                        Value::Varchar("it's".into()),
                        Value::Bool(false),
                        Value::BigInt(i64::MIN),
                    ],
                ],
            })
        );
    }

    #[test]
    fn insert_rejects_unsupported_parts() {
        assert_unsupported("INSERT INTO t (id) VALUES (1)");
        assert_unsupported("INSERT INTO t SELECT * FROM u");
        assert_unsupported("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING");
        assert_unsupported("INSERT INTO t VALUES (1) RETURNING *");
        assert_unsupported("INSERT INTO t VALUES (1 + 1)");
        assert_unsupported("INSERT INTO t VALUES (1.5)");
    }

    #[test]
    fn integer_literal_out_of_range() {
        assert!(matches!(
            parse_one("INSERT INTO t VALUES (9223372036854775808)"),
            Err(ParseError::IntegerOutOfRange(_))
        ));
    }

    #[test]
    fn select_star() {
        assert_eq!(
            parse_one("select * from T").unwrap(),
            Statement::Select(Select { table: "t".into() })
        );
    }

    #[test]
    fn select_rejects_unsupported_parts() {
        assert_unsupported("SELECT id FROM t");
        assert_unsupported("SELECT * FROM t WHERE id = 1");
        assert_unsupported("SELECT * FROM t ORDER BY id");
        assert_unsupported("SELECT * FROM t LIMIT 1");
        assert_unsupported("SELECT DISTINCT * FROM t");
        assert_unsupported("SELECT * FROM t AS x");
        assert_unsupported("SELECT * FROM t, u");
        assert_unsupported("SELECT * FROM t JOIN u ON true");
        assert_unsupported("SELECT 1");
        assert_unsupported("WITH x AS (SELECT * FROM t) SELECT * FROM x");
        assert_unsupported("SELECT * FROM t UNION SELECT * FROM u");
    }

    #[test]
    fn other_statements_are_unsupported() {
        assert_unsupported("DROP TABLE t");
        assert_unsupported("UPDATE t SET id = 1");
        assert_unsupported("DELETE FROM t");
        assert_unsupported("CREATE INDEX i ON t (id)");
    }

    #[test]
    fn syntax_error() {
        assert!(matches!(
            parse("SELEC * FROM t"),
            Err(ParseError::Syntax(_))
        ));
    }

    #[test]
    fn multiple_statements() {
        assert_eq!(parse("SELECT * FROM a; SELECT * FROM b;").unwrap().len(), 2);
    }
}
