//! Converts sqlparser's AST into [`Statement`]s.
//!
//! sqlparser's statements carry dozens of dialect-specific clauses. Rather
//! than check each field, every conversion rebuilds the only form it accepts
//! from the parts it extracted and compares that with sqlparser's rendering
//! of the whole statement. Any clause we didn't look at makes the two differ,
//! so it is rejected instead of silently ignored.
//!
//! The parts themselves are checked as they are converted: expressions,
//! joins and ORDER BY items by matching every field of the variants we
//! accept, and table references and function calls, which have many
//! dialect-specific fields of their own, by the same rendering comparison.

use common::{Column, DataType, Value};
use sqlparser::ast::{
    self as sql, Assignment, AssignmentTarget, BinaryOperator, CharacterLength, ColumnDef,
    ColumnOption, ExactNumberInfo, Expr, FromTable, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, IndexColumn, JoinConstraint, JoinOperator, LimitClause,
    ObjectName, ObjectNamePart, OrderByKind, OrderByOptions, OrderBySort, PrimaryKeyConstraint,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, TableConstraint, TableFactor,
    TableObject, TableWithJoins, UnaryOperator, WindowType,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::ast::{
    self, BinaryOp, CreateIndex, CreateTable, Delete, FromItem, Insert, JoinKind, OrderByItem,
    Select, Statement, TableRef, UnaryOp, Update, WindowSpec,
};
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
        sql::Statement::CreateIndex(create) => convert_create_index(stmt, create),
        sql::Statement::Insert(insert) => convert_insert(stmt, insert),
        sql::Statement::Query(query) => {
            convert_query(query).map(|select| Statement::Select(Box::new(select)))
        }
        sql::Statement::Update(update) => convert_update(stmt, update),
        sql::Statement::Delete(delete) => convert_delete(stmt, delete),
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
    if create.columns.is_empty() {
        return Err(ParseError::Unsupported("a table with no columns".into()));
    }

    let elements = create
        .columns
        .iter()
        .map(ToString::to_string)
        .chain(create.constraints.iter().map(ToString::to_string));
    let canonical = format!("CREATE TABLE {} ({})", create.name, join(elements));
    ensure_canonical(
        stmt,
        &canonical,
        "CREATE TABLE <name> (<column> <type>, ... [, PRIMARY KEY (<column>, ...)])",
    )?;

    let name = object_name(&create.name)?;
    let mut columns = Vec::with_capacity(create.columns.len());
    let mut primary_keys = Vec::new();
    for column in &create.columns {
        let (column, primary_key) = convert_column(column)?;
        if primary_key {
            primary_keys.push(vec![column.name.clone()]);
        }
        columns.push(column);
    }
    for constraint in &create.constraints {
        primary_keys.push(convert_primary_key(constraint)?);
    }
    let primary_key = match primary_keys.len() {
        0 => Vec::new(),
        1 => primary_keys.remove(0),
        _ => return Err(ParseError::MultiplePrimaryKeys(name)),
    };
    Ok(Statement::CreateTable(CreateTable {
        name,
        columns,
        primary_key,
    }))
}

/// The column, and whether it is declared `PRIMARY KEY`.
fn convert_column(column: &ColumnDef) -> Result<(Column, bool)> {
    let mut nullable = true;
    let mut primary_key = false;
    for option in &column.options {
        match &option.option {
            _ if option.name.is_some() => {
                return Err(ParseError::Unsupported(format!(
                    "named column constraint `{option}`"
                )));
            }
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            ColumnOption::PrimaryKey(PrimaryKeyConstraint {
                name: None,
                index_name: None,
                index_type: None,
                columns,
                include,
                index_options,
                characteristics: None,
            }) if columns.is_empty() && include.is_empty() && index_options.is_empty() => {
                primary_key = true;
            }
            _ => {
                return Err(ParseError::Unsupported(format!("column option `{option}`")));
            }
        }
    }
    let column = Column::new(
        ident(&column.name),
        convert_data_type(&column.data_type)?,
        nullable,
    );
    Ok((column, primary_key))
}

/// The columns of a `PRIMARY KEY (col, ...)` table constraint, the only
/// kind supported.
fn convert_primary_key(constraint: &TableConstraint) -> Result<Vec<String>> {
    match constraint {
        TableConstraint::PrimaryKey(PrimaryKeyConstraint {
            name: None,
            index_name: None,
            index_type: None,
            columns,
            include,
            index_options,
            characteristics: None,
        }) if include.is_empty() && index_options.is_empty() => {
            columns.iter().map(index_column).collect()
        }
        _ => Err(ParseError::Unsupported(format!(
            "table constraint `{constraint}`"
        ))),
    }
}

fn convert_create_index(stmt: &sql::Statement, create: &sql::CreateIndex) -> Result<Statement> {
    let Some(name) = &create.name else {
        return Err(ParseError::Unsupported(
            "CREATE INDEX without a name".into(),
        ));
    };
    let canonical = format!(
        "CREATE INDEX {name} ON {}({})",
        create.table_name,
        join(create.columns.iter())
    );
    ensure_canonical(
        stmt,
        &canonical,
        "CREATE INDEX <name> ON <table> (<column>, ...)",
    )?;
    Ok(Statement::CreateIndex(CreateIndex {
        name: object_name(name)?,
        table: object_name(&create.table_name)?,
        columns: create
            .columns
            .iter()
            .map(index_column)
            .collect::<Result<_>>()?,
    }))
}

/// A key column of an index or primary key: a column name, with no sort
/// order or operator class.
fn index_column(column: &IndexColumn) -> Result<String> {
    match column {
        IndexColumn {
            column:
                sql::OrderByExpr {
                    expr: Expr::Identifier(id),
                    options:
                        OrderByOptions {
                            sort: None,
                            nulls_first: None,
                        },
                    with_fill: None,
                },
            operator_class: None,
        } => Ok(ident(id)),
        _ => Err(ParseError::Unsupported(format!("index column `{column}`"))),
    }
}

fn convert_data_type(data_type: &sql::DataType) -> Result<DataType> {
    use sql::DataType as T;
    match data_type {
        T::Int(None) | T::Integer(None) | T::Int4(None) => Ok(DataType::Int),
        T::BigInt(None) | T::Int8(None) => Ok(DataType::BigInt),
        // FLOAT without a precision is a double in PostgreSQL.
        T::DoublePrecision
        | T::Double(ExactNumberInfo::None)
        | T::Float8
        | T::Float(ExactNumberInfo::None) => Ok(DataType::Double),
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
    let Some(source) = &insert.source else {
        return Err(unsupported(stmt));
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(ParseError::Unsupported("INSERT ... SELECT".into()));
    };

    let columns = if insert.columns.is_empty() {
        String::new()
    } else {
        format!(" ({})", join(insert.columns.iter()))
    };
    let canonical = format!("INSERT INTO {table}{columns} {values}");
    ensure_canonical(
        stmt,
        &canonical,
        "INSERT INTO <table> [(<column>, ...)] VALUES (...), ...",
    )?;

    let rows = values
        .rows
        .iter()
        .map(|row| row.content.iter().map(convert_expr).collect())
        .collect::<Result<_>>()?;
    Ok(Statement::Insert(Insert {
        table: object_name(table)?,
        columns: insert
            .columns
            .iter()
            .map(object_name)
            .collect::<Result<_>>()?,
        rows,
    }))
}

/// A SELECT, either a whole statement or a subquery.
fn convert_query(query: &sql::Query) -> Result<Select> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(ParseError::Unsupported(format!("`{query}`")));
    };
    // GROUP BY ALL and modifiers such as WITH ROLLUP render differently
    // from this list, so the canonical check rejects them.
    let group_by: &[Expr] = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        GroupByExpr::All(_) => &[],
    };

    let mut canonical = format!("SELECT {}", join(select.projection.iter()));
    if !select.from.is_empty() {
        canonical += &format!(" FROM {}", join(select.from.iter()));
    }
    if let Some(selection) = &select.selection {
        canonical += &format!(" WHERE {selection}");
    }
    if !group_by.is_empty() {
        canonical += &format!(" GROUP BY {}", join(group_by.iter()));
    }
    if let Some(having) = &select.having {
        canonical += &format!(" HAVING {having}");
    }
    if let Some(order_by) = &query.order_by {
        canonical += &format!(" {order_by}");
    }
    if let Some(limit) = &query.limit_clause {
        canonical += &limit.to_string();
    }
    ensure_canonical(
        query,
        &canonical,
        "SELECT <items> [FROM <tables>] [WHERE <expr>] [GROUP BY <expr>, ...] \
         [HAVING <expr>] [ORDER BY <expr>, ...] [LIMIT <n>]",
    )?;

    let projection = select
        .projection
        .iter()
        .map(convert_select_item)
        .collect::<Result<_>>()?;
    let from = convert_from(&select.from)?;
    let selection = select.selection.as_ref().map(convert_expr).transpose()?;
    let group_by = group_by.iter().map(convert_expr).collect::<Result<_>>()?;
    let having = select.having.as_ref().map(convert_expr).transpose()?;
    let order_by = match &query.order_by {
        None => Vec::new(),
        Some(order_by) => {
            if order_by.interpolate.is_some() {
                return Err(ParseError::Unsupported(format!("`{order_by}`")));
            }
            let OrderByKind::Expressions(items) = &order_by.kind else {
                return Err(ParseError::Unsupported(format!("`{order_by}`")));
            };
            items.iter().map(convert_order_by).collect::<Result<_>>()?
        }
    };
    let limit = query.limit_clause.as_ref().map(convert_limit).transpose()?;

    Ok(Select {
        projection,
        from,
        selection,
        group_by,
        having,
        order_by,
        limit,
    })
}

fn convert_select_item(item: &SelectItem) -> Result<ast::SelectItem> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(ast::SelectItem::Expr {
            expr: convert_expr(expr)?,
            alias: None,
        }),
        SelectItem::ExprWithAlias { expr, alias } => Ok(ast::SelectItem::Expr {
            expr: convert_expr(expr)?,
            alias: Some(ident(alias)),
        }),
        // The options (`* EXCLUDE (...)` and the like) render after the `*`.
        SelectItem::Wildcard(_) if item.to_string() == "*" => Ok(ast::SelectItem::Wildcard),
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _)
            if item.to_string() == format!("{name}.*") =>
        {
            Ok(ast::SelectItem::QualifiedWildcard(object_name(name)?))
        }
        _ => Err(ParseError::Unsupported(format!("select item `{item}`"))),
    }
}

/// Folds `FROM a, b, c` into `Cross(Cross(a, b), c)`, each item being its
/// own chain of joins.
fn convert_from(from: &[TableWithJoins]) -> Result<Option<FromItem>> {
    let mut items = from.iter().map(convert_table_with_joins);
    let Some(first) = items.next().transpose()? else {
        return Ok(None);
    };
    items
        .try_fold(first, |left, right| {
            Ok(FromItem::Join {
                kind: JoinKind::Cross,
                left: Box::new(left),
                right: Box::new(right?),
                on: None,
            })
        })
        .map(Some)
}

fn convert_table_with_joins(table: &TableWithJoins) -> Result<FromItem> {
    let mut item = convert_from_factor(&table.relation)?;
    for join in &table.joins {
        if join.global {
            return Err(ParseError::Unsupported(format!("`{join}`")));
        }
        let (kind, on) = match &join.join_operator {
            JoinOperator::Join(JoinConstraint::On(on))
            | JoinOperator::Inner(JoinConstraint::On(on)) => (JoinKind::Inner, Some(on)),
            JoinOperator::Left(JoinConstraint::On(on))
            | JoinOperator::LeftOuter(JoinConstraint::On(on)) => (JoinKind::Left, Some(on)),
            JoinOperator::CrossJoin(JoinConstraint::None) => (JoinKind::Cross, None),
            _ => {
                return Err(ParseError::Unsupported(format!(
                    "`{}` (only CROSS JOIN, [INNER] JOIN ... ON and LEFT [OUTER] JOIN ... ON)",
                    join.to_string().trim()
                )));
            }
        };
        item = FromItem::Join {
            kind,
            left: Box::new(item),
            right: Box::new(convert_from_factor(&join.relation)?),
            on: on.map(convert_expr).transpose()?,
        };
    }
    Ok(item)
}

/// A table or `(SELECT ...) [AS] alias` in FROM.
fn convert_from_factor(factor: &TableFactor) -> Result<FromItem> {
    let TableFactor::Derived {
        lateral,
        subquery,
        alias,
        sample,
    } = factor
    else {
        return convert_table_factor(factor).map(FromItem::Table);
    };
    let reject =
        || ParseError::Unsupported(format!("table `{factor}` (only (<query>) [AS] <alias>)"));
    let Some(alias) = alias else {
        return Err(ParseError::Unsupported(format!(
            "subquery in FROM without an alias: `{factor}`"
        )));
    };
    if *lateral || sample.is_some() || !alias.columns.is_empty() || alias.at.is_some() {
        return Err(reject());
    }
    if factor.to_string() != format!("({subquery}) {alias}") {
        return Err(reject());
    }
    Ok(FromItem::Subquery {
        query: Box::new(convert_query(subquery)?),
        alias: ident(&alias.name),
    })
}

/// `name [[AS] alias]`. Table functions and column aliases are not
/// supported.
fn convert_table_factor(factor: &TableFactor) -> Result<TableRef> {
    let reject = || ParseError::Unsupported(format!("table `{factor}` (only <name> [AS <alias>])"));
    let TableFactor::Table { name, alias, .. } = factor else {
        return Err(reject());
    };
    let canonical = match alias {
        None => name.to_string(),
        Some(alias) if alias.columns.is_empty() && alias.at.is_none() => {
            format!("{name} {alias}")
        }
        Some(_) => return Err(reject()),
    };
    if factor.to_string() != canonical {
        return Err(reject());
    }
    Ok(TableRef {
        name: object_name(name)?,
        alias: alias.as_ref().map(|alias| ident(&alias.name)),
    })
}

fn convert_order_by(item: &sql::OrderByExpr) -> Result<OrderByItem> {
    let desc = match &item.options.sort {
        None | Some(OrderBySort::Asc) => false,
        Some(OrderBySort::Desc) => true,
        Some(OrderBySort::Using(_)) => {
            return Err(ParseError::Unsupported(format!("ORDER BY `{item}`")));
        }
    };
    if item.options.nulls_first.is_some() || item.with_fill.is_some() {
        return Err(ParseError::Unsupported(format!("ORDER BY `{item}`")));
    }
    Ok(OrderByItem {
        expr: convert_expr(&item.expr)?,
        desc,
    })
}

/// `LIMIT <n>` with a non-negative integer literal. OFFSET is not supported.
fn convert_limit(limit: &LimitClause) -> Result<u64> {
    let reject =
        || ParseError::Unsupported(format!("`{}` (only LIMIT <n>)", limit.to_string().trim()));
    let LimitClause::LimitOffset {
        limit: Some(n),
        offset: None,
        limit_by,
    } = limit
    else {
        return Err(reject());
    };
    let Some(digits) = number(n) else {
        return Err(reject());
    };
    if !limit_by.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(reject());
    }
    digits
        .parse()
        .map_err(|_| ParseError::IntegerOutOfRange(digits.to_owned()))
}

fn convert_update(stmt: &sql::Statement, update: &sql::Update) -> Result<Statement> {
    if !update.table.joins.is_empty() {
        return Err(ParseError::Unsupported("UPDATE with a JOIN".into()));
    }
    let mut canonical = format!(
        "UPDATE {} SET {}",
        update.table,
        join(update.assignments.iter())
    );
    if let Some(selection) = &update.selection {
        canonical += &format!(" WHERE {selection}");
    }
    ensure_canonical(
        stmt,
        &canonical,
        "UPDATE <table> SET <column> = <expr>, ... [WHERE <expr>]",
    )?;

    Ok(Statement::Update(Update {
        table: convert_table_factor(&update.table.relation)?,
        assignments: update
            .assignments
            .iter()
            .map(convert_assignment)
            .collect::<Result<_>>()?,
        selection: update.selection.as_ref().map(convert_expr).transpose()?,
    }))
}

fn convert_assignment(assignment: &Assignment) -> Result<(String, ast::Expr)> {
    let AssignmentTarget::ColumnName(column) = &assignment.target else {
        return Err(ParseError::Unsupported(format!(
            "assignment `{assignment}`"
        )));
    };
    Ok((object_name(column)?, convert_expr(&assignment.value)?))
}

fn convert_delete(stmt: &sql::Statement, delete: &sql::Delete) -> Result<Statement> {
    let FromTable::WithFromKeyword(from) = &delete.from else {
        return Err(unsupported(stmt));
    };
    let [table] = from.as_slice() else {
        return Err(ParseError::Unsupported(
            "DELETE from zero or several tables".into(),
        ));
    };
    if !table.joins.is_empty() {
        return Err(ParseError::Unsupported("DELETE with a JOIN".into()));
    }
    let mut canonical = format!("DELETE FROM {table}");
    if let Some(selection) = &delete.selection {
        canonical += &format!(" WHERE {selection}");
    }
    ensure_canonical(stmt, &canonical, "DELETE FROM <table> [WHERE <expr>]")?;

    Ok(Statement::Delete(Delete {
        table: convert_table_factor(&table.relation)?,
        selection: delete.selection.as_ref().map(convert_expr).transpose()?,
    }))
}

fn convert_expr(expr: &Expr) -> Result<ast::Expr> {
    match expr {
        Expr::Identifier(id) => Ok(ast::Expr::Column {
            table: None,
            name: ident(id),
        }),
        Expr::CompoundIdentifier(ids) => match ids.as_slice() {
            [table, column] => Ok(ast::Expr::Column {
                table: Some(ident(table)),
                name: ident(column),
            }),
            _ => Err(unsupported_expr(expr)),
        },
        Expr::Value(value) => {
            convert_literal(&value.value).unwrap_or_else(|| Err(unsupported_expr(expr)))
        }
        // The tree already encodes the grouping.
        Expr::Nested(inner) => convert_expr(inner),
        Expr::BinaryOp { left, op, right } => Ok(ast::Expr::Binary {
            op: convert_binary_op(op).ok_or_else(|| unsupported_expr(expr))?,
            left: Box::new(convert_expr(left)?),
            right: Box::new(convert_expr(right)?),
        }),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match number(inner) {
            // Parse the sign together with the digits so i64::MIN fits.
            Some(digits) => number_literal(&format!("-{digits}")).map(ast::Expr::Literal),
            None => Ok(ast::Expr::Unary {
                op: UnaryOp::Minus,
                expr: Box::new(convert_expr(inner)?),
            }),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Ok(ast::Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(convert_expr(inner)?),
        }),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => Ok(ast::Expr::IsNull {
            expr: Box::new(convert_expr(inner)?),
            negated: matches!(expr, Expr::IsNotNull(_)),
        }),
        Expr::Function(function) => convert_function(function),
        Expr::Array(array) => Ok(ast::Expr::Array(
            array.elem.iter().map(convert_expr).collect::<Result<_>>()?,
        )),
        Expr::Subquery(query) => Ok(ast::Expr::Subquery(Box::new(convert_query(query)?))),
        Expr::Exists { subquery, negated } => Ok(ast::Expr::Exists {
            subquery: Box::new(convert_query(subquery)?),
            negated: *negated,
        }),
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => Ok(ast::Expr::InSubquery {
            expr: Box::new(convert_expr(inner)?),
            subquery: Box::new(convert_query(subquery)?),
            negated: *negated,
        }),
        _ => Err(unsupported_expr(expr)),
    }
}

/// A literal, or `None` for a kind of literal we don't support.
fn convert_literal(value: &sql::Value) -> Option<Result<ast::Expr>> {
    let value = match value {
        sql::Value::Number(digits, _) => {
            return Some(number_literal(digits).map(ast::Expr::Literal));
        }
        sql::Value::SingleQuotedString(s) => Value::Varchar(s.clone()),
        sql::Value::Boolean(b) => Value::Bool(*b),
        sql::Value::Null => Value::Null,
        _ => return None,
    };
    Some(Ok(ast::Expr::Literal(value)))
}

/// The digits of a numeric literal.
fn number(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(value) => match &value.value {
            sql::Value::Number(digits, _) => Some(digits),
            _ => None,
        },
        _ => None,
    }
}

fn convert_binary_op(op: &BinaryOperator) -> Option<BinaryOp> {
    Some(match op {
        BinaryOperator::Plus => BinaryOp::Add,
        BinaryOperator::Minus => BinaryOp::Sub,
        BinaryOperator::Multiply => BinaryOp::Mul,
        BinaryOperator::Divide => BinaryOp::Div,
        BinaryOperator::Modulo => BinaryOp::Rem,
        BinaryOperator::Eq => BinaryOp::Eq,
        BinaryOperator::NotEq => BinaryOp::NotEq,
        BinaryOperator::Lt => BinaryOp::Lt,
        BinaryOperator::LtEq => BinaryOp::LtEq,
        BinaryOperator::Gt => BinaryOp::Gt,
        BinaryOperator::GtEq => BinaryOp::GtEq,
        BinaryOperator::And => BinaryOp::And,
        BinaryOperator::Or => BinaryOp::Or,
        _ => return None,
    })
}

/// `name(arg, ...)` or `name(*)` with plain positional arguments, and
/// optionally `OVER (...)`. DISTINCT, FILTER and the like render outside the
/// rebuilt form and are rejected.
fn convert_function(function: &sql::Function) -> Result<ast::Expr> {
    let reject = || ParseError::Unsupported(format!("function call `{function}`"));
    let FunctionArguments::List(list) = &function.args else {
        return Err(reject());
    };
    let star = matches!(
        list.args.as_slice(),
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
    );
    let args = if star {
        Vec::new()
    } else {
        list.args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
                _ => Err(reject()),
            })
            .collect::<Result<Vec<_>>>()?
    };
    let mut canonical = if star {
        format!("{}(*)", function.name)
    } else {
        format!("{}({})", function.name, join(args.iter()))
    };
    let over = match &function.over {
        None => None,
        Some(over @ WindowType::WindowSpec(spec)) => {
            canonical += &format!(" OVER {over}");
            Some(convert_window(spec)?)
        }
        Some(WindowType::NamedWindow(_)) => return Err(reject()),
    };
    if function.to_string() != canonical {
        return Err(reject());
    }
    Ok(ast::Expr::Function {
        name: object_name(&function.name)?,
        args: args.into_iter().map(convert_expr).collect::<Result<_>>()?,
        star,
        over,
    })
}

/// `OVER ([PARTITION BY ...] [ORDER BY ...])`. Named windows and frames
/// (`ROWS BETWEEN ...`) are not supported.
fn convert_window(spec: &sql::WindowSpec) -> Result<WindowSpec> {
    if spec.window_name.is_some() || spec.window_frame.is_some() {
        return Err(ParseError::Unsupported(format!("window `({spec})`")));
    }
    Ok(WindowSpec {
        partition_by: spec
            .partition_by
            .iter()
            .map(convert_expr)
            .collect::<Result<_>>()?,
        order_by: spec
            .order_by
            .iter()
            .map(convert_order_by)
            .collect::<Result<_>>()?,
    })
}

/// A numeric literal: DOUBLE if it has a decimal point or an exponent, as
/// `1.5` or `1e3`, and otherwise INT when it fits and BIGINT when not.
fn number_literal(text: &str) -> Result<Value> {
    if text.contains(['.', 'e', 'E']) {
        double(text)
    } else {
        integer(text)
    }
}

/// Fails, as PostgreSQL does, on a literal too large to be finite or so
/// small that it rounds to zero.
fn double(text: &str) -> Result<Value> {
    let Ok(v) = text.parse::<f64>() else {
        return Err(ParseError::Unsupported(format!("numeric literal {text}")));
    };
    let mantissa = text.split(['e', 'E']).next().unwrap_or(text);
    let underflow = v == 0.0 && mantissa.contains(|c: char| ('1'..='9').contains(&c));
    if !v.is_finite() || underflow {
        return Err(ParseError::DoubleOutOfRange(text.to_owned()));
    }
    Ok(Value::Double(v))
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

fn unsupported_expr(expr: &Expr) -> ParseError {
    ParseError::Unsupported(format!("expression `{expr}`"))
}

/// Rejects `stmt` unless it renders exactly as `canonical`, i.e. it has no
/// clauses beyond the ones the caller extracted.
fn ensure_canonical(stmt: &impl std::fmt::Display, canonical: &str, supported: &str) -> Result<()> {
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

/// A table, column or function name. Qualified names (`schema.table`) are
/// not supported.
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
    use crate::ast::SelectItem as Item;

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

    fn select(sql: &str) -> Select {
        match parse_one(sql).unwrap() {
            Statement::Select(select) => *select,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    /// The expression of `SELECT <expr>`.
    fn expr(sql: &str) -> ast::Expr {
        match select(&format!("SELECT {sql}")).projection.remove(0) {
            Item::Expr { expr, .. } => expr,
            other => panic!("expected an expression, got {other:?}"),
        }
    }

    fn col(name: &str) -> ast::Expr {
        ast::Expr::Column {
            table: None,
            name: name.into(),
        }
    }

    fn qcol(table: &str, name: &str) -> ast::Expr {
        ast::Expr::Column {
            table: Some(table.into()),
            name: name.into(),
        }
    }

    fn lit(value: impl Into<Value>) -> ast::Expr {
        ast::Expr::Literal(value.into())
    }

    fn bin(op: BinaryOp, left: ast::Expr, right: ast::Expr) -> ast::Expr {
        ast::Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn table(name: &str, alias: Option<&str>) -> FromItem {
        FromItem::Table(TableRef {
            name: name.into(),
            alias: alias.map(Into::into),
        })
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
                primary_key: vec![],
            })
        );
    }

    #[test]
    fn double_column_types() {
        for name in ["DOUBLE PRECISION", "DOUBLE", "FLOAT8", "FLOAT"] {
            let stmt = parse_one(&format!("CREATE TABLE t (x {name})")).unwrap();
            let Statement::CreateTable(create) = stmt else {
                panic!("not a CREATE TABLE: {stmt:?}");
            };
            assert_eq!(
                create.columns,
                vec![Column::new("x", DataType::Double, true)]
            );
        }
        assert_unsupported("CREATE TABLE t (x REAL)");
        assert_unsupported("CREATE TABLE t (x FLOAT(10))");
    }

    #[test]
    fn double_literals() {
        assert_eq!(expr("1.5"), lit(1.5));
        assert_eq!(expr("-1.5"), lit(-1.5));
        assert_eq!(expr(".5"), lit(0.5));
        assert_eq!(expr("2."), lit(2.0));
        assert_eq!(expr("1e3"), lit(1000.0));
        assert_eq!(expr("2.5E-3"), lit(0.0025));
        assert_eq!(expr("0.0"), lit(0.0));
        for sql in ["SELECT 1e309", "SELECT 1e-400"] {
            assert!(matches!(
                parse_one(sql),
                Err(ParseError::DoubleOutOfRange(_))
            ));
        }
    }

    fn primary_key(sql: &str) -> Vec<String> {
        match parse_one(sql).unwrap() {
            Statement::CreateTable(create) => create.primary_key,
            other => panic!("expected CREATE TABLE, got {other:?}"),
        }
    }

    #[test]
    fn create_table_with_primary_key() {
        assert_eq!(primary_key("CREATE TABLE t (id INT PRIMARY KEY)"), ["id"]);
        assert_eq!(
            primary_key("CREATE TABLE t (a INT NOT NULL PRIMARY KEY, b INT)"),
            ["a"]
        );
        assert_eq!(
            primary_key("CREATE TABLE t (a INT, B INT, PRIMARY KEY (b, a))"),
            ["b", "a"]
        );
        // Columns declared PRIMARY KEY keep their nullability here; the
        // binder makes them NOT NULL.
        let Statement::CreateTable(create) =
            parse_one("CREATE TABLE t (id INT PRIMARY KEY)").unwrap()
        else {
            panic!("expected CREATE TABLE");
        };
        assert!(create.columns[0].nullable);
    }

    #[test]
    fn create_table_rejects_more_than_one_primary_key() {
        for sql in [
            "CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)",
            "CREATE TABLE t (a INT PRIMARY KEY, b INT, PRIMARY KEY (b))",
            "CREATE TABLE t (a INT, PRIMARY KEY (a), PRIMARY KEY (a))",
        ] {
            assert!(
                matches!(parse_one(sql), Err(ParseError::MultiplePrimaryKeys(t)) if t == "t"),
                "{sql}"
            );
        }
    }

    #[test]
    fn create_index() {
        assert_eq!(
            parse_one("CREATE INDEX Users_Name ON Users (name, \"Id\")").unwrap(),
            Statement::CreateIndex(CreateIndex {
                name: "users_name".into(),
                table: "users".into(),
                columns: vec!["name".into(), "Id".into()],
            })
        );
    }

    #[test]
    fn create_index_rejects_unsupported_parts() {
        assert_unsupported("CREATE INDEX ON t (a)");
        assert_unsupported("CREATE UNIQUE INDEX i ON t (a)");
        assert_unsupported("CREATE INDEX IF NOT EXISTS i ON t (a)");
        assert_unsupported("CREATE INDEX CONCURRENTLY i ON t (a)");
        assert_unsupported("CREATE INDEX i ON t USING hash (a)");
        assert_unsupported("CREATE INDEX i ON t (a DESC)");
        assert_unsupported("CREATE INDEX i ON t (a NULLS FIRST)");
        assert_unsupported("CREATE INDEX i ON t (lower(a))");
        assert_unsupported("CREATE INDEX i ON t (a text_pattern_ops)");
        assert_unsupported("CREATE INDEX i ON t (a) INCLUDE (b)");
        assert_unsupported("CREATE INDEX i ON t (a) WHERE a > 0");
        assert_unsupported("CREATE INDEX i ON s.t (a)");
        assert_unsupported("CREATE INDEX s.i ON t (a)");
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
        assert_unsupported("CREATE TABLE t (id INT DEFAULT 1)");
        assert_unsupported("CREATE TABLE t (id INT UNIQUE)");
        assert_unsupported("CREATE TABLE t (id INT CONSTRAINT pk PRIMARY KEY)");
        assert_unsupported("CREATE TABLE t (id INT PRIMARY KEY DEFERRABLE)");
        assert_unsupported("CREATE TABLE t (id INT, CONSTRAINT pk PRIMARY KEY (id))");
        assert_unsupported("CREATE TABLE t (id INT, PRIMARY KEY (id DESC))");
        assert_unsupported("CREATE TABLE t (id INT, PRIMARY KEY (id) INCLUDE (id))");
        assert_unsupported("CREATE TABLE t (id INT, UNIQUE (id))");
        assert_unsupported("CREATE TABLE t (id INT, CHECK (id > 0))");
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
                columns: vec![],
                rows: vec![
                    vec![lit(1), lit("a"), lit(true), lit(Value::Null)],
                    vec![lit(-2147483649i64), lit("it's"), lit(false), lit(i64::MIN),],
                ],
            })
        );
    }

    #[test]
    fn insert_with_columns_and_expressions() {
        let stmt = parse_one("INSERT INTO t (B, a) VALUES (1 + 2, -x)").unwrap();
        assert_eq!(
            stmt,
            Statement::Insert(Insert {
                table: "t".into(),
                columns: vec!["b".into(), "a".into()],
                rows: vec![vec![
                    bin(BinaryOp::Add, lit(1), lit(2)),
                    ast::Expr::Unary {
                        op: UnaryOp::Minus,
                        expr: Box::new(col("x")),
                    },
                ]],
            })
        );
    }

    #[test]
    fn insert_rejects_unsupported_parts() {
        assert_unsupported("INSERT INTO t SELECT * FROM u");
        assert_unsupported("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING");
        assert_unsupported("INSERT INTO t VALUES (1) RETURNING *");
        assert_unsupported("INSERT INTO t VALUES ($1)");
        assert_unsupported("INSERT INTO t DEFAULT VALUES");
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
            select("select * from T"),
            Select {
                projection: vec![Item::Wildcard],
                from: Some(table("t", None)),
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
            }
        );
    }

    #[test]
    fn select_items_and_aliases() {
        let s = select("SELECT a, t.b AS x, c y, u.*, * FROM t");
        assert_eq!(
            s.projection,
            vec![
                Item::Expr {
                    expr: col("a"),
                    alias: None
                },
                Item::Expr {
                    expr: qcol("t", "b"),
                    alias: Some("x".into())
                },
                Item::Expr {
                    expr: col("c"),
                    alias: Some("y".into())
                },
                Item::QualifiedWildcard("u".into()),
                Item::Wildcard,
            ]
        );
    }

    #[test]
    fn select_without_from() {
        let s = select("SELECT 1");
        assert_eq!(s.from, None);
        assert_eq!(
            s.projection,
            vec![Item::Expr {
                expr: lit(1),
                alias: None
            }]
        );
    }

    #[test]
    fn select_where_order_by_limit() {
        let s = select("SELECT a FROM t WHERE a > 1 ORDER BY a DESC, b ASC, c LIMIT 10");
        assert_eq!(s.selection, Some(bin(BinaryOp::Gt, col("a"), lit(1))));
        assert_eq!(
            s.order_by,
            vec![
                OrderByItem {
                    expr: col("a"),
                    desc: true
                },
                OrderByItem {
                    expr: col("b"),
                    desc: false
                },
                OrderByItem {
                    expr: col("c"),
                    desc: false
                },
            ]
        );
        assert_eq!(s.limit, Some(10));
    }

    #[test]
    fn table_aliases() {
        assert_eq!(
            select("SELECT * FROM users AS u").from,
            Some(table("users", Some("u")))
        );
        assert_eq!(
            select("SELECT * FROM users u").from,
            Some(table("users", Some("u")))
        );
    }

    #[test]
    fn joins() {
        let s = select("SELECT * FROM a, b JOIN c ON b.x = c.x LEFT JOIN d ON TRUE CROSS JOIN e");
        let inner = FromItem::Join {
            kind: JoinKind::Inner,
            left: Box::new(table("b", None)),
            right: Box::new(table("c", None)),
            on: Some(bin(BinaryOp::Eq, qcol("b", "x"), qcol("c", "x"))),
        };
        let left = FromItem::Join {
            kind: JoinKind::Left,
            left: Box::new(inner),
            right: Box::new(table("d", None)),
            on: Some(lit(true)),
        };
        let cross = FromItem::Join {
            kind: JoinKind::Cross,
            left: Box::new(left),
            right: Box::new(table("e", None)),
            on: None,
        };
        assert_eq!(
            s.from,
            Some(FromItem::Join {
                kind: JoinKind::Cross,
                left: Box::new(table("a", None)),
                right: Box::new(cross),
                on: None,
            })
        );

        for sql in [
            "SELECT * FROM a INNER JOIN b ON TRUE",
            "SELECT * FROM a LEFT OUTER JOIN b ON TRUE",
        ] {
            assert!(matches!(select(sql).from, Some(FromItem::Join { .. })));
        }
    }

    #[test]
    fn expressions_and_precedence() {
        // * binds tighter than +, AND tighter than OR; parentheses regroup.
        assert_eq!(
            expr("1 + 2 * 3"),
            bin(BinaryOp::Add, lit(1), bin(BinaryOp::Mul, lit(2), lit(3)))
        );
        assert_eq!(
            expr("(1 + 2) * 3"),
            bin(BinaryOp::Mul, bin(BinaryOp::Add, lit(1), lit(2)), lit(3))
        );
        assert_eq!(
            expr("a OR b AND NOT c"),
            bin(
                BinaryOp::Or,
                col("a"),
                bin(
                    BinaryOp::And,
                    col("b"),
                    ast::Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(col("c"))
                    }
                )
            )
        );

        let ops = [
            ("-", BinaryOp::Sub),
            ("/", BinaryOp::Div),
            ("%", BinaryOp::Rem),
            ("=", BinaryOp::Eq),
            ("<>", BinaryOp::NotEq),
            ("!=", BinaryOp::NotEq),
            ("<", BinaryOp::Lt),
            ("<=", BinaryOp::LtEq),
            (">", BinaryOp::Gt),
            (">=", BinaryOp::GtEq),
        ];
        for (symbol, op) in ops {
            assert_eq!(expr(&format!("a {symbol} b")), bin(op, col("a"), col("b")));
        }
    }

    #[test]
    fn is_null_functions_and_arrays() {
        assert_eq!(
            expr("a IS NOT NULL"),
            ast::Expr::IsNull {
                expr: Box::new(col("a")),
                negated: true
            }
        );
        assert_eq!(
            expr("Upper(name)"),
            ast::Expr::Function {
                name: "upper".into(),
                args: vec![col("name")],
                star: false,
                over: None,
            }
        );
        assert_eq!(
            expr("ARRAY[1, a]"),
            ast::Expr::Array(vec![lit(1), col("a")])
        );
        assert_eq!(expr("-5"), lit(-5));
        assert_eq!(expr("'x'"), lit("x"));
    }

    #[test]
    fn select_rejects_unsupported_parts() {
        assert_unsupported("SELECT DISTINCT * FROM t");
        assert_unsupported("SELECT * FROM t GROUP BY ALL");
        assert_unsupported("SELECT a FROM t GROUP BY ROLLUP (a)");
        assert_unsupported("SELECT * FROM t ORDER BY a NULLS FIRST");
        assert_unsupported("SELECT * FROM t LIMIT 1 OFFSET 2");
        assert_unsupported("SELECT * FROM t LIMIT a");
        assert_unsupported("SELECT * FROM t AS x (a, b)");
        assert_unsupported("SELECT * FROM (SELECT 1)");
        assert_unsupported("SELECT * FROM (SELECT 1) AS x (a)");
        assert_unsupported("SELECT * FROM t, LATERAL (SELECT 1) AS x");
        assert_unsupported("SELECT * FROM s.t");
        assert_unsupported("SELECT * FROM t JOIN u USING (a)");
        assert_unsupported("SELECT * FROM t NATURAL JOIN u");
        assert_unsupported("SELECT * FROM t RIGHT JOIN u ON TRUE");
        assert_unsupported("SELECT * FROM t FULL OUTER JOIN u ON TRUE");
        assert_unsupported("WITH x AS (SELECT * FROM t) SELECT * FROM x");
        assert_unsupported("SELECT * FROM t UNION SELECT * FROM u");
        assert_unsupported("SELECT * FROM t FOR UPDATE");
    }

    #[test]
    fn rejects_unsupported_expressions() {
        assert_unsupported("SELECT $1");
        assert_unsupported("SELECT 'a' || 'b'");
        assert_unsupported("SELECT a.b.c FROM t");
        assert_unsupported("SELECT CAST(a AS INT) FROM t");
        assert_unsupported("SELECT a IN (1, 2) FROM t");
        assert_unsupported("SELECT count(DISTINCT a) FROM t");
        assert_unsupported("SELECT sum(a) FILTER (WHERE a > 1) FROM t");
        assert_unsupported("SELECT count(t.*) FROM t");
        assert_unsupported("SELECT row_number() OVER w FROM t WINDOW w AS ()");
        assert_unsupported("SELECT sum(a) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t");
        assert_unsupported("SELECT rank() OVER (ORDER BY a NULLS LAST) FROM t");
        assert_unsupported("SELECT (SELECT 1 UNION SELECT 2)");
        assert_unsupported("SELECT CURRENT_TIMESTAMP");
    }

    fn func(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
        ast::Expr::Function {
            name: name.into(),
            args,
            star: false,
            over: None,
        }
    }

    #[test]
    fn group_by_and_having() {
        let s = select("SELECT a, count(*) FROM t GROUP BY a, b + 1 HAVING sum(b) > 2");
        assert_eq!(
            s.group_by,
            vec![col("a"), bin(BinaryOp::Add, col("b"), lit(1))]
        );
        assert_eq!(
            s.having,
            Some(bin(BinaryOp::Gt, func("sum", vec![col("b")]), lit(2)))
        );
        assert_eq!(
            s.projection[1],
            Item::Expr {
                expr: ast::Expr::Function {
                    name: "count".into(),
                    args: vec![],
                    star: true,
                    over: None,
                },
                alias: None,
            }
        );
        assert_eq!(
            select("SELECT 1 FROM t HAVING TRUE").having,
            Some(lit(true))
        );
    }

    #[test]
    fn window_functions() {
        let over = |partition_by, order_by| {
            Some(WindowSpec {
                partition_by,
                order_by,
            })
        };
        assert_eq!(
            expr("rank() OVER (PARTITION BY a, b ORDER BY c DESC)"),
            ast::Expr::Function {
                name: "rank".into(),
                args: vec![],
                star: false,
                over: over(
                    vec![col("a"), col("b")],
                    vec![OrderByItem {
                        expr: col("c"),
                        desc: true,
                    }],
                ),
            }
        );
        assert_eq!(
            expr("count(*) OVER ()"),
            ast::Expr::Function {
                name: "count".into(),
                args: vec![],
                star: true,
                over: over(vec![], vec![]),
            }
        );
    }

    #[test]
    fn subqueries_in_expressions() {
        let inner = || Box::new(select("SELECT b FROM u WHERE b > 1 ORDER BY b LIMIT 3"));
        assert_eq!(
            expr("(SELECT b FROM u WHERE b > 1 ORDER BY b LIMIT 3)"),
            ast::Expr::Subquery(inner())
        );
        assert_eq!(
            expr("NOT EXISTS (SELECT b FROM u WHERE b > 1 ORDER BY b LIMIT 3)"),
            ast::Expr::Exists {
                subquery: inner(),
                negated: true,
            }
        );
        assert_eq!(
            expr("a IN (SELECT b FROM u WHERE b > 1 ORDER BY b LIMIT 3)"),
            ast::Expr::InSubquery {
                expr: Box::new(col("a")),
                subquery: inner(),
                negated: false,
            }
        );
        assert!(matches!(
            expr("a NOT IN (SELECT b FROM u)"),
            ast::Expr::InSubquery { negated: true, .. }
        ));
    }

    #[test]
    fn subquery_in_from() {
        let s = select("SELECT x.a FROM (SELECT a FROM t GROUP BY a) AS x JOIN u ON x.a = u.a");
        let Some(FromItem::Join { left, .. }) = s.from else {
            panic!("expected a join");
        };
        assert_eq!(
            *left,
            FromItem::Subquery {
                query: Box::new(select("SELECT a FROM t GROUP BY a")),
                alias: "x".into(),
            }
        );
        assert!(matches!(
            select("SELECT * FROM (SELECT 1) y").from,
            Some(FromItem::Subquery { alias, .. }) if alias == "y"
        ));
    }

    #[test]
    fn subqueries_are_checked_for_unsupported_parts() {
        assert_unsupported("SELECT * FROM (SELECT DISTINCT a FROM t) AS x");
        assert_unsupported("SELECT EXISTS (SELECT * FROM t LIMIT 1 OFFSET 1)");
        assert_unsupported("SELECT a IN (SELECT $1) FROM t");
    }

    #[test]
    fn update() {
        let stmt = parse_one("UPDATE t AS x SET a = a + 1, b = 'z' WHERE x.id = 3").unwrap();
        assert_eq!(
            stmt,
            Statement::Update(Update {
                table: TableRef {
                    name: "t".into(),
                    alias: Some("x".into())
                },
                assignments: vec![
                    ("a".into(), bin(BinaryOp::Add, col("a"), lit(1))),
                    ("b".into(), lit("z")),
                ],
                selection: Some(bin(BinaryOp::Eq, qcol("x", "id"), lit(3))),
            })
        );
    }

    #[test]
    fn update_rejects_unsupported_parts() {
        assert_unsupported("UPDATE t SET a = 1 FROM u");
        assert_unsupported("UPDATE t SET a = 1 RETURNING *");
        assert_unsupported("UPDATE t SET (a, b) = (1, 2)");
        assert_unsupported("UPDATE t SET x.a = 1");
    }

    #[test]
    fn delete() {
        assert_eq!(
            parse_one("DELETE FROM t").unwrap(),
            Statement::Delete(Delete {
                table: TableRef {
                    name: "t".into(),
                    alias: None
                },
                selection: None,
            })
        );
        assert_eq!(
            parse_one("DELETE FROM t WHERE a IS NULL").unwrap(),
            Statement::Delete(Delete {
                table: TableRef {
                    name: "t".into(),
                    alias: None
                },
                selection: Some(ast::Expr::IsNull {
                    expr: Box::new(col("a")),
                    negated: false
                }),
            })
        );
    }

    #[test]
    fn delete_rejects_unsupported_parts() {
        assert_unsupported("DELETE FROM t USING u WHERE t.a = u.a");
        assert_unsupported("DELETE FROM t RETURNING *");
        assert_unsupported("DELETE FROM t, u");
    }

    #[test]
    fn other_statements_are_unsupported() {
        assert_unsupported("DROP TABLE t");
        assert_unsupported("DROP INDEX i");
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
