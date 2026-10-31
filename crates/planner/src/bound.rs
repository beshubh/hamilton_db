//! Statements after binding: every name resolved against the catalog and
//! every expression type-checked.
//!
//! A column reference is a position in the row its expression is evaluated
//! on, which depends on where the expression sits:
//!
//! - WHERE, the select list and ORDER BY of a SELECT see the whole FROM row:
//!   the columns of every table in FROM, left to right ([`BoundFrom::schema`]).
//!   So do GROUP BY and the arguments of aggregate calls.
//! - In an aggregating SELECT (one with GROUP BY, HAVING or an aggregate
//!   call), HAVING, the select list, ORDER BY and the parts of window calls
//!   see the aggregation row instead: the GROUP BY keys, then the results of
//!   [`BoundSelect::aggregates`].
//! - With window calls, the select list and ORDER BY see that row (FROM or
//!   aggregation) followed by the results of [`BoundSelect::windows`].
//! - A join's ON sees only that join's row: its left side's columns, then its
//!   right side's.
//! - UPDATE and DELETE see their one table's row.
//! - INSERT values see no row at all.
//! - A subquery sees only its own FROM row; correlated subqueries are not
//!   supported.

use catalog::TableOid;
use common::{Column, Schema, Value};
use parser::{BinaryOp, JoinKind, UnaryOp};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    CreateIndex(BoundCreateIndex),
    Insert(BoundInsert),
    Select(BoundSelect),
    Update(BoundUpdate),
    Delete(BoundDelete),
    Explain(Box<BoundStatement>),
    /// BEGIN, COMMIT or ROLLBACK. These change the session's transaction
    /// state rather than data, so they are run by the `ExecutionEngine`
    /// and never planned.
    Transaction(TransactionCommand),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionCommand {
    Begin,
    Commit,
    Rollback,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCreateTable {
    pub name: String,
    /// Primary key columns are NOT NULL.
    pub schema: Schema,
    /// The primary key columns, in key order. Empty for no primary key.
    pub primary_key: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCreateIndex {
    pub name: String,
    pub table: String,
    /// The key columns, in key order: distinct columns of the table.
    pub columns: Vec<String>,
}

/// A table resolved in the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundTable {
    pub oid: TableOid,
    pub name: String,
    /// The name the statement refers to it by: its alias, or else its name.
    pub alias: String,
    pub schema: Schema,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundInsert {
    pub table: BoundTable,
    /// One expression per column of the table, in table order, already of
    /// the column's type. Columns the INSERT did not list are NULL.
    pub rows: Vec<Vec<BoundExpr>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSelect {
    /// `None` for a SELECT without FROM, which produces one empty row.
    pub from: Option<BoundFrom>,
    /// A BOOL condition on the FROM row.
    pub selection: Option<BoundExpr>,
    /// Grouping keys on the FROM row.
    pub group_by: Vec<BoundExpr>,
    /// The distinct aggregate calls of HAVING, the select list and ORDER BY,
    /// whose results follow the GROUP BY keys in the aggregation row.
    pub aggregates: Vec<BoundAggregate>,
    /// A BOOL condition on the aggregation row.
    pub having: Option<BoundExpr>,
    /// The distinct window calls of the select list and ORDER BY, whose
    /// results follow the FROM or aggregation row.
    pub windows: Vec<BoundWindow>,
    /// The output columns, with `*` expanded.
    pub projection: Vec<BoundSelectItem>,
    /// Sort keys on the FROM row, so they can use columns the select list
    /// leaves out. Output names and positions (`ORDER BY 1`) are replaced
    /// with the select item's expression.
    pub order_by: Vec<BoundOrderBy>,
    pub limit: Option<u64>,
}

impl BoundSelect {
    /// Whether the rows are grouped: with GROUP BY, HAVING or an aggregate
    /// call. Without GROUP BY, all rows form one group.
    pub fn is_aggregate(&self) -> bool {
        !self.group_by.is_empty() || !self.aggregates.is_empty() || self.having.is_some()
    }

    pub fn output_schema(&self) -> Schema {
        Schema::new(
            self.projection
                .iter()
                .map(|item| {
                    let ty = &item.expr.return_type;
                    Column::new(item.name.clone(), ty.data_type, ty.nullable)
                })
                .collect(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSelectItem {
    pub expr: BoundExpr,
    /// The output column's name: the alias, else the column or function
    /// name, else `?column?`.
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundOrderBy {
    pub expr: BoundExpr,
    pub desc: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundUpdate {
    pub table: BoundTable,
    /// `(column index, new value)` in the order written, each value already
    /// of its column's type and evaluated on the old row.
    pub assignments: Vec<(usize, BoundExpr)>,
    pub selection: Option<BoundExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundDelete {
    pub table: BoundTable,
    pub selection: Option<BoundExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundFrom {
    Table(BoundTable),
    /// `(SELECT ...) alias`: the subquery's output columns, named `alias`.
    Subquery {
        select: Box<BoundSelect>,
        alias: String,
    },
    Join {
        kind: JoinKind,
        left: Box<BoundFrom>,
        right: Box<BoundFrom>,
        /// A BOOL condition on this join's row; `None` for a cross join.
        on: Option<BoundExpr>,
    },
}

impl BoundFrom {
    /// The columns this produces: every table's columns, left to right. The
    /// right side of a LEFT JOIN is nullable, since unmatched left rows pad
    /// it with NULLs.
    pub fn schema(&self) -> Schema {
        let mut columns = Vec::new();
        self.collect_columns(false, &mut columns);
        Schema::new(columns)
    }

    fn collect_columns(&self, null_padded: bool, out: &mut Vec<Column>) {
        match self {
            BoundFrom::Table(table) => out.extend(table.schema.columns().iter().map(|column| {
                let mut column = column.clone();
                column.nullable |= null_padded;
                column
            })),
            BoundFrom::Subquery { select, .. } => {
                out.extend(select.output_schema().columns().iter().map(|column| {
                    let mut column = column.clone();
                    column.nullable |= null_padded;
                    column
                }))
            }
            BoundFrom::Join {
                kind, left, right, ..
            } => {
                left.collect_columns(null_padded, out);
                right.collect_columns(null_padded || *kind == JoinKind::Left, out);
            }
        }
    }
}

/// A type-checked expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundExpr {
    pub kind: BoundExprKind,
    /// The type and nullability of the result. Its name is the column's for
    /// a column reference and `?column?` otherwise.
    pub return_type: Column,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundExprKind {
    /// Column `idx` of the row the expression is evaluated on (see the
    /// module docs). `table` is the alias it was resolved through, or empty
    /// for a column of an aggregation or window row.
    Column {
        table: String,
        idx: usize,
    },
    Constant(Value),
    /// Arithmetic on integers, a comparison, AND or OR. The operand types
    /// have been checked.
    Binary {
        op: BinaryOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<BoundExpr>,
    },
    /// `expr IS NULL`, or `IS NOT NULL` when `negated`. Never NULL itself.
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    Function {
        function: Function,
        args: Vec<BoundExpr>,
    },
    Array(Vec<BoundExpr>),
    /// Converts an INT to BIGINT or a BIGINT to INT, the type in
    /// `return_type`; narrowing fails at run time if the value does not
    /// fit. Inserted only where a value is stored into a column.
    Cast(Box<BoundExpr>),
    /// An aggregate call, with its argument on the FROM row. Exists only
    /// while a SELECT is being bound: it is replaced by a column of the
    /// aggregation row.
    Aggregate(Box<BoundAggregate>),
    /// A window call. Exists only while a SELECT is being bound: it is
    /// replaced by a column of the window row.
    Window(Box<BoundWindow>),
    /// `(SELECT ...)` with one output column: its value in the first row, or
    /// NULL if there are none. More than one row is a run-time error.
    Subquery(Box<BoundSelect>),
    /// `[NOT] EXISTS (SELECT ...)`. Never NULL.
    Exists {
        subquery: Box<BoundSelect>,
        negated: bool,
    },
    /// `expr [NOT] IN (SELECT ...)` with one output column, which `expr`
    /// is comparable with. NULL if `expr` is, or if there is no match and the
    /// subquery produces a NULL, as with `=` on each row.
    InSubquery {
        expr: Box<BoundExpr>,
        subquery: Box<BoundSelect>,
        negated: bool,
    },
}

impl BoundExpr {
    /// The expressions directly inside this one that are evaluated on the
    /// same row, so not those inside a subquery.
    pub fn children(&self) -> Vec<&BoundExpr> {
        match &self.kind {
            BoundExprKind::Column { .. }
            | BoundExprKind::Constant(_)
            | BoundExprKind::Subquery(_)
            | BoundExprKind::Exists { .. } => vec![],
            BoundExprKind::Binary { left, right, .. } => vec![left, right],
            BoundExprKind::Unary { expr, .. }
            | BoundExprKind::IsNull { expr, .. }
            | BoundExprKind::Cast(expr)
            | BoundExprKind::InSubquery { expr, .. } => vec![expr],
            BoundExprKind::Function { args, .. } => args.iter().collect(),
            BoundExprKind::Array(elements) => elements.iter().collect(),
            BoundExprKind::Aggregate(aggregate) => aggregate.arg.iter().collect(),
            BoundExprKind::Window(window) => window.children(),
        }
    }

    /// Whether this expression or one inside it (outside subqueries)
    /// satisfies `pred`.
    pub fn contains(&self, pred: &impl Fn(&BoundExprKind) -> bool) -> bool {
        pred(&self.kind)
            || self
                .children()
                .into_iter()
                .any(|child| child.contains(pred))
    }
}

/// An aggregate call such as `sum(x)` or `count(*)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundAggregate {
    pub function: AggregateFunction,
    /// `None` exactly for `count(*)`.
    pub arg: Option<BoundExpr>,
    /// `count` is BIGINT and never NULL; `sum` is BIGINT; `min` and `max`
    /// have the argument's type. All but `count` are NULL over no values.
    pub return_type: Column,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    /// `count(*)` counts rows; `count(x)` counts non-NULL values.
    Count,
    /// Of numbers.
    Sum,
    /// Of numbers, as a DOUBLE.
    Avg,
    /// Of integers, BOOLs or VARCHARs.
    Min,
    Max,
}

impl AggregateFunction {
    pub fn lookup(name: &str) -> Option<Self> {
        match name {
            "count" => Some(AggregateFunction::Count),
            "sum" => Some(AggregateFunction::Sum),
            "avg" => Some(AggregateFunction::Avg),
            "min" => Some(AggregateFunction::Min),
            "max" => Some(AggregateFunction::Max),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            AggregateFunction::Count => "count",
            AggregateFunction::Sum => "sum",
            AggregateFunction::Avg => "avg",
            AggregateFunction::Min => "min",
            AggregateFunction::Max => "max",
        }
    }
}

/// `function OVER (PARTITION BY partition_by ORDER BY order_by)`. The
/// argument, partition and sort keys are on the row before the windows: the
/// FROM row, or the aggregation row in an aggregating SELECT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundWindow {
    pub function: BoundWindowFunction,
    pub partition_by: Vec<BoundExpr>,
    pub order_by: Vec<BoundOrderBy>,
    pub return_type: Column,
}

impl BoundWindow {
    /// The argument, partition and sort keys.
    pub fn children(&self) -> Vec<&BoundExpr> {
        let arg = match &self.function {
            BoundWindowFunction::Aggregate(aggregate) => aggregate.arg.as_ref(),
            BoundWindowFunction::Rank | BoundWindowFunction::RowNumber => None,
        };
        arg.into_iter()
            .chain(&self.partition_by)
            .chain(self.order_by.iter().map(|item| &item.expr))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundWindowFunction {
    /// An aggregate over the partition, or with ORDER BY over the rows up to
    /// the current one and its peers.
    Aggregate(BoundAggregate),
    /// `rank()`: BIGINT; rows tied on the sort keys share a rank.
    Rank,
    /// `row_number()`: BIGINT, the row's position in its partition from 1.
    RowNumber,
}

impl BoundWindowFunction {
    /// The window-only functions; aggregates are looked up with
    /// [`AggregateFunction::lookup`].
    pub fn lookup(name: &str) -> Option<Self> {
        match name {
            "rank" => Some(BoundWindowFunction::Rank),
            "row_number" => Some(BoundWindowFunction::RowNumber),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            BoundWindowFunction::Aggregate(aggregate) => aggregate.function.name(),
            BoundWindowFunction::Rank => "rank",
            BoundWindowFunction::RowNumber => "row_number",
        }
    }
}

/// The functions the binder knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Function {
    /// `UPPER(varchar)`
    Upper,
    /// `LOWER(varchar)`
    Lower,
}

impl Function {
    pub fn lookup(name: &str) -> Option<Self> {
        match name {
            "upper" => Some(Function::Upper),
            "lower" => Some(Function::Lower),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Function::Upper => "upper",
            Function::Lower => "lower",
        }
    }
}
