//! Statements after binding: every name resolved against the catalog and
//! every expression type-checked.
//!
//! A column reference is a position in the row its expression is evaluated
//! on, which depends on where the expression sits:
//!
//! - WHERE, the select list and ORDER BY of a SELECT see the whole FROM row:
//!   the columns of every table in FROM, left to right ([`BoundFrom::schema`]).
//! - A join's ON sees only that join's row: its left side's columns, then its
//!   right side's.
//! - UPDATE and DELETE see their one table's row.
//! - INSERT values see no row at all.

use catalog::TableOid;
use common::{Column, Schema, Value};
use parser::{BinaryOp, JoinKind, UnaryOp};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundStatement {
    CreateTable(BoundCreateTable),
    Insert(BoundInsert),
    Select(BoundSelect),
    Update(BoundUpdate),
    Delete(BoundDelete),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCreateTable {
    pub name: String,
    pub schema: Schema,
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
    /// The output columns, with `*` expanded.
    pub projection: Vec<BoundSelectItem>,
    /// Sort keys on the FROM row, so they can use columns the select list
    /// leaves out. Output names and positions (`ORDER BY 1`) are replaced
    /// with the select item's expression.
    pub order_by: Vec<BoundOrderBy>,
    pub limit: Option<u64>,
}

impl BoundSelect {
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
    /// module docs). `table` is the alias it was resolved through.
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
