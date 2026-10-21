//! The statements hamilton_db understands.
//!
//! This is deliberately much smaller than sqlparser's AST: only what the
//! current milestone supports, using the database's own [`Column`] and
//! [`Value`] types. Names are not resolved against the catalog here; that is
//! the binder's job.

use common::{Column, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Statement {
    CreateTable(CreateTable),
    Insert(Insert),
    Select(Select),
    Update(Update),
    Delete(Delete),
}

/// `CREATE TABLE name (col type [NULL | NOT NULL], ...)`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTable {
    pub name: String,
    /// Columns are nullable unless declared `NOT NULL`.
    pub columns: Vec<Column>,
}

/// `INSERT INTO table [(col, ...)] VALUES (...), (...)`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Insert {
    pub table: String,
    /// The columns each row gives values for, in row order. Empty means
    /// every column of the table, in table order.
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Expr>>,
}

/// `SELECT items [FROM from] [WHERE selection] [ORDER BY ...] [LIMIT n]`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    /// `None` for a SELECT without FROM, such as `SELECT 1`.
    pub from: Option<FromItem>,
    pub selection: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
}

/// `UPDATE table [[AS] alias] SET col = expr, ... [WHERE selection]`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Update {
    pub table: TableRef,
    /// `(column, value)` in the order written.
    pub assignments: Vec<(String, Expr)>,
    pub selection: Option<Expr>,
}

/// `DELETE FROM table [[AS] alias] [WHERE selection]`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delete {
    pub table: TableRef,
    pub selection: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectItem {
    /// `*`: every column of every table in FROM.
    Wildcard,
    /// `t.*`: every column of the table named or aliased `t`.
    QualifiedWildcard(String),
    /// `expr [AS alias]`
    Expr { expr: Expr, alias: Option<String> },
}

/// A table named in FROM, UPDATE or DELETE.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

/// The FROM clause as a tree. `FROM a, b JOIN c ON ...` is
/// `Cross(a, Inner(b, c))`: a comma binds looser than JOIN.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromItem {
    Table(TableRef),
    Join {
        kind: JoinKind,
        left: Box<FromItem>,
        right: Box<FromItem>,
        /// The ON condition; `None` exactly when `kind` is `Cross`.
        on: Option<Expr>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    /// `CROSS JOIN` or a comma.
    Cross,
    /// `[INNER] JOIN ... ON`
    Inner,
    /// `LEFT [OUTER] JOIN ... ON`
    Left,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderByItem {
    pub expr: Expr,
    /// `DESC`; ascending otherwise.
    pub desc: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    /// `name` or `table.name`.
    Column {
        table: Option<String>,
        name: String,
    },
    /// A literal. Integers are INT when they fit and BIGINT otherwise.
    Literal(Value),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    /// `expr IS NULL`, or `expr IS NOT NULL` when `negated`.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `name(args)`. Which functions exist is up to the binder.
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// `ARRAY[elements]`
    Array(Vec<Expr>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

impl BinaryOp {
    pub fn symbol(self) -> &'static str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Rem => "%",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    /// `-expr`. A minus directly on an integer literal is folded into the
    /// literal instead.
    Minus,
}

impl UnaryOp {
    pub fn symbol(self) -> &'static str {
        match self {
            UnaryOp::Not => "NOT",
            UnaryOp::Minus => "-",
        }
    }
}
