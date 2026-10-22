//! Inputs and constructors shared by the expression tests.

use common::{Column, DataType, Schema, Tuple, Value};

use super::{AbstractExpr, ColumnValueExpr, ConstantExpr, Expr, UNNAMED_COLUMN};
use crate::Result;

/// The single input, or the left input of a join.
pub fn schema() -> Schema {
    Schema::new(vec![
        Column::new("a", DataType::Int, true),
        Column::new("b", DataType::BigInt, false),
        Column::new("s", DataType::Varchar(16), true),
        Column::new("f", DataType::Bool, true),
    ])
}

/// The right input of a join.
pub fn right_schema() -> Schema {
    Schema::new(vec![Column::new("c", DataType::Int, false)])
}

/// A row of [`schema`].
pub fn row(a: Option<i32>, b: i64, s: Option<&str>, f: Option<bool>) -> Vec<Value> {
    vec![
        a.map_or(Value::Null, Value::Int),
        Value::BigInt(b),
        s.map_or(Value::Null, Value::from),
        f.map_or(Value::Null, Value::Bool),
    ]
}

/// A row of [`schema`] where only the integers matter.
pub fn ints(a: Option<i32>, b: i64) -> Vec<Value> {
    row(a, b, Some("x"), Some(true))
}

pub fn side_column(tuple_idx: usize, schema: &Schema, idx: usize) -> AbstractExpr {
    ColumnValueExpr::new(tuple_idx, idx, schema.column(idx).unwrap().clone()).into()
}

/// Column `idx` of [`schema`].
pub fn column(idx: usize) -> AbstractExpr {
    side_column(0, &schema(), idx)
}

pub fn constant(value: Value, data_type: DataType) -> AbstractExpr {
    let nullable = value.is_null();
    ConstantExpr::new(value, Column::new(UNNAMED_COLUMN, data_type, nullable)).into()
}

pub fn int(v: i32) -> AbstractExpr {
    constant(Value::Int(v), DataType::Int)
}

pub fn boolean(v: Option<bool>) -> AbstractExpr {
    constant(v.map_or(Value::Null, Value::Bool), DataType::Bool)
}

pub fn eval(expr: &AbstractExpr, values: &[Value]) -> Result<Value> {
    let schema = schema();
    let tuple = Tuple::encode(&schema, values).unwrap();
    expr.evaluate(&tuple, &schema)
}

pub fn eval_join(expr: &AbstractExpr, left: &[Value], right: &[Value]) -> Result<Value> {
    let (ls, rs) = (schema(), right_schema());
    let l = Tuple::encode(&ls, left).unwrap();
    let r = Tuple::encode(&rs, right).unwrap();
    expr.evaluate_join(&l, &ls, &r, &rs)
}
