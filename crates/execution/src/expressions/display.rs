//! Expressions as SQL text, for EXPLAIN.
//!
//! A column is shown by its name in the schema of the input it reads, which
//! the caller passes in; without it, by the name in the column's own type,
//! which is `?column?` for a computed column such as an aggregate. Columns
//! of a join can be prefixed with the name of their input. An operand that
//! is itself an operation is parenthesized, so the text never depends on
//! operator precedence.

use std::fmt;

use common::{Schema, Value};

use super::{AbstractExpr, Expr, UnaryOp};

/// An input of an expression, which names the columns read from it.
#[derive(Clone, Copy, Debug)]
pub struct ExprInput<'a> {
    pub schema: &'a Schema,
    /// Shown before each column, as `left` in `left.id`.
    pub prefix: Option<&'a str>,
}

impl AbstractExpr {
    /// Shows the expression with the columns of input `tuple_idx` named by
    /// `inputs[tuple_idx]`.
    pub fn display_with<'a>(&'a self, inputs: &'a [ExprInput<'a>]) -> impl fmt::Display + 'a {
        ExprDisplay { expr: self, inputs }
    }
}

impl fmt::Display for AbstractExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display_with(&[]).fmt(f)
    }
}

struct ExprDisplay<'a> {
    expr: &'a AbstractExpr,
    inputs: &'a [ExprInput<'a>],
}

impl ExprDisplay<'_> {
    fn child(&self, idx: usize) -> Self {
        ExprDisplay {
            expr: &self.expr.children()[idx],
            inputs: self.inputs,
        }
    }

    /// Child `idx`, parenthesized if it is an operation.
    fn operand(&self, idx: usize, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let child = self.child(idx);
        let operation = matches!(
            child.expr,
            AbstractExpr::ArithmeticExpr(_)
                | AbstractExpr::ComparisonExpr(_)
                | AbstractExpr::LogicExpr(_)
                | AbstractExpr::UnaryExpr(_)
                | AbstractExpr::IsNullExpr(_)
        );
        if operation {
            write!(f, "({child})")
        } else {
            write!(f, "{child}")
        }
    }

    fn binary(&self, symbol: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.operand(0, f)?;
        write!(f, " {symbol} ")?;
        self.operand(1, f)
    }
}

impl fmt::Display for ExprDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.expr {
            AbstractExpr::ColumnValueExpr(column) => {
                let input = self.inputs.get(column.tuple_idx());
                if let Some(prefix) = input.and_then(|input| input.prefix) {
                    write!(f, "{prefix}.")?;
                }
                let named = input.and_then(|input| input.schema.column(column.col_idx()));
                f.write_str(&named.unwrap_or(column.return_type()).name)
            }
            AbstractExpr::ConstantExpr(constant) => write_literal(constant.value(), f),
            AbstractExpr::ArithmeticExpr(expr) => self.binary(expr.op().symbol(), f),
            AbstractExpr::ComparisonExpr(expr) => self.binary(expr.op().symbol(), f),
            AbstractExpr::LogicExpr(expr) => self.binary(expr.op().symbol(), f),
            AbstractExpr::UnaryExpr(expr) => {
                match expr.op() {
                    UnaryOp::Not => f.write_str("NOT ")?,
                    UnaryOp::Neg => f.write_str("-")?,
                }
                self.operand(0, f)
            }
            AbstractExpr::IsNullExpr(expr) => {
                self.operand(0, f)?;
                f.write_str(if expr.negated() {
                    " IS NOT NULL"
                } else {
                    " IS NULL"
                })
            }
            AbstractExpr::StringExpr(expr) => write!(f, "{}({})", expr.op().name(), self.child(0)),
            AbstractExpr::ArrayExpr(expr) => {
                f.write_str("ARRAY[")?;
                for idx in 0..expr.children().len() {
                    if idx > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", self.child(idx))?;
                }
                f.write_str("]")
            }
            AbstractExpr::CastExpr(expr) => {
                write!(
                    f,
                    "CAST({} AS {})",
                    self.child(0),
                    expr.return_type().data_type
                )
            }
        }
    }
}

/// `value` as a SQL literal: strings quoted, arrays as `ARRAY[...]`.
fn write_literal(value: &Value, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match value {
        Value::Varchar(s) => write!(f, "'{}'", s.replace('\'', "''")),
        Value::Array(values) => {
            f.write_str("ARRAY[")?;
            for (idx, value) in values.iter().enumerate() {
                if idx > 0 {
                    f.write_str(", ")?;
                }
                write_literal(value, f)?;
            }
            f.write_str("]")
        }
        _ => write!(f, "{value}"),
    }
}

#[cfg(test)]
mod tests {
    use common::{Column, DataType, Schema, Value};

    use super::super::test_util::{
        boolean, column, constant, int, right_schema, schema, side_column,
    };
    use crate::expressions::{
        AbstractExpr, ArithmeticExpr, ArithmeticOp, ArrayExpr, CastExpr, ComparisonExpr,
        ComparisonOp, ExprInput, IsNullExpr, LogicExpr, LogicOp, StringExpr, StringOp, UnaryExpr,
        UnaryOp,
    };

    fn arith(op: ArithmeticOp, l: AbstractExpr, r: AbstractExpr) -> AbstractExpr {
        ArithmeticExpr::new(op, l, r).unwrap().into()
    }

    fn cmp(op: ComparisonOp, l: AbstractExpr, r: AbstractExpr) -> AbstractExpr {
        ComparisonExpr::new(op, l, r).unwrap().into()
    }

    #[test]
    fn columns_and_literals() {
        assert_eq!(column(0).to_string(), "a");
        assert_eq!(int(-3).to_string(), "-3");
        assert_eq!(boolean(None).to_string(), "NULL");
        let quoted = constant(Value::from("it's"), DataType::Varchar(8));
        assert_eq!(quoted.to_string(), "'it''s'");
        let array = Value::Array(vec![Value::Int(1), Value::from("x")]);
        assert_eq!(
            constant(array, DataType::Array).to_string(),
            "ARRAY[1, 'x']"
        );
    }

    #[test]
    fn nested_operations_are_parenthesized() {
        let sum = arith(ArithmeticOp::Add, column(0), int(1));
        let product = arith(ArithmeticOp::Mul, sum, column(1));
        let gt = cmp(ComparisonOp::Gt, product, int(10));
        let is_null = IsNullExpr::new(column(2), true).into();
        let and: AbstractExpr = LogicExpr::new(LogicOp::And, gt, is_null).unwrap().into();
        assert_eq!(and.to_string(), "(((a + 1) * b) > 10) AND (s IS NOT NULL)");
        let not: AbstractExpr = UnaryExpr::new(UnaryOp::Not, and).unwrap().into();
        assert!(not.to_string().starts_with("NOT ((("));
        let neg: AbstractExpr = UnaryExpr::new(UnaryOp::Neg, column(0)).unwrap().into();
        assert_eq!(neg.to_string(), "-a");
    }

    #[test]
    fn functions() {
        let lower: AbstractExpr = StringExpr::new(StringOp::Lower, column(2)).unwrap().into();
        assert_eq!(lower.to_string(), "LOWER(s)");
        let cast: AbstractExpr = CastExpr::new(column(0), DataType::Double).unwrap().into();
        assert_eq!(cast.to_string(), "CAST(a AS DOUBLE)");
        let array: AbstractExpr = ArrayExpr::new(vec![column(0), int(2)]).into();
        assert_eq!(array.to_string(), "ARRAY[a, 2]");
    }

    #[test]
    fn columns_are_named_by_their_input() {
        let renamed = Schema::new(vec![Column::new("x", DataType::Int, true)]);
        let input = [ExprInput {
            schema: &renamed,
            prefix: None,
        }];
        assert_eq!(column(0).display_with(&input).to_string(), "x");
    }

    #[test]
    fn join_columns_name_their_side() {
        let eq = cmp(
            ComparisonOp::Eq,
            column(0),
            side_column(1, &right_schema(), 0),
        );
        assert_eq!(eq.to_string(), "a = c");
        let (left, right) = (schema(), right_schema());
        let inputs = [
            ExprInput {
                schema: &left,
                prefix: Some("left"),
            },
            ExprInput {
                schema: &right,
                prefix: Some("right"),
            },
        ];
        assert_eq!(eq.display_with(&inputs).to_string(), "left.a = right.c");
    }
}
