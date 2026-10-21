use common::{Column, DataType, Schema, Tuple, Value, ValueError};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN, type_name};
use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl ArithmeticOp {
    pub fn symbol(self) -> &'static str {
        match self {
            ArithmeticOp::Add => "+",
            ArithmeticOp::Sub => "-",
            ArithmeticOp::Mul => "*",
            ArithmeticOp::Div => "/",
            ArithmeticOp::Rem => "%",
        }
    }

    fn apply(self, left: &Value, right: &Value) -> Result<Value> {
        let out = match self {
            ArithmeticOp::Add => left.checked_add(right),
            ArithmeticOp::Sub => left.checked_sub(right),
            ArithmeticOp::Mul => left.checked_mul(right),
            ArithmeticOp::Div => left.checked_div(right),
            ArithmeticOp::Rem => left.checked_rem(right),
        };
        Ok(out?)
    }
}

/// `left <op> right` over integers.
///
/// The result is INT if both sides are INT and BIGINT otherwise, and is NULL
/// if either side is NULL (see [`Value`]).
#[derive(Clone, Debug)]
pub struct ArithmeticExpr {
    op: ArithmeticOp,
    /// Always `[left, right]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl ArithmeticExpr {
    /// Fails if either side is not an integer type.
    pub fn new(op: ArithmeticOp, left: AbstractExpr, right: AbstractExpr) -> Result<Self> {
        let (l, r) = (left.return_type(), right.return_type());
        let data_type = match (l.data_type, r.data_type) {
            (DataType::Int, DataType::Int) => DataType::Int,
            (DataType::Int | DataType::BigInt, DataType::Int | DataType::BigInt) => {
                DataType::BigInt
            }
            _ => {
                return Err(ValueError::InvalidOperands {
                    op: op.symbol(),
                    left: type_name(l.data_type),
                    right: type_name(r.data_type),
                }
                .into());
            }
        };
        let return_type = Column::new(UNNAMED_COLUMN, data_type, l.nullable || r.nullable);
        Ok(Self {
            op,
            children: vec![left, right],
            return_type,
        })
    }

    pub fn op(&self) -> ArithmeticOp {
        self.op
    }
}

impl Expr for ArithmeticExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        let left = self.children[0].evaluate(tuple, schema)?;
        let right = self.children[1].evaluate(tuple, schema)?;
        self.op.apply(&left, &right)
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        let l = self.children[0].evaluate_join(left, left_schema, right, right_schema)?;
        let r = self.children[1].evaluate_join(left, left_schema, right, right_schema)?;
        self.op.apply(&l, &r)
    }

    fn children(&self) -> &[AbstractExpr] {
        &self.children
    }

    fn return_type(&self) -> &Column {
        &self.return_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionError;
    use crate::expressions::test_util::*;

    fn arith(op: ArithmeticOp, left: AbstractExpr, right: AbstractExpr) -> AbstractExpr {
        ArithmeticExpr::new(op, left, right).unwrap().into()
    }

    #[test]
    fn evaluates_each_op() {
        let cases = [
            (ArithmeticOp::Add, 10),
            (ArithmeticOp::Sub, 4),
            (ArithmeticOp::Mul, 21),
            (ArithmeticOp::Div, 2),
            (ArithmeticOp::Rem, 1),
        ];
        for (op, want) in cases {
            let expr = arith(op, column(0), int(3));
            assert_eq!(eval(&expr, &ints(Some(7), 0)).unwrap(), Value::Int(want));
        }
    }

    #[test]
    fn nested_expressions() {
        // (a + 1) * b
        let expr = arith(
            ArithmeticOp::Mul,
            arith(ArithmeticOp::Add, column(0), int(1)),
            column(1),
        );
        assert_eq!(eval(&expr, &ints(Some(4), 10)).unwrap(), Value::BigInt(50));
        assert_eq!(expr.children().len(), 2);
    }

    #[test]
    fn return_type_widens_and_tracks_nullability() {
        let int_int = arith(ArithmeticOp::Add, int(1), int(2));
        assert_eq!(int_int.return_type().data_type, DataType::Int);
        assert!(!int_int.return_type().nullable);

        let int_bigint = arith(ArithmeticOp::Add, column(0), column(1));
        assert_eq!(int_bigint.return_type().data_type, DataType::BigInt);
        assert!(int_bigint.return_type().nullable);
    }

    #[test]
    fn null_operand_gives_null() {
        let expr = arith(ArithmeticOp::Add, column(0), int(1));
        assert_eq!(eval(&expr, &ints(None, 0)).unwrap(), Value::Null);
    }

    #[test]
    fn rejects_non_integer_operands() {
        let err = ArithmeticExpr::new(ArithmeticOp::Add, column(2), int(1)).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Value(ValueError::InvalidOperands {
                op: "+",
                left: "VARCHAR",
                right: "INT",
            })
        ));
    }

    #[test]
    fn runtime_errors_propagate() {
        let div = arith(ArithmeticOp::Div, column(0), int(0));
        assert!(matches!(
            eval(&div, &ints(Some(1), 0)),
            Err(ExecutionError::Value(ValueError::DivisionByZero))
        ));

        let overflow = arith(ArithmeticOp::Add, column(0), int(1));
        assert!(matches!(
            eval(&overflow, &ints(Some(i32::MAX), 0)),
            Err(ExecutionError::Value(ValueError::Overflow))
        ));
    }

    #[test]
    fn join_reads_columns_from_each_side() {
        // left.b - right.c
        let expr = arith(
            ArithmeticOp::Sub,
            side_column(0, &schema(), 1),
            side_column(1, &right_schema(), 0),
        );
        let got = eval_join(&expr, &ints(Some(1), 10), &[Value::Int(3)]).unwrap();
        assert_eq!(got, Value::BigInt(7));
    }
}
