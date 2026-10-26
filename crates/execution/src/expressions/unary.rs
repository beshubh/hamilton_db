use common::{Column, DataType, Schema, Tuple, Value, ValueError};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN, is_number, type_name};
use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    /// `NOT` on a BOOL.
    Not,
    /// `-` on a number.
    Neg,
}

impl UnaryOp {
    pub fn symbol(self) -> &'static str {
        match self {
            UnaryOp::Not => "NOT",
            UnaryOp::Neg => "-",
        }
    }

    fn apply(self, value: &Value) -> Result<Value> {
        let out = match self {
            UnaryOp::Not => value.logical_not(),
            UnaryOp::Neg => value.checked_neg(),
        };
        Ok(out?)
    }
}

/// `NOT arg` or `-arg`. The result has the argument's type, and is NULL if
/// the argument is.
#[derive(Clone, Debug)]
pub struct UnaryExpr {
    op: UnaryOp,
    /// Always `[arg]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl UnaryExpr {
    /// Fails if `arg` is not BOOL for NOT, or not a number for `-`.
    pub fn new(op: UnaryOp, arg: AbstractExpr) -> Result<Self> {
        let ty = arg.return_type();
        let accepted = match op {
            UnaryOp::Not => ty.data_type == DataType::Bool,
            UnaryOp::Neg => is_number(ty.data_type),
        };
        if !accepted {
            return Err(ValueError::InvalidOperand {
                op: op.symbol(),
                operand: type_name(ty.data_type),
            }
            .into());
        }
        let return_type = Column::new(UNNAMED_COLUMN, ty.data_type, ty.nullable);
        Ok(Self {
            op,
            children: vec![arg],
            return_type,
        })
    }

    pub fn op(&self) -> UnaryOp {
        self.op
    }
}

impl Expr for UnaryExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        self.op.apply(&self.children[0].evaluate(tuple, schema)?)
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        let value = self.children[0].evaluate_join(left, left_schema, right, right_schema)?;
        self.op.apply(&value)
    }

    fn children(&self) -> &[AbstractExpr] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<AbstractExpr>) -> AbstractExpr {
        debug_assert_eq!(children.len(), self.children.len());
        Self {
            children,
            ..self.clone()
        }
        .into()
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

    fn unary(op: UnaryOp, arg: AbstractExpr) -> AbstractExpr {
        UnaryExpr::new(op, arg).unwrap().into()
    }

    #[test]
    fn not() {
        let expr = unary(UnaryOp::Not, column(3));
        let got = eval(&expr, &row(None, 0, None, Some(true))).unwrap();
        assert_eq!(got, Value::Bool(false));
        assert_eq!(eval(&expr, &row(None, 0, None, None)).unwrap(), Value::Null);
    }

    #[test]
    fn neg_keeps_the_width() {
        let expr = unary(UnaryOp::Neg, column(1));
        assert_eq!(eval(&expr, &ints(None, 5)).unwrap(), Value::BigInt(-5));
        assert_eq!(expr.return_type().data_type, DataType::BigInt);
    }

    #[test]
    fn neg_overflows() {
        let expr = unary(UnaryOp::Neg, column(0));
        assert!(matches!(
            eval(&expr, &ints(Some(i32::MIN), 0)),
            Err(ExecutionError::Value(ValueError::Overflow))
        ));
    }

    #[test]
    fn rejects_wrong_operand() {
        let err = UnaryExpr::new(UnaryOp::Not, int(1)).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Value(ValueError::InvalidOperand {
                op: "NOT",
                operand: "INT",
            })
        ));
    }
}
