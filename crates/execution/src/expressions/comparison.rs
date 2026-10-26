use common::{Column, DataType, Schema, Tuple, Value, ValueError, ValueRef};

use super::{
    AbstractExpr, Expr, UNNAMED_COLUMN, evaluate_in_place, evaluate_join_in_place, is_number,
    type_name,
};
use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl ComparisonOp {
    pub fn symbol(self) -> &'static str {
        match self {
            ComparisonOp::Eq => "=",
            ComparisonOp::NotEq => "<>",
            ComparisonOp::Lt => "<",
            ComparisonOp::LtEq => "<=",
            ComparisonOp::Gt => ">",
            ComparisonOp::GtEq => ">=",
        }
    }

    fn apply(self, left: ValueRef<'_>, right: ValueRef<'_>) -> Result<Value> {
        let Some(ordering) = left.compare(right)? else {
            return Ok(Value::Null);
        };
        Ok(Value::Bool(match self {
            ComparisonOp::Eq => ordering.is_eq(),
            ComparisonOp::NotEq => ordering.is_ne(),
            ComparisonOp::Lt => ordering.is_lt(),
            ComparisonOp::LtEq => ordering.is_le(),
            ComparisonOp::Gt => ordering.is_gt(),
            ComparisonOp::GtEq => ordering.is_ge(),
        }))
    }
}

/// `left <op> right` between two numbers (of any numeric type), two BOOLs
/// or two VARCHARs. The result is BOOL, and NULL if either side is NULL.
/// Columns and constants are compared in place, without copying them.
#[derive(Clone, Debug)]
pub struct ComparisonExpr {
    op: ComparisonOp,
    /// Always `[left, right]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl ComparisonExpr {
    /// Fails if the two sides are not comparable.
    pub fn new(op: ComparisonOp, left: AbstractExpr, right: AbstractExpr) -> Result<Self> {
        let (l, r) = (left.return_type(), right.return_type());
        match (l.data_type, r.data_type) {
            (l, r) if is_number(l) && is_number(r) => {}
            (DataType::Bool, DataType::Bool) | (DataType::Varchar(_), DataType::Varchar(_)) => {}
            _ => {
                return Err(ValueError::InvalidOperands {
                    op: op.symbol(),
                    left: type_name(l.data_type),
                    right: type_name(r.data_type),
                }
                .into());
            }
        }
        let return_type = Column::new(UNNAMED_COLUMN, DataType::Bool, l.nullable || r.nullable);
        Ok(Self {
            op,
            children: vec![left, right],
            return_type,
        })
    }

    pub fn op(&self) -> ComparisonOp {
        self.op
    }
}

impl Expr for ComparisonExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        let left = evaluate_in_place(&self.children[0], tuple, schema)?;
        let right = evaluate_in_place(&self.children[1], tuple, schema)?;
        self.op.apply(left.as_value_ref(), right.as_value_ref())
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        let l = evaluate_join_in_place(&self.children[0], left, left_schema, right, right_schema)?;
        let r = evaluate_join_in_place(&self.children[1], left, left_schema, right, right_schema)?;
        self.op.apply(l.as_value_ref(), r.as_value_ref())
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

    fn cmp(op: ComparisonOp, left: AbstractExpr, right: AbstractExpr) -> AbstractExpr {
        ComparisonExpr::new(op, left, right).unwrap().into()
    }

    #[test]
    fn compares_integers_across_widths() {
        // a < b with a INT and b BIGINT
        let expr = cmp(ComparisonOp::Lt, column(0), column(1));
        assert_eq!(eval(&expr, &ints(Some(1), 2)).unwrap(), Value::Bool(true));
        assert_eq!(eval(&expr, &ints(Some(3), 2)).unwrap(), Value::Bool(false));
        assert_eq!(eval(&expr, &ints(None, 2)).unwrap(), Value::Null);
        assert!(expr.return_type().nullable);
    }

    #[test]
    fn every_operator() {
        let cases = [
            (ComparisonOp::Eq, [false, true, false]),
            (ComparisonOp::NotEq, [true, false, true]),
            (ComparisonOp::Lt, [true, false, false]),
            (ComparisonOp::LtEq, [true, true, false]),
            (ComparisonOp::Gt, [false, false, true]),
            (ComparisonOp::GtEq, [false, true, true]),
        ];
        for (op, expected) in cases {
            for (right, want) in [2, 1, 0].into_iter().zip(expected) {
                let got = eval(&cmp(op, int(1), int(right)), &ints(None, 0)).unwrap();
                assert_eq!(got, Value::Bool(want), "1 {} {right}", op.symbol());
            }
        }
    }

    #[test]
    fn compares_strings() {
        let expr = cmp(ComparisonOp::Eq, column(2), column(2));
        let got = eval(&expr, &row(None, 0, Some("x"), None)).unwrap();
        assert_eq!(got, Value::Bool(true));
    }

    #[test]
    fn rejects_mixed_types() {
        let err = ComparisonExpr::new(ComparisonOp::Eq, int(1), boolean(Some(true))).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Value(ValueError::InvalidOperands {
                op: "=",
                left: "INT",
                right: "BOOL",
            })
        ));
    }

    #[test]
    fn join_compares_both_sides() {
        // left.a = right.c
        let expr = cmp(
            ComparisonOp::Eq,
            side_column(0, &schema(), 0),
            side_column(1, &right_schema(), 0),
        );
        let got = eval_join(&expr, &ints(Some(3), 0), &[Value::Int(3)]).unwrap();
        assert_eq!(got, Value::Bool(true));
    }
}
