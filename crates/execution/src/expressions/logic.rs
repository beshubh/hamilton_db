use common::{Column, DataType, Schema, Tuple, Value, ValueError};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN, type_name};
use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicOp {
    And,
    Or,
}

impl LogicOp {
    pub fn symbol(self) -> &'static str {
        match self {
            LogicOp::And => "AND",
            LogicOp::Or => "OR",
        }
    }

    /// The left value that decides the result without the right one: false
    /// for AND, true for OR.
    fn deciding_value(self) -> Value {
        Value::Bool(self == LogicOp::Or)
    }

    fn apply(self, left: &Value, right: &Value) -> Result<Value> {
        let out = match self {
            LogicOp::And => left.logical_and(right),
            LogicOp::Or => left.logical_or(right),
        };
        Ok(out?)
    }
}

/// `left AND right` or `left OR right` with SQL three-valued logic: false
/// decides AND and true decides OR even when the other side is NULL.
///
/// The right side is not evaluated when the left decides the result, so
/// `b <> 0 AND a / b > 1` does not divide by zero.
#[derive(Clone, Debug)]
pub struct LogicExpr {
    op: LogicOp,
    /// Always `[left, right]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl LogicExpr {
    /// Fails if either side is not BOOL.
    pub fn new(op: LogicOp, left: AbstractExpr, right: AbstractExpr) -> Result<Self> {
        let (l, r) = (left.return_type(), right.return_type());
        if l.data_type != DataType::Bool || r.data_type != DataType::Bool {
            return Err(ValueError::InvalidOperands {
                op: op.symbol(),
                left: type_name(l.data_type),
                right: type_name(r.data_type),
            }
            .into());
        }
        let return_type = Column::new(UNNAMED_COLUMN, DataType::Bool, l.nullable || r.nullable);
        Ok(Self {
            op,
            children: vec![left, right],
            return_type,
        })
    }

    pub fn op(&self) -> LogicOp {
        self.op
    }

    /// Combines `left` with the right side, which `right` evaluates only if
    /// it is needed.
    fn combine(&self, left: Value, right: impl FnOnce() -> Result<Value>) -> Result<Value> {
        if left == self.op.deciding_value() {
            return Ok(left);
        }
        self.op.apply(&left, &right()?)
    }
}

impl Expr for LogicExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        let left = self.children[0].evaluate(tuple, schema)?;
        self.combine(left, || self.children[1].evaluate(tuple, schema))
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        let l = self.children[0].evaluate_join(left, left_schema, right, right_schema)?;
        self.combine(l, || {
            self.children[1].evaluate_join(left, left_schema, right, right_schema)
        })
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
    use crate::expressions::ColumnValueExpr;
    use crate::expressions::test_util::*;

    fn logic(op: LogicOp, left: AbstractExpr, right: AbstractExpr) -> AbstractExpr {
        LogicExpr::new(op, left, right).unwrap().into()
    }

    #[test]
    fn three_valued_truth_tables() {
        let (t, f, n) = (Some(true), Some(false), None);
        let cases = [
            (t, t, t, t),
            (t, f, f, t),
            (f, f, f, f),
            (t, n, n, t),
            (f, n, f, n),
            (n, t, n, t),
            (n, f, f, n),
            (n, n, n, n),
        ];
        for (l, r, and, or) in cases {
            let input = ints(None, 0);
            let got_and = eval(&logic(LogicOp::And, boolean(l), boolean(r)), &input).unwrap();
            let got_or = eval(&logic(LogicOp::Or, boolean(l), boolean(r)), &input).unwrap();
            assert_eq!(
                got_and,
                and.map_or(Value::Null, Value::Bool),
                "{l:?} AND {r:?}"
            );
            assert_eq!(
                got_or,
                or.map_or(Value::Null, Value::Bool),
                "{l:?} OR {r:?}"
            );
        }
    }

    #[test]
    fn reads_columns() {
        // f AND true
        let expr = logic(LogicOp::And, column(3), boolean(Some(true)));
        let got = eval(&expr, &row(None, 0, None, Some(false))).unwrap();
        assert_eq!(got, Value::Bool(false));
        assert!(expr.return_type().nullable);
    }

    #[test]
    fn deciding_left_skips_the_right_side() {
        // A right-input column fails when evaluated outside a join.
        let failing = || -> AbstractExpr {
            ColumnValueExpr::new(1, 0, Column::new("x", DataType::Bool, false)).into()
        };
        let input = ints(None, 0);

        let and = logic(LogicOp::And, boolean(Some(false)), failing());
        assert_eq!(eval(&and, &input).unwrap(), Value::Bool(false));
        let or = logic(LogicOp::Or, boolean(Some(true)), failing());
        assert_eq!(eval(&or, &input).unwrap(), Value::Bool(true));

        // NULL decides neither, so the right side runs.
        let and = logic(LogicOp::And, boolean(None), failing());
        assert!(matches!(
            eval(&and, &input),
            Err(ExecutionError::TupleIndexOutOfRange { .. })
        ));
    }

    #[test]
    fn rejects_non_bool_operands() {
        let err = LogicExpr::new(LogicOp::Or, int(1), boolean(Some(true))).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Value(ValueError::InvalidOperands {
                op: "OR",
                left: "INT",
                right: "BOOL",
            })
        ));
    }

    #[test]
    fn join_reads_both_sides() {
        let left_flag = side_column(0, &schema(), 3);
        let right_flag = boolean(None);
        let expr = logic(LogicOp::Or, left_flag, right_flag);
        let got = eval_join(&expr, &row(None, 0, None, Some(true)), &[Value::Int(1)]).unwrap();
        assert_eq!(got, Value::Bool(true));
    }
}
