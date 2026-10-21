use common::{Column, DataType, Schema, Tuple, Value, ValueError};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN, type_name};
use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringOp {
    Lower,
    Upper,
}

impl StringOp {
    pub fn name(self) -> &'static str {
        match self {
            StringOp::Lower => "LOWER",
            StringOp::Upper => "UPPER",
        }
    }

    fn apply(self, value: &Value) -> Result<Value> {
        match value {
            Value::Null => Ok(Value::Null),
            Value::Varchar(s) => Ok(Value::Varchar(match self {
                StringOp::Lower => s.to_lowercase(),
                StringOp::Upper => s.to_uppercase(),
            })),
            _ => Err(ValueError::InvalidOperand {
                op: self.name(),
                operand: value.type_name(),
            }
            .into()),
        }
    }
}

/// `LOWER(arg)` or `UPPER(arg)` on a VARCHAR, using Unicode case mapping.
///
/// The result has the argument's VARCHAR type, though case mapping can
/// lengthen a string (`UPPER('ß')` is `'SS'`), so it may not fit when stored.
#[derive(Clone, Debug)]
pub struct StringExpr {
    op: StringOp,
    /// Always `[arg]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl StringExpr {
    /// Fails if `arg` is not a VARCHAR.
    pub fn new(op: StringOp, arg: AbstractExpr) -> Result<Self> {
        let column = arg.return_type();
        if !matches!(column.data_type, DataType::Varchar(_)) {
            return Err(ValueError::InvalidOperand {
                op: op.name(),
                operand: type_name(column.data_type),
            }
            .into());
        }
        let return_type = Column::new(UNNAMED_COLUMN, column.data_type, column.nullable);
        Ok(Self {
            op,
            children: vec![arg],
            return_type,
        })
    }

    pub fn op(&self) -> StringOp {
        self.op
    }
}

impl Expr for StringExpr {
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
        let arg = self.children[0].evaluate_join(left, left_schema, right, right_schema)?;
        self.op.apply(&arg)
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

    fn string(op: StringOp, arg: AbstractExpr) -> AbstractExpr {
        StringExpr::new(op, arg).unwrap().into()
    }

    #[test]
    fn changes_case() {
        let input = row(None, 0, Some("Hello Wörld"), None);
        let lower = eval(&string(StringOp::Lower, column(2)), &input).unwrap();
        let upper = eval(&string(StringOp::Upper, column(2)), &input).unwrap();
        assert_eq!(lower, Value::from("hello wörld"));
        assert_eq!(upper, Value::from("HELLO WÖRLD"));
    }

    #[test]
    fn null_gives_null() {
        let expr = string(StringOp::Upper, column(2));
        assert_eq!(eval(&expr, &row(None, 0, None, None)).unwrap(), Value::Null);
    }

    #[test]
    fn keeps_the_argument_type() {
        let expr = string(StringOp::Lower, column(2));
        assert_eq!(expr.return_type().data_type, DataType::Varchar(16));
        assert!(expr.return_type().nullable);
    }

    #[test]
    fn rejects_non_varchar() {
        let err = StringExpr::new(StringOp::Lower, int(1)).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Value(ValueError::InvalidOperand {
                op: "LOWER",
                operand: "INT",
            })
        ));
    }

    #[test]
    fn evaluates_in_a_join() {
        let expr = string(StringOp::Upper, side_column(0, &schema(), 2));
        let got = eval_join(&expr, &row(None, 0, Some("ab"), None), &[Value::Int(1)]).unwrap();
        assert_eq!(got, Value::from("AB"));
    }
}
