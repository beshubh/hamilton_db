use common::{Column, DataType, Schema, Tuple, Value, ValueError};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN, type_name};
use crate::Result;

/// Converts an integer to INT or BIGINT. Narrowing a BIGINT that does not
/// fit in an INT fails with [`ValueError::Overflow`]; NULL stays NULL.
#[derive(Clone, Debug)]
pub struct CastExpr {
    /// Always `[arg]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl CastExpr {
    /// Fails unless `arg` is an integer and `to` is INT or BIGINT.
    pub fn new(arg: AbstractExpr, to: DataType) -> Result<Self> {
        let ty = arg.return_type();
        let is_integer = |t| matches!(t, DataType::Int | DataType::BigInt);
        if !is_integer(ty.data_type) || !is_integer(to) {
            return Err(ValueError::InvalidOperands {
                op: "CAST",
                left: type_name(ty.data_type),
                right: type_name(to),
            }
            .into());
        }
        let return_type = Column::new(UNNAMED_COLUMN, to, ty.nullable);
        Ok(Self {
            children: vec![arg],
            return_type,
        })
    }

    fn cast(&self, value: Value) -> Result<Value> {
        let out = match (value, self.return_type.data_type) {
            (Value::Int(v), DataType::BigInt) => Value::BigInt(i64::from(v)),
            (Value::BigInt(v), DataType::Int) => {
                Value::Int(i32::try_from(v).map_err(|_| ValueError::Overflow)?)
            }
            (value @ (Value::Null | Value::Int(_) | Value::BigInt(_)), _) => value,
            (value, _) => {
                return Err(ValueError::InvalidOperand {
                    op: "CAST",
                    operand: value.type_name(),
                }
                .into());
            }
        };
        Ok(out)
    }
}

impl Expr for CastExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        self.cast(self.children[0].evaluate(tuple, schema)?)
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        self.cast(self.children[0].evaluate_join(left, left_schema, right, right_schema)?)
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

    fn cast(arg: AbstractExpr, to: DataType) -> AbstractExpr {
        CastExpr::new(arg, to).unwrap().into()
    }

    #[test]
    fn widens_and_narrows() {
        let wide = cast(column(0), DataType::BigInt);
        assert_eq!(eval(&wide, &ints(Some(7), 0)).unwrap(), Value::BigInt(7));
        assert_eq!(eval(&wide, &ints(None, 0)).unwrap(), Value::Null);
        assert!(wide.return_type().nullable);

        let narrow = cast(column(1), DataType::Int);
        assert_eq!(eval(&narrow, &ints(None, -3)).unwrap(), Value::Int(-3));
        assert!(!narrow.return_type().nullable);
    }

    #[test]
    fn narrowing_out_of_range_overflows() {
        let narrow = cast(column(1), DataType::Int);
        assert!(matches!(
            eval(&narrow, &ints(None, i64::MAX)),
            Err(ExecutionError::Value(ValueError::Overflow))
        ));
    }

    #[test]
    fn rejects_non_integers() {
        let err = CastExpr::new(column(2), DataType::Int).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Value(ValueError::InvalidOperands { op: "CAST", .. })
        ));
    }
}
