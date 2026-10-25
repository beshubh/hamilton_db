use common::{Column, DataType, Schema, Tuple, Value, ValueError};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN, is_number, type_name};
use crate::Result;

/// Converts a number to INT, BIGINT or DOUBLE, as [`Value::cast_number`]
/// does: a DOUBLE is rounded to an integer, and a value that does not fit
/// fails with [`ValueError::Overflow`]. NULL stays NULL.
#[derive(Clone, Debug)]
pub struct CastExpr {
    /// Always `[arg]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl CastExpr {
    /// Fails unless `arg` and `to` are numeric types.
    pub fn new(arg: AbstractExpr, to: DataType) -> Result<Self> {
        let ty = arg.return_type();
        if !is_number(ty.data_type) || !is_number(to) {
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
        Ok(value.cast_number(self.return_type.data_type)?)
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
    fn converts_doubles() {
        let to_double = cast(column(0), DataType::Double);
        assert_eq!(to_double.return_type().data_type, DataType::Double);
        assert_eq!(
            eval(&to_double, &ints(Some(7), 0)).unwrap(),
            Value::Double(7.0)
        );
        let round = cast(cast(column(1), DataType::Double), DataType::Int);
        assert_eq!(eval(&round, &ints(None, -3)).unwrap(), Value::Int(-3));
    }

    #[test]
    fn rejects_non_numbers() {
        let err = CastExpr::new(column(2), DataType::Int).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Value(ValueError::InvalidOperands { op: "CAST", .. })
        ));
    }
}
