use common::{Column, DataType, Schema, Tuple, Value};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN};
use crate::Result;

/// `arg IS NULL`, or `arg IS NOT NULL` when `negated`. Takes any type and is
/// never NULL itself.
#[derive(Clone, Debug)]
pub struct IsNullExpr {
    negated: bool,
    /// Always `[arg]`.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl IsNullExpr {
    pub fn new(arg: AbstractExpr, negated: bool) -> Self {
        Self {
            negated,
            children: vec![arg],
            return_type: Column::new(UNNAMED_COLUMN, DataType::Bool, false),
        }
    }

    pub fn negated(&self) -> bool {
        self.negated
    }

    fn test(&self, value: &Value) -> Value {
        Value::Bool(value.is_null() != self.negated)
    }
}

impl Expr for IsNullExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        Ok(self.test(&self.children[0].evaluate(tuple, schema)?))
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        let value = self.children[0].evaluate_join(left, left_schema, right, right_schema)?;
        Ok(self.test(&value))
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
    use crate::expressions::test_util::*;

    #[test]
    fn is_null_and_is_not_null() {
        let is_null: AbstractExpr = IsNullExpr::new(column(0), false).into();
        let is_not_null: AbstractExpr = IsNullExpr::new(column(0), true).into();
        for (a, null) in [(None, true), (Some(1), false)] {
            let input = ints(a, 0);
            assert_eq!(eval(&is_null, &input).unwrap(), Value::Bool(null));
            assert_eq!(eval(&is_not_null, &input).unwrap(), Value::Bool(!null));
        }
        assert!(!is_null.return_type().nullable);
    }
}
