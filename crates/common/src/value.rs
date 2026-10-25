//! SQL values and their types.
//!
//! Operators follow SQL semantics: a NULL operand makes the result NULL
//! (except where AND / OR can decide without it), INT is widened to BIGINT
//! when the two are mixed, and an integer mixed with a DOUBLE is widened to
//! DOUBLE. NULL is untyped, so `NULL + 'a'` is NULL rather than a type error;
//! catching that is left to the planner.
//!
//! DOUBLE follows PostgreSQL's `double precision`: arithmetic that would
//! give an infinity fails instead, so no value is ever infinite or NaN, and
//! `-0` equals `0`.
//!
//! `==` on [`Value`] is structural identity (`NULL == NULL`,
//! `Int(1) != BigInt(1)`, and doubles are equal when their bits are). SQL
//! comparison is [`Value::compare`] and the `cmp_*` methods.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::error::{Result, ValueError};

/// The declared type of a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    BigInt,
    /// 64-bit IEEE 754 floating point, PostgreSQL's `double precision`.
    Double,
    Bool,
    /// UTF-8 string of at most this many bytes.
    Varchar(u32),
    /// A list of values, built by expressions. It has no element type and
    /// cannot be stored in a tuple.
    Array,
}

impl DataType {
    /// The SQL name of the type without its parameters, for error messages;
    /// matches [`Value::type_name`].
    pub fn type_name(self) -> &'static str {
        match self {
            DataType::Int => "INT",
            DataType::BigInt => "BIGINT",
            DataType::Double => "DOUBLE",
            DataType::Bool => "BOOL",
            DataType::Varchar(_) => "VARCHAR",
            DataType::Array => "ARRAY",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int => f.write_str("INT"),
            DataType::BigInt => f.write_str("BIGINT"),
            DataType::Double => f.write_str("DOUBLE"),
            DataType::Bool => f.write_str("BOOL"),
            DataType::Varchar(max_len) => write!(f, "VARCHAR({max_len})"),
            DataType::Array => f.write_str("ARRAY"),
        }
    }
}

/// A single SQL value.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Int(i32),
    BigInt(i64),
    /// Never infinite or NaN.
    Double(f64),
    Bool(bool),
    Varchar(String),
    Array(Vec<Value>),
}

/// 2^63, the smallest double above every `i64`. Every double in
/// `-2^63..2^63` has an integer part that fits in an `i64`.
pub(crate) const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

#[derive(Clone, Copy)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl ArithOp {
    fn symbol(self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Rem => "%",
        }
    }

    fn apply(self, l: i64, r: i64) -> Result<i64> {
        let out = match self {
            ArithOp::Add => l.checked_add(r),
            ArithOp::Sub => l.checked_sub(r),
            ArithOp::Mul => l.checked_mul(r),
            ArithOp::Div | ArithOp::Rem if r == 0 => return Err(ValueError::DivisionByZero),
            ArithOp::Div => l.checked_div(r),
            // `i64::MIN % -1` is 0, but `checked_rem` reports it as overflow.
            ArithOp::Rem => Some(l.wrapping_rem(r)),
        };
        out.ok_or(ValueError::Overflow)
    }

    /// Never called for `%`, which does not take doubles. The operands are
    /// finite, so an infinite result means it overflowed.
    fn apply_double(self, l: f64, r: f64) -> Result<f64> {
        let out = match self {
            ArithOp::Add => l + r,
            ArithOp::Sub => l - r,
            ArithOp::Mul => l * r,
            ArithOp::Div if r == 0.0 => return Err(ValueError::DivisionByZero),
            ArithOp::Div => l / r,
            ArithOp::Rem => unreachable!("% does not take doubles"),
        };
        if out.is_finite() {
            Ok(out)
        } else {
            Err(ValueError::DoubleOverflow)
        }
    }
}

/// A number of any numeric type, for the operators that mix them.
#[derive(Clone, Copy)]
enum Number {
    Integer(i64),
    Double(f64),
}

impl Number {
    fn compare(self, other: Number) -> Ordering {
        match (self, other) {
            (Number::Integer(l), Number::Integer(r)) => l.cmp(&r),
            (Number::Double(l), Number::Double(r)) => compare_doubles(l, r),
            (Number::Integer(l), Number::Double(r)) => compare_integer_with_double(l, r),
            (Number::Double(l), Number::Integer(r)) => compare_integer_with_double(r, l).reverse(),
        }
    }

    /// The nearest double; exact for integers up to 2^53.
    fn to_f64(self) -> f64 {
        match self {
            Number::Integer(v) => v as f64,
            Number::Double(v) => v,
        }
    }
}

/// Orders doubles as PostgreSQL does: `-0` equals `0`, and NaN equals
/// itself and is above everything else. No value is NaN, but the order is
/// total anyway, as an index needs.
fn compare_doubles(l: f64, r: f64) -> Ordering {
    l.partial_cmp(&r)
        .unwrap_or_else(|| l.is_nan().cmp(&r.is_nan()))
}

/// Orders an integer and a double exactly. Rounding the integer to a double
/// first would make 2^53 + 1 equal to 2^53 as a double, and equal values
/// must hash alike (see [`crate::hash`]).
fn compare_integer_with_double(i: i64, d: f64) -> Ordering {
    if d.is_nan() || d >= TWO_POW_63 {
        return Ordering::Less;
    }
    if d < -TWO_POW_63 {
        return Ordering::Greater;
    }
    let whole = d.trunc();
    // `whole` fits in an i64, so the cast is exact; then the fraction decides.
    i.cmp(&(whole as i64))
        .then_with(|| compare_doubles(0.0, d - whole))
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// The SQL name of the value's type, for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Int(_) => "INT",
            Value::BigInt(_) => "BIGINT",
            Value::Double(_) => "DOUBLE",
            Value::Bool(_) => "BOOL",
            Value::Varchar(_) => "VARCHAR",
            Value::Array(_) => "ARRAY",
        }
    }

    /// Orders two values, or returns `None` if either is NULL.
    ///
    /// Numbers compare by value across types, strings compare bytewise,
    /// and `false < true`.
    pub fn compare(&self, other: &Value) -> Result<Option<Ordering>> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(None),
            (Value::Bool(l), Value::Bool(r)) => Ok(Some(l.cmp(r))),
            (Value::Varchar(l), Value::Varchar(r)) => Ok(Some(l.cmp(r))),
            _ => match (self.as_number(), other.as_number()) {
                (Some(l), Some(r)) => Ok(Some(l.compare(r))),
                _ => Err(ValueError::Incomparable {
                    left: self.type_name(),
                    right: other.type_name(),
                }),
            },
        }
    }

    /// SQL `=`: a BOOL, or NULL if either side is NULL.
    pub fn cmp_eq(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_eq)
    }

    /// SQL `<>`.
    pub fn cmp_ne(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_ne)
    }

    /// SQL `<`.
    pub fn cmp_lt(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_lt)
    }

    /// SQL `<=`.
    pub fn cmp_le(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_le)
    }

    /// SQL `>`.
    pub fn cmp_gt(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_gt)
    }

    /// SQL `>=`.
    pub fn cmp_ge(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_ge)
    }

    pub fn checked_add(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Add, other)
    }

    pub fn checked_sub(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Sub, other)
    }

    pub fn checked_mul(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Mul, other)
    }

    /// Integer division truncates toward zero.
    pub fn checked_div(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Div, other)
    }

    /// Remainder of truncating division; takes the sign of `self`. Only
    /// for integers.
    pub fn checked_rem(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Rem, other)
    }

    pub fn checked_neg(&self) -> Result<Value> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Int(v) => v.checked_neg().map(Value::Int).ok_or(ValueError::Overflow),
            Value::BigInt(v) => v
                .checked_neg()
                .map(Value::BigInt)
                .ok_or(ValueError::Overflow),
            Value::Double(v) => Ok(Value::Double(-v)),
            _ => Err(ValueError::InvalidOperand {
                op: "-",
                operand: self.type_name(),
            }),
        }
    }

    /// SQL `AND`: false if either side is false, even when the other is NULL.
    pub fn logical_and(&self, other: &Value) -> Result<Value> {
        Ok(match self.bool_operands(other, "AND")? {
            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
            (Some(true), Some(true)) => Value::Bool(true),
            _ => Value::Null,
        })
    }

    /// SQL `OR`: true if either side is true, even when the other is NULL.
    pub fn logical_or(&self, other: &Value) -> Result<Value> {
        Ok(match self.bool_operands(other, "OR")? {
            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
            (Some(false), Some(false)) => Value::Bool(false),
            _ => Value::Null,
        })
    }

    /// SQL `NOT`.
    pub fn logical_not(&self) -> Result<Value> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            _ => Err(ValueError::InvalidOperand {
                op: "NOT",
                operand: self.type_name(),
            }),
        }
    }

    /// Converts a number to `to`: INT, BIGINT or DOUBLE. A double becomes
    /// the nearest integer, ties to even, as in PostgreSQL. Fails with
    /// [`ValueError::Overflow`] if the result does not fit in `to`. NULL
    /// stays NULL.
    pub fn cast_number(&self, to: DataType) -> Result<Value> {
        let invalid = || ValueError::InvalidOperands {
            op: "CAST",
            left: self.type_name(),
            right: to.type_name(),
        };
        if self.is_null() {
            return Ok(Value::Null);
        }
        let number = self.as_number().ok_or_else(invalid)?;
        let integer = match (number, to) {
            (_, DataType::Double) => return Ok(Value::Double(number.to_f64())),
            (_, DataType::Int | DataType::BigInt) => match number {
                Number::Integer(v) => v,
                Number::Double(v) => {
                    let v = v.round_ties_even();
                    if !(-TWO_POW_63..TWO_POW_63).contains(&v) {
                        return Err(ValueError::Overflow);
                    }
                    v as i64
                }
            },
            _ => return Err(invalid()),
        };
        Ok(if to == DataType::Int {
            Value::Int(i32::try_from(integer).map_err(|_| ValueError::Overflow)?)
        } else {
            Value::BigInt(integer)
        })
    }

    fn as_number(&self) -> Option<Number> {
        match self {
            Value::Int(v) => Some(Number::Integer(i64::from(*v))),
            Value::BigInt(v) => Some(Number::Integer(*v)),
            Value::Double(v) => Some(Number::Double(*v)),
            _ => None,
        }
    }

    /// `Some(None)` for NULL, `Some(Some(b))` for a BOOL, `None` otherwise.
    fn as_bool(&self) -> Option<Option<bool>> {
        match self {
            Value::Null => Some(None),
            Value::Bool(b) => Some(Some(*b)),
            _ => None,
        }
    }

    fn compare_by(&self, other: &Value, pred: fn(Ordering) -> bool) -> Result<Value> {
        Ok(self
            .compare(other)?
            .map_or(Value::Null, |ord| Value::Bool(pred(ord))))
    }

    fn arith(&self, op: ArithOp, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            // i32 results always fit in i64, so compute wide and narrow back.
            (Value::Int(l), Value::Int(r)) => {
                let out = op.apply(i64::from(*l), i64::from(*r))?;
                i32::try_from(out)
                    .map(Value::Int)
                    .map_err(|_| ValueError::Overflow)
            }
            _ => match (self.as_number(), other.as_number()) {
                (Some(Number::Integer(l)), Some(Number::Integer(r))) => {
                    op.apply(l, r).map(Value::BigInt)
                }
                (Some(l), Some(r)) if !matches!(op, ArithOp::Rem) => {
                    op.apply_double(l.to_f64(), r.to_f64()).map(Value::Double)
                }
                _ => Err(ValueError::InvalidOperands {
                    op: op.symbol(),
                    left: self.type_name(),
                    right: other.type_name(),
                }),
            },
        }
    }

    fn bool_operands(
        &self,
        other: &Value,
        op: &'static str,
    ) -> Result<(Option<bool>, Option<bool>)> {
        match (self.as_bool(), other.as_bool()) {
            (Some(l), Some(r)) => Ok((l, r)),
            _ => Err(ValueError::InvalidOperands {
                op,
                left: self.type_name(),
                right: other.type_name(),
            }),
        }
    }
}

/// Structural identity; see the module docs.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int(l), Value::Int(r)) => l == r,
            (Value::BigInt(l), Value::BigInt(r)) => l == r,
            (Value::Double(l), Value::Double(r)) => l.to_bits() == r.to_bits(),
            (Value::Bool(l), Value::Bool(r)) => l == r,
            (Value::Varchar(l), Value::Varchar(r)) => l == r,
            (Value::Array(l), Value::Array(r)) => l == r,
            _ => false,
        }
    }
}

impl Eq for Value {}

/// Agrees with `==`, not with SQL `=`; [`crate::hash`] hashes for `=`.
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Int(v) => v.hash(state),
            Value::BigInt(v) => v.hash(state),
            Value::Double(v) => v.to_bits().hash(state),
            Value::Bool(v) => v.hash(state),
            Value::Varchar(v) => v.hash(state),
            Value::Array(values) => values.hash(state),
        }
    }
}

/// Writes `v` as PostgreSQL does: the fewest digits that read back as `v`,
/// in scientific notation when the exponent is below -4 or at least 15.
fn format_double(v: f64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if v.is_nan() {
        return f.write_str("NaN");
    }
    if v.is_infinite() {
        return f.write_str(if v > 0.0 { "Infinity" } else { "-Infinity" });
    }
    // `{:e}` gives the fewest digits, as `1.5e-7`.
    let scientific = format!("{v:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("a finite double formats with an exponent");
    let exponent: i32 = exponent.parse().expect("the exponent is an integer");
    if (-4..15).contains(&exponent) {
        write!(f, "{v}")
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        write!(f, "{mantissa}e{sign}{:02}", exponent.abs())
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Int(v) => write!(f, "{v}"),
            Value::BigInt(v) => write!(f, "{v}"),
            Value::Double(v) => format_double(*v, f),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Varchar(v) => f.write_str(v),
            Value::Array(values) => {
                f.write_str("{")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{v}")?;
                }
                f.write_str("}")
            }
        }
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::BigInt(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Varchar(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Varchar(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NULL: Value = Value::Null;

    fn v(x: impl Into<Value>) -> Value {
        x.into()
    }

    #[test]
    fn compares_within_a_type() {
        assert_eq!(v(1).compare(&v(2)), Ok(Some(Ordering::Less)));
        assert_eq!(v("b").compare(&v("a")), Ok(Some(Ordering::Greater)));
        assert_eq!(v(false).compare(&v(true)), Ok(Some(Ordering::Less)));
    }

    #[test]
    fn compares_int_with_bigint() {
        assert_eq!(v(5).compare(&v(5_i64)), Ok(Some(Ordering::Equal)));
        assert_eq!(
            v(i64::MAX).compare(&v(i32::MAX)),
            Ok(Some(Ordering::Greater))
        );
    }

    #[test]
    fn comparison_with_null_is_null() {
        assert_eq!(v(1).compare(&NULL), Ok(None));
        assert_eq!(NULL.cmp_eq(&NULL), Ok(NULL));
        assert_eq!(v("a").cmp_lt(&NULL), Ok(NULL));
    }

    #[test]
    fn comparison_operators() {
        assert_eq!(v(1).cmp_eq(&v(1_i64)), Ok(v(true)));
        assert_eq!(v(1).cmp_ne(&v(1)), Ok(v(false)));
        assert_eq!(v(1).cmp_lt(&v(2)), Ok(v(true)));
        assert_eq!(v(2).cmp_le(&v(2)), Ok(v(true)));
        assert_eq!(v("a").cmp_gt(&v("b")), Ok(v(false)));
        assert_eq!(v(true).cmp_ge(&v(false)), Ok(v(true)));
    }

    #[test]
    fn comparing_mismatched_types_fails() {
        let err = v(1).compare(&v("1")).unwrap_err();
        assert!(matches!(
            err,
            ValueError::Incomparable {
                left: "INT",
                right: "VARCHAR"
            }
        ));
        assert_eq!(err.to_string(), "cannot compare INT with VARCHAR");
        assert!(v(true).cmp_eq(&v(1)).is_err());
    }

    #[test]
    fn int_arithmetic_stays_int() {
        assert_eq!(v(2).checked_add(&v(3)), Ok(v(5)));
        assert_eq!(v(2).checked_sub(&v(3)), Ok(v(-1)));
        assert_eq!(v(4).checked_mul(&v(3)), Ok(v(12)));
        assert_eq!(v(7).checked_div(&v(2)), Ok(v(3)));
        assert_eq!(v(-7).checked_div(&v(2)), Ok(v(-3)));
        assert_eq!(v(-7).checked_rem(&v(2)), Ok(v(-1)));
    }

    #[test]
    fn mixed_arithmetic_widens_to_bigint() {
        assert_eq!(v(2).checked_add(&v(3_i64)), Ok(v(5_i64)));
        assert_eq!(v(i64::MAX).checked_sub(&v(1)), Ok(v(i64::MAX - 1)));
    }

    #[test]
    fn arithmetic_with_null_is_null() {
        assert_eq!(v(1).checked_add(&NULL), Ok(NULL));
        assert_eq!(NULL.checked_div(&v(0)), Ok(NULL));
        assert_eq!(NULL.checked_neg(), Ok(NULL));
    }

    #[test]
    fn overflow_is_an_error() {
        assert_eq!(v(i32::MAX).checked_add(&v(1)), Err(ValueError::Overflow));
        assert_eq!(v(i32::MIN).checked_div(&v(-1)), Err(ValueError::Overflow));
        assert_eq!(v(i64::MAX).checked_mul(&v(2)), Err(ValueError::Overflow));
        assert_eq!(v(i32::MIN).checked_neg(), Err(ValueError::Overflow));
        assert_eq!(v(i64::MIN).checked_neg(), Err(ValueError::Overflow));
    }

    #[test]
    fn min_rem_minus_one_is_zero() {
        assert_eq!(v(i32::MIN).checked_rem(&v(-1)), Ok(v(0)));
        assert_eq!(v(i64::MIN).checked_rem(&v(-1)), Ok(v(0_i64)));
    }

    #[test]
    fn division_by_zero_is_an_error() {
        assert_eq!(v(1).checked_div(&v(0)), Err(ValueError::DivisionByZero));
        assert_eq!(v(1).checked_rem(&v(0_i64)), Err(ValueError::DivisionByZero));
    }

    #[test]
    fn arithmetic_on_non_numbers_fails() {
        let err = v("a").checked_add(&v(1)).unwrap_err();
        assert_eq!(err.to_string(), "cannot apply + to VARCHAR and INT");
        assert!(matches!(
            v(true).checked_neg(),
            Err(ValueError::InvalidOperand {
                op: "-",
                operand: "BOOL"
            })
        ));
    }

    #[test]
    fn compares_doubles_with_integers_exactly() {
        assert_eq!(v(1.5).compare(&v(2)), Ok(Some(Ordering::Less)));
        assert_eq!(v(2.0).compare(&v(2_i64)), Ok(Some(Ordering::Equal)));
        assert_eq!(v(-1).compare(&v(-1.5)), Ok(Some(Ordering::Greater)));
        assert_eq!(v(-0.0).compare(&v(0.0)), Ok(Some(Ordering::Equal)));
        // 2^53 + 1 is not a double: it rounds to 2^53.
        let big = (1_i64 << 53) + 1;
        assert_eq!(
            v(big).compare(&v((1_i64 << 53) as f64)),
            Ok(Some(Ordering::Greater))
        );
        assert_eq!(
            v(i64::MAX).compare(&v(TWO_POW_63)),
            Ok(Some(Ordering::Less))
        );
        assert_eq!(
            v(i64::MIN).compare(&v(-TWO_POW_63)),
            Ok(Some(Ordering::Equal))
        );
        assert!(v(1.0).compare(&v("1")).is_err());
    }

    #[test]
    fn nan_is_above_everything() {
        let nan = v(f64::NAN);
        assert_eq!(nan.compare(&v(f64::NAN)), Ok(Some(Ordering::Equal)));
        assert_eq!(nan.compare(&v(1e300)), Ok(Some(Ordering::Greater)));
        assert_eq!(v(i64::MAX).compare(&nan), Ok(Some(Ordering::Less)));
    }

    #[test]
    fn arithmetic_with_a_double_is_double() {
        assert_eq!(v(1.5).checked_add(&v(1)), Ok(v(2.5)));
        assert_eq!(v(1_i64).checked_sub(&v(0.5)), Ok(v(0.5)));
        assert_eq!(v(2.0).checked_mul(&v(1.25)), Ok(v(2.5)));
        assert_eq!(v(7).checked_div(&v(2.0)), Ok(v(3.5)));
        assert_eq!(v(1.5).checked_neg(), Ok(v(-1.5)));
        assert_eq!(v(1.5).checked_add(&NULL), Ok(NULL));
    }

    #[test]
    fn double_arithmetic_errors() {
        assert_eq!(
            v(1e308).checked_mul(&v(10)),
            Err(ValueError::DoubleOverflow)
        );
        assert_eq!(v(1.0).checked_div(&v(0)), Err(ValueError::DivisionByZero));
        assert_eq!(v(1).checked_div(&v(0.0)), Err(ValueError::DivisionByZero));
        assert!(matches!(
            v(1.5).checked_rem(&v(1)),
            Err(ValueError::InvalidOperands { op: "%", .. })
        ));
    }

    #[test]
    fn casts_between_numbers() {
        assert_eq!(v(7).cast_number(DataType::Double), Ok(v(7.0)));
        assert_eq!(v(7_i64).cast_number(DataType::Int), Ok(v(7)));
        assert_eq!(v(2.5).cast_number(DataType::Int), Ok(v(2)));
        assert_eq!(v(3.5).cast_number(DataType::BigInt), Ok(v(4_i64)));
        assert_eq!(v(-2.5).cast_number(DataType::Int), Ok(v(-2)));
        assert_eq!(NULL.cast_number(DataType::Double), Ok(NULL));
        assert_eq!(v(3e9).cast_number(DataType::Int), Err(ValueError::Overflow));
        assert_eq!(
            v(TWO_POW_63).cast_number(DataType::BigInt),
            Err(ValueError::Overflow)
        );
        assert_eq!(
            v(-TWO_POW_63).cast_number(DataType::BigInt),
            Ok(v(i64::MIN))
        );
        assert!(v("1").cast_number(DataType::Int).is_err());
        assert!(v(1).cast_number(DataType::Bool).is_err());
    }

    #[test]
    fn doubles_are_identical_by_bits() {
        assert_eq!(v(1.5), v(1.5));
        assert_ne!(v(0.0), v(-0.0));
        assert_ne!(v(1.0), v(1));
        assert_eq!(v(f64::NAN), v(f64::NAN));
    }

    #[test]
    fn displays_doubles_like_postgres() {
        let shown = |x: f64| v(x).to_string();
        assert_eq!(shown(1.5), "1.5");
        assert_eq!(shown(3.0), "3");
        assert_eq!(shown(-0.0), "-0");
        assert_eq!(shown(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(shown(0.0001), "0.0001");
        assert_eq!(shown(0.00001), "1e-05");
        assert_eq!(shown(123456789012345.0), "123456789012345");
        assert_eq!(shown(1e15), "1e+15");
        assert_eq!(shown(-1.25e100), "-1.25e+100");
        assert_eq!(shown(f64::NAN), "NaN");
        assert_eq!(DataType::Double.to_string(), "DOUBLE");
    }

    #[test]
    fn and_is_three_valued() {
        let t = v(true);
        let f = v(false);
        assert_eq!(t.logical_and(&t), Ok(v(true)));
        assert_eq!(t.logical_and(&f), Ok(v(false)));
        assert_eq!(f.logical_and(&NULL), Ok(v(false)));
        assert_eq!(NULL.logical_and(&f), Ok(v(false)));
        assert_eq!(t.logical_and(&NULL), Ok(NULL));
        assert_eq!(NULL.logical_and(&NULL), Ok(NULL));
    }

    #[test]
    fn or_is_three_valued() {
        let t = v(true);
        let f = v(false);
        assert_eq!(f.logical_or(&f), Ok(v(false)));
        assert_eq!(f.logical_or(&t), Ok(v(true)));
        assert_eq!(t.logical_or(&NULL), Ok(v(true)));
        assert_eq!(NULL.logical_or(&t), Ok(v(true)));
        assert_eq!(f.logical_or(&NULL), Ok(NULL));
    }

    #[test]
    fn not() {
        assert_eq!(v(true).logical_not(), Ok(v(false)));
        assert_eq!(NULL.logical_not(), Ok(NULL));
        assert!(v(1).logical_not().is_err());
    }

    #[test]
    fn logic_on_non_bools_fails() {
        assert!(matches!(
            v(1).logical_and(&v(true)),
            Err(ValueError::InvalidOperands { op: "AND", .. })
        ));
    }

    #[test]
    fn display() {
        assert_eq!(NULL.to_string(), "NULL");
        assert_eq!(v(-3).to_string(), "-3");
        assert_eq!(v(true).to_string(), "true");
        assert_eq!(v("hi").to_string(), "hi");
        assert_eq!(DataType::Varchar(32).to_string(), "VARCHAR(32)");
        assert_eq!(
            Value::Array(vec![v(1), NULL, v(3)]).to_string(),
            "{1,NULL,3}"
        );
        assert_eq!(Value::Array(vec![]).to_string(), "{}");
    }
}
