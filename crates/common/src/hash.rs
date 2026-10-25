//! Hashes of SQL values with xxHash (XXH3), for operators that match or
//! group rows by value, such as the hash join.
//!
//! Values that are equal under SQL `=` hash the same: an INT, and a DOUBLE
//! with an integer value, are hashed as the BIGINT with the same value, so
//! keys of different numeric types can be matched. Values that are not equal
//! can still collide, so callers compare the values of rows whose hashes
//! match.
//!
//! Every hash takes a `seed`. The same values hashed with different seeds
//! give unrelated hashes, so an operator that splits rows more than once,
//! like a grace hash join, uses a new seed for each round.

use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

use crate::value::{TWO_POW_63, Value};

pub fn hash_bytes(bytes: &[u8], seed: u64) -> u64 {
    xxh3_64_with_seed(bytes, seed)
}

pub fn hash_bool(v: bool, seed: u64) -> u64 {
    hash_bytes(&[u8::from(v)], seed)
}

/// The same as [`hash_bigint`] of the value, as `=` compares integers
/// across widths.
pub fn hash_int(v: i32, seed: u64) -> u64 {
    hash_bigint(i64::from(v), seed)
}

pub fn hash_bigint(v: i64, seed: u64) -> u64 {
    hash_bytes(&v.to_le_bytes(), seed)
}

/// The same as [`hash_bigint`] for a double with an integer value that
/// fits in a BIGINT, as `=` compares numbers by value. So `-0` hashes as `0`.
pub fn hash_double(v: f64, seed: u64) -> u64 {
    if v.fract() == 0.0 && (-TWO_POW_63..TWO_POW_63).contains(&v) {
        return hash_bigint(v as i64, seed);
    }
    // Every NaN equals every other.
    let bits = if v.is_nan() { f64::NAN } else { v }.to_bits();
    hash_bytes(&bits.to_le_bytes(), seed)
}

pub fn hash_varchar(v: &str, seed: u64) -> u64 {
    hash_bytes(v.as_bytes(), seed)
}

/// Hashes `value` by its type. NULL equals nothing, but still has a hash,
/// for operators that put NULLs together, such as GROUP BY.
pub fn hash_value(value: &Value, seed: u64) -> u64 {
    match value {
        Value::Null => hash_bytes(&[], seed),
        Value::Int(v) => hash_int(*v, seed),
        Value::BigInt(v) => hash_bigint(*v, seed),
        Value::Double(v) => hash_double(*v, seed),
        Value::Bool(v) => hash_bool(*v, seed),
        Value::Varchar(v) => hash_varchar(v, seed),
        Value::Array(values) => hash_values(values, seed),
    }
}

/// Hashes `values` in order, such as the columns of a multi-column key.
pub fn hash_values<'a>(values: impl IntoIterator<Item = &'a Value>, seed: u64) -> u64 {
    values
        .into_iter()
        .fold(hash_bytes(&[], seed), |hash, value| {
            combine_hashes(hash, hash_value(value, seed))
        })
}

/// The partition, out of `partitions`, a row whose key hashes to `hash`
/// goes to.
pub fn partition_index(hash: u64, partitions: usize) -> usize {
    (hash % partitions as u64) as usize
}

/// Mixes two hashes into one. The order matters: `(a, b)` and `(b, a)` give
/// different hashes.
pub fn combine_hashes(left: u64, right: u64) -> u64 {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&left.to_le_bytes());
    bytes[8..].copy_from_slice(&right.to_le_bytes());
    xxh3_64(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_xxh3() {
        assert_eq!(hash_bytes(b"", 0), 0x2D06_8005_38D3_94C2);
    }

    #[test]
    fn integers_hash_the_same_across_widths() {
        for v in [0, 1, -1, i32::MIN, i32::MAX] {
            assert_eq!(hash_int(v, 7), hash_bigint(i64::from(v), 7));
            assert_eq!(
                hash_value(&Value::Int(v), 7),
                hash_value(&Value::BigInt(i64::from(v)), 7)
            );
        }
        assert_eq!(
            hash_value(&Value::Array(vec![Value::Int(1), Value::Int(2)]), 0),
            hash_value(&Value::Array(vec![Value::BigInt(1), Value::BigInt(2)]), 0)
        );
    }

    #[test]
    fn doubles_hash_like_equal_integers() {
        assert_eq!(hash_double(3.0, 7), hash_bigint(3, 7));
        assert_eq!(hash_double(-0.0, 7), hash_double(0.0, 7));
        assert_eq!(hash_double(-TWO_POW_63, 7), hash_bigint(i64::MIN, 7));
        assert_ne!(hash_double(1.5, 7), hash_double(2.5, 7));
        assert_ne!(hash_double(TWO_POW_63, 7), hash_bigint(i64::MIN, 7));
        assert_eq!(
            hash_value(&Value::Double(2.0), 7),
            hash_value(&Value::Int(2), 7)
        );
    }

    #[test]
    fn values_hash_by_type() {
        let seed = 3;
        assert_eq!(hash_value(&Value::Bool(true), seed), hash_bool(true, seed));
        assert_eq!(
            hash_value(&Value::from("pen"), seed),
            hash_varchar("pen", seed)
        );
        assert_ne!(hash_bool(true, seed), hash_bool(false, seed));
        assert_ne!(hash_varchar("pen", seed), hash_varchar("ink", seed));
    }

    #[test]
    fn seeds_give_different_hashes() {
        let value = Value::from("pen");
        assert_ne!(hash_value(&value, 0), hash_value(&value, 1));
        assert_ne!(hash_bigint(42, 0), hash_bigint(42, 1));
    }

    #[test]
    fn combining_depends_on_order() {
        let (a, b) = (hash_int(1, 0), hash_int(2, 0));
        assert_ne!(combine_hashes(a, b), combine_hashes(b, a));

        let (one, two) = (Value::Int(1), Value::Int(2));
        assert_eq!(hash_values([&one, &two], 0), hash_values([&one, &two], 0));
        assert_ne!(hash_values([&one, &two], 0), hash_values([&two, &one], 0));
        assert_ne!(hash_values([&one], 0), hash_values([&one, &one], 0));
    }
}
