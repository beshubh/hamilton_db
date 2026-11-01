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

use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::value::{TWO_POW_63, Value, ValueRef};

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
    hash_value_ref(value.as_value_ref(), seed)
}

/// [`hash_value`] of a value read in place.
pub fn hash_value_ref(value: ValueRef<'_>, seed: u64) -> u64 {
    match value {
        ValueRef::Null => hash_bytes(&[], seed),
        ValueRef::Int(v) => hash_int(v, seed),
        ValueRef::BigInt(v) => hash_bigint(v, seed),
        ValueRef::Double(v) => hash_double(v, seed),
        ValueRef::Bool(v) => hash_bool(v, seed),
        ValueRef::Varchar(v) => hash_varchar(v, seed),
        ValueRef::Array(values) => hash_values(values, seed),
    }
}

/// Hashes `values` in order, such as the columns of a multi-column key.
pub fn hash_values<'a>(values: impl IntoIterator<Item = &'a Value>, seed: u64) -> u64 {
    let mut hasher = ValuesHasher::new(seed);
    for value in values {
        hasher.add(value.as_value_ref());
    }
    hasher.finish()
}

/// Hashes values one at a time, giving what [`hash_values`] gives for all
/// of them, for callers that read each value in place.
#[derive(Clone, Copy)]
pub struct ValuesHasher {
    seed: u64,
    hash: u64,
}

/// `hash_bytes(&[], 0)`, where every hash of values with seed 0 starts.
const EMPTY_HASH_SEED_0: u64 = 0x2D06_8005_38D3_94C2;

impl ValuesHasher {
    pub fn new(seed: u64) -> Self {
        let hash = match seed {
            0 => EMPTY_HASH_SEED_0,
            seed => hash_bytes(&[], seed),
        };
        Self { seed, hash }
    }

    pub fn add(&mut self, value: ValueRef<'_>) {
        self.hash = combine_hashes(self.hash, hash_value_ref(value, self.seed));
    }

    pub fn finish(self) -> u64 {
        self.hash
    }
}

/// Builds hashers for maps keyed by a hash from this module, which is
/// already well mixed, so hashing it again would only cost time.
#[derive(Clone, Copy, Default)]
pub struct HashedKeys;

impl std::hash::BuildHasher for HashedKeys {
    type Hasher = HashedKeyHasher;

    fn build_hasher(&self) -> HashedKeyHasher {
        HashedKeyHasher(0)
    }
}

/// Passes a `u64` key through as its own hash. See [`HashedKeys`].
#[derive(Default)]
pub struct HashedKeyHasher(u64);

impl std::hash::Hasher for HashedKeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(byte);
        }
    }

    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}

/// Builds hashers for maps keyed by small integers, such as page and frame
/// ids and rids, which need mixing but not a hash that resists attacks.
#[derive(Clone, Copy, Default)]
pub struct IntKeys;

impl std::hash::BuildHasher for IntKeys {
    type Hasher = IntKeyHasher;

    fn build_hasher(&self) -> IntKeyHasher {
        IntKeyHasher(0)
    }
}

/// Mixes each integer in with a rotate and a multiply, as FxHash does. See
/// [`IntKeys`].
#[derive(Default)]
pub struct IntKeyHasher(u64);

impl IntKeyHasher {
    #[inline]
    fn add(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(5) ^ v).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

impl std::hash::Hasher for IntKeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.add(u64::from(byte));
        }
    }

    fn write_u16(&mut self, v: u16) {
        self.add(u64::from(v));
    }

    fn write_u32(&mut self, v: u32) {
        self.add(u64::from(v));
    }

    fn write_i32(&mut self, v: i32) {
        self.add(v as u32 as u64);
    }

    fn write_u64(&mut self, v: u64) {
        self.add(v);
    }

    fn write_i64(&mut self, v: i64) {
        self.add(v as u64);
    }

    fn write_usize(&mut self, v: usize) {
        self.add(v as u64);
    }
}

/// The partition, out of `partitions`, a row whose key hashes to `hash`
/// goes to.
pub fn partition_index(hash: u64, partitions: usize) -> usize {
    (hash % partitions as u64) as usize
}

/// Mixes two hashes into one. The order matters: `(a, b)` and `(b, a)` give
/// different hashes. Both are already well mixed, so a multiply-fold, as in
/// wyhash, is enough and much quicker than hashing their bytes again.
#[inline]
pub fn combine_hashes(left: u64, right: u64) -> u64 {
    let product =
        u128::from(left ^ 0xa076_1d64_78bd_642f) * u128::from(right ^ 0xe703_7ed1_a0b4_28db);
    (product as u64) ^ ((product >> 64) as u64)
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

    #[test]
    fn hashing_in_place_matches_hashing_values() {
        let values = [Value::Int(1), Value::from("pen"), Value::Null];
        let mut hasher = ValuesHasher::new(5);
        for value in &values {
            assert_eq!(
                hash_value_ref(value.as_value_ref(), 5),
                hash_value(value, 5)
            );
            hasher.add(value.as_value_ref());
        }
        assert_eq!(hasher.finish(), hash_values(&values, 5));
        assert_eq!(ValuesHasher::new(5).finish(), hash_values([], 5));
    }
}
