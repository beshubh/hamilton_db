use std::cmp::Ordering;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::sync::Arc;

use common::{Column, DataType, Rid, Schema, Tuple, Value, ValueError};
use storage::StorageError;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::btree_index::{BtreeIndex, KeySchemaComparator};
use storage::index::{Index, IndexMetadata, IndexTrait, LocatorKind, RowLocator};
use storage::page::btree_page::KeyComparator;
use tempfile::TempDir;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Column::new("id", DataType::Int, true),
        Column::new("name", DataType::Varchar(16), true),
    ]))
}

fn key(schema: &Schema, id: Option<i32>, name: &str) -> Vec<u8> {
    let id = id.map_or(Value::Null, Value::Int);
    Tuple::encode(schema, &[id, Value::from(name)])
        .unwrap()
        .into_data()
}

#[test]
fn compares_column_by_column() {
    let schema = schema();
    let cmp = KeySchemaComparator::new(Arc::clone(&schema));
    let ordered = [
        key(&schema, None, "a"),
        key(&schema, Some(-1), "z"),
        key(&schema, Some(1), "ab"),
        key(&schema, Some(1), "b"),
        key(&schema, Some(256), ""),
    ];
    for (i, a) in ordered.iter().enumerate() {
        for (j, b) in ordered.iter().enumerate() {
            assert_eq!(cmp.compare(a, b), i.cmp(&j), "key {i} against key {j}");
        }
    }
}

#[test]
fn nulls_are_equal() {
    let schema = schema();
    let cmp = KeySchemaComparator::new(Arc::clone(&schema));
    assert_eq!(
        cmp.compare(&key(&schema, None, "a"), &key(&schema, None, "a")),
        Ordering::Equal
    );
}

#[test]
fn long_strings_compare_by_value() {
    let schema = Arc::new(Schema::new(vec![Column::new(
        "name",
        DataType::Varchar(900),
        false,
    )]));
    let cmp = KeySchemaComparator::new(Arc::clone(&schema));
    let encode = |s: &str| {
        Tuple::encode(&schema, &[Value::from(s)])
            .unwrap()
            .into_data()
    };
    let (a, b) = (encode(&"a".repeat(800)), encode("b"));
    assert!(a.len() > 800 && a.len() <= BtreeIndex::MAX_KEY_SIZE);
    assert_eq!(cmp.compare(&a, &b), Ordering::Less);
    assert_eq!(cmp.compare(&a, &a.clone()), Ordering::Equal);
}

#[test]
fn new_index_is_empty() {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(4, dm));
    let header_page_id = bpm.new_page();
    let metadata = IndexMetadata::new(
        "users_id_name",
        "users",
        vec![0, 1],
        schema(),
        false,
        LocatorKind::Rid,
    );
    let index = BtreeIndex::new(metadata, header_page_id, bpm).unwrap();
    assert!(index.tree().is_empty().unwrap());
    assert_eq!(index.tree().header_page_id(), header_page_id);
}

/// The table the index tests put an index on.
fn table_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("age", DataType::Int, true),
        Column::new("name", DataType::Varchar(2000), true),
    ])
}

fn row(id: i32, age: Option<i32>, name: &str) -> Tuple {
    let age = age.map_or(Value::Null, Value::Int);
    Tuple::encode(&table_schema(), &[Value::Int(id), age, Value::from(name)]).unwrap()
}

struct IndexFixture {
    _dir: TempDir,
    index: Index,
}

/// An empty index on the columns of `table_schema` at `key_attrs`, storing
/// rids.
fn index_on(name: &str, key_attrs: Vec<usize>, is_primary_key: bool) -> IndexFixture {
    index_with(name, key_attrs, is_primary_key, LocatorKind::Rid)
}

fn index_with(
    name: &str,
    key_attrs: Vec<usize>,
    is_primary_key: bool,
    locator: LocatorKind,
) -> IndexFixture {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(32, dm));
    let header_page_id = bpm.new_page();
    let table = table_schema();
    let key_schema = Arc::new(Schema::new(
        key_attrs
            .iter()
            .map(|&i| table.columns()[i].clone())
            .collect(),
    ));
    let metadata = IndexMetadata::new(
        name,
        "users",
        key_attrs,
        key_schema,
        is_primary_key,
        locator,
    );
    let index = BtreeIndex::new(metadata, header_page_id, bpm).unwrap();
    IndexFixture {
        _dir: dir,
        index: index.into(),
    }
}

/// An empty index on `(name, age)` of `table_schema`.
fn name_age_index() -> IndexFixture {
    index_on("users_name_age", vec![2, 1], false)
}

/// An empty index on `age` of `table_schema`.
fn age_index() -> IndexFixture {
    index_on("users_age", vec![1], false)
}

/// An empty primary key index on `age` of `table_schema`.
fn age_pk_index() -> IndexFixture {
    index_on("users_age_pk", vec![1], true)
}

impl IndexFixture {
    fn btree(&self) -> &BtreeIndex {
        let Index::BtreeIndex(index) = &self.index;
        index
    }

    fn key(&self, row: &Tuple) -> Tuple {
        let meta = self.index.metadata();
        row.to_key(&table_schema(), meta.key_schema(), meta.key_attrs())
            .unwrap()
    }

    fn insert(&self, row: &Tuple, locator: RowLocator) {
        assert!(
            self.index.insert_entry(&self.key(row), &locator).unwrap(),
            "{locator:?}"
        );
    }

    fn lookup(&self, row: &Tuple) -> Vec<RowLocator> {
        let mut result = Vec::new();
        self.index.scan_key(&self.key(row), &mut result).unwrap();
        result
    }

    fn range(&self, lower: Bound<&[Value]>, upper: Bound<&[Value]>) -> Vec<RowLocator> {
        self.btree()
            .scan_range(lower, upper)
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// Every entry in the index, in order, as its key decoded as values and
    /// its record id.
    fn entries(&self) -> Vec<(Vec<Value>, Rid)> {
        let key_schema = self.index.metadata().key_schema();
        self.btree()
            .tree()
            .begin()
            .unwrap()
            .map(|entry| {
                let (entry, rid) = entry.unwrap();
                let key = self.btree().tree().comparator().key_of(&entry);
                let values = Tuple::from_bytes(key.to_vec()).decode(key_schema).unwrap();
                (values, rid)
            })
            .collect()
    }

    /// Every key in the index, in order, decoded as `(name, age)`.
    fn keys(&self) -> Vec<(Value, Value)> {
        self.entries()
            .into_iter()
            .map(|(values, _)| (values[0].clone(), values[1].clone()))
            .collect()
    }
}

fn rid(n: i32) -> RowLocator {
    Rid::new(n, n as u16).into()
}

#[test]
fn insert_then_scan_key() {
    let f = name_age_index();
    let (alice, bob) = (row(1, Some(30), "alice"), row(2, Some(25), "bob"));
    assert!(f.index.insert_entry(&f.key(&alice), &rid(1)).unwrap());
    assert!(f.index.insert_entry(&f.key(&bob), &rid(2)).unwrap());

    assert_eq!(f.lookup(&alice), [rid(1)]);
    assert_eq!(f.lookup(&bob), [rid(2)]);
    // Same name, different age: a different key. The id is not in the key.
    assert!(f.lookup(&row(1, Some(31), "alice")).is_empty());
    assert_eq!(f.lookup(&row(9, Some(30), "alice")), [rid(1)]);
}

#[test]
fn insert_of_existing_entry_changes_nothing() {
    let f = name_age_index();
    let key = f.key(&row(1, Some(30), "alice"));
    assert!(f.index.insert_entry(&key, &rid(1)).unwrap());
    assert!(!f.index.insert_entry(&key, &rid(1)).unwrap());
    assert_eq!(f.lookup(&row(1, Some(30), "alice")), [rid(1)]);
    assert_eq!(f.entries().len(), 1);
}

#[test]
fn rows_with_the_same_key_are_all_found_in_rid_order() {
    let f = name_age_index();
    // Other rows with the same key columns, inserted out of order.
    for n in [3, 1, 4, 2] {
        f.insert(&row(n, Some(30), "alice"), rid(n));
    }
    f.insert(&row(9, Some(31), "alice"), rid(9));
    assert_eq!(
        f.lookup(&row(0, Some(30), "alice")),
        [rid(1), rid(2), rid(3), rid(4)]
    );
    assert_eq!(f.lookup(&row(0, Some(31), "alice")), [rid(9)]);
}

#[test]
fn null_keys_do_not_collide() {
    let f = age_index();
    for n in 0..3 {
        f.insert(&row(n, None, ""), rid(n));
    }
    f.insert(&row(3, Some(1), ""), rid(3));
    assert_eq!(f.lookup(&row(0, None, "")), [rid(0), rid(1), rid(2)]);
}

#[test]
fn delete_entry_removes_only_that_row() {
    let f = name_age_index();
    let alice = row(0, Some(30), "alice");
    for n in 0..3 {
        f.insert(&alice, rid(n));
    }
    f.index.delete_entry(&f.key(&alice), &rid(1)).unwrap();
    assert_eq!(f.lookup(&alice), [rid(0), rid(2)]);
    // A record id the key is not held for is a no-op.
    f.index.delete_entry(&f.key(&alice), &rid(7)).unwrap();
    assert_eq!(f.lookup(&alice), [rid(0), rid(2)]);
}

#[test]
fn keys_are_ordered_by_the_key_schema() {
    let f = name_age_index();
    // Inserted out of order; by bytes, little-endian 256 would sort before 2.
    let rows = [
        row(1, Some(256), "b"),
        row(2, None, "b"),
        row(3, Some(2), "b"),
        row(4, Some(1), "ab"),
        row(5, Some(7), ""),
    ];
    for (i, r) in rows.iter().enumerate() {
        assert!(f.index.insert_entry(&f.key(r), &rid(i as i32)).unwrap());
    }
    let s = |v: &str| Value::from(v);
    assert_eq!(
        f.keys(),
        [
            (s(""), Value::Int(7)),
            (s("ab"), Value::Int(1)),
            // NULL sorts first.
            (s("b"), Value::Null),
            (s("b"), Value::Int(2)),
            (s("b"), Value::Int(256)),
        ]
    );
    assert_eq!(f.lookup(&row(0, None, "b")), [rid(1)]);
}

#[test]
fn many_long_keys_split_and_merge() {
    let f = name_age_index();
    let name = |n: i32| format!("{n:05}{}", "x".repeat((n as usize * 37) % 700));
    let rows: Vec<Tuple> = (0..2000).map(|n| row(n, Some(n % 50), &name(n))).collect();
    for (n, r) in rows.iter().enumerate() {
        assert!(f.index.insert_entry(&f.key(r), &rid(n as i32)).unwrap());
    }
    for (n, r) in rows.iter().enumerate() {
        assert_eq!(f.lookup(r), [rid(n as i32)], "row {n}");
    }
    let names: Vec<Value> = f.keys().into_iter().map(|(name, _)| name).collect();
    assert_eq!(
        names,
        (0..2000)
            .map(|n| Value::from(name(n).as_str()))
            .collect::<Vec<_>>()
    );

    for (n, r) in rows.iter().enumerate().filter(|(n, _)| n % 4 != 0) {
        f.index.delete_entry(&f.key(r), &rid(n as i32)).unwrap();
    }
    for (n, r) in rows.iter().enumerate() {
        let expected = if n % 4 == 0 {
            vec![rid(n as i32)]
        } else {
            vec![]
        };
        assert_eq!(f.lookup(r), expected, "row {n}");
    }
    assert_eq!(f.keys().len(), 500);
}

#[test]
fn key_over_max_size_is_rejected() {
    let f = name_age_index();
    let long = row(1, Some(1), &"x".repeat(BtreeIndex::MAX_KEY_SIZE));
    let err = f.index.insert_entry(&f.key(&long), &rid(1)).unwrap_err();
    assert!(
        matches!(err, StorageError::KeyTooLarge { max, .. } if max == BtreeIndex::MAX_KEY_SIZE)
    );
    assert!(f.lookup(&long).is_empty());

    // The longest key that is accepted, with room left for the record id.
    let empty_len = f.key(&row(1, Some(1), "")).len();
    let longest = row(
        1,
        Some(1),
        &"x".repeat(BtreeIndex::MAX_KEY_SIZE - empty_len),
    );
    assert_eq!(f.key(&longest).len(), BtreeIndex::MAX_KEY_SIZE);
    f.insert(&longest, rid(1));
    f.insert(&longest, rid(2));
    assert_eq!(f.lookup(&longest), [rid(1), rid(2)]);
}

/// Every entry of `f` with its key, as `(key, entry)`.
fn raw_entries(f: &IndexFixture) -> Vec<(Vec<u8>, Vec<u8>)> {
    let tree = f.btree().tree();
    tree.begin()
        .unwrap()
        .map(|e| {
            let entry = e.unwrap().0;
            (tree.comparator().key_of(&entry).to_vec(), entry)
        })
        .collect()
}

#[test]
fn entries_end_in_their_record_id() {
    let f = age_index();
    f.insert(&row(0, Some(30), ""), Rid::new(0x0102_0304, 0x0506).into());
    let [(key, entry)] = &raw_entries(&f)[..] else {
        panic!("not one entry");
    };
    assert_eq!(key, f.key(&row(0, Some(30), "")).data());
    assert_eq!(entry[key.len()..], [4, 3, 2, 1, 6, 5]);
}

#[test]
fn primary_key_entries_end_in_their_record_id() {
    let f = age_pk_index();
    f.insert(&row(0, Some(30), ""), Rid::new(0x0102_0304, 0x0506).into());
    let [(key, entry)] = &raw_entries(&f)[..] else {
        panic!("not one entry");
    };
    assert_eq!(key, f.key(&row(0, Some(30), "")).data());
    assert_eq!(entry[key.len()..], [4, 3, 2, 1, 6, 5]);
}

/// The index holds a primary key for many rids: old versions of a row keep
/// their entries. `Transaction::insert_entry` checks uniqueness.
#[test]
fn primary_key_holds_a_key_for_many_rids() {
    let f = age_pk_index();
    let key = f.key(&row(1, Some(30), ""));
    assert!(f.index.insert_entry(&key, &rid(1)).unwrap());
    assert!(f.index.insert_entry(&key, &rid(2)).unwrap());
    assert!(!f.index.insert_entry(&key, &rid(1)).unwrap());
    assert_eq!(f.lookup(&row(0, Some(30), "")), [rid(1), rid(2)]);
    assert!(f.index.delete_entry(&key, &rid(1)).unwrap());
    assert_eq!(f.lookup(&row(0, Some(30), "")), [rid(2)]);
}

#[test]
fn primary_key_delete_frees_the_key() {
    let f = age_pk_index();
    let key = f.key(&row(1, Some(30), ""));
    f.insert(&row(1, Some(30), ""), rid(1));
    f.index.delete_entry(&key, &rid(1)).unwrap();
    assert!(f.lookup(&row(0, Some(30), "")).is_empty());
    assert!(f.index.insert_entry(&key, &rid(2)).unwrap());
    assert_eq!(f.lookup(&row(0, Some(30), "")), [rid(2)]);
}

#[test]
fn primary_key_range_scans() {
    let f = age_pk_index();
    for (n, age) in [(0, 30), (1, 10), (2, 40), (3, 20)] {
        f.insert(&row(n, Some(age), ""), rid(n));
    }
    f.insert(&row(4, None, ""), rid(4));
    let (a10, a15, a20, a30, a40) = (age(10), age(15), age(20), age(30), age(40));

    assert_eq!(f.range(Unbounded, Unbounded), rids(&[4, 1, 3, 0, 2]));
    assert_eq!(f.range(Included(&a20), Unbounded), rids(&[3, 0, 2]));
    assert_eq!(f.range(Excluded(&a20), Unbounded), rids(&[0, 2]));
    // A bound the index does not hold.
    assert_eq!(f.range(Excluded(&a15), Unbounded), rids(&[3, 0, 2]));
    assert_eq!(f.range(Included(&a15), Unbounded), rids(&[3, 0, 2]));
    assert_eq!(f.range(Unbounded, Excluded(&a20)), rids(&[4, 1]));
    assert_eq!(f.range(Unbounded, Included(&a20)), rids(&[4, 1, 3]));
    assert_eq!(f.range(Included(&a10), Excluded(&a40)), rids(&[1, 3, 0]));
    assert_eq!(f.range(Excluded(&a10), Excluded(&a30)), rids(&[3]));
    assert!(f.range(Excluded(&a40), Unbounded).is_empty());
}

#[test]
fn primary_key_of_max_size() {
    let f = index_on("users_name_pk", vec![2], true);
    let empty_len = f.key(&row(1, None, "")).len();
    let longest = row(1, None, &"x".repeat(BtreeIndex::MAX_KEY_SIZE - empty_len));
    assert_eq!(f.key(&longest).len(), BtreeIndex::MAX_KEY_SIZE);
    f.insert(&longest, rid(1));
    assert_eq!(f.lookup(&longest), [rid(1)]);
    let too_long = row(
        1,
        None,
        &"x".repeat(BtreeIndex::MAX_KEY_SIZE - empty_len + 1),
    );
    assert!(f.index.insert_entry(&f.key(&too_long), &rid(2)).is_err());
}

#[test]
fn many_primary_keys_split_and_merge() {
    let f = index_on("users_name_pk", vec![2], true);
    let name = |n: i32| format!("{n:05}{}", "x".repeat((n as usize * 37) % 700));
    for n in 0..2000 {
        f.insert(&row(n, None, &name(n)), rid(n));
    }
    for n in (0..2000).filter(|n| n % 4 != 0) {
        f.index
            .delete_entry(&f.key(&row(n, None, &name(n))), &rid(n))
            .unwrap();
    }
    assert_eq!(
        f.range(Unbounded, Unbounded),
        (0..2000)
            .filter(|n| n % 4 == 0)
            .map(rid)
            .collect::<Vec<_>>()
    );
    for n in 0..2000 {
        let expected = if n % 4 == 0 { vec![rid(n)] } else { vec![] };
        assert_eq!(f.lookup(&row(0, None, &name(n))), expected, "row {n}");
    }
}

/// An index on `age` holding, in order:
///
/// ```text
/// age  NULL NULL  20  25  30  30  30  40  40
/// rid     6    7   3   8   0   1   2   4   5
/// ```
fn ages() -> IndexFixture {
    let f = age_index();
    let ages = [30, 30, 30, 20, 40, 40, -1, -1, 25];
    for n in [2, 7, 0, 5, 3, 8, 1, 6, 4] {
        let age = ages[n as usize];
        f.insert(&row(n, (age >= 0).then_some(age), ""), rid(n));
    }
    f
}

fn age(age: i32) -> [Value; 1] {
    [Value::Int(age)]
}

fn rids(ns: &[i32]) -> Vec<RowLocator> {
    ns.iter().map(|&n| rid(n)).collect()
}

#[test]
fn range_scans() {
    let f = ages();
    let (a20, a21, a30, a31, a39, a40) = (age(20), age(21), age(30), age(31), age(39), age(40));

    // The whole index.
    assert_eq!(
        f.range(Unbounded, Unbounded),
        rids(&[6, 7, 3, 8, 0, 1, 2, 4, 5])
    );
    // `>= 30` and `> 30`.
    assert_eq!(f.range(Included(&a30), Unbounded), rids(&[0, 1, 2, 4, 5]));
    assert_eq!(f.range(Excluded(&a30), Unbounded), rids(&[4, 5]));
    // `< 30` and `<= 30`, NULLs first.
    assert_eq!(f.range(Unbounded, Excluded(&a30)), rids(&[6, 7, 3, 8]));
    assert_eq!(
        f.range(Unbounded, Included(&a30)),
        rids(&[6, 7, 3, 8, 0, 1, 2])
    );
    // Between, with bounds held and not held.
    assert_eq!(
        f.range(Included(&a20), Included(&a30)),
        rids(&[3, 8, 0, 1, 2])
    );
    assert_eq!(f.range(Included(&a20), Excluded(&a30)), rids(&[3, 8]));
    assert_eq!(f.range(Excluded(&a20), Excluded(&a40)), rids(&[8, 0, 1, 2]));
    assert_eq!(f.range(Included(&a21), Excluded(&a40)), rids(&[8, 0, 1, 2]));
    // Nothing in range.
    assert!(f.range(Included(&a31), Excluded(&a39)).is_empty());
    assert!(f.range(Excluded(&a40), Unbounded).is_empty());
    assert!(f.range(Included(&a30), Excluded(&a30)).is_empty());
}

#[test]
fn range_scan_of_empty_index() {
    let f = age_index();
    assert!(f.range(Unbounded, Unbounded).is_empty());
    assert!(f.range(Included(&age(1)), Unbounded).is_empty());
}

#[test]
fn many_rows_with_one_key_span_leaves() {
    let f = name_age_index();
    let name = "x".repeat(500);
    let shared = row(0, Some(1), &name);
    for n in 0..2000 {
        f.insert(&shared, rid(n));
    }
    // Keys on either side.
    f.insert(&row(0, Some(0), &name), rid(5000));
    f.insert(&row(0, Some(2), &name), rid(5001));
    assert_eq!(f.lookup(&shared), (0..2000).map(rid).collect::<Vec<_>>());

    for n in (0..2000).filter(|n| n % 3 != 0) {
        f.index.delete_entry(&f.key(&shared), &rid(n)).unwrap();
    }
    assert_eq!(
        f.lookup(&shared),
        (0..2000)
            .filter(|n| n % 3 == 0)
            .map(rid)
            .collect::<Vec<_>>()
    );
    let key = [Value::from(name.as_str()), Value::Int(1)];
    assert_eq!(f.range(Excluded(&key), Unbounded), [rid(5001)]);
    assert_eq!(f.range(Unbounded, Excluded(&key)), [rid(5000)]);
}

/// A `(name, age)` index holding, in order:
///
/// ```text
/// name  alice  alice  alice  bob   bob  bob  carol
/// age      25     30     30  NULL   40   40     20
/// rid       1      0      5     3    2    6      4
/// ```
fn names_and_ages() -> IndexFixture {
    let f = name_age_index();
    let rows = [
        (0, "alice", Some(30)),
        (1, "alice", Some(25)),
        (2, "bob", Some(40)),
        (3, "bob", None),
        (4, "carol", Some(20)),
        (5, "alice", Some(30)),
        (6, "bob", Some(40)),
    ];
    for (n, name, age) in rows {
        f.insert(&row(n, age, name), rid(n));
    }
    f
}

fn name(name: &str) -> [Value; 1] {
    [Value::from(name)]
}

fn name_age(name: &str, age: i32) -> [Value; 2] {
    [Value::from(name), Value::Int(age)]
}

#[test]
fn range_scans_on_a_key_prefix() {
    let f = names_and_ages();
    let (alice, bob, carol) = (name("alice"), name("bob"), name("carol"));

    // `name = 'alice'`.
    assert_eq!(
        f.range(Included(&alice), Included(&alice)),
        rids(&[1, 0, 5])
    );
    // `name > 'alice'` and `name >= 'bob'`.
    assert_eq!(f.range(Excluded(&alice), Unbounded), rids(&[3, 2, 6, 4]));
    assert_eq!(f.range(Included(&bob), Unbounded), rids(&[3, 2, 6, 4]));
    // `name < 'bob'` and `name <= 'bob'`.
    assert_eq!(f.range(Unbounded, Excluded(&bob)), rids(&[1, 0, 5]));
    assert_eq!(
        f.range(Unbounded, Included(&bob)),
        rids(&[1, 0, 5, 3, 2, 6])
    );
    // `name > 'alice' AND name < 'carol'`.
    assert_eq!(
        f.range(Excluded(&alice), Excluded(&carol)),
        rids(&[3, 2, 6])
    );
    // Names the index does not hold.
    assert_eq!(
        f.range(Excluded(&name("b")), Unbounded),
        rids(&[3, 2, 6, 4])
    );
    assert_eq!(
        f.range(Included(&name("b")), Excluded(&name("c"))),
        rids(&[3, 2, 6])
    );
    assert!(
        f.range(Included(&name("bo")), Excluded(&name("bob")))
            .is_empty()
    );
    assert!(f.range(Excluded(&carol), Unbounded).is_empty());
}

#[test]
fn bounds_of_different_lengths() {
    let f = names_and_ages();
    let (alice, bob) = (name("alice"), name("bob"));
    // `name = 'alice' AND age > 25`.
    assert_eq!(
        f.range(Excluded(&name_age("alice", 25)), Included(&alice)),
        rids(&[0, 5])
    );
    // `name = 'bob' AND age >= 30`: NULL sorts first, so it is left out.
    assert_eq!(
        f.range(Included(&name_age("bob", 30)), Included(&bob)),
        rids(&[2, 6])
    );
    // `(name, age) >= ('alice', 30) AND name < 'bob'`.
    assert_eq!(
        f.range(Included(&name_age("alice", 30)), Excluded(&bob)),
        rids(&[0, 5])
    );
    // `(name, age) > ('alice', 30)`, up to every `bob`.
    assert_eq!(
        f.range(Excluded(&name_age("alice", 30)), Included(&bob)),
        rids(&[3, 2, 6])
    );
}

#[test]
fn empty_prefix_bounds_every_key() {
    let f = names_and_ages();
    let all = rids(&[1, 0, 5, 3, 2, 6, 4]);
    assert_eq!(f.range(Included(&[]), Included(&[])), all);
    assert_eq!(f.range(Unbounded, Unbounded), all);
    assert!(f.range(Excluded(&[]), Unbounded).is_empty());
    assert!(f.range(Unbounded, Excluded(&[])).is_empty());
}

#[test]
fn bad_bounds_are_rejected() {
    let f = names_and_ages();
    let too_long = [Value::from("alice"), Value::Int(30), Value::Int(1)];
    let Err(err) = f.btree().scan_range(Included(&too_long), Unbounded) else {
        panic!("a bound longer than the key");
    };
    assert!(matches!(
        err,
        StorageError::Value(ValueError::ColumnCountMismatch {
            expected: 2,
            found: 3
        })
    ));
    let wrong_type = [Value::Int(1)];
    assert!(matches!(
        f.btree().scan_range(Unbounded, Included(&wrong_type)),
        Err(StorageError::Value(_))
    ));
}

#[test]
fn primary_key_range_scans_on_a_key_prefix() {
    let f = index_on("users_name_age_pk", vec![2, 1], true);
    let rows = [
        (0, "alice", 30),
        (1, "alice", 25),
        (2, "bob", 40),
        (3, "carol", 20),
    ];
    for (n, name, age) in rows {
        f.insert(&row(n, Some(age), name), rid(n));
    }
    let alice = name("alice");
    assert_eq!(f.range(Included(&alice), Included(&alice)), rids(&[1, 0]));
    assert_eq!(f.range(Excluded(&alice), Unbounded), rids(&[2, 3]));
    assert_eq!(
        f.range(Excluded(&name_age("alice", 25)), Excluded(&name("carol"))),
        rids(&[0, 2])
    );
}

#[test]
fn prefix_scans_across_many_leaves() {
    for is_primary_key in [false, true] {
        let f = index_on("users_name_age", vec![2, 1], is_primary_key);
        // 50 names of 300 bytes, each with ages 0..40.
        let name_of = |i: i32| format!("{i:03}{}", "x".repeat(300));
        let mut order: Vec<i32> = (0..2000).collect();
        order.sort_by_key(|n| (n * 7919) % 2000);
        for n in order {
            f.insert(&row(n, Some(n % 40), &name_of(n / 40)), rid(n));
        }
        let (lo, hi) = (name(&name_of(10)), name(&name_of(12)));
        assert_eq!(
            f.range(Included(&lo), Included(&lo)),
            (400..440).map(rid).collect::<Vec<_>>(),
            "primary key {is_primary_key}"
        );
        assert_eq!(
            f.range(Excluded(&lo), Excluded(&hi)),
            (440..480).map(rid).collect::<Vec<_>>(),
            "primary key {is_primary_key}"
        );
        assert_eq!(
            f.range(Excluded(&lo), Unbounded),
            (440..2000).map(rid).collect::<Vec<_>>(),
            "primary key {is_primary_key}"
        );
        assert_eq!(
            f.range(Unbounded, Excluded(&lo)),
            (0..400).map(rid).collect::<Vec<_>>(),
            "primary key {is_primary_key}"
        );
    }
}

/// The schema of the primary keys the `pk_*` indexes store: `id`.
fn pk_schema() -> Schema {
    Schema::new(vec![Column::new("id", DataType::Int, false)])
}

/// A locator for the row whose primary key is `id`.
fn pk(id: i32) -> RowLocator {
    RowLocator::PrimaryKey(Tuple::encode(&pk_schema(), &[Value::Int(id)]).unwrap())
}

fn pks(ids: &[i32]) -> Vec<RowLocator> {
    ids.iter().map(|&id| pk(id)).collect()
}

/// An empty index on `(name, age)` of `table_schema`, storing primary keys.
fn pk_name_age_index() -> IndexFixture {
    index_with("users_name_age", vec![2, 1], false, LocatorKind::PrimaryKey)
}

/// An empty index on `age` of `table_schema`, storing primary keys.
fn pk_age_index() -> IndexFixture {
    index_with("users_age", vec![1], false, LocatorKind::PrimaryKey)
}

/// Inserts each row of `table_schema` as `(id, name, age)`, with its id as
/// its primary key.
fn insert_by_pk(f: &IndexFixture, rows: &[(i32, &str, Option<i32>)]) {
    for &(id, name, age) in rows {
        f.insert(&row(id, age, name), pk(id));
    }
}

#[test]
fn pk_locators_are_found_by_key_and_range() {
    let f = pk_name_age_index();
    assert_eq!(f.index.metadata().locator(), LocatorKind::PrimaryKey);
    insert_by_pk(
        &f,
        &[
            (7, "alice", Some(30)),
            (3, "bob", Some(25)),
            (5, "carol", None),
        ],
    );
    assert_eq!(f.lookup(&row(0, Some(30), "alice")), pks(&[7]));
    assert_eq!(f.lookup(&row(0, Some(25), "bob")), pks(&[3]));
    assert!(f.lookup(&row(0, Some(31), "alice")).is_empty());
    assert_eq!(f.range(Unbounded, Unbounded), pks(&[7, 3, 5]));
    assert_eq!(
        f.range(Excluded(&name("alice")), Included(&name("bob"))),
        pks(&[3])
    );
}

#[test]
fn pk_entries_end_in_the_primary_key_and_its_length() {
    let f = pk_age_index();
    f.insert(&row(0, Some(30), ""), pk(9));
    let [(key, entry)] = &raw_entries(&f)[..] else {
        panic!("not one entry");
    };
    assert_eq!(key, f.key(&row(0, Some(30), "")).data());
    let RowLocator::PrimaryKey(pk_bytes) = pk(9) else {
        unreachable!()
    };
    let pk_bytes = pk_bytes.data();
    let len = (pk_bytes.len() as u16).to_le_bytes();
    assert_eq!(entry[key.len()..], [pk_bytes, &len[..]].concat());
}

#[test]
fn rows_with_the_same_key_are_all_found_in_primary_key_byte_order() {
    let f = pk_name_age_index();
    // Inserted out of order, with another key on either side.
    for id in [3, 1, 4, 2] {
        f.insert(&row(id, Some(30), "alice"), pk(id));
    }
    insert_by_pk(&f, &[(9, "alice", Some(29)), (8, "alice", Some(31))]);
    assert_eq!(f.lookup(&row(0, Some(30), "alice")), pks(&[1, 2, 3, 4]));

    // The primary keys are ordered by their encoded bytes, not their
    // values: little-endian 256 comes before 1.
    f.insert(&row(256, Some(30), "alice"), pk(256));
    assert_eq!(
        f.lookup(&row(0, Some(30), "alice")),
        pks(&[256, 1, 2, 3, 4])
    );
}

#[test]
fn pk_insert_of_existing_entry_changes_nothing() {
    let f = pk_age_index();
    let key = f.key(&row(1, Some(30), ""));
    assert!(f.index.insert_entry(&key, &pk(1)).unwrap());
    assert!(!f.index.insert_entry(&key, &pk(1)).unwrap());
    // Another row with the same key is a different entry.
    assert!(f.index.insert_entry(&key, &pk(2)).unwrap());
    assert_eq!(f.lookup(&row(0, Some(30), "")), pks(&[1, 2]));
}

#[test]
fn pk_delete_entry_removes_only_that_row() {
    let f = pk_name_age_index();
    let alice = row(0, Some(30), "alice");
    for id in 0..3 {
        f.insert(&alice, pk(id));
    }
    f.insert(&row(1, Some(31), "alice"), pk(1));
    f.index.delete_entry(&f.key(&alice), &pk(1)).unwrap();
    assert_eq!(f.lookup(&alice), pks(&[0, 2]));
    // The same primary key under another key keeps its entry.
    assert_eq!(f.lookup(&row(0, Some(31), "alice")), pks(&[1]));
    // A primary key the key is not held for is a no-op.
    f.index.delete_entry(&f.key(&alice), &pk(7)).unwrap();
    assert_eq!(f.lookup(&alice), pks(&[0, 2]));
}

#[test]
fn pk_prefix_bounds_ignore_the_primary_key() {
    // As `names_and_ages`, storing primary keys.
    let f = pk_name_age_index();
    insert_by_pk(
        &f,
        &[
            (0, "alice", Some(30)),
            (1, "alice", Some(25)),
            (2, "bob", Some(40)),
            (3, "bob", None),
            (4, "carol", Some(20)),
            (5, "alice", Some(30)),
            (6, "bob", Some(40)),
        ],
    );
    let (alice, bob, carol) = (name("alice"), name("bob"), name("carol"));
    assert_eq!(f.range(Included(&alice), Included(&alice)), pks(&[1, 0, 5]));
    assert_eq!(f.range(Excluded(&alice), Unbounded), pks(&[3, 2, 6, 4]));
    assert_eq!(f.range(Unbounded, Excluded(&bob)), pks(&[1, 0, 5]));
    assert_eq!(f.range(Excluded(&alice), Excluded(&carol)), pks(&[3, 2, 6]));
    assert_eq!(
        f.range(Excluded(&name_age("alice", 25)), Included(&alice)),
        pks(&[0, 5])
    );
    assert_eq!(
        f.range(Excluded(&name_age("alice", 30)), Included(&bob)),
        pks(&[3, 2, 6])
    );
}

#[test]
fn many_pk_entries_with_one_key_span_leaves() {
    let f = pk_name_age_index();
    let name = "x".repeat(500);
    let shared = row(0, Some(1), &name);
    for id in 0..2000 {
        f.insert(&shared, pk(id));
    }
    f.insert(&row(0, Some(0), &name), pk(5000));
    f.insert(&row(0, Some(2), &name), pk(5001));
    let mut found = f.lookup(&shared);
    assert_eq!(found.len(), 2000);
    let id = |locator: &RowLocator| match locator {
        RowLocator::PrimaryKey(pk) => match pk.decode(&pk_schema()).unwrap()[..] {
            [Value::Int(id)] => id,
            ref values => panic!("{values:?}"),
        },
        RowLocator::Rid(_) => panic!("a record id"),
    };
    found.sort_by_key(id);
    assert_eq!(found, (0..2000).map(pk).collect::<Vec<_>>());

    for id in (0..2000).filter(|id| id % 3 != 0) {
        f.index.delete_entry(&f.key(&shared), &pk(id)).unwrap();
    }
    assert_eq!(f.lookup(&shared).len(), 667);
    let key = [Value::from(name.as_str()), Value::Int(1)];
    assert_eq!(f.range(Excluded(&key), Unbounded), pks(&[5001]));
    assert_eq!(f.range(Unbounded, Excluded(&key)), pks(&[5000]));
}

#[test]
fn key_too_large_counting_the_primary_key_is_rejected() {
    let f = pk_name_age_index();
    // A long primary key, which the index stores but does not decode.
    let long_pk_schema = Schema::new(vec![Column::new("code", DataType::Varchar(500), false)]);
    let long_pk = RowLocator::PrimaryKey(
        Tuple::encode(&long_pk_schema, &[Value::from("p".repeat(100).as_str())]).unwrap(),
    );
    let RowLocator::PrimaryKey(pk_bytes) = &long_pk else {
        unreachable!()
    };
    // The tree's limit on entries, less the primary key and its length.
    let max = BtreeIndex::MAX_KEY_SIZE + 6 - pk_bytes.data().len() - 2;
    assert!(max < BtreeIndex::MAX_KEY_SIZE);

    let empty_len = f.key(&row(1, Some(1), "")).len();
    let longest = row(1, Some(1), &"x".repeat(max - empty_len));
    let too_long = row(1, Some(1), &"x".repeat(max - empty_len + 1));
    assert_eq!(f.key(&longest).len(), max);

    let err = f
        .index
        .insert_entry(&f.key(&too_long), &long_pk)
        .unwrap_err();
    assert!(
        matches!(err, StorageError::KeyTooLarge { size, max: m } if size == max + 1 && m == max),
        "{err:?}"
    );
    assert!(f.lookup(&too_long).is_empty());
    // A key the primary key leaves room for.
    f.insert(&longest, long_pk.clone());
    assert_eq!(f.lookup(&longest), [long_pk]);
}
