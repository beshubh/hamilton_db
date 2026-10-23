use std::cmp::Ordering;
use std::sync::Arc;

use common::{Column, DataType, Rid, Schema, Tuple, Value};
use storage::StorageError;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::btree_index::{BtreeIndex, KeySchemaComparator};
use storage::index::{Index, IndexMetadata, IndexTrait};
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
    let metadata = IndexMetadata::new("users_id_name", "users", vec![0, 1], schema(), false);
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

/// An empty index on `(name, age)` of `table_schema`.
fn name_age_index() -> IndexFixture {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(32, dm));
    let header_page_id = bpm.new_page();
    let key_schema = Arc::new(Schema::new(vec![
        Column::new("name", DataType::Varchar(2000), true),
        Column::new("age", DataType::Int, true),
    ]));
    let metadata = IndexMetadata::new("users_name_age", "users", vec![2, 1], key_schema, false);
    let index = BtreeIndex::new(metadata, header_page_id, bpm).unwrap();
    IndexFixture {
        _dir: dir,
        index: index.into(),
    }
}

impl IndexFixture {
    fn key(&self, row: &Tuple) -> Tuple {
        let meta = self.index.metadata();
        row.to_key(&table_schema(), meta.key_schema(), meta.key_attrs())
            .unwrap()
    }

    fn lookup(&self, row: &Tuple) -> Vec<Rid> {
        let mut result = Vec::new();
        self.index.scan_key(&self.key(row), &mut result).unwrap();
        result
    }

    /// Every key in the index, in order, decoded as `(name, age)`.
    fn keys(&self) -> Vec<(Value, Value)> {
        let Index::BtreeIndex(index) = &self.index;
        let key_schema = self.index.metadata().key_schema();
        index
            .tree()
            .begin()
            .unwrap()
            .map(|entry| {
                let values = Tuple::from_bytes(entry.unwrap().0)
                    .decode(key_schema)
                    .unwrap();
                (values[0].clone(), values[1].clone())
            })
            .collect()
    }
}

fn rid(n: i32) -> Rid {
    Rid::new(n, n as u16)
}

#[test]
fn insert_then_scan_key() {
    let f = name_age_index();
    let (alice, bob) = (row(1, Some(30), "alice"), row(2, Some(25), "bob"));
    assert!(f.index.insert_entry(&f.key(&alice), rid(1)).unwrap());
    assert!(f.index.insert_entry(&f.key(&bob), rid(2)).unwrap());

    assert_eq!(f.lookup(&alice), [rid(1)]);
    assert_eq!(f.lookup(&bob), [rid(2)]);
    // Same name, different age: a different key. The id is not in the key.
    assert!(f.lookup(&row(1, Some(31), "alice")).is_empty());
    assert_eq!(f.lookup(&row(9, Some(30), "alice")), [rid(1)]);
}

#[test]
fn insert_of_existing_key_changes_nothing() {
    let f = name_age_index();
    let key = f.key(&row(1, Some(30), "alice"));
    assert!(f.index.insert_entry(&key, rid(1)).unwrap());
    // Another row with the same key columns.
    assert!(
        !f.index
            .insert_entry(&f.key(&row(2, Some(30), "alice")), rid(2))
            .unwrap()
    );
    assert_eq!(f.lookup(&row(1, Some(30), "alice")), [rid(1)]);
}

#[test]
fn delete_entry_removes_the_key() {
    let f = name_age_index();
    let (alice, bob) = (row(1, Some(30), "alice"), row(2, Some(25), "bob"));
    f.index.insert_entry(&f.key(&alice), rid(1)).unwrap();
    f.index.insert_entry(&f.key(&bob), rid(2)).unwrap();

    f.index.delete_entry(&f.key(&alice), rid(1)).unwrap();
    assert!(f.lookup(&alice).is_empty());
    assert_eq!(f.lookup(&bob), [rid(2)]);
    // Deleting a missing key is a no-op.
    f.index.delete_entry(&f.key(&alice), rid(1)).unwrap();

    assert!(f.index.insert_entry(&f.key(&alice), rid(3)).unwrap());
    assert_eq!(f.lookup(&alice), [rid(3)]);
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
        assert!(f.index.insert_entry(&f.key(r), rid(i as i32)).unwrap());
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
        assert!(f.index.insert_entry(&f.key(r), rid(n as i32)).unwrap());
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
        f.index.delete_entry(&f.key(r), rid(n as i32)).unwrap();
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
    let err = f.index.insert_entry(&f.key(&long), rid(1)).unwrap_err();
    assert!(
        matches!(err, StorageError::KeyTooLarge { max, .. } if max == BtreeIndex::MAX_KEY_SIZE)
    );
    assert!(f.lookup(&long).is_empty());
}
