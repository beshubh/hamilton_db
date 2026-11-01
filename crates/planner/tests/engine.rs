//! The execution engine's sessions: autocommit, transaction blocks, failed
//! blocks, index entries undone with their transaction, and indexes created
//! on tables other sessions are using.

use std::sync::{Arc, Barrier};
use std::thread;

use catalog::Catalog;
use common::Value;
use execution::ExecutionError;
use planner::{EngineError, ExecutionEngine, Output, TransactionStatus};
use storage::StorageError;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::Index;
use tempfile::TempDir;

struct Db {
    _dir: TempDir,
    engine: ExecutionEngine,
}

impl Db {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(16, dm));
        let catalog = Arc::new(Catalog::open(bpm).unwrap());
        Self {
            _dir: dir,
            engine: ExecutionEngine::new(catalog).unwrap(),
        }
    }

    /// `accounts`, with a primary key and a secondary index storing it, and
    /// `log`, with no primary key and an index storing rids.
    fn with_tables() -> Self {
        let mut db = Self::new();
        db.ok("CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(16), balance INT)");
        db.ok("CREATE INDEX accounts_name ON accounts (name)");
        db.ok("CREATE TABLE log (id INT, note VARCHAR(16))");
        db.ok("CREATE INDEX log_id ON log (id)");
        db.ok("INSERT INTO accounts VALUES (1, 'ann', 100), (2, 'bo', 50)");
        db.ok("INSERT INTO log VALUES (1, 'opened'), (2, 'opened')");
        db
    }

    /// Runs one statement.
    fn try_run(&mut self, sql: &str) -> Result<Output, EngineError> {
        let mut stmts = self.engine.parse(sql)?;
        assert_eq!(stmts.len(), 1, "{sql}");
        self.engine.execute(&stmts.remove(0))
    }

    fn ok(&mut self, sql: &str) -> Output {
        self.try_run(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.ok(sql) {
            Output::Rows { rows, .. } => rows,
            output => panic!("{sql}: {output:?}"),
        }
    }

    /// The raw entries of index `name`, in order.
    fn index_entries(&self, name: &str) -> Vec<Vec<u8>> {
        let index = self.engine.catalog().get_index(name).unwrap();
        let Index::BtreeIndex(btree) = &*index.index;
        btree
            .tree()
            .begin()
            .unwrap()
            .map(|entry| entry.unwrap().0)
            .collect()
    }

    /// The entries of every index, by name.
    fn all_entries(&self) -> Vec<Vec<Vec<u8>>> {
        ["accounts_pkey", "accounts_name", "log_id"]
            .iter()
            .map(|name| self.index_entries(name))
            .collect()
    }
}

fn int(v: i32) -> Value {
    Value::Int(v)
}

fn text(s: &str) -> Value {
    Value::from(s)
}

fn account(id: i32, name: &str, balance: i32) -> Vec<Value> {
    vec![int(id), text(name), int(balance)]
}

fn done(tag: &'static str) -> Output {
    Output::Done(tag)
}

#[test]
fn autocommit_commits_on_success_and_aborts_on_failure() {
    let mut db = Db::with_tables();
    assert_eq!(db.engine.status(), TransactionStatus::Idle);
    // Row 3 is inserted before row 1 fails as a duplicate.
    let err = db.try_run("INSERT INTO accounts VALUES (3, 'cy', 0), (1, 'dup', 0)");
    assert!(matches!(
        err,
        Err(EngineError::Execution(ExecutionError::DuplicateKey { .. }))
    ));
    assert_eq!(db.engine.status(), TransactionStatus::Idle);
    assert_eq!(
        db.rows("SELECT id FROM accounts WHERE id = 3"),
        Vec::<Vec<Value>>::new()
    );
    assert!(
        db.rows("SELECT id FROM accounts WHERE name = 'cy'")
            .is_empty()
    );

    db.ok("INSERT INTO accounts VALUES (3, 'cy', 0)");
    assert_eq!(db.rows("SELECT id FROM accounts WHERE id = 3"), [[int(3)]]);
}

#[test]
fn rollback_of_inserts_leaves_no_rows_or_index_entries() {
    let mut db = Db::with_tables();
    let before = db.all_entries();
    assert_eq!(db.ok("BEGIN"), done("BEGIN"));
    assert_eq!(db.engine.status(), TransactionStatus::InBlock);
    db.ok("INSERT INTO accounts VALUES (3, 'cy', 10), (4, 'ann', 20)");
    db.ok("INSERT INTO log VALUES (3, 'opened')");
    assert_ne!(db.all_entries(), before);
    assert_eq!(db.ok("ROLLBACK"), done("ROLLBACK"));
    assert_eq!(db.engine.status(), TransactionStatus::Idle);

    assert_eq!(db.all_entries(), before);
    assert_eq!(db.rows("SELECT id FROM accounts").len(), 2);
    assert_eq!(db.rows("SELECT id FROM log").len(), 2);
    // The keys are free again.
    db.ok("INSERT INTO accounts VALUES (3, 'cy', 10)");
    assert_eq!(
        db.rows("SELECT name FROM accounts WHERE id = 3"),
        [[text("cy")]]
    );
}

#[test]
fn rollback_of_updates_and_deletes_restores_rows_and_index_entries() {
    let mut db = Db::with_tables();
    let before = db.all_entries();
    let accounts = db.rows("SELECT * FROM accounts");
    let log = db.rows("SELECT * FROM log");

    db.ok("BEGIN");
    // Moves every primary key, and with it each row's accounts_name entry.
    db.ok("UPDATE accounts SET id = id + 10, name = 'zed'");
    db.ok("UPDATE log SET id = id * 100");
    db.ok("DELETE FROM accounts WHERE id = 11");
    db.ok("DELETE FROM log WHERE id = 200");
    assert_eq!(db.rows("SELECT * FROM accounts"), [account(12, "zed", 50)]);
    db.ok("ROLLBACK");

    assert_eq!(db.all_entries(), before);
    assert_eq!(db.rows("SELECT * FROM accounts"), accounts);
    assert_eq!(db.rows("SELECT * FROM log"), log);
    assert_eq!(
        db.rows("SELECT id FROM accounts WHERE name = 'bo'"),
        [[int(2)]]
    );
    assert_eq!(
        db.rows("SELECT note FROM log WHERE id = 2"),
        [[text("opened")]]
    );
}

#[test]
fn committed_changes_are_seen_by_later_statements_and_transactions() {
    let mut db = Db::with_tables();
    db.ok("BEGIN");
    db.ok("UPDATE accounts SET balance = balance - 10 WHERE id = 1");
    db.ok("UPDATE accounts SET balance = balance + 10 WHERE id = 2");
    assert_eq!(db.ok("COMMIT"), done("COMMIT"));
    assert_eq!(
        db.rows("SELECT * FROM accounts"),
        [account(1, "ann", 90), account(2, "bo", 60)]
    );

    db.ok("BEGIN");
    assert_eq!(
        db.rows("SELECT balance FROM accounts WHERE id = 2"),
        [[int(60)]]
    );
    db.ok("END");
}

#[test]
fn reads_in_a_block_see_its_writes() {
    let mut db = Db::with_tables();
    db.ok("BEGIN");
    db.ok("INSERT INTO accounts VALUES (3, 'cy', 5)");
    db.ok("UPDATE accounts SET name = 'ann2' WHERE id = 1");
    db.ok("DELETE FROM accounts WHERE id = 2");
    // Through a scan, the primary key index and the secondary index.
    assert_eq!(
        db.rows("SELECT * FROM accounts"),
        [account(1, "ann2", 100), account(3, "cy", 5)]
    );
    assert_eq!(
        db.rows("SELECT name FROM accounts WHERE id = 3"),
        [[text("cy")]]
    );
    assert!(db.rows("SELECT name FROM accounts WHERE id = 2").is_empty());
    assert_eq!(
        db.rows("SELECT id FROM accounts WHERE name = 'ann2'"),
        [[int(1)]]
    );
    assert!(
        db.rows("SELECT id FROM accounts WHERE name = 'ann'")
            .is_empty()
    );
    db.ok("COMMIT");
}

#[test]
fn an_error_fails_the_block() {
    let mut db = Db::with_tables();
    let before = db.all_entries();
    db.ok("BEGIN");
    db.ok("INSERT INTO accounts VALUES (3, 'cy', 5)");
    assert!(matches!(
        db.try_run("SELECT 1 / 0"),
        Err(EngineError::Execution(_))
    ));
    assert_eq!(db.engine.status(), TransactionStatus::Failed);
    // Its writes are gone at once.
    assert_eq!(db.all_entries(), before);

    for sql in [
        "SELECT * FROM accounts",
        "INSERT INTO accounts VALUES (4, 'dee', 0)",
        "SELECT * FROM missing",
        "BEGIN",
    ] {
        assert!(
            matches!(db.try_run(sql), Err(EngineError::InFailedTransaction)),
            "{sql}"
        );
    }
    assert_eq!(db.ok("COMMIT"), done("ROLLBACK"));
    assert_eq!(db.engine.status(), TransactionStatus::Idle);
    assert_eq!(db.rows("SELECT id FROM accounts").len(), 2);
}

#[test]
fn errors_before_execution_fail_the_block_too() {
    let mut db = Db::with_tables();
    for bad in ["SELEC 1", "SELECT * FROM missing"] {
        db.ok("BEGIN");
        db.ok("INSERT INTO accounts VALUES (3, 'cy', 5)");
        assert!(db.try_run(bad).is_err(), "{bad}");
        assert_eq!(db.engine.status(), TransactionStatus::Failed, "{bad}");
        assert_eq!(db.ok("ROLLBACK"), done("ROLLBACK"));
        assert_eq!(db.rows("SELECT id FROM accounts").len(), 2);
    }
}

#[test]
fn ddl_in_a_block_fails_it() {
    let mut db = Db::with_tables();
    db.ok("BEGIN");
    db.ok("INSERT INTO accounts VALUES (3, 'cy', 5)");
    assert!(matches!(
        db.try_run("CREATE TABLE t (a INT)"),
        Err(EngineError::NotInTransactionBlock("CREATE TABLE"))
    ));
    assert_eq!(db.engine.status(), TransactionStatus::Failed);
    db.ok("ROLLBACK");
    assert!(db.engine.catalog().get_table("t").is_none());

    db.ok("BEGIN");
    assert!(matches!(
        db.try_run("CREATE INDEX log_note ON log (note)"),
        Err(EngineError::NotInTransactionBlock("CREATE INDEX"))
    ));
    db.ok("ROLLBACK");
    assert!(db.engine.catalog().get_index("log_note").is_none());
}

#[test]
fn begin_twice_and_commit_outside_a_block_change_nothing() {
    let mut db = Db::with_tables();
    for sql in ["COMMIT", "ROLLBACK", "ABORT", "END"] {
        assert!(
            matches!(db.try_run(sql), Err(EngineError::NoTransaction)),
            "{sql}"
        );
        assert_eq!(db.engine.status(), TransactionStatus::Idle);
    }

    db.ok("START TRANSACTION");
    db.ok("INSERT INTO accounts VALUES (3, 'cy', 5)");
    assert!(matches!(
        db.try_run("BEGIN"),
        Err(EngineError::AlreadyInTransaction)
    ));
    // The block goes on.
    assert_eq!(db.engine.status(), TransactionStatus::InBlock);
    db.ok("COMMIT");
    assert_eq!(db.rows("SELECT id FROM accounts").len(), 3);
}

#[test]
fn explain_of_a_transaction_command_is_an_error() {
    let mut db = Db::new();
    assert!(matches!(
        db.try_run("EXPLAIN BEGIN"),
        Err(EngineError::Parse(_))
    ));
    assert_eq!(db.engine.status(), TransactionStatus::Idle);
}

#[test]
fn dropping_the_engine_rolls_back_an_open_block() {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(16, dm));
    let catalog = Arc::new(Catalog::open(bpm).unwrap());

    let mut engine = ExecutionEngine::new(Arc::clone(&catalog)).unwrap();
    engine
        .execute_sql("CREATE TABLE t (a INT); BEGIN; INSERT INTO t VALUES (1)")
        .unwrap();
    drop(engine);

    let mut engine = ExecutionEngine::new(catalog).unwrap();
    let outputs = engine.execute_sql("SELECT * FROM t").unwrap();
    assert_eq!(
        outputs,
        [Output::Rows {
            columns: vec![common::Column::new("a", common::DataType::Int, true)],
            rows: vec![],
        }]
    );
}

/// The next Tid is recorded at each commit, so a new engine on the same
/// catalog starts above every committed row.
#[test]
fn a_new_engine_starts_above_committed_tids() {
    let mut db = Db::with_tables();
    db.ok("UPDATE accounts SET balance = 0");
    let next_tid = db.engine.transaction_manager().next_tid();
    let catalog = Arc::clone(db.engine.catalog());
    assert_eq!(catalog.next_tid().unwrap(), next_tid);

    let mut engine = ExecutionEngine::new(catalog).unwrap();
    assert_eq!(engine.transaction_manager().next_tid(), next_tid);
    let outputs = engine.execute_sql("SELECT balance FROM accounts").unwrap();
    assert!(matches!(
        &outputs[0],
        Output::Rows { rows, .. } if rows == &[[int(0)], [int(0)]]
    ));
}

/// A conflict is a failed statement like any other.
#[test]
fn a_storage_error_in_a_block_fails_it() {
    let mut db = Db::with_tables();
    // An older transaction on the same manager takes row 2's lock.
    let other = db.engine.transaction_manager().begin();
    let catalog = Arc::clone(db.engine.catalog());
    let table = catalog.get_table("accounts").unwrap();
    let mut scan = other.scan(&table.heap, &table.schema).unwrap();
    let rows = scan.next_batch().unwrap().unwrap();
    let rid = rows[1].1.rid().unwrap();
    other
        .update(&table.heap, &table.schema, rid, &rows[1].1)
        .unwrap();

    db.ok("BEGIN");
    db.ok("UPDATE accounts SET balance = 1 WHERE id = 1");
    let err = db.try_run("UPDATE accounts SET balance = 2 WHERE id = 2");
    assert!(
        matches!(
            err,
            Err(EngineError::Execution(ExecutionError::Storage(
                StorageError::Conflict { .. }
            )))
        ),
        "{err:?}"
    );
    assert_eq!(db.engine.status(), TransactionStatus::Failed);
    db.ok("ROLLBACK");
    db.engine.transaction_manager().abort(&other).unwrap();
    assert_eq!(
        db.rows("SELECT * FROM accounts"),
        [account(1, "ann", 100), account(2, "bo", 50)]
    );
}

/// Runs one statement in `session`.
fn run_in(session: &mut ExecutionEngine, sql: &str) -> Result<Output, EngineError> {
    let mut stmts = session.parse(sql)?;
    assert_eq!(stmts.len(), 1, "{sql}");
    session.execute(&stmts.remove(0))
}

fn rows_in(session: &mut ExecutionEngine, sql: &str) -> Vec<Vec<Value>> {
    match run_in(session, sql) {
        Ok(Output::Rows { rows, .. }) => rows,
        result => panic!("{sql}: {result:?}"),
    }
}

/// Whether `sql`, a SELECT, reads through an index.
fn uses_an_index(db: &mut Db, sql: &str) -> bool {
    let plan = db.rows(&format!("EXPLAIN {sql}"));
    plan.iter()
        .any(|line| matches!(&line[0], Value::Varchar(s) if s.contains("IndexScan")))
}

/// A block that began before another session deleted and changed rows still
/// finds them through every kind of index, by their old keys.
#[test]
fn an_older_block_reads_old_versions_through_indexes() {
    let mut db = Db::with_tables();
    let mut other = db.engine.session();
    let by_id = "SELECT * FROM accounts WHERE id = 1";
    let by_name = "SELECT * FROM accounts WHERE name = 'bo'";
    let log = "SELECT note FROM log WHERE id = 1";
    for sql in [by_id, by_name, log] {
        assert!(uses_an_index(&mut db, sql), "{sql}");
    }

    db.ok("BEGIN");
    assert_eq!(db.rows(by_id), [account(1, "ann", 100)]);
    run_in(&mut other, "DELETE FROM accounts WHERE id = 1").unwrap();
    run_in(
        &mut other,
        "UPDATE accounts SET id = 20, name = 'zed' WHERE id = 2",
    )
    .unwrap();
    run_in(&mut other, "DELETE FROM log WHERE id = 1").unwrap();

    assert_eq!(db.rows(by_id), [account(1, "ann", 100)]);
    assert_eq!(db.rows(by_name), [account(2, "bo", 50)]);
    assert_eq!(db.rows(log), [[text("opened")]]);
    assert!(
        db.rows("SELECT * FROM accounts WHERE name = 'zed'")
            .is_empty()
    );
    db.ok("COMMIT");

    assert!(db.rows(by_id).is_empty());
    assert!(db.rows(by_name).is_empty());
    assert!(db.rows(log).is_empty());
    assert_eq!(
        db.rows("SELECT * FROM accounts WHERE name = 'zed'"),
        [account(20, "zed", 50)]
    );
}

#[test]
fn a_deleted_primary_key_can_be_inserted_again() {
    let mut db = Db::with_tables();
    db.ok("DELETE FROM accounts WHERE id = 1");
    db.ok("INSERT INTO accounts VALUES (1, 'new', 5)");
    assert_eq!(
        db.rows("SELECT * FROM accounts WHERE id = 1"),
        [account(1, "new", 5)]
    );
    db.ok("BEGIN");
    db.ok("DELETE FROM accounts WHERE id = 2");
    db.ok("INSERT INTO accounts VALUES (2, 'again', 6)");
    db.ok("COMMIT");
    assert_eq!(
        db.rows("SELECT * FROM accounts WHERE id = 2"),
        [account(2, "again", 6)]
    );
}

/// While a block holds a deleted key, another session can't insert it, so
/// rolling the delete back leaves one row with the key. Deleting used to
/// remove the index entry, letting the insert in, and the rollback had no
/// entry to put back.
#[test]
fn a_key_a_block_deleted_is_not_free_until_it_commits() {
    let mut db = Db::with_tables();
    let mut other = db.engine.session();
    db.ok("BEGIN");
    db.ok("DELETE FROM accounts WHERE id = 1");
    let err = run_in(&mut other, "INSERT INTO accounts VALUES (1, 'cy', 0)");
    assert!(
        matches!(
            err,
            Err(EngineError::Execution(ExecutionError::Storage(
                StorageError::Conflict { .. }
            )))
        ),
        "{err:?}"
    );
    db.ok("ROLLBACK");

    assert_eq!(
        db.rows("SELECT * FROM accounts WHERE id = 1"),
        [account(1, "ann", 100)]
    );
    assert_eq!(db.rows("SELECT * FROM accounts").len(), 2);
    assert_eq!(
        rows_in(&mut other, "SELECT * FROM accounts WHERE name = 'ann'"),
        [account(1, "ann", 100)]
    );
}

/// An insert that another session's block rolls back doesn't keep the key
/// taken.
#[test]
fn a_key_a_rolled_back_block_inserted_is_free() {
    let mut db = Db::with_tables();
    let mut other = db.engine.session();
    db.ok("BEGIN");
    db.ok("INSERT INTO accounts VALUES (3, 'cy', 0)");
    db.ok("ROLLBACK");
    run_in(&mut other, "INSERT INTO accounts VALUES (3, 'di', 1)").unwrap();
    assert_eq!(
        db.rows("SELECT * FROM accounts WHERE id = 3"),
        [account(3, "di", 1)]
    );
}

/// `accounts` and `log` as in [`Db::with_tables`], with their rows but
/// without the secondary indexes, for tests that create those later.
fn without_indexes() -> Db {
    let mut db = Db::new();
    db.ok("CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(16), balance INT)");
    db.ok("CREATE TABLE log (id INT, note VARCHAR(16))");
    db.ok("INSERT INTO accounts VALUES (1, 'ann', 100), (2, 'bo', 50), (3, 'ann', 7)");
    db.ok("INSERT INTO log VALUES (1, 'opened'), (2, 'opened'), (1, 'closed')");
    db
}

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

#[test]
fn create_index_adds_the_rows_already_in_the_table() {
    let mut db = without_indexes();
    db.ok("CREATE INDEX accounts_name ON accounts (name)");
    db.ok("CREATE INDEX log_id ON log (id)");
    let by_name = "SELECT id FROM accounts WHERE name = 'ann'";
    let log = "SELECT note FROM log WHERE id = 1";
    for sql in [by_name, log] {
        assert!(uses_an_index(&mut db, sql), "{sql}");
    }
    assert_eq!(sorted(db.rows(by_name)), [[int(1)], [int(3)]]);
    assert_eq!(sorted(db.rows(log)), [[text("closed")], [text("opened")]]);
    assert_eq!(db.index_entries("accounts_name").len(), 3);
    assert_eq!(db.index_entries("log_id").len(), 3);
}

/// A block that began before another session changed rows and then created
/// an index finds the rows' old versions through the new index. The entries
/// for those versions stay until the block ends.
#[test]
fn an_older_block_reads_old_versions_through_a_new_index() {
    let mut db = without_indexes();
    let mut other = db.engine.session();
    db.ok("BEGIN");
    assert_eq!(db.rows("SELECT * FROM accounts").len(), 3);
    for sql in [
        "DELETE FROM accounts WHERE id = 1",
        "UPDATE accounts SET name = 'zed' WHERE id = 2",
        "DELETE FROM log WHERE note = 'closed'",
        "CREATE INDEX accounts_name ON accounts (name)",
        "CREATE INDEX log_id ON log (id)",
    ] {
        run_in(&mut other, sql).unwrap();
    }

    let ann = "SELECT * FROM accounts WHERE name = 'ann'";
    let bo = "SELECT * FROM accounts WHERE name = 'bo'";
    let zed = "SELECT * FROM accounts WHERE name = 'zed'";
    let log = "SELECT note FROM log WHERE id = 1";
    assert!(uses_an_index(&mut db, ann));
    assert!(uses_an_index(&mut db, log));
    assert_eq!(
        sorted(db.rows(ann)),
        [account(1, "ann", 100), account(3, "ann", 7)]
    );
    assert_eq!(db.rows(bo), [account(2, "bo", 50)]);
    assert!(db.rows(zed).is_empty());
    assert_eq!(sorted(db.rows(log)), [[text("closed")], [text("opened")]]);
    db.ok("COMMIT");

    assert_eq!(db.rows(ann), [account(3, "ann", 7)]);
    assert!(db.rows(bo).is_empty());
    assert_eq!(db.rows(zed), [account(2, "zed", 50)]);
    assert_eq!(db.rows(log), [[text("opened")]]);

    // Only the entries of the rows' current versions are left.
    db.engine.transaction_manager().wait_for_gc();
    assert_eq!(db.index_entries("accounts_name").len(), 2);
    assert_eq!(db.index_entries("log_id").len(), 2);
}

/// Rows another session's block inserted or changed, uncommitted while the
/// index was created, are found by their new keys once the block commits.
#[test]
fn a_new_index_has_the_rows_of_a_block_that_commits_later() {
    let mut db = without_indexes();
    let mut other = db.engine.session();
    for sql in [
        "BEGIN",
        "INSERT INTO log VALUES (5, 'new')",
        "UPDATE log SET id = 9 WHERE note = 'closed'",
    ] {
        run_in(&mut other, sql).unwrap();
    }
    db.ok("CREATE INDEX log_id ON log (id)");
    run_in(&mut other, "COMMIT").unwrap();

    assert_eq!(
        db.rows("SELECT note FROM log WHERE id = 5"),
        [[text("new")]]
    );
    assert_eq!(
        db.rows("SELECT note FROM log WHERE id = 9"),
        [[text("closed")]]
    );
    assert_eq!(
        db.rows("SELECT note FROM log WHERE id = 1"),
        [[text("opened")]]
    );
    db.engine.transaction_manager().wait_for_gc();
    assert_eq!(db.index_entries("log_id").len(), 4);
}

/// The entries a new index got for another block's uncommitted versions
/// are removed once the block rolls back.
#[test]
fn a_new_index_drops_the_rows_of_a_block_that_rolls_back() {
    let mut db = without_indexes();
    let mut other = db.engine.session();
    for sql in [
        "BEGIN",
        "INSERT INTO log VALUES (5, 'new')",
        "UPDATE log SET id = 9 WHERE note = 'closed'",
    ] {
        run_in(&mut other, sql).unwrap();
    }
    db.ok("CREATE INDEX log_id ON log (id)");
    run_in(&mut other, "ROLLBACK").unwrap();

    assert!(db.rows("SELECT note FROM log WHERE id = 5").is_empty());
    assert!(db.rows("SELECT note FROM log WHERE id = 9").is_empty());
    assert_eq!(
        sorted(db.rows("SELECT note FROM log WHERE id = 1")),
        [[text("closed")], [text("opened")]]
    );
    db.engine.transaction_manager().wait_for_gc();
    assert_eq!(db.index_entries("log_id").len(), 3);
}

/// Runs `sql` in `session` until it doesn't fail with `Conflict`.
fn run_until_no_conflict(session: &mut ExecutionEngine, sql: &str) {
    loop {
        match run_in(session, sql) {
            Ok(_) => return,
            Err(EngineError::Execution(ExecutionError::Storage(StorageError::Conflict {
                ..
            }))) => {}
            Err(e) => panic!("{sql}: {e}"),
        }
    }
}

/// Sessions insert and update rows while another creates an index on them:
/// every row ends up in the index under its current key.
#[test]
fn writes_while_an_index_is_created_are_not_lost() {
    const SEEDED: i32 = 300;
    const MOVED: i32 = 50;
    const WRITERS: i32 = 4;
    const ROWS: i32 = 100;

    let mut db = Db::new();
    db.ok("CREATE TABLE log (id INT, note VARCHAR(16))");
    let seeded: Vec<String> = (0..SEEDED).map(|id| format!("({id}, 'seed')")).collect();
    db.ok(&format!("INSERT INTO log VALUES {}", seeded.join(", ")));

    let start = Barrier::new(WRITERS as usize + 2);
    let writers: Vec<_> = (0..WRITERS).map(|_| db.engine.session()).collect();
    let mut mover = db.engine.session();
    let mut creator = db.engine.session();
    thread::scope(|s| {
        for (writer, mut session) in (0..).zip(writers) {
            let start = &start;
            s.spawn(move || {
                start.wait();
                for i in 0..ROWS {
                    let id = 1000 + writer * ROWS + i;
                    run_in(&mut session, &format!("INSERT INTO log VALUES ({id}, 'w')")).unwrap();
                }
            });
        }
        s.spawn(|| {
            start.wait();
            for id in 0..MOVED {
                let sql = format!("UPDATE log SET id = {} WHERE id = {id}", id + 1_000_000);
                run_until_no_conflict(&mut mover, &sql);
            }
        });
        s.spawn(|| {
            start.wait();
            run_in(&mut creator, "CREATE INDEX log_id ON log (id)").unwrap();
        });
    });

    let by_id = |id: i32| format!("SELECT note FROM log WHERE id = {id}");
    assert!(uses_an_index(&mut db, &by_id(0)));
    for id in 0..MOVED {
        assert!(db.rows(&by_id(id)).is_empty(), "{id}");
        assert_eq!(db.rows(&by_id(id + 1_000_000)), [[text("seed")]], "{id}");
    }
    for id in MOVED..SEEDED {
        assert_eq!(db.rows(&by_id(id)), [[text("seed")]], "{id}");
    }
    for id in 1000..1000 + WRITERS * ROWS {
        assert_eq!(db.rows(&by_id(id)), [[text("w")]], "{id}");
    }
}
