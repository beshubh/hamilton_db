# Index management

Index entries are not versioned. Only the heap keeps versions. Every version that a reader can see must stay reachable through the indexes, so a write only adds entries, and garbage collection removes them. The code for these rules is in [`index_versions.rs`](../../crates/storage/src/concurrency/index_versions.rs).

## What an entry stores

An entry is the key followed by a locator, which is the value that leads from the entry to its row. Two entries with equal keys and different locators are separate entries, so the B+ tree holds duplicate keys. A primary key index holds duplicate keys too.

- The primary key index stores the rid, which is the row's page and slot.
- A secondary index stores the primary key if the table has one, and the rid otherwise. See `locator_kind` in [`catalog.rs`](../../crates/catalog/src/catalog.rs). When a row moves to a new rid, its secondary entries do not change. The cost is a second lookup, through the primary key index.

## Writes add entries and never remove them

- INSERT and UPDATE add the entries that their new versions need (`Transaction::insert_entry`).
- UPDATE and DELETE *retire* the entries that their writes no longer use (`retire_entry`). A retired entry stays in the index until garbage collection removes it.
- Abort removes the entries that the transaction added, by key, as each insert's log record says ([Recovery](recovery.md#undo-is-in-each-changes-record)). Abort retires the entries that the transaction found already in the index, because another transaction may need them.
- Garbage collection removes a committed transaction's retired entries after the watermark passes that transaction's Tid. It keeps an entry if a version visible at or above the watermark still has the entry's key. A later write may have given the row that key again. A crash loses the queue of retired entries, so opening the database checks every entry this way ([Recovery](recovery.md#opening-the-database-ends-with-a-sweep)).
- Writers add entries while they hold the read side of the manager's entry lock. Garbage collection checks and removes each entry while it holds the write side. A writer's add therefore lands either before garbage collection checks the entry or after garbage collection removes it, and a writer that finds its entry already in the index can count on it staying.

The executors must keep two orders. `write_indexes` in [`executors.rs`](../../crates/execution/src/executors.rs) returns the indexes in the first one.

- A row's primary key entry goes in before its other entries. Garbage collection finds the rows behind a primary key through the primary key index.
- UPDATE writes every row before it adds any entry. `SET id = id + 1` then checks each new key against the new versions and does not collide with the statement's own rows.

## Readers check the key again

An entry can outlive the version it was added for. The entry may lead to a row whose visible version has another key, or to no visible row. A reader must fetch the row through `Transaction::row_at` and compare the key again, as `IndexScanExecutor` and `NestedIndexJoinExecutor` do. A new executor that reads through an index must do the same.

## Uniqueness

One check enforces both primary keys and UNIQUE indexes. The tree does not enforce uniqueness. A key can be at several rids, because old versions keep their entries after a row moves, and after a row is deleted and its key is inserted again. `insert_entry` adds the entry first. Then it looks at the other rows that the key leads to:

- If one of them has a version with the key, written by another transaction that is running or that committed after this one, the insert fails with `Conflict`.
- Otherwise, if one of them has a visible version with the key, the insert fails with a duplicate key error.

Because the entry goes in before the check, when two transactions insert the same key at the same time, at least one of them sees the other. Storage never waits on a conflict. Outside a transaction block, the engine waits for the other transaction and retries the statement. A concurrent duplicate therefore ends as a duplicate key error, as in Postgres. See [the read and write rules of MVTO](mvto.md#read-and-write-rules).

- "Other rows" means other rids, not other locators. In an index that stores the primary key, an entry for the row's old primary key leads back to the same row after `SET id = ...`.
- A key with a NULL column is not checked, unless the index is `NULLS NOT DISTINCT`. See `must_be_free` in [`index_versions.rs`](../../crates/storage/src/concurrency/index_versions.rs).

## CREATE INDEX

- An INSERT, UPDATE, or DELETE takes the table's write lock (`TableInfo::lock_writes`) before it lists the indexes, and holds the lock until its last write. The build takes the same lock exclusively, and the index goes into the catalog only after the fill. Every write is therefore either in the table when the build fills the index, or adds its own entries.
- The build adds an entry for every version visible at or above the watermark, not only the newest. Locked versions are included. An older transaction that is still open can use the index in its next statement.
- Writers that never knew about the index later roll back or replace some of those versions, so nothing would retire the entries of those versions. The build therefore retires every entry that it adds. Garbage collection checks those entries after every transaction that was running when the build ended has ended.
- The tree and the index's catalog rows are changes of CREATE INDEX's transaction, and the build's entries are redo-only records in that tree. A build that fails, or a crash before the commit, leaves nothing: undo frees the tree with every page the build added ([Recovery](recovery.md#new-storage-is-a-change-of-its-transaction)).
- The build of a unique index first fails with `Conflict` if another transaction has an uncommitted or newer version of any row in the table. CREATE INDEX retries after that transaction ends. Past that check, every head was committed before the build, and no write can happen during it. A row's head, its newest version, is then the only version that counts. After the fill, two heads with the same key are a duplicate, and the catalog does not record the index.
