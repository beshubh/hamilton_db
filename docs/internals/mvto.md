# MVTO

Transactions run under multi-version timestamp ordering (MVTO). A transaction's id, its Tid, is its timestamp for reads and for the versions it writes. The result must equal running the transactions one after another in Tid order. The rules are in [`transaction.rs`](../../crates/storage/src/concurrency/transaction.rs), [`read_ts.rs`](../../crates/storage/src/concurrency/read_ts.rs), and [`undo_log.rs`](../../crates/storage/src/table/undo_log.rs). This page covers how the rules fit together. [Version storage](version-storage.md) covers how versions are stored, and [Index management](index-management.md) covers indexes.

## Where the state lives

The tuple header in the heap page is on disk. It holds:

- `ts`, the Tid of the transaction that wrote the version.
- `undo_ptr`, which points at the previous version in the undo log.
- The deleted flag.
- The write lock, which is not a separate field. While the writer is running, `ts` holds the writer's Tid with `TXN_UNCOMMITTED_BIT` set. There is no lock table.

The next Tid is not stored on its own: the log holds every Tid that wrote anything, and a restart starts above them ([Recovery](recovery.md#analysis-analyze)).

The rest is in memory only, and a restart loses it:

- `UndoLog` holds the older versions.
- `ReadTimestamps` holds the largest reader Tid for each tuple.
- `Watermark` holds the running Tids and the watermark.

## Read and write rules

A reader R looks at a tuple's head, which is its newest version:

- **Committed with `ts <= R`.** R reads the head.
- **Locked by R itself.** R reads the head.
- **Committed after R, or locked by a newer transaction.** R walks the undo chain back to the newest version with `ts <= R`.
- **Locked by an older transaction.** R fails with `Conflict`. R cannot know whether that write will commit, and if the write commits, it belongs before R.

A writer T fails with `Conflict` if the head is any of these:

- locked by another transaction,
- committed after T, or
- read by a transaction newer than T.

A version keeps its writer's Tid as its `ts` when it commits, so a long transaction commits with a timestamp older than its commit time. The old timestamp is safe, because every newer transaction that touched those tuples while the long one ran has already failed with `Conflict`.

Storage never waits. It returns `Conflict`, and the caller aborts the transaction. What happens next depends on where the statement ran. The code is in [`engine.rs`](../../crates/planner/src/engine.rs).

- **In a transaction block**, the engine fails the whole block. The client sees SQLSTATE `40001` and retries the block. There are no savepoints, so the engine cannot undo only the statement that conflicted after it wrote some rows.
- **Outside a block**, the statement was the whole transaction and no one has seen its writes, so the engine runs it again with a new Tid. If the other transaction is newer, the new Tid is newer still, and the retry does not wait. If the other transaction is older, only its end changes the outcome, so the engine waits for it (`TransactionManager::wait_for_end`). The engine stops after the conflict timeout and returns the `Conflict`, so a block that a client leaves open cannot hang a statement forever. The statement that waits holds nothing, so waits cannot deadlock.

## Watermark and garbage collection

The watermark is the oldest running Tid, or the next Tid if no transaction is running. The watermark never decreases, so anything that was garbage once stays garbage. No reader can need any of these below the watermark:

- undo records,
- read timestamps (the oldest running transaction records no reads at all),
- tuples that committed transactions left deleted, and inserts that were aborted, and
- retired index entries.

One collector thread in [`TransactionManager`](../../crates/storage/src/concurrency/transaction_manager.rs) frees all of them. Every commit and every abort wakes the thread and returns, so neither waits for the collection. A transaction that never ends holds the watermark back forever. Debug builds panic when a transaction is dropped while it is still running.

## Restart and crashes

Opening a database replays the log, finishes the commits a crash cut off between their stamps, and undoes the transactions it cut off before they committed, so no tuple stays locked ([Recovery](recovery.md#undo-finish_transactions)). A commit survives a crash once `TransactionManager::commit` returns, and a reader never sees one that a crash can lose. The log does not hold what MVTO keeps in memory. A restart, clean or not, loses the undo log, the read timestamps, and the watermark, and a crash also loses the garbage collector's queue. That does no harm:

- The new Tid counter starts above every Tid in the log. New readers therefore see the committed versions as committed.
- No reader is older than the versions on disk, so no reader walks an undo chain. Stale `undo_ptr`s are still on disk. `older_record` rejects them, because it requires the record's `rid` and `writer_ts` to match.
- Opening ends with a sweep of every table and index, which collects what the queue held ([Recovery](recovery.md#the-startup-sweep)).

`nextval` hands values out from a counter in memory, outside any transaction ([`sequence.rs`](../../crates/catalog/src/sequence.rs)). When the sequence's row stops covering the next value, `nextval` first writes it 32 values ahead, a redo-only record that nothing syncs. A value reaches a committed row only through an insert whose transaction logs `Commit` after that record, and the commit's sync makes the record durable too. So a crash skips up to 32 values and never repeats one that a committed row holds. A clean shutdown writes each row back to the last value handed out (`Catalog::save_sequences`), so only a crash skips. A way to hand a value out with no commit after it, such as `SELECT nextval(...)`, would need its own sync.

`TRUNCATE ... RESTART IDENTITY` is the only change that moves a sequence back. It is a change of the truncate's transaction, so an abort, or a crash before the commit, puts the row back ([Recovery](recovery.md#how-ddl-is-logged)).

## Table locks, TRUNCATE and DROP TABLE

TRUNCATE does not delete rows. It gives the table a new, empty heap and new indexes ([`Catalog::truncate`](../../crates/catalog/src/catalog.rs)), and DROP TABLE takes the table away with its storage (`Catalog::drop_table`), so every reader loses the old versions at the same moment. No row timestamp is left to check either against other transactions. Table locks ([`locks.rs`](../../crates/catalog/src/locks.rs)) do that check:

- Each statement locks the tables that its plan uses before it runs (`lock_tables` in [`executors.rs`](../../crates/execution/src/executors.rs)). Reads, writes, and CREATE INDEX take modes that conflict only with the exclusive mode of TRUNCATE and DROP TABLE, which CREATE TABLE also holds on its new table until it ends. A transaction holds its locks until it ends.
- A lock never waits. A conflict fails the statement with `LockConflict`. Outside a block, the engine retries after the holder ends, whether the holder is older or newer. In a block, the block fails. No transaction waits while it holds a lock, so there are no deadlocks to detect. The cost is that an exclusive request can keep failing while readers keep arriving.
- Locks also do the job of the timestamps that MVTO keeps on rows, and fail with `ChangedSince`. A transaction older than a committed truncate cannot use the table, because it comes first in Tid order and should still see the rows. A truncate or drop cannot run in a transaction older than one that has used the table, because it would remove rows that the newer transaction wrote or read. Postgres has neither check. There, older snapshots see an empty table.
- Garbage collection frees the replaced or dropped pages after it has handled any garbage that still points into them.

A truncate's new storage and the catalog rows that point at it, and a drop's catalog deletes, are changes of their statement's transaction, so they commit or roll back with it, across a crash too ([Recovery](recovery.md#how-ddl-is-logged)).

## Known gaps

- **Phantoms.** Read timestamps guard tuples that exist. If an older transaction inserts a row that a newer transaction's scan should have seen, nothing detects it. Schedules with inserts are therefore not serializable.
- **DDL is a transaction of its own.** CREATE TABLE, CREATE INDEX and DROP TABLE commit or roll back whole, but are rejected inside a block ([Recovery](recovery.md#how-ddl-is-logged)).
- **The catalog has no versions.** A transaction older than a committed DROP TABLE finds the table gone, or, if its name was made again, the new table, where Tid order says it should still see the old rows. Table locks catch only a transaction that looked the table up before the drop removed it.
