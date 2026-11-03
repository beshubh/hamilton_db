# MVTO

Transactions run under multi-version timestamp ordering. A transaction's Tid is its timestamp for reads and for the versions it writes. The result must equal running the transactions one after another in Tid order. The rules themselves are in [`transaction.rs`](../../crates/storage/src/concurrency/transaction.rs), [`read_ts.rs`](../../crates/storage/src/concurrency/read_ts.rs) and [`undo_log.rs`](../../crates/storage/src/table/undo_log.rs). This page covers how they fit together. How versions are stored is in [version-storage.md](version-storage.md), and indexes are in [index-management.md](index-management.md).

## Where the state lives

On disk, in the heap page's tuple header:
- **`ts`**: the Tid of the transaction that wrote the version.
- **`undo_ptr`**: points at the previous version in the undo log.
- **Deleted flag.**
- **Write lock**: not a separate field. While the writer is running, `ts` holds its Tid with `TXN_UNCOMMITTED_BIT` set. There is no lock table.

Also on disk: the **next Tid**, in the catalog header page, written before each commit.

In memory only, lost on restart:
- **`UndoLog`**: older versions.
- **`ReadTimestamps`**: the largest reader Tid per tuple.
- **`Watermark`**: running Tids and the watermark.

## Rules

A reader R looks at a tuple's head version:
- **Committed with `ts <= R`**: R reads the head.
- **Locked by R itself**: R reads the head.
- **Committed after R, or locked by a newer transaction**: R walks the undo chain back to the newest version with `ts <= R`.
- **Locked by an older transaction**: `Conflict`. R can't know whether that write will commit, and if it does it belongs before R.

A writer T fails with `Conflict` if the head is:
- locked by another transaction,
- committed after T, or
- read by a transaction newer than T.

A version keeps its writer's Tid as its `ts` when it commits, so a long transaction commits "in the past". That is safe because every newer transaction that touched those tuples while it ran has already failed with `Conflict`.

Storage never waits: it returns `Conflict` and the caller aborts the transaction. What happens next depends on where the statement ran ([`engine.rs`](../../crates/planner/src/engine.rs)):
- **In a block**, the engine fails the whole block, and the client sees SQLSTATE `40001` and retries it. There are no savepoints, so a statement that wrote some rows before conflicting can't be undone on its own.
- **Outside a block**, the statement was the whole transaction and nobody has seen it, so the engine runs it again with a new Tid. If the other transaction is newer, the new Tid is newer still and the retry goes ahead at once. If it is older, only its end changes anything, so the engine waits for it (`TransactionManager::wait_for_end`). It gives up after the conflict timeout and returns the `Conflict`, so a block left open can't hang a statement for good. The waiting statement holds nothing, so waits can't deadlock.

## Watermark and garbage collection

The watermark is the oldest running Tid, or the next Tid if none are running. It never goes down, so anything once found to be garbage stays garbage. Below the watermark no reader can need:
- undo records,
- read timestamps (and the oldest running transaction records no reads at all),
- tuples left deleted by committed transactions, and inserts that were aborted,
- retired index entries.

One collector thread in [`TransactionManager`](../../crates/storage/src/concurrency/transaction_manager.rs) frees all of these. Every commit and abort wakes it, off their latency. A transaction that is never ended holds the watermark back forever. Debug builds panic when one is dropped while still running.

## Restart and crashes

There is no WAL. While the server runs, dirty pages reach disk when evicted and at a periodic checkpoint ([`Checkpointer`](../../crates/hamilton-server/src/database.rs)), which writes every one of them, uncommitted versions included, and is not atomic across pages. A commit does not wait for either. Only a clean shutdown is safe: it stops the checkpoints and writes everything once no transaction is running. The undo log, read timestamps and watermark are lost on restart. That is fine after a clean shutdown:
- The new Tid counter starts from the catalog header, above every committed `ts` on disk, so new readers see those versions as committed.
- No reader is older than the versions on disk, so none walks an undo chain. Stale `undo_ptr`s are still on disk; `older_record` rejects them by matching the record's `rid` and `writer_ts`.

A crash is not. Commits since the last checkpoint are lost, and a crash during one leaves some pages written and others not. Any locked tuple that reached disk keeps the uncommitted bit and an older Tid, so every later reader and writer fails on it with `Conflict`. Nothing clears these when the database opens.

Sequence rows ([`sequence.rs`](../../crates/catalog/src/sequence.rs)) are changed in place outside any transaction, at `ts` 0, so a crash loses their latest changes too. That alone would hand out ids already on disk again: a sequence's page is hot and is written mostly at checkpoints, while the table pages holding its ids are written whenever they are evicted. So `nextval` writes the row to disk 32 values ahead before handing them out, and again on its first call after a checkpoint or eviction writes the page as it is (the page is clean then). A crash skips up to 32 values instead. This stands in for the WAL record Postgres writes. `TRUNCATE ... RESTART IDENTITY` is the one change that moves a sequence back, so the row isn't changed until the truncate is on disk (see below). With no fsync it survives a process crash, not a power loss.

## Table locks and TRUNCATE

TRUNCATE gives a table a new, empty heap and indexes ([`Catalog::truncate`](../../crates/catalog/src/catalog.rs)) rather than deleting rows, so the old versions are gone for every reader at once, and no row's timestamps check the truncate against other transactions. Table locks ([`locks.rs`](../../crates/catalog/src/locks.rs)) do that instead:

- Each statement locks the tables its plan uses before it runs (`lock_tables` in [`executors.rs`](../../crates/execution/src/executors.rs)). Reads, writes and CREATE INDEX take modes that conflict only with TRUNCATE's exclusive one. Locks are held until the transaction ends.
- A lock never waits. A conflict fails the statement with `LockConflict`; outside a block the engine retries once the holder ends, however new it is, and in a block the block fails. Nothing waits holding a lock, so there are no deadlocks to detect, and an exclusive request can lose to a steady stream of readers.
- Locks also stand in for the timestamps MVTO keeps on rows, failing with `ChangedSince`. A transaction older than a committed truncate can't use the table: it comes first in Tid order and should still see the rows. A truncate can't run in a transaction older than one that has used the table: it would remove rows that one wrote, or that it read after them. Postgres has neither check: older snapshots see an empty table.
- The replaced pages are freed by `after_readers` work in garbage collection, after any garbage still pointing into them.

The engine prepares a commit that truncated before committing it (`prepare_commit`): a checkpoint writes every page and the disk's list of them, then the catalog rows are pointed at the new pages in memory. A failure there aborts the transaction. After the commit, the new heap is written again with its rows stamped committed, then the pages holding the catalog rows, before the restarted sequence and before the old pages are freed. A crash before the rows reach disk finds the old table, complete. So every TRUNCATE commit costs a checkpoint, whose sync holds the disk manager's lock and so stalls every page read from or written to disk until it ends.

## Known gaps

- **Phantoms.** Read timestamps guard tuples that exist. A row inserted by an older transaction that a newer one's scan should have seen is not caught, so schedules with inserts are not serializable.
- **The catalog isn't transactional.** CREATE TABLE and CREATE INDEX are rejected inside a block.
- **A commit or abort that fails partway** leaves the transaction running, with some tuples done and the rest locked.
