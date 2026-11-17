# MVTO

Transactions run under multi-version timestamp ordering (MVTO). A transaction's id, its Tid, is its timestamp for reads and for the versions it writes. The result must equal running the transactions one after another in Tid order. The rules are in [`transaction.rs`](../../crates/storage/src/concurrency/transaction.rs), [`read_ts.rs`](../../crates/storage/src/concurrency/read_ts.rs), and [`undo_log.rs`](../../crates/storage/src/table/undo_log.rs). This page covers how the rules fit together. [Version storage](version-storage.md) covers how versions are stored, and [Index management](index-management.md) covers indexes.

## Where the state lives

The tuple header in the heap page is on disk. It holds:

- `ts`, the Tid of the transaction that wrote the version.
- `undo_ptr`, which points at the previous version in the undo log.
- The deleted flag.
- The write lock, which is not a separate field. While the writer is running, `ts` holds the writer's Tid with `TXN_UNCOMMITTED_BIT` set. There is no lock table.

The next Tid is also on disk, in the catalog header page. The engine writes it before each commit.

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

There is no WAL. While the server runs, dirty pages reach disk in two ways: when the buffer pool evicts them, and at a periodic checkpoint ([`Checkpointer`](../../crates/hamilton-server/src/database.rs)). A checkpoint writes every dirty page, uncommitted versions included, and is not atomic across pages. A commit waits for neither.

Only a clean shutdown is safe. It stops the checkpoints and writes everything after the last transaction has ended. A restart loses the undo log, the read timestamps, and the watermark. After a clean shutdown the loss does no harm:

- The new Tid counter starts from the catalog header, above every committed `ts` on disk. New readers therefore see those versions as committed.
- No reader is older than the versions on disk, so no reader walks an undo chain. Stale `undo_ptr`s are still on disk. `older_record` rejects them, because it requires the record's `rid` and `writer_ts` to match.

A crash is not safe. Commits since the last checkpoint are lost, and a crash during a checkpoint leaves some pages written and others not. A locked tuple that reached disk keeps the uncommitted bit and an older Tid, so every later reader and writer fails on that tuple with `Conflict`. Nothing clears these locks when the database opens.

Sequences need their own protection. `nextval` changes a sequence row ([`sequence.rs`](../../crates/catalog/src/sequence.rs)) in place, outside any transaction, at `ts` 0, so a crash also loses the latest changes to the row. Without more care, the sequence would then hand out ids that are already on disk. A sequence's page is in constant use, so it is written mostly at checkpoints, while the table pages that hold its ids are written whenever they are evicted.

So `nextval` writes the row to disk 32 values ahead before it hands those values out. A checkpoint or an eviction can then write the page with the row's current value, which leaves the page clean. On its first call after that, `nextval` writes ahead again. After a crash, the sequence skips up to 32 values and never repeats one. The write ahead does the job of the WAL record that Postgres writes. `nextval` does not fsync, so the write survives a process crash but not a power loss.

`TRUNCATE ... RESTART IDENTITY` is the only change that moves a sequence back. The row therefore does not change until the truncate is on disk, as the next section explains.

## Table locks and TRUNCATE

TRUNCATE does not delete rows. It gives the table a new, empty heap and new indexes ([`Catalog::truncate`](../../crates/catalog/src/catalog.rs)), so every reader loses the old versions at the same moment. No row timestamp is left to check the truncate against other transactions. Table locks ([`locks.rs`](../../crates/catalog/src/locks.rs)) do that check:

- Each statement locks the tables that its plan uses before it runs (`lock_tables` in [`executors.rs`](../../crates/execution/src/executors.rs)). Reads, writes, and CREATE INDEX take modes that conflict only with TRUNCATE's exclusive mode. A transaction holds its locks until it ends.
- A lock never waits. A conflict fails the statement with `LockConflict`. Outside a block, the engine retries after the holder ends, whether the holder is older or newer. In a block, the block fails. No transaction waits while it holds a lock, so there are no deadlocks to detect. The cost is that an exclusive request can keep failing while readers keep arriving.
- Locks also do the job of the timestamps that MVTO keeps on rows, and fail with `ChangedSince`. A transaction older than a committed truncate cannot use the table, because it comes first in Tid order and should still see the rows. A truncate cannot run in a transaction older than one that has used the table, because it would remove rows that the newer transaction wrote or read. Postgres has neither check. There, older snapshots see an empty table.
- Garbage collection frees the replaced pages through `after_readers` work, after it has handled any garbage that still points into them.

A commit that truncated costs a checkpoint. The engine prepares the commit first (`prepare_commit`):

1. A checkpoint writes every page and the disk's list of pages.
2. The catalog rows are pointed at the new pages in memory. A failure up to here aborts the transaction.
3. The transaction commits.
4. The new heap is written again, with its rows now stamped committed. Then the pages that hold the catalog rows are written.
5. The restarted sequence is written, and the old pages are freed.

A crash before the catalog rows reach disk finds the old table, complete. The checkpoint's sync holds the disk manager's lock, so every page read from disk and every page write to disk stalls until the sync ends.

## Known gaps

- **Phantoms.** Read timestamps guard tuples that exist. If an older transaction inserts a row that a newer transaction's scan should have seen, nothing detects it. Schedules with inserts are therefore not serializable.
- **The catalog is not transactional.** CREATE TABLE and CREATE INDEX are rejected inside a block.
- **A commit or abort that fails partway** leaves the transaction running, with some tuples finished and the rest locked.
