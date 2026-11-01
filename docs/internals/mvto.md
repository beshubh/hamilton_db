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

`Conflict` never waits. The caller aborts the transaction, and the engine fails the whole block, as Postgres does ([`engine.rs`](../../crates/planner/src/engine.rs)).

## Watermark and garbage collection

The watermark is the oldest running Tid, or the next Tid if none are running. It never goes down, so anything once found to be garbage stays garbage. Below the watermark no reader can need:
- undo records,
- read timestamps (and the oldest running transaction records no reads at all),
- tuples left deleted by committed transactions, and inserts that were aborted,
- retired index entries.

One collector thread in [`TransactionManager`](../../crates/storage/src/concurrency/transaction_manager.rs) frees all of these. Every commit and abort wakes it, off their latency. A transaction that is never ended holds the watermark back forever. Debug builds panic when one is dropped while still running.

## Restart and crashes

There is no WAL. The undo log, read timestamps and watermark are lost on restart. That is fine for a clean shutdown:
- The new Tid counter starts from the catalog header, above every committed `ts` on disk, so new readers see those versions as committed.
- No reader is older than the versions on disk, so none walks an undo chain. Stale `undo_ptr`s are still on disk; `older_record` rejects them by matching the record's `rid` and `writer_ts`.

A crash with transactions running is not fine. Any locked tuple that reached disk keeps the uncommitted bit and an older Tid, so every later reader and writer fails on it with `Conflict`. Nothing clears these when the database opens.

## Known gaps

- **Phantoms.** Read timestamps guard tuples that exist. A row inserted by an older transaction that a newer one's scan should have seen is not caught, so schedules with inserts are not serializable.
- **The catalog isn't transactional.** CREATE TABLE and CREATE INDEX are rejected inside a block.
- **A commit or abort that fails partway** leaves the transaction running, with some tuples done and the rest locked.
