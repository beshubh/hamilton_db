# Index management

Index entries are not versioned; only the heap is. Every version a reader can see must still be reachable through the indexes, so writes only add entries, and garbage collection removes them. The rules are in [`index_versions.rs`](../../crates/storage/src/concurrency/index_versions.rs).

## What an entry stores

An entry is the key followed by what it stores for its row, so equal keys are separate entries and the B+ tree holds duplicates, in a primary key index too.

- The primary key index stores the rid.
- A secondary index stores the primary key if the table has one, and the rid otherwise (`locator_kind` in [`catalog.rs`](../../crates/catalog/src/catalog.rs)). A row that moves to a new rid leaves its secondary entries alone. The cost is a second lookup, through the primary key index.

## Writes

- INSERT and UPDATE add the entries their new versions need (`Transaction::insert_entry`). UPDATE and DELETE *retire* the entries their writes left behind (`retire_entry`). No write removes an entry.
- Abort removes the entries the transaction added. It retires the ones it found already in the index, since another transaction may need them.
- Once the watermark passes a committed transaction, garbage collection removes each entry it retired, unless some version visible at or above the watermark still has that key. A later write may have set the key back.
- Writers add entries under the read side of the manager's entry lock, and garbage collection checks and removes under the write side. So an entry a writer found can't be removed under it.

The executors must keep two orders ([`executors.rs`](../../crates/execution/src/executors.rs), `write_indexes`):
- A row's primary key entry goes in before its other entries. Garbage collection finds the rows behind a primary key through that index.
- UPDATE writes every row before adding any entry, so `SET id = id + 1` checks each new key against the new versions and doesn't collide with itself.

## Reads

An entry can outlive the version it was added for. It may lead to a row whose visible version has another key, or to no visible row. A reader must fetch through `Transaction::row_at` and check the key again, as `IndexScanExecutor` and `NestedIndexJoinExecutor` do. A new executor that reads through an index has to do the same.

## Uniqueness

Primary keys and UNIQUE indexes are checked the same way. The tree doesn't enforce it: a key may sit at several rids, because old versions keep their entries after a row moves, or after it is deleted and the key inserted again. `insert_entry` adds the entry first, then looks at the other rows the key leads to:
- a version with the key, written by another transaction that is running or committed after this one: `Conflict`;
- otherwise, a visible version with the key: duplicate key.

Adding before checking means that when two transactions insert the same key at once, at least one of them sees the other. A conflict never waits in storage; outside a block the engine waits for the other transaction and retries, so a concurrent duplicate ends as a duplicate key, as in Postgres ([mvto.md](mvto.md#rules)).

- "Other rows" means other rids, not other locators. In an index that stores the primary key, an entry for the row's old primary key leads back to the row itself after `SET id = ...`.
- A key with a NULL column is never checked, unless the index is `NULLS NOT DISTINCT` (`must_be_free` in [`index_versions.rs`](../../crates/storage/src/concurrency/index_versions.rs)).

## CREATE INDEX

- An INSERT, UPDATE or DELETE takes the table's write lock (`TableInfo::lock_writes`) before it lists the indexes, and holds it until its last write. The build takes the lock exclusively, and the index goes into the catalog only after the fill. So every write is either in the table when the index is filled, or adds its own entries.
- The build adds an entry for every version visible at or above the watermark, locked versions included, not just the newest. An older transaction that is still open may use the index in its next statement.
- Some of those versions are later rolled back or replaced by writers that never knew about the index, so nothing would retire their entries. The build retires every entry it adds instead. Garbage collection checks them once every transaction running when the build ended has ended.
- A crash during a build wastes the index's pages. The index was never recorded, so it doesn't come back.
- A unique index's build first fails with `Conflict` if another transaction has an uncommitted or newer version of any row in the table, and CREATE INDEX retries once that transaction ends. Then every head is committed before the build and no write can land during it, so a row's head is the one version that counts: after the fill, two heads with the same key are a duplicate, and the index is not recorded.
