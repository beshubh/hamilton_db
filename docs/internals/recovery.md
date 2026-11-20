# Recovery

Hamilton DB survives a crash (kill -9, panic, power loss) at any moment. After a restart:

- every commit the client saw acknowledged is there,
- nothing an unfinished or rolled-back transaction wrote is there,
- every index matches its table.

It does this with a write-ahead log (WAL) and the ARIES recovery algorithm, adapted to MVTO. This page explains how. It starts with the big picture, then follows one INSERT, UPDATE and DELETE, one B+ tree change and one TRUNCATE through the log. It ends with what happens on restart.

## The files on disk

A database named `app` is three kinds of file:

| File | What it holds |
|---|---|
| `app.db` | The pages: table pages, B+ tree pages, the catalog. Written lazily, in any order. |
| `app.log.00000001`, `app.log.00000002`, ... | The log, split into 64 MiB segments. Appended in order, and synced at commit. |
| `app.meta` | The last checkpoint (ARIES's "master record") and the page directory. Replaced with one atomic rename. |

The rule that makes the rest work is that **a page is written to `app.db` only after every log record that changed it is synced to the log.** The log is therefore always at least as new as the pages, and recovery can rebuild any page from it.

## Where steal and no-force are

**Steal.** The buffer pool may write a dirty page to disk at any time, even when it holds rows of a transaction that has not committed. That happens when the page is evicted or a checkpoint writes it. So after a crash, `app.db` can contain uncommitted rows, and recovery must **undo** them. The WAL rule is in `queue_write` in [`page_guard.rs`](../../crates/storage/src/page/page_guard.rs). Before it writes a page, it calls `log.flush_to(page_lsn)`, so an eviction may wait for a log sync.

**No-force.** A commit does not write the transaction's pages. It writes a `Commit` record and syncs the log up to it (`TxnLog::commit` in [`txn_log.rs`](../../crates/storage/src/wal/txn_log.rs)). The pages reach disk later. So after a crash, `app.db` can lack committed rows, and recovery must **redo** them.

Several transactions that commit at the same moment share one log sync (group commit, in [`log.rs`](../../crates/storage/src/wal/log.rs)).

## What kind of logging, and where

Nothing is logged at the SQL level. No record ever says "UPDATE users SET ...". Each record describes changes to pages, in one of two forms:

| Change | How redo is logged | How undo is logged |
|---|---|---|
| A row inserted, updated, deleted or committed (stamped) on a table page | **Physiological.** The page id plus a small slot-level op, such as "insert this tuple in slot 3". The page code decides where the bytes go inside the page. | **Physiological.** The slot's meta and bytes from before the change. |
| A key inserted into or removed from a B+ tree leaf | **Physiological.** The page id plus "insert this key at position 5" or "remove the key at position 2". | **Logical.** "Remove this key from the tree whose header is page H", wherever splits have moved it since. |
| A B+ tree split or merge, a new root, a tree header, catalog page 0 | **Physical.** A full 4 KiB image of every page it changed. | Not undone. See [B+ tree changes](#b-tree-changes-are-atomic). |
| The first change to any page after that page was last written to disk | **Physical.** A full image, even for a row change. See [Redo never reads a page from disk](#redo-never-reads-a-page-from-disk). | As the row above. |
| CREATE TABLE, CREATE INDEX, TRUNCATE, DROP TABLE | The new pages' images or ops. A drop changes no page. | **Logical.** "Free every page of the heap or tree that starts at page P." A drop's undo frees nothing. |

The ops are the `PageOp` enum in [`op.rs`](../../crates/storage/src/wal/op.rs):

- table pages: `HeapInit`, `HeapSetNext`, `HeapInsert`, `HeapSetTuple`, `HeapSetMeta`, `HeapStamp`, `HeapMarkDead`;
- B+ tree leaves: `LeafInsert`, `LeafRemove`.

Each op is a few bytes plus the tuple or key it carries. A full image is about 4 KiB.

## Log records

Every record has the same header ([`record.rs`](../../crates/storage/src/wal/record.rs)):

```text
len | crc | lsn | prev_lsn | tid | kind | body
```

- `lsn` is the record's byte position in the log, as if all the segments were one file. It only grows.
- `prev_lsn` is the previous record of the same transaction. Each transaction's records form a backward chain, which abort and undo walk.
- Every page holds the LSN of the last record that changed it (its pageLSN).

There are seven kinds of record:

| Kind | Written by | Undone? | Body |
|---|---|---|---|
| `Update` | A transaction's change | Yes | how to undo it, then the page changes |
| `Clr` | The undo of one `Update` (a compensation log record) | Never | `undo_next`, then the page changes |
| `Completion` | Work after a commit: stamping rows, freeing the pages a truncate replaced or a drop gave up | No | page changes |
| `System` | A change that belongs to no transaction: garbage collection, a sequence logging ahead, an index build's entries | No | page changes |
| `Commit`, `Abort`, `End` | Transaction status | | empty |

**A record is not "a row record" or "a page record".** It is one atomic change, which may touch several pages. Inside the record, each page is described either by its ops (row-level) or by a full image (page-level). For example, an insert that fills a page and starts a new one writes one record covering two pages: `HeapSetNext` on the old page, and `HeapInit` + `HeapInsert` on the new one. A record is in the log whole or not at all, so recovery never sees half of a change.

## Mini-transactions: how a page change becomes a record

Every change to a durable page goes through a mini-transaction, or mtr ([`mtr.rs`](../../crates/storage/src/wal/mtr.rs)). `WritePageGuard` gives no mutable access to a page, so code that tries to change a page another way does not compile.

An mtr:

1. latches the pages it needs;
2. changes them, either with `MtrPage::apply(op)`, which logs the op, or with `MtrPage::raw_mut()`, which logs the whole page;
3. when it ends, appends **one** record covering every page it changed, writes that record's LSN into each page, and marks each clean page dirty from that LSN (its `rec_lsn`);
4. releases the latches.

Who the change belongs to decides the record's kind: `Logged::Txn` → `Update`, `Logged::Clr` → `Clr`, `Logged::Completion` → `Completion`, and `Logged::System` → `System`.

Nothing may fail between an mtr's first change and its last, because the record logs the pages as they are when it ends. A B+ tree split that fails halfway therefore stops the process (`finish_change` in [`btree.rs`](../../crates/storage/src/index/btree.rs)) instead of logging a broken tree.

Debug builds check every mtr (`check_redo`). They replay the logged ops on a copy of the page from before the mtr and abort unless the copy equals the page. Every test that changes a row also tests redo of that change.

## How INSERT, UPDATE and DELETE are logged

In MVTO, a row's newest version lives on the table page. A row that is written but not committed has `ts = its transaction's Tid | UNCOMMITTED_BIT` in its meta, and that bit is the row's lock. Older versions are kept in an in-memory version chain (`UndoLog`, see [Version storage](version-storage.md)) for readers.

**The version chain is not logged.** After a restart no transaction is running, so no reader needs an old version. The WAL keeps its own undo information inside each `Update` record, for rollback and crash recovery only.

### INSERT

`INSERT INTO users VALUES (7, 'shubh')` in transaction 42, where `users` has a primary key index:

```text
LSN 100  Update  tid 42  prev 0     undo: Heap{rid (5,3), before: Inserted}
                                    page 5: [HeapInsert slot 3, ts 42|U, bytes of (7,'shubh')]
LSN 160  Update  tid 42  prev 100   undo: IndexEntry{tree header 9, key 7 → (5,3)}
                                    page 12 (leaf): [LeafInsert position 4, key 7, rid (5,3)]
LSN 210  Commit  tid 42  prev 160                              ← log synced here; client gets OK
LSN 220  Completion tid 42 prev 210 page 5: [HeapStamp slot 3]  ← clears the U bit: row now visible
LSN 240  End     tid 42  prev 220
```

1. `Transaction::insert` calls `TableHeap::insert` ([`table_heap.rs`](../../crates/storage/src/table/table_heap.rs)), which applies `HeapInsert` in an mtr. Because the mtr belongs to a transaction, `MtrPage::apply` reads the slot before changing it and adds the undo to the record. Callers never build undo themselves.
2. `Transaction::insert_entry` inserts the index entry, logged with an `IndexEntry` undo. The heap write is always logged before its index entries.
3. At commit, the `Commit` record is synced. Only then are the rows stamped, with one `Completion` record per page in page order, and then `End` is appended without waiting for a sync. A reader never sees a row whose commit a crash could lose.

If page 5 had last been written to disk before this change, its part of record 100 is a full image instead of the op.

### UPDATE

An update overwrites the row in place with `HeapSetTuple`, and pushes the old version onto the in-memory version chain. The `Update` record's undo is `Before::Tuple`, the slot's old meta and bytes. While the transaction runs, the slot keeps at least the old row's length (`padded_to`), so undo always fits back in place. The `HeapStamp` at commit trims the padding. If the new row does not fit on the page, the update becomes a delete plus an insert at a new rid.

If the update changes an indexed key, the new key gets a new entry, logged like the insert above. The old entry is not removed by the transaction. Readers of older versions may still need it, so garbage collection removes it later in a `System` record.

### DELETE

A delete only changes the row's meta to "deleted, `ts 42|U`" with `HeapSetMeta`. The undo is `Before::Meta`, the old meta. The row's bytes and its index entries stay. After the commit, once no reader can see the old version, garbage collection marks the slot dead (`HeapMarkDead`) and removes the index entries (`LeafRemove`), both as `System` records.

### When the pages reach `app.db`

Nothing above writes `app.db`. The pages stay dirty in the buffer pool until they are evicted or a checkpoint writes them, and each write first syncs the log up to the page's LSN.

## B+ tree changes are atomic

An index insert is one mtr, so one record, even when it splits pages all the way up to the root. Suppose leaf 12 is full:

```text
LSN 160  Update  tid 42  undo: IndexEntry{tree header 9, key 7 → (5,3)}
                         page 12: image   (left half, after the split)
                         page 31: image   (new right sibling)
                         page 4:  image   (parent, with the new separator)
```

Either all three pages change or none do. Splits, merges and new roots move many entries, so they use `raw_mut` and are logged as images. A plain leaf insert or remove is a small op.

**Undo of an index insert is logical: remove the key.** After transaction 42 inserts key 7, another transaction's split may move key 7 to a different leaf, so undo looks it up by key instead of by page. The split itself is never undone. A tree with the key removed is still a valid tree. Undo runs before the catalog is readable, so each tree's header page describes how it orders keys (`KeyComparator::describe`). Undo opens the tree from its header alone.

Transactions only ever add index entries. Entries are removed by garbage collection, or by a unique check that takes back an entry it just added, and both are logged as `System` records.

## How DDL is logged

CREATE TABLE, CREATE INDEX, TRUNCATE and DROP TABLE change storage, not rows. All but TRUNCATE run as a transaction of their own and are refused inside a `BEGIN` block. The code is in [`transaction.rs`](../../crates/storage/src/concurrency/transaction.rs) (`create_table`, `create_index`, `truncate`, `drop_storage`) and [`catalog.rs`](../../crates/catalog/src/catalog.rs).

`TRUNCATE users` in transaction 50, where `users` has a primary key index:

```text
LSN 500  Update  tid 50  undo: Truncate{old: [heap 5, tree 9], new: [heap 70, tree 71]}
                         page 70: [HeapInit]            ← new empty heap
                         page 71: image                 ← new empty tree header
LSN 560  Update  tid 50  undo: Heap{rid of users' catalog row, before: old bytes}
                         catalog page: [HeapSetTuple ... first_page_id = 70]
LSN 600  Update  tid 50  undo: Heap{rid of the index's catalog row, ...}
                         catalog page: [HeapSetTuple ... header_page_id = 71]
LSN 640  Commit  tid 50                                  ← synced; client gets OK
         ... later, once no running transaction can still read the old rows ...
LSN 900  Completion tid 50  frees: 5, 6, 7, 9, 12, 31, 4  ← old heap and tree pages
LSN 960  End     tid 50
```

1. **New storage is made in one record that also says how to free it.** The mtr allocates and formats the new pages, and the record's undo names their root pages. A crash can never leave pages that nobody will free. CREATE TABLE and CREATE INDEX do the same with `Undo::Create{heaps, trees}`.
2. **The catalog rows are ordinary undoable row changes** (`Transaction::undoable`): a create's new catalog rows, a truncate pointing the table's rows at the new storage, or a drop marking them deleted.
3. **Writes into storage the transaction just made need no undo of their own,** so they are `System` records. An index build's entries and a new sequence's row are examples. Freeing the storage takes them away too.
4. **At commit,** a create is done. A truncate's old pages, and a drop's, are freed later, by garbage collection, once no reader can still see the old rows. The free is logged as a `Completion` record listing the pages, and only then is `End` logged.
5. **On abort or a crash before the commit,** undo runs newest first. It puts the catalog rows back first, then frees the new storage.

`TRUNCATE ... RESTART IDENTITY` also rewrites the sequence's row in the same transaction, so a rollback puts the counter back.

DROP TABLE makes no storage. After deleting the catalog rows of the table, its indexes and its sequences, it logs one `Update` that changes no page, whose undo, `Undo::Drop{heaps, trees}`, names the storage it gives up. Undoing it frees nothing, so an abort or a crash before the commit keeps the table whole. The commit frees the storage the way it frees a truncate's old pages.

A create appears in the in-memory catalog when its statement ends, and a dropped table leaves it only when the drop commits. A new table stays locked exclusively until the create ends, so no other transaction writes into storage that the create's undo might free.

**Sequences.** `nextval` counts in memory. Every 32 values, it logs the sequence row 32 ahead as a `System` record. That record is not synced on its own, but it is appended before the value is handed out, so the `Commit` of any transaction that stores the value syncs it too. A crash can skip up to 32 values and never reuses one. A clean shutdown saves the exact counter.

## Commit and abort

**Commit** ([`transaction.rs`](../../crates/storage/src/concurrency/transaction.rs) `commit_writes`, [`txn_log.rs`](../../crates/storage/src/wal/txn_log.rs)):

1. Append `Commit` and wait for the log sync. From this moment the transaction is committed.
2. Stamp its rows, one `Completion` record per page.
3. Append `End`, without waiting.

A transaction that wrote nothing logs nothing. A failure after step 1 stops the process instead of returning an error, because the caller would otherwise roll back a commit that is already durable. Recovery finishes the commit on restart.

**Abort** ([`undo.rs`](../../crates/storage/src/wal/undo.rs) `rollback`). This is the example from the design doc, with Hamilton's records:

```text
LSN 100  Update  tid 7  prev 0     undo: Heap{(5,3) before: Inserted}
LSN 160  Update  tid 7  prev 100   undo: IndexEntry{key 7}
LSN 200  Abort   tid 7  prev 160
LSN 210  Clr     tid 7  prev 200   undo_next 100   page 12: [LeafRemove key 7]
LSN 250  Clr     tid 7  prev 210   undo_next 0     page 5: [HeapSetMeta slot 3 deleted, ts 0]
LSN 280  End     tid 7  prev 250
```

The abort walks the transaction's records backward through `prev_lsn`, reading them from the log. It undoes each `Update` in an mtr that logs a `Clr`, whose `undo_next` is the record to undo after it. A `Clr` is never undone, and a walk that meets one jumps to its `undo_next`. So if a crash interrupts an undo, recovery continues where the last `Clr` left off and never undoes a change twice.

An undone insert is left as a deleted row with `ts 0`, which no reader sees. The startup sweep reclaims it.

Runtime abort and recovery use this same walk, so every conflict abort in a test also exercises recovery's undo.

Two orderings make undo safe, and nothing checks them automatically:

- **The heap write is logged before its index entries.** Undo runs newest first, so it removes the entry before it gives the row back. In the other order, another transaction could reuse the key, commit, and then have its entry removed by a resumed undo.
- **`End` is logged before the transaction's garbage is queued.** Garbage collection must not mark a row dead while recovery might still need to stamp it.

**The order of a transaction's own records is checked.** A transaction's handle on the log, `TxnLog<S>` in [`txn_log.rs`](../../crates/storage/src/wal/txn_log.rs), is a typestate. `commit` and `abort` consume a `Running` handle, and `end` consumes a `Committed` or `Aborting` one. Each `Logged` variant takes a handle in one state, so a `Completion` before the `Commit`, or an `Update` after it, does not compile. Analysis reads the kinds back from disk, where the compiler cannot help, so it checks them against one table, `TxnStatus::after` in the same file. A record the table refuses fails the open with `CorruptLog`. `Log::append` follows the same table, and stops the process on a refused record, since only a bug can append one.

## Checkpoints

A checkpoint limits how much log recovery reads, and lets old log segments be deleted. A background thread takes a **fuzzy** checkpoint every 30 seconds, or after a segment's worth of log, while transactions keep running ([`checkpoint.rs`](../../crates/storage/src/wal/checkpoint.rs)).

The two ARIES tables are not separate structures here:

- **The dirty page table** is the set of buffer frames whose `rec_lsn` is set. A frame's `rec_lsn` is the first record that changed it since it was last written.
- **The active transaction table** lives in the log (`Att` in [`txn_log.rs`](../../crates/storage/src/wal/txn_log.rs)). It is updated in the same critical section that appends each record. Each entry holds a status (running, committed or aborting) and the transaction's first and last LSNs.

A checkpoint:

1. writes the pages first changed before the previous checkpoint;
2. briefly holds off log appends, then reads the log's end (`start_lsn`), copies the active transaction table, and collects the dirty pages with their `rec_lsn`;
3. syncs the log and `app.db`;
4. writes `app.meta.tmp` with the checkpoint (`start_lsn`, `redo_lsn` = oldest `rec_lsn`, the dirty pages, the active transactions, the next Tid and the next page id) and the page directory, then renames it over `app.meta`;
5. deletes every log segment that recovery from this checkpoint will never read. That means everything before the oldest of `redo_lsn`, `start_lsn` and the first record of each active transaction.

The log has no checkpoint records. `app.meta` is the checkpoint, and the rename publishes it atomically. A transaction left open keeps every log segment since its first record.

A **sharp** checkpoint writes every dirty page first. A clean shutdown (`Storage::close`) takes one, and so does the end of recovery.

## Restart: analysis, redo, undo

Opening a database runs, in order:

1. `storage::open` ([`wal.rs`](../../crates/storage/src/wal.rs)) opens the log and `app.meta`.
2. `recovery::recover` ([`recovery.rs`](../../crates/storage/src/wal/recovery.rs)) runs analysis, redo and undo, then a sharp checkpoint.
3. `Database::open` ([`database.rs`](../../crates/hamilton-server/src/database.rs)) opens the catalog, then runs the [startup sweep](#the-startup-sweep).
4. The server starts accepting connections.

Here is a running example. The last checkpoint was taken at LSN 1000. Then:

```text
LSN 1000  Update  tid 60  page 5: image                 (insert by T60)
LSN 1100  Update  tid 61  page 8: [HeapSetMeta ...]     (delete by T61; page 8 was dirty at the checkpoint, rec_lsn 900)
LSN 1150  Commit  tid 61
LSN 1160  Completion tid 61  page 8: [HeapStamp ...]
LSN 1200  Update  tid 60  page 5: [HeapInsert ...]
          --- crash ---    T60 never committed. T61 committed but has no End.
```

### Analysis (`analyze`)

Analysis reads the log from the checkpoint's `start_lsn` to the end. It starts from the checkpoint's active transactions and dirty pages:

- Each record updates the transaction's entry. `Commit` marks it committed and `End` removes it. Analysis also checks that the transaction may log the record in its status (`TxnStatus::after`), and that the record's `prev_lsn` is that transaction's previous record. If not, the log is corrupt and the open fails.
- Each page a record changes, if not already dirty, becomes dirty from that record's LSN. A freed page is dropped.
- It raises the next Tid and the next page id above everything the log names, starting from the values the checkpoint saved, so neither is ever handed out twice. A reused Tid would let a new transaction treat a crashed one's rows as its own.

In the example, T60 is a loser (running) and T61 is committed without `End`. The dirty pages are page 8 from 900 and page 5 from 1000.

### Redo (`redo`)

Redo reads from `redo_lsn`, the oldest `rec_lsn`, here 900. It applies every record to every page that is dirty from at or before that record, losers' records included. This repeats history exactly up to the crash. Redo writes nothing to the log.

#### Redo never reads a page from disk

Textbook ARIES reads each page and skips records older than its pageLSN. Hamilton does not need to, because of one logging rule: the first change to a page after it was written to disk logs the whole page. A page's `rec_lsn` is set at exactly that change, so the first record redo applies to a page is always a full image, or a `HeapInit` that formats the page. Redo starts each page from zeros and builds it up from the log. This also repairs pages a crash tore halfway through a write. If a page's first record does not replace the page, the open fails with `CorruptLog`.

The cost is one image each time a page changes after it was written, which is more often than once per checkpoint under heavy eviction. `crates/bench/src/bin/wal.rs` measures the log bytes each statement writes.

### Undo (`finish_transactions`)

This step differs from textbook ARIES because of MVTO:

- **Committed transactions without `End`** are finished, not undone (`txn_log::complete`). Rows whose stamps did not reach the log get stamped, and the storage their truncates replaced and their drops gave up is freed. Then `End` is logged. In the example, T61's stamp at 1160 was redone, so there is nothing left to stamp, and T61 gets its `End`.
- **Every other transaction is a loser** and is undone (`undo::undo_losers`). A loser that was still running first gets its `Abort`, as a runtime abort does. Recovery keeps a to-undo list holding each loser's last LSN. It repeatedly takes the largest LSN, undoes that record and logs a `Clr`, then adds the record's `prev_lsn`, or the `Clr`'s `undo_next`, back to the list. A loser whose chain reaches the start gets its `End`. This is a single backward pass over all losers, using the same walk as a runtime abort. In the example, T60 gets its `Abort`, then its insert at 1200 is undone, then the one at 1000.

Afterwards no row stays locked. MVTO's in-memory state (the version chain, read timestamps and watermark) starts empty, as [Restart and crashes in MVTO](mvto.md#restart-and-crashes) describes.

### The end of recovery

Recovery ends with a sharp checkpoint. It then checks that no page is dirty and no transaction is left. The next open has nothing to redo, and every log segment but the last is deleted.

**A crash during recovery is just another crash.** Redo logs nothing. Completion and undo do log, so the next recovery redoes the stamps and `Clr`s that reached the log and continues from there.

## The startup sweep

Garbage collection keeps its to-do list in memory, so a crash loses it. Some garbage never joins that list at all: rows that undo left deleted at `ts 0`, and catalog rows of an undone CREATE. So `Database::open` runs `Catalog::sweep` after recovery and before the server accepts connections. It scans every heap and index, and removes dead rows and unneeded index entries with garbage collection's own code (see [Index management](index-management.md)).

It runs after recovery, not during it, because deciding whether an index entry is still needed requires decoding the row's key, and that needs the catalog's schema. Its changes are `System` records, and each leaves a valid state on its own, so a crash mid-sweep just means the next open sweeps again. It works in fixed batches, so its memory does not grow with the amount of garbage. It reads the whole database, so startup time grows with its size: about 330 ms for 300k rows.

## Rules for new code

Breaking one of these makes some crash unrecoverable:

- Change a durable page only inside an mtr, and let nothing fail between its first change and its last.
- A new `PageOp`'s `apply` must read only the page and the op, never a schema or comparator, because redo runs before the catalog is readable. It must check before it changes anything, so a failed op changes nothing.
- Log a transaction's change as an `Update` whose undo needs nothing from the catalog and always fits back in place. Log a change that no transaction undoes as `System`, and make it leave a valid state on its own.
- Make new storage in the same record whose undo frees it.
- Log a row's heap write before its index entries, and `End` before the transaction's garbage is queued.
- Log anything a committed row depends on, such as a sequence's log-ahead, before that row's `Commit`.
- Give a new kind of record a row in `TxnStatus::after`, and log it through a `TxnLog` in the state the row allows. Decide what the undo walk and `txn_log::complete` do with it.
- Bump the log's `VERSION` when the record format, or the order `TxnStatus::after` allows, changes.

## Crash tests

`cargo nextest run -p storage -p catalog -p planner` runs the crash tests:

- [`recovery.rs`](../../crates/storage/tests/recovery.rs) runs a workload on [`CrashFs`](../../crates/storage/tests/util/crash_fs.rs), which records every write, sync, rename and removal. It then recovers the files a crash after each event would leave, with unsynced writes kept, dropped or torn. Every page must equal what the surviving log records say. A new redo form needs a matching change to this test's oracle.
- [`txn_recovery.rs`](../../crates/storage/tests/txn_recovery.rs) does the same with transactions that commit, abort or are left open. It checks the rows, the locks and the index entries.
- [`truncate_recovery.rs`](../../crates/catalog/tests/truncate_recovery.rs), [`drop_recovery.rs`](../../crates/catalog/tests/drop_recovery.rs) and [`ddl_recovery.rs`](../../crates/catalog/tests/ddl_recovery.rs) also check that the pages in `app.db` are exactly those the catalog reaches, so no page leaks.
- [`sweep.rs`](../../crates/planner/tests/sweep.rs) crashes the startup sweep at every step.

`uv run sqltest/run.py recovery` ([`recovery.py`](../../sqltest/recovery.py)) kill -9s a real server while several clients write, restarts it, and checks what the clients saw acknowledged. SIGKILL keeps every write the OS accepted, so only the `CrashFs` tests cover torn pages and lost unsynced writes. `--seed N` replays a run.
