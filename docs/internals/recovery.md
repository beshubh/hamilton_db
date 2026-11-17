# Recovery

A durable page reaches disk only after the log records that changed it are durable, so a crash loses nothing that the log kept. Opening a database (`storage::open` in [`wal.rs`](../../crates/storage/src/wal.rs)) replays the log into the pages, then finishes the transactions a crash cut off, before anything reads them. The design is ARIES with steal and no-force.

## One mtr writes one record

A durable page changes only through a mini-transaction ([`mtr.rs`](../../crates/storage/src/wal/mtr.rs)). `WritePageGuard` gives no mutable access, so code that changes a page another way does not compile. One mtr writes one log record, which covers every page the mtr changed. A record is in the log whole or not at all, so recovery never finds half of an mtr. That is why a B+ tree split is atomic. The leaf, its new sibling, and the parent go into one record. Temporary pages, from `TEMP_PAGE_BASE` up, are never logged, because nothing reads them after a restart.

Dropping an mtr logs whatever its pages hold, even when the code that changed them stopped halfway. So nothing may fail between an mtr's first change and its last. A B+ tree split or merge that fails after its first change aborts the process (`finish_change` in [`btree.rs`](../../crates/storage/src/index/btree.rs)). So does a panic while an mtr holds changes. The half-made change never reaches the log, and recovery redoes only whole mtrs.

## Rows are logged as ops, everything else as images

A caller changes a page in one of two ways. `MtrPage::apply` makes a `PageOp` ([`op.rs`](../../crates/storage/src/wal/op.rs)), one slot-level change to a table page or a B+ tree leaf, and logs the op in a few bytes. `MtrPage::raw_mut` hands out the page's bytes, and the mtr logs the whole page. Splits, merges, header pages, and page 0 go through `raw_mut`, so redo has no code for them.

Redo makes an op with the same `PageOp::apply` that made the change. Three rules keep the two equal:

- `apply` reads nothing but the page and the op. Redo runs before the catalog can be read, so an op never needs a schema or a comparator. The caller does that work first and logs its result. `LeafInsert` carries the slot that the comparator chose, and `HeapStamp` carries the length that the schema gave.
- `apply` checks before it changes anything. An op that fails leaves the page as it was, and nothing is logged for it.
- A leaf's slots start after its tombstone buffer, and the page does not record the buffer's size. The leaf ops carry it.

Debug builds check the rules in every mtr (`check_redo` in [`mtr.rs`](../../crates/storage/src/wal/mtr.rs)). The mtr decodes each page's logged ops, makes them on a copy of the page from before the mtr, and aborts the process unless the copy then equals the page. Every test that changes a row therefore also tests redo of that change.

## A page's first change after a write logs all of it

The mtr picks each page's form when it commits, and the caller has no say. A page changed through `raw_mut` is logged as an image. So is a page that was clean, with no `rec_lsn`, unless its ops start with `HeapInit`, which formats the whole page. Ops larger than a page are logged as an image too. Every other page is logged as its ops. The page's write latch keeps its `rec_lsn` from changing between that choice and the append.

The rule makes the record at a dirty page's `rec_lsn` replace the whole page. Redo starts each page that the checkpoint lists as dirty at that record, so it needs nothing of the page from disk, and it repairs three failures the same way:

- a torn page write,
- a page whose first write the saved directory does not know about, which reads as zeros, and
- a slot that still holds bytes the page wrote earlier.

The price is an image each time a page changes after a write. Under heavy eviction that comes more often than the one image per page per checkpoint that Postgres logs. [`wal.rs`](../../crates/bench/src/bin/wal.rs) in the bench crate measures the log that each kind of statement writes.

## Pages wait for their records

`queue_write` in [`page_guard.rs`](../../crates/storage/src/page/page_guard.rs) syncs the log through a page's LSN before it writes the page. An eviction can therefore wait on a log sync.

A frame's `rec_lsn` is the first record that changed the page since the page was last written. The frames that have a `rec_lsn` are the dirty page table. No other table exists that could disagree with them.

## The log is numbered segments

The log is a run of segment files next to the database, `<name>.log.00000001` and up ([`log.rs`](../../crates/storage/src/wal/log.rs)). An LSN is a position in the log as if it were one file. A segment starts at the LSN where the one before it ends, and its header names that LSN, so page LSNs and checkpoints never need to know which segment holds a record.

Segments are numbered instead of named by LSN, so a gap in the numbers shows a missing segment. Opening the log then fails with `CorruptLog` instead of reading on past the hole.

A batch goes whole into one segment, and a new segment starts only once every record before it is durable. Every segment but the last is therefore complete, and opening the log checks that each one ends where the next begins. Only the last segment can end in a torn record, and only there does a bad record mark the end of the log. Anywhere else it is an error. A new segment appears with its header in one atomic step (`Fs::replace`), so a crash while starting one leaves no new segment or an empty one.

## The `.meta` file is the checkpoint

The log has no checkpoint records. A checkpoint saves its record in the `.meta` file, next to the page directory. The record holds where analysis starts, where redo starts, the next page id, the next Tid, the dirty pages, and the transactions that have records but no `End`. One atomic rename publishes the record and the directory together, so recovery always starts from a directory that matches its checkpoint. Only [`checkpoint.rs`](../../crates/storage/src/wal/checkpoint.rs) reads the record.

A saved directory never maps a page to bytes that another page wrote:

- A slot that a durable page frees stays quarantined until a `.meta` that no longer lists the page is durable ([`disk_manager.rs`](../../crates/storage/src/disk/disk_manager.rs)).
- The log is synced before the `.meta` is saved, so every free that the saved directory shows is in the log.
- Temporary pages are never saved, so their slots are reused at once.

## Checkpoint steps come in a fixed order

The thread that `storage::open` starts takes a fuzzy checkpoint every 30 seconds, or after 64 MiB of log, while transactions keep running. Each step depends on the ones before it:

1. Write the pages first changed before the previous checkpoint began. Redo then reads about two checkpoint intervals of log.
2. Holding off appends, read the end of the log, where analysis starts, copy the active transaction table, scan the frames for dirty pages, and note the oldest free that is logged but not applied yet.
3. Copy the directory. A first write adds the page's slot to the directory before its frame is marked clean, so a page that the scan finds clean is in the copy, and a page still dirty gets redone from the record at its `rec_lsn`. An mtr applies its frees after it appends its record, so the copy may still list a freed page. Redo starts no later than that free and replays it.
4. Sync the log, then the db file. Every free in the copy is now logged, and every write that the dirty page table leaves out is durable.
5. Save the `.meta`, then release the slots that the copy no longer lists.
6. Remove the log segments that the saved checkpoint no longer needs.

A sharp checkpoint (`BufferPoolManager::checkpoint`) writes every dirty page first. `Storage::close` takes one, and recovery takes one when it ends. Dropping `Storage` without closing it is a crash, which is how tests crash a database.

## Checkpoints remove the log they no longer need

After each `.meta` rename, the checkpoint removes every segment whose records all come before `Checkpoint::log_needed_from`, the oldest record that recovery from that checkpoint reads ([`checkpoint.rs`](../../crates/storage/src/wal/checkpoint.rs)): the earliest of the checkpoint's start, its redo start, and the first record of each transaction in its table, which recovery may have to finish or undo. A transaction left open therefore keeps every segment from its first record on.

The bound must never move back from one checkpoint to the next, or a new checkpoint would need log that an older one removed. It holds because `rec_lsn`, pending frees and the transaction table all change only under the log's lock, and the checkpoint copies them under it. A dirty page, pending free or transaction older than the previous checkpoint's start was therefore already in that checkpoint, and pulled its bound down as far. A checkpoint whose bound is below the log's first LSN fails, and the saved `.meta` stays.

Segments go oldest first, and each removal is durable before the next starts, so a crash never leaves a gap in the numbers. A crash after the rename and before a removal leaves old segments behind, and the next checkpoint removes them. The segment being written is never removed, so after recovery the log is one segment, which may still hold records from before recovery's checkpoint. Nothing reads them.

## Recovery replays frees

Redo replays every free from the checkpoint's redo start, not only those after a page's first record. Replaying a free twice does no harm, because page ids are never reused. The next page id is raised past every page the log names. An id that was handed out but never logged can be handed out again after a crash, since nothing durable names it.

Redo appends nothing to the log. Finishing transactions after redo does, so a crash in the middle of it leaves records that the next recovery redoes, and that recovery goes on where they stop. Recovery ends with a sharp checkpoint. A log that lacks a record the checkpoint needs makes the open fail. A `.meta` with no checkpoint belongs to a database whose first open never finished, so records in the log next to it are left from other files, and recovery skips them.

## Tids come from the log

A Tid that wrote anything must never be handed out again. A new transaction with that Tid would take the crashed one's rows for its own, and one below it would read committed rows as newer than itself. The log keeps a Tid above every record it appends (`Log::next_tid`), each checkpoint saves it, and analysis raises it past every record it reads, so a restart hands out Tids above every Tid the log ever held, those in removed segments included. A transaction that logs nothing, such as a read-only one, leaves nothing on disk, so its Tid may come again after a crash. The `TransactionManager` starts from the log's.

## Redo never reads a page from disk

The checkpoint scans the frames while appends are held off, at the moment it reads the log's end. A page it finds clean takes its next change after that moment, and a change to a clean page logs all of it. So the first record that redo applies to every page replaces the page, and redo never trusts a page's copy on disk, which may be torn, stale, or another page's bytes. Redo fails the open with `CorruptLog` if a page's first record does not replace it ([`recovery.rs`](../../crates/storage/src/wal/recovery.rs)). Scanning the frames outside the lock would let a page take an op and be written in between, and then redo would have to read that page and trust its checksum.

## Undo is in each change's record

A transaction's change is logged as an `Update` record that says how to take it back ([`record.rs`](../../crates/storage/src/wal/record.rs)). Recovery runs before the catalog can be read, so undo needs nothing the catalog knows:

- A heap change is undone by slot, to the tuple's meta and bytes from before it. The mtr reads them from the page when the transaction's op is made (`MtrPage::apply`), so callers never build undo. The transaction holds the tuple's lock until it ends, so nothing else changes the slot; for the catalog rows a truncate changes, its table lock does that. A shrinking locked update keeps the tuple's length, so undoing a change never makes a tuple longer, and it always fits back in place ([Version storage](version-storage.md)).
- An index insert is undone by removing its entry by key, since splits move entries between pages. The B+ tree header records how the tree orders its keys, as its comparator describes them (`KeyComparator::describe`), so undo opens the tree from its header alone. Opening a tree with a comparator that orders keys another way fails with `BtreeMismatch`.
- New storage, which a create or a truncate makes, is undone by freeing all of it, as [New storage](#new-storage-is-a-change-of-its-transaction) describes.
- The `Update` of a heap insert or tuple change also holds the payload length that its commit keeps, which is shorter than the tuple when the update padded it. Recovery stamps from it, as it cannot decode the tuple without a schema.

Writes that no transaction undoes are logged as `System` records: garbage collection, `nextval` logging a sequence ahead, and an index entry that a unique check takes back at once. So are writes into storage that a transaction made and has not committed, such as an index build's entries: undo frees that storage whole.

## Undo logs a compensation for each change

Runtime abort and recovery undo a transaction the same way ([`undo.rs`](../../crates/storage/src/wal/undo.rs)). They walk its records from the newest through `prev_lsn`, and undo each `Update` in an mtr that logs a `Clr` naming the record before it (`undo_next`). A `Clr` is never undone: a walk that meets one jumps to its `undo_next`. So undo that a crash cuts off goes on after the last change it compensated, and never undoes a change twice. That matters once undo has given a row back: another transaction may write the row and commit, and undoing it again would overwrite that commit. An undo that finds nothing to change, such as an index entry already gone, still logs its `Clr`.

A runtime abort logs `Abort`, then the compensations, then `End`. It reads the transaction's records back from the log, so every abort exercises the code recovery uses.

Two orderings keep this safe, and nothing checks them:

- A row's heap write is logged before its index entries. Undo runs newest first, so it removes the entries before the `Clr` that gives the row back. In the other order, a later transaction could add the same key and locator, commit, and then a resumed undo would remove that entry.
- `End` is logged before the transaction's garbage is queued, so garbage collection marks a slot dead only after the `End` of the transaction that wrote it. Otherwise recovery could try to stamp a dead slot and fail the open.

A `Transaction` dropped while running, which debug builds refuse, never logs `End`. Its first record then holds back log removal until the next restart.

## Commit logs `Commit` before the stamps

A transaction that wrote anything commits by logging `Commit` and waiting for it to be durable (group commit). Only then does it stamp its tuples, one `Completion` record per page in page order, run its `on_end` hooks, and log `End` without waiting, or, after a truncate, once it has freed the old storage. A stamp makes a version visible, so a reader never sees a commit that a crash can lose. A reader that meets a tuple during the sync gets `Conflict`, as for any locked tuple. A transaction that wrote nothing logs nothing.

Once `Commit` is logged, the transaction has committed. A failure after it, or during undo, stops the process instead of returning: the caller would otherwise abort a durable commit, or leave a half-undone transaction running. Recovery finishes either from the log.

## Recovery finishes the transactions a crash cut off

Analysis starts from the checkpoint's transaction table and follows the log, with the same rules that keep the log's own table ([`txn_log.rs`](../../crates/storage/src/wal/txn_log.rs)), and checks that each record's `prev_lsn` is its transaction's previous record. After redo:

- A transaction whose `Commit` is in the log but whose `End` is not gets the rest of its stamps: every tuple its `Update`s name that it still holds. A truncate's old storage is freed. Then its `End`.
- Every other transaction in the table is undone, the newest record of any of them first, and gets its `End`.

No tuple stays locked. The undo log, the read timestamps, and the watermark restart empty, as [Restart and crashes in MVTO](mvto.md#restart-and-crashes) describes. Tuples that recovery's undo leaves deleted, and the garbage of transactions that ended before the crash, stay until a cleanup reclaims them.

## New storage is a change of its transaction

CREATE TABLE, CREATE INDEX and TRUNCATE make new storage, heaps and B+ trees ([`transaction.rs`](../../crates/storage/src/concurrency/transaction.rs)). One `Update` record makes each relation's new storage. Its mtr allocates and formats the pages, and its undo frees them: `Undo::Create` for a create, `Undo::Truncate` for a truncate, which also names the storage it replaces. So no crash leaves new storage that no transaction will free, and analysis raises the next page id past it. Allocating the pages in one record and logging their undo in another would leak them if a crash came in between.

Writes into the new storage before the commit need no undo of their own, so they are `System` records: a new sequence's row, and the entries an index build adds. Undo frees the storage whole, walking its heap and trees as they are then, so the pages those writes and the transaction's own inserts added go too. A runtime abort drops the transaction's garbage in that storage, an index build's entries included, as nothing is left to collect.

The catalog rows come after the storage, as the transaction's own changes (`Transaction::undoable`): a create's new rows, and the rows a truncate points at its new storage. Catalog rows carry `ts` 0 and are read without a snapshot, so undo leaves a new row deleted, and every scan of the system tables skips it. Undo runs newest first, so it takes the rows back before it frees the storage they name. A commit keeps all of it, and an abort or a crash before the commit takes all of it back.

The catalog in memory shows a create to every session once its statement ends ([`Catalog::create_table_with_constraints`](../../crates/catalog/src/catalog.rs)), and other transactions then plan on its index and add entries to its tree. An undo that freed the tree after that would leave their own undo nothing to remove its entries from. So a create commits when its statement ends: it changes the catalog in memory after everything that can fail, and CREATE TABLE and CREATE INDEX are rejected in a transaction block. A new table stays locked exclusively by its create until the create ends, so no other transaction puts rows in a heap that the create's undo would free.

The rows a truncate changes have no tuple lock. Its exclusive table lock keeps every other transaction off them until it ends, and that is what makes undoing them by their old bytes safe. For the sequence it means more: undoing the restart also takes back every `nextval` since, which is right only because the table lock keeps other transactions from inserting meanwhile. An abort puts back the counter that `nextval` keeps in memory as well. A way to call `nextval` that skips the table lock would break this.

A commit frees the old storage during garbage collection, once every transaction running when it ended has ended and their garbage, which may point into it, is collected. It logs `End` only after the free, so until then the transaction stays in the transaction table and holds back log removal. Recovery frees the old storage of a committed truncate without `End`. Each storage goes in one record, so the transaction's own records show recovery whether it went, and which pages need no stamp because they are gone.

Recovery finishes committed transactions in Tid order. Table locks make a truncate newer than every transaction that wrote the storage it replaces, so their stamps come before its free.

## The log decides what a crash test expects

[`recovery.rs`](../../crates/storage/tests/recovery.rs) runs a workload on [`CrashFs`](../../crates/storage/tests/util/crash_fs.rs), which records every write, sync, rename, and removal. The test then recovers the files that a crash after each of those events would leave, with none, all, or a random part of the unsynced writes lost or torn. Every page must equal what the records that survived leave, which is its last image with the ops logged after it made on it. The workload changes raw pages, table pages, and leaves, and the test checks that the log holds both forms. `CrashFs` keeps every segment a checkpoint removed, and the oracle reads those too, so a page whose last record left the log is still checked against it. A new redo form needs a matching change to that oracle.

[`txn_recovery.rs`](../../crates/storage/tests/txn_recovery.rs) does the same with transactions that commit, abort, and are left open. The rows after recovery must be exactly what the transactions whose `Commit` is in the log wrote, and each commit acknowledged before the crash must be among them. No tuple may stay locked, and every row must have its index entries. Undo is idempotent, so a crash test alone does not show a missing `Clr`. A concurrent test writes a row that a rollback gave back, and checks that a crash during the rollback keeps that commit.

A crash sweep must also start from a recovered database, because recovery leaves states a first crash never meets: pages last logged before the log's first segment, and free slots rebuilt from the `.meta`.

A page that no one frees leaves every row right, so row checks miss it. [`truncate_recovery.rs`](../../crates/catalog/tests/truncate_recovery.rs) and [`ddl_recovery.rs`](../../crates/catalog/tests/ddl_recovery.rs) also check that the pages the db file holds are exactly those the catalog reaches. The second crashes during creates that commit, fail, and are left open, and checks that each is whole or gone.
