# Recovery

A durable page reaches disk only after the log records that changed it are durable, so a crash loses nothing that the log kept. Opening a database (`storage::open` in [`wal.rs`](../../crates/storage/src/wal.rs)) replays the log into the pages before anything reads them. The design is ARIES with steal and no-force. It has no undo yet, so transactions are not recovered, as a later section explains.

## One mtr writes one record of full images

A durable page changes only through a mini-transaction ([`mtr.rs`](../../crates/storage/src/wal/mtr.rs)). `WritePageGuard` gives no mutable access, so code that changes a page another way does not compile. One mtr writes one log record, and the record holds the full image of every page the mtr changed. A record is in the log whole or not at all, so recovery never finds half of an mtr. That is why a B+ tree split is atomic. The leaf, its new sibling, and the parent go into one record.

With full images, redo needs no schema, no comparator, and nothing from disk. It never reads a page. It writes the image over whatever the frame holds. Temporary pages, from `TEMP_PAGE_BASE` up, are never logged, because nothing reads them after a restart.

Dropping an mtr logs whatever its pages hold, even when the code that changed them stopped halfway. So nothing may fail between an mtr's first change and its last. A B+ tree split or merge that fails after its first change aborts the process (`finish_change` in [`btree.rs`](../../crates/storage/src/index/btree.rs)). So does a panic while an mtr holds changes. The half-made change never reaches the log, and recovery redoes only whole mtrs.

## Pages wait for their records

`queue_write` in [`page_guard.rs`](../../crates/storage/src/page/page_guard.rs) syncs the log through a page's LSN before it writes the page. An eviction can therefore wait on a log sync.

A frame's `rec_lsn` is the first record that changed the page since the page was last written. The frames that have a `rec_lsn` are the dirty page table. No other table exists that could disagree with them.

The record at a dirty page's `rec_lsn` holds the page's image, because every record does. Redo starts each page at that record, so it repairs three failures the same way:

- a torn page write,
- a page whose first write the saved directory does not know about, which reads as zeros, and
- a slot that still holds bytes the page wrote earlier.

## The `.meta` file is the checkpoint

The log has no checkpoint records. A checkpoint saves its record in the `.meta` file, next to the page directory. The record holds where analysis starts, where redo starts, the next page id, and the dirty pages. One atomic rename publishes the record and the directory together, so recovery always starts from a directory that matches its checkpoint. Only [`checkpoint.rs`](../../crates/storage/src/wal/checkpoint.rs) reads the record.

A saved directory never maps a page to bytes that another page wrote:

- A slot that a durable page frees stays quarantined until a `.meta` that no longer lists the page is durable ([`disk_manager.rs`](../../crates/storage/src/disk/disk_manager.rs)).
- The log is synced before the `.meta` is saved, so every free that the saved directory shows is in the log.
- Temporary pages are never saved, so their slots are reused at once.

## Checkpoint steps come in a fixed order

The thread that `storage::open` starts takes a fuzzy checkpoint every 30 seconds, or after 64 MiB of log, while transactions keep running. Each step depends on the ones before it:

1. Write the pages first changed before the previous checkpoint began. Redo then reads about two checkpoint intervals of log.
2. Read the end of the log. Analysis starts there.
3. Scan the frames for dirty pages, note the oldest free that is logged but not applied yet, and only then copy the directory. A first write adds the page's slot to the directory before its frame is marked clean, so a page that the scan finds clean is in the copy, and a page still dirty gets redone from its image. An mtr applies its frees after it appends its record, so the copy may still list a freed page. Redo starts no later than that free and replays it.
4. Sync the log, then the db file. Every free in the copy is now logged, and every write that the dirty page table leaves out is durable.
5. Save the `.meta`, then release the slots that the copy no longer lists.

A sharp checkpoint (`BufferPoolManager::checkpoint`) writes every dirty page first. `Storage::close` takes one. Recovery takes one when it ends, and TRUNCATE takes one before its commit. Dropping `Storage` without closing it is a crash, which is how tests crash a database.

## Recovery replays frees and starts the log over

Redo replays every free from the checkpoint's redo start, not only those after a page's first record. Replaying a free twice does no harm, because page ids are never reused. The next page id is raised past every page the log names. An id that was handed out but never logged can be handed out again after a crash, since nothing durable names it.

Recovery appends nothing to the log, so a crash anywhere in it leaves the files recovery started from. It ends with a sharp checkpoint, then starts the log over in a new file whose header names its first LSN, so page LSNs on disk stay comparable with the log's. The `.meta` is saved before the log starts over, and recovery refuses to start over if that checkpoint still needs any log. A log that ends before the checkpoint began makes the open fail. A `.meta` with no checkpoint belongs to a database whose first open never finished, so records in the log next to it are left from other files, and recovery skips them.

## Commits sync the log

The engine syncs the log after a commit stamps its tuples, so a commit survives a crash once the client hears of it ([`engine.rs`](../../crates/planner/src/engine.rs)). A reader can see the stamps before the sync ends. The engine also records the next Tid in the catalog header before each statement that can write. A crash that keeps any of a transaction's writes therefore keeps a next Tid above that transaction's Tid.

## Transactions are not recovered

No log record names a transaction, so recovery replays page changes without knowing whose they are:

- A transaction that was running at the crash keeps its locked tuples. Every later reader and writer fails on them with `Conflict`. A commit that the crash cut off between its stamps leaves its other tuples locked in the same way.
- A crash during a TRUNCATE's commit can leave the table on its new heap.
- The undo log, the read timestamps, and the watermark restart empty, as [Restart and crashes in MVTO](mvto.md#restart-and-crashes) describes.

## The log decides what a crash test expects

[`recovery.rs`](../../crates/storage/tests/recovery.rs) runs a workload on [`CrashFs`](../../crates/storage/tests/util/crash_fs.rs), which records every write, sync, and rename. The test then recovers the files that a crash after each of those events would leave, with none, all, or a random part of the unsynced writes lost or torn. Every page must equal its last image in the records that survived. A new redo form needs a matching change to that oracle.
