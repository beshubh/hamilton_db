# CLAUDE.md

Use this file to write down any surprises we face in development or testing. Any time you are corrected you should note that down here for future purposes.

## What this is

hamilton_db is a page-based OLTP relational database written in Rust.

## Commands

```sh
cargo build --workspace
cargo test --workspace
cargo test -p storage
cargo test -p storage buffer_pool_manager
cargo test -p storage buffer_pool_manager::tests::delete_page -- --exact
cargo clippy --workspace
cargo fmt --all
```

## Decisions and corrections

- The page directory and page metadata (page_id → offset, free space per page, free and allocated slots) live in a separate `<db>.meta` file, not inside the db file. The db file holds only page data.
- Don't run `cargo fmt --all`: it reformats unrelated files. Format only the files you changed (`rustfmt --edition 2024 <file>`).
  - `rustfmt` follows `mod` declarations, so formatting `lib.rs` (or any file declaring modules) also rewrites those submodules. Check `git diff --stat` afterwards and revert files you didn't mean to touch.
- Commits and PRs carry no Claude attribution: no `Co-Authored-By` trailer, no "Generated with Claude Code" line.
- Library crates return typed `thiserror` errors (`storage::StorageError`, `execution::ExecutionError`, …), each with a crate-level `Result<T>` alias. Higher crates wrap lower ones with `#[from]`. Only binaries (`bench`, future CLI/server) use `anyhow`. Tests match on variants (`matches!(err, StorageError::NoFreeFrame)`), not `io::ErrorKind`.
- MVCC uses an undo log, not timestamps in the slot. Each tuple header has `ts` (commit ts, or txn id with the top bit set while uncommitted, which acts as the write lock) and `undo_ptr`, pointing to the newest delta in a newest-to-oldest chain of column before-images. A delete is a logical flag in the tuple header plus an undo entry. Vacuum reclaims space later: it removes the index entry and then frees the slot (`TablePage::free_slot`), after which inserts may reuse the slot.
- SQL parsing lives in the `parser` crate, which wraps `sqlparser` (PostgreSqlDialect) and produces its own small `Statement` AST. sqlparser's nodes have dozens of dialect-specific fields, so each conversion rebuilds the one form it accepts and compares it with sqlparser's `Display` of the whole statement. Any clause we didn't extract makes them differ and is rejected as `Unsupported`, never silently ignored.

# Run database CLI
`cargo run -p hamilton-cli`
