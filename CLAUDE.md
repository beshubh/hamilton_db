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
