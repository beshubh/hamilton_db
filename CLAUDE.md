# CLAUDE.md

Use this file to write down any surprises we face in development or testing.

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
