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
- `DataType::Array` / `Value::Array` exist only for expressions (`ArrayExpr`). ARRAY has no element type (so `DataType` stays `Copy`) and cannot be stored: `Tuple::encode` returns `ValueError::NotStorable` for a non-NULL array and the catalog rejects ARRAY columns in `create_table`.
- Expressions live one per file in `crates/execution/src/expressions/`; `expressions.rs` holds only the `Expr` trait, the `AbstractExpr` enum (`enum_dispatch`) and shared helpers.
- The binder (`planner::Binder`) turns a `parser::Statement` into a `planner::BoundStatement` with its own `BoundExpr` tree. It does not build `execution` expressions: the planner turns `BoundExpr` into `AbstractExpr`, so `planner` doesn't depend on `execution` for binding. All type checking happens in the binder; its rules mirror the execution expressions.
  - A bound column is `idx` into the row it is evaluated on: the whole FROM row (all tables left to right) for WHERE / select list / ORDER BY, the join's own `left ++ right` row for ON, the single table for UPDATE/DELETE. The planner splits an ON column into `(tuple_idx, col_idx)` using the left side's width.
  - A NULL literal is untyped and takes its type from context (the other operand, BOOL under AND/OR/WHERE, the target column), defaulting to INT.
  - INSERT rows come out in table column order with omitted columns as typed NULLs. INT↔BIGINT stores get a `Cast`, folded at bind time for literals.
  - Aggregates and windows: the select list, HAVING and ORDER BY are first bound on the FROM row with `BoundExprKind::Aggregate` / `Window` left in place. Then `binder::aggregate::Rewriter` replaces those calls, and in an aggregating SELECT the GROUP BY keys too, with columns (empty `table`) of the aggregation row (keys ++ `BoundSelect::aggregates`) or the window row (that row ++ `BoundSelect::windows`). `Aggregate` / `Window` never leave the binder. A window aggregate's argument may contain an aggregate (`sum(count(*)) OVER ()`); a plain aggregate's may not.
  - Subqueries: a correlated reference is `PlannerError::Unsupported` (`Scope` keeps the outer scope only to detect it). FROM subqueries are planned inline; scalar / EXISTS / IN subqueries bind, but the planner returns `Unsupported` for them until executors can run a sub-plan.
- Executors (`execution::executors`, one per file) are built from a plan by `create_executor` and pulled in batches: `next` replaces the contents of `tuples` and `rids`, returns `true` only with at least one row, and returns `false` with both empty once done. `rids` holds the rids of rows read from a table (scan and filter); computed rows (values, projection, DML counts) have none. Plans without an executor fail with `ExecutionError::NoExecutor`.
  - CREATE TABLE has a plan too (`CreateTablePlan`), so every statement runs the same way: parse → bind → plan → execute.
  - Until there are transactions, writes are committed at once at timestamp 0 (`COMMIT_TS`, the catalog's timestamp too), deletes get no undo entry, and no index is maintained. Insert and delete read all their input before writing anything, so a statement that fails on a row changes nothing.
  - `TableIterator` holds the buffer pool `Arc`, not a borrow of the heap, so a scan executor can keep it next to its `Arc<TableInfo>`.
- SQL parsing lives in the `parser` crate, which wraps `sqlparser` (PostgreSqlDialect) and produces its own small `Statement` AST. sqlparser's nodes have dozens of dialect-specific fields, so each conversion rebuilds the one form it accepts and compares it with sqlparser's `Display` of the whole statement. Any clause we didn't extract makes them differ and is rejected as `Unsupported`, never silently ignored.

# Run database CLI
`cargo run -p hamilton-cli [db file]` (default `hamilton.db`, with `.meta` and `.log` beside it; all three are gitignored). It runs statements through parse → bind → plan → execute (`hamilton-cli/src/database.rs`) and prints results like psql. Every statement is flushed to disk as it runs, since there is no WAL to recover from.
