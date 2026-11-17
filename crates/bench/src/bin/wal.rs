//! Measures the log each kind of row change writes: single-row INSERT,
//! UPDATE and DELETE statements, each its own transaction, on a table with
//! a primary key and a secondary index. Every statement also logs page 0,
//! where the engine keeps the next Tid, so its images are counted apart.
//!
//! ```sh
//! cargo run --release -p bench --bin wal -- [--rows N] [--frames N]
//! ```

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use catalog::Catalog;
use common::Lsn;
use planner::ExecutionEngine;
use storage::buffer::buffer_pool_manager::DEFAULT_POOL_SIZE;
use storage::disk::fs::OsFs;
use storage::wal::record::{Kind, PageChanges, PageRedo};
use tempfile::TempDir;

/// Bytes of log over a run of statements, by what they hold.
#[derive(Default)]
struct Volume {
    records: usize,
    total: u64,
    page0_images: usize,
    images: usize,
    ops_entries: usize,
}

fn main() -> Result<()> {
    let (mut rows, mut frames) = (2_000, DEFAULT_POOL_SIZE);
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().with_context(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--rows" => rows = value?.parse()?,
            "--frames" => frames = value?.parse()?,
            _ => bail!("unknown flag {flag}; see the top of crates/bench/src/bin/wal.rs"),
        }
    }

    let dir = TempDir::new()?;
    // No checkpoints: each one makes every page clean, and the next change
    // to it logs it whole, so they would add noise that depends on timing.
    let storage = storage::open_with(
        Arc::new(OsFs),
        dir.path().join("wal.db"),
        frames,
        None,
        storage::wal::SEGMENT_SIZE,
    )?;
    let catalog = Arc::new(Catalog::open(Arc::clone(&storage.bpm))?);
    let mut engine = ExecutionEngine::new(catalog)?;
    let mut run = |sql: &str| -> Result<()> {
        engine
            .execute_sql(sql)
            .with_context(|| format!("running {sql}"))?;
        Ok(())
    };
    run("CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, body VARCHAR(100) NOT NULL)")?;
    run("CREATE INDEX t_k ON t (k)")?;

    println!("{rows} statements per kind, {frames} frames");
    println!(
        "{:<12}{:>12}{:>10}{:>12}{:>12}{:>12}",
        "statement", "bytes/stmt", "records", "page 0 img", "other img", "op entries"
    );
    let log = Arc::clone(storage.bpm.log());
    for name in ["insert", "update body", "update key", "delete"] {
        let start = log.end();
        for i in 0..rows {
            run(&statement(name, i))?;
        }
        log.flush_all()?;
        let volume = measure(&log, start)?;
        let per = |n: usize| n as f64 / rows as f64;
        println!(
            "{name:<12}{:>12.0}{:>10.2}{:>12.2}{:>12.2}{:>12.2}",
            volume.total as f64 / rows as f64,
            per(volume.records),
            per(volume.page0_images),
            per(volume.images),
            per(volume.ops_entries),
        );
    }
    Ok(())
}

/// Statement `i` of the kind `name`, each on the row with id `i`.
fn statement(name: &str, i: usize) -> String {
    let body = |tag: &str| format!("{tag}-{i:06}-{}", "x".repeat(60));
    match name {
        "insert" => format!(
            "INSERT INTO t VALUES ({i}, {}, '{}')",
            i * 7 % 1000,
            body("a")
        ),
        "update body" => format!("UPDATE t SET body = '{}' WHERE id = {i}", body("b")),
        "update key" => format!("UPDATE t SET k = {} WHERE id = {i}", i * 13 % 1000 + 1000),
        _ => format!("DELETE FROM t WHERE id = {i}"),
    }
}

/// What the log holds from `start` to its end.
fn measure(log: &storage::wal::log::Log, start: Lsn) -> Result<Volume> {
    let mut volume = Volume {
        total: log.end() - start,
        ..Volume::default()
    };
    let mut scan = log.scan(start);
    while let Some(record) = scan.next_record()? {
        volume.records += 1;
        if !matches!(
            record.kind,
            Kind::Update | Kind::Clr | Kind::Completion | Kind::System
        ) {
            continue;
        }
        for (page_id, redo) in PageChanges::decode(record.body)?.pages() {
            match redo {
                PageRedo::Image(_) if page_id == 0 => volume.page0_images += 1,
                PageRedo::Image(_) => volume.images += 1,
                PageRedo::Ops(_) => volume.ops_entries += 1,
            }
        }
    }
    Ok(volume)
}
