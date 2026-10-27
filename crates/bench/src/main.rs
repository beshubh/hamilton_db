//! Runs the same join queries on hamilton and Postgres and compares how
//! long they take. Both get the same freshly generated `products` and
//! `orders` tables; each query runs once to warm up, then `--runs` times,
//! and the median is reported. Results are checked to be the same rows.
//!
//! ```sh
//! cargo run --release -p bench -- [--orders N] [--products N] [--runs N]
//!     [--frames N] [--query NAME]... [--explain]
//!     [--pg URL] [--pg-db NAME] [--pg-parallel] [--no-pg]
//! ```

mod data;
mod engine;
mod queries;

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use engine::{Engine, Hamilton, Postgres, Row};
use queries::QUERIES;
use storage::buffer::buffer_pool_manager::DEFAULT_POOL_SIZE;

struct Args {
    orders: usize,
    products: usize,
    runs: usize,
    /// hamilton's buffer pool size, in pages.
    frames: usize,
    /// Run only the queries with these names; all if empty.
    queries: Vec<String>,
    explain: bool,
    pg_url: String,
    pg_db: String,
    pg_parallel: bool,
    no_pg: bool,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Args {
            orders: 200_000,
            products: 2_000,
            runs: 5,
            // As in the CLI.
            frames: DEFAULT_POOL_SIZE,
            queries: Vec::new(),
            explain: false,
            pg_url: "postgres://postgres@127.0.0.1:5432".to_owned(),
            pg_db: "hamilton_bench".to_owned(),
            pg_parallel: false,
            no_pg: false,
        };
        let mut it = std::env::args().skip(1);
        while let Some(flag) = it.next() {
            let mut value = || it.next().with_context(|| format!("{flag} needs a value"));
            match flag.as_str() {
                "--orders" => args.orders = value()?.parse()?,
                "--products" => args.products = value()?.parse()?,
                "--runs" => args.runs = value()?.parse()?,
                "--frames" => args.frames = value()?.parse()?,
                "--query" => args.queries.push(value()?),
                "--explain" => args.explain = true,
                "--pg" => args.pg_url = value()?,
                "--pg-db" => args.pg_db = value()?,
                "--pg-parallel" => args.pg_parallel = true,
                "--no-pg" => args.no_pg = true,
                _ => bail!("unknown flag {flag}; see the top of crates/bench/src/main.rs"),
            }
        }
        if args.runs == 0 || args.products == 0 {
            bail!("--runs and --products must be at least 1");
        }
        for name in &args.queries {
            if !QUERIES.iter().any(|q| q.name == name) {
                let names: Vec<_> = QUERIES.iter().map(|q| q.name).collect();
                bail!("no query named {name}; there are {}", names.join(", "));
            }
        }
        Ok(args)
    }
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    let mut engines: Vec<Box<dyn Engine>> = vec![Box::new(Hamilton::new(args.frames)?)];
    if !args.no_pg {
        let pg = Postgres::connect(&args.pg_url, &args.pg_db, args.pg_parallel)?;
        engines.push(Box::new(pg));
    }

    println!(
        "loading {} products and {} orders",
        args.products, args.orders
    );
    let inserts = data::inserts(args.products, args.orders);
    for engine in &mut engines {
        let start = Instant::now();
        for sql in data::SCHEMA
            .iter()
            .copied()
            .chain(inserts.iter().map(String::as_str))
        {
            engine
                .execute(sql)
                .with_context(|| format!("{} failed to load", engine.name()))?;
        }
        engine.finish_load()?;
        println!("  {:<10}{:>10}", engine.name(), ms(start.elapsed()));
    }
    println!();

    let selected: Vec<_> = QUERIES
        .iter()
        .filter(|query| args.queries.is_empty() || args.queries.iter().any(|q| q == query.name))
        .collect();
    if args.explain {
        for query in &selected {
            for engine in &mut engines {
                println!("{} plan for {}:", engine.name(), query.name);
                for row in engine.execute(&format!("EXPLAIN {}", query.sql))? {
                    println!("  {}", row[0].as_deref().unwrap_or(""));
                }
                println!();
            }
        }
    }

    let mut header = format!("{:<34}{:>9}", "query", "rows");
    for engine in &engines {
        header += &format!("{:>12}", engine.name());
    }
    if engines.len() == 2 {
        header += &format!("{:>9}", "ratio");
    }
    println!("{header}\n{}", "-".repeat(header.len()));

    let mut mismatches = Vec::new();
    for query in selected {
        let mut results: Vec<(Vec<Row>, Duration)> = Vec::new();
        for engine in &mut engines {
            let run = |engine: &mut Box<dyn Engine>| {
                engine
                    .execute(query.sql)
                    .with_context(|| format!("{} failed {}", engine.name(), query.name))
            };
            let rows = run(engine)?;
            let mut times = Vec::with_capacity(args.runs);
            for _ in 0..args.runs {
                let start = Instant::now();
                run(engine)?;
                times.push(start.elapsed());
            }
            times.sort();
            results.push((rows, times[times.len() / 2]));
        }

        let mut line = format!("{:<34}{:>9}", query.name, results[0].0.len());
        for (_, median) in &results {
            line += &format!("{:>12}", ms(*median));
        }
        if let [(hamilton, h_time), (postgres, p_time)] = results.as_mut_slice() {
            line += &format!("{:>8.1}x", h_time.as_secs_f64() / p_time.as_secs_f64());
            // ORDER BY is not checked: the rows are compared as multisets.
            hamilton.sort();
            postgres.sort();
            if hamilton != postgres {
                line += "  MISMATCH";
                mismatches.push(describe_mismatch(query.name, hamilton, postgres));
            }
        }
        println!("{line}");
    }

    for mismatch in &mismatches {
        println!("\n{mismatch}");
    }
    if !mismatches.is_empty() {
        bail!("{} queries gave different rows", mismatches.len());
    }
    Ok(())
}

fn describe_mismatch(name: &str, hamilton: &[Row], postgres: &[Row]) -> String {
    let mut text = format!(
        "{name}: hamilton gave {} rows, postgres {}",
        hamilton.len(),
        postgres.len()
    );
    if let Some((h, p)) = hamilton.iter().zip(postgres).find(|(h, p)| h != p) {
        text += &format!("\n  first difference (sorted): {h:?} vs {p:?}");
    }
    text
}

fn ms(d: Duration) -> String {
    format!("{:.1} ms", d.as_secs_f64() * 1000.0)
}
