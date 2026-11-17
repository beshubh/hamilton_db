use anyhow::{Context, bail};
use hamilton_server::{DEFAULT_PORT, Database, Server};
use storage::buffer::buffer_pool_manager::DEFAULT_POOL_SIZE;
use storage::wal::SEGMENT_SIZE;

const USAGE: &str = "\
usage: hamilton-server [--host HOST] [--port PORT] [--pool-size FRAMES]
                       [--log-segment-size BYTES] [FILE]

Serves the database in FILE (default hamilton.db, created if missing) over
the Postgres wire protocol until Ctrl-C or SIGTERM.

  --host HOST                address to listen on (default 127.0.0.1)
  --port PORT                port to listen on (default 6942; 0 picks a free one)
  --pool-size FRAMES         4 KiB pages the buffer pool holds (default 16384,
                             at least 64)
  --log-segment-size BYTES   log growth that starts a new log segment and a
                             checkpoint (default 64 MiB, at least 65536)

Set HAMILTON_LOG to error, warn, info, debug or trace to log to stderr.";

/// Writes `log` records to stderr. Off unless HAMILTON_LOG is set, not
/// RUST_LOG: a test runner that reads only the startup line would inherit a
/// RUST_LOG exported for other tools, fill its stderr pipe and block us.
struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!(
                "hamilton-server: {} {}: {}",
                record.level(),
                record.target(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

fn init_logging() -> anyhow::Result<()> {
    let Ok(level) = std::env::var("HAMILTON_LOG") else {
        return Ok(());
    };
    let level: log::LevelFilter = level.parse().map_err(|_| {
        anyhow::anyhow!(
            "HAMILTON_LOG must be off, error, warn, info, debug or trace, not {level:?}"
        )
    })?;
    log::set_logger(&StderrLogger).map_err(|err| anyhow::anyhow!("{err}"))?;
    log::set_max_level(level);
    Ok(())
}

struct Args {
    host: String,
    port: u16,
    pool_size: usize,
    log_segment_size: u64,
    file: String,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args {
        host: "127.0.0.1".to_owned(),
        port: DEFAULT_PORT,
        pool_size: DEFAULT_POOL_SIZE,
        log_segment_size: SEGMENT_SIZE,
        file: "hamilton.db".to_owned(),
    };
    let mut file = None;
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "--host" => args.host = iter.next().context("--host needs a value")?,
            "--port" => {
                let port = iter.next().context("--port needs a value")?;
                args.port = port.parse().with_context(|| format!("bad port {port:?}"))?;
            }
            "--pool-size" => args.pool_size = at_least(iter.next(), "--pool-size", MIN_POOL_SIZE)?,
            "--log-segment-size" => {
                args.log_segment_size =
                    at_least(iter.next(), "--log-segment-size", MIN_LOG_SEGMENT_SIZE)?
            }
            flag if flag.starts_with('-') => bail!("unknown option {flag}\n\n{USAGE}"),
            _ if file.is_some() => bail!("more than one database file given\n\n{USAGE}"),
            _ => file = Some(arg),
        }
    }
    if let Some(file) = file {
        args.file = file;
    }
    Ok(args)
}

/// Below these, the pool runs out of frames under a few connections, or the
/// log starts a segment and a checkpoint on almost every flush.
const MIN_POOL_SIZE: usize = 64;
const MIN_LOG_SEGMENT_SIZE: u64 = 64 << 10;

fn at_least<T: std::str::FromStr + PartialOrd + std::fmt::Display>(
    value: Option<String>,
    flag: &str,
    min: T,
) -> anyhow::Result<T> {
    let value = value.with_context(|| format!("{flag} needs a value"))?;
    match value.parse() {
        Ok(n) if n >= min => Ok(n),
        _ => bail!("{flag} must be a number of at least {min}, not {value:?}"),
    }
}

fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    init_logging()?;
    let db = Database::open_with(&args.file, args.pool_size, args.log_segment_size)
        .with_context(|| format!("opening {}", args.file))?;
    let server = Server::bind(db, (args.host.as_str(), args.port))
        .with_context(|| format!("listening on {}:{}", args.host, args.port))?;
    let handle = server.shutdown_handle();
    ctrlc::set_handler(move || handle.shutdown())?;
    // sqltest reads the port from this line.
    eprintln!(
        "hamilton-server: listening on {}, database {}",
        server.local_addr(),
        args.file
    );
    let db = server.run();
    db.close()?;
    eprintln!("hamilton-server: shut down");
    Ok(())
}
