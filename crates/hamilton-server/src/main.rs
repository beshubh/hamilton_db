use anyhow::{Context, bail};
use hamilton_server::{DEFAULT_PORT, Database, Server};

const USAGE: &str = "\
usage: hamilton-server [--host HOST] [--port PORT] [FILE]

Serves the database in FILE (default hamilton.db, created if missing) over
the Postgres wire protocol until Ctrl-C or SIGTERM.

  --host HOST   address to listen on (default 127.0.0.1)
  --port PORT   port to listen on (default 6942; 0 picks a free one)

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
    file: String,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args {
        host: "127.0.0.1".to_owned(),
        port: DEFAULT_PORT,
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

fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    init_logging()?;
    let db = Database::open(&args.file).with_context(|| format!("opening {}", args.file))?;
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
