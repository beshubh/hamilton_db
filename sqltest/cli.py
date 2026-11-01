"""Builds hamilton-server and hamilton-cli, serves databases, and runs SQL
scripts through the CLI."""
import re
import signal
import subprocess
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLI = ROOT / "target" / "release" / "hamilton-cli"
SERVER = ROOT / "target" / "release" / "hamilton-server"
TIME = re.compile(r"\nTime: ([0-9.]+) ms$")
LISTENING = re.compile(r"listening on \S+:(\d+),")


def build():
    subprocess.run(
        ["cargo", "build", "--release", "-q", "-p", "hamilton-cli", "-p", "hamilton-server"],
        cwd=ROOT,
        check=True,
    )


class Server:
    """hamilton-server on a free port, serving the database at `db`, for as
    long as the `with` block runs."""

    def __init__(self, db: Path):
        self.db = db
        self.port = None
        self.log = []

    def __enter__(self):
        self.proc = subprocess.Popen(
            [SERVER, "--port", "0", self.db], stderr=subprocess.PIPE, text=True
        )
        line = self.proc.stderr.readline()
        match = LISTENING.search(line)
        if not match:
            self.proc.kill()
            raise RuntimeError(f"hamilton-server didn't start: {line}{self.proc.stderr.read()}")
        self.port = int(match[1])
        # Drained so the server never blocks on a full pipe.
        self.drain = threading.Thread(target=lambda: self.log.extend(self.proc.stderr), daemon=True)
        self.drain.start()
        return self

    def __exit__(self, *exc):
        self.proc.send_signal(signal.SIGTERM)
        code = self.proc.wait(timeout=120)
        self.drain.join()
        if code != 0 and exc[0] is None:
            raise RuntimeError(f"hamilton-server exited with {code}:\n{''.join(self.log)}")


def run(server: Server, script: str) -> list[str]:
    """Runs `script` through the CLI on `server` and returns what each
    statement and command printed, in order: the CLI ends each with a blank
    line."""
    out = subprocess.run(
        [CLI, "--host", "127.0.0.1", "--port", str(server.port)],
        input=script + "\n",
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return out.strip("\n").split("\n\n")


def split_time(block: str) -> tuple[str, float | None]:
    """A block printed with `\\timing` on, without its time, and the time."""
    match = TIME.search(block)
    if not match:
        return block, None
    return block[: match.start()], float(match[1])
