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
    long as the `with` block runs. `log` holds what it wrote to stderr."""

    def __init__(self, db: Path, binary: Path = SERVER, args=(), env=None):
        self.command = [binary, "--port", "0", *args, db]
        self.env = env
        self.port = None
        self.log = []

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        if self.proc.poll() is None:
            self.stop(check=exc[0] is None)

    def start(self):
        """Starts the server and waits until it listens."""
        self.spawn()
        self.wait()

    def wait(self, timeout=120):
        """Waits until the server that `spawn` started listens, and fails if
        it exits first or takes longer than `timeout` seconds."""
        try:
            if not self.listening.wait(timeout):
                raise RuntimeError(f"hamilton-server didn't listen within {timeout} s: {''.join(self.log)}")
            if self.port is None:
                self.proc.wait()
                raise RuntimeError(f"hamilton-server didn't start: {''.join(self.log)}")
        except BaseException:
            self.kill()
            raise

    def stop(self, check=True):
        """Stops the server with SIGTERM, as Ctrl-C would, and fails unless
        it exits cleanly."""
        self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=120)
        except BaseException:
            self.kill()
            raise
        self.drain.join()
        if code != 0 and check:
            raise RuntimeError(f"hamilton-server exited with {code}:\n{''.join(self.log)}")

    def spawn(self):
        """Starts the server without waiting for it to listen."""
        self.proc = subprocess.Popen(self.command, stderr=subprocess.PIPE, text=True, env=self.env)
        self.listening = threading.Event()
        # Drained so the server never blocks on a full pipe.
        self.drain = threading.Thread(target=self._read_log, daemon=True)
        self.drain.start()

    def _read_log(self):
        for line in self.proc.stderr:
            self.log.append(line)
            if self.port is None and (match := LISTENING.search(line)):
                self.port = int(match[1])
                self.listening.set()
        self.listening.set()

    def kill(self) -> int:
        """Kills the server with SIGKILL, as a crash would, and returns its
        exit status: -9, unless it had died already."""
        self.proc.kill()
        code = self.proc.wait()
        self.drain.join()
        return code


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
