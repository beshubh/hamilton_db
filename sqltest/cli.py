"""Builds hamilton-cli and runs SQL scripts through it."""
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLI = ROOT / "target" / "release" / "hamilton-cli"
TIME = re.compile(r"\nTime: ([0-9.]+) ms$")


def build():
    subprocess.run(["cargo", "build", "--release", "-q", "-p", "hamilton-cli"], cwd=ROOT, check=True)


def run(db: Path, script: str) -> list[str]:
    """Runs `script` on the database at `db` and returns what each statement
    and command printed, in order: the CLI ends each with a blank line."""
    out = subprocess.run(
        [CLI, db], input=script + "\n", capture_output=True, text=True, check=True
    ).stdout
    return out.strip("\n").split("\n\n")


def split_time(block: str) -> tuple[str, float | None]:
    """A block printed with `\\timing` on, without its time, and the time."""
    match = TIME.search(block)
    if not match:
        return block, None
    return block[: match.start()], float(match[1])
