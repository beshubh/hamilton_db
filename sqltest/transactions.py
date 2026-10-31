"""The transactions suite: runs transactions.sql and checks each statement's
result.

The file is a schema, then cases. Each case starts with `-- case: <name>`
and runs on a fresh database with the schema loaded. Statements take one
line each, and may end with their expected result:

    SELECT balance FROM accounts WHERE id = 1;   -- => 100

    -- => 100                 rows: values joined by ", ", rows by "; "
    -- => UPDATE 1            a command tag
    -- => error: conflicts    an error containing the text

A statement with no `-- =>` must not fail. `\\session x` lines switch
sessions, as in the CLI.
"""
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

import cli

FILE = Path(__file__).resolve().parent / "transactions.sql"
STATEMENT = re.compile(r"^(.*?;)\s*(?:--\s*=>\s*(.*))?$")


@dataclass
class Line:
    number: int
    text: str
    expected: str | None = None


def parse(path: Path) -> tuple[str, list[tuple[str, list[Line]]]]:
    schema, cases = [], []
    for number, text in enumerate(path.read_text().splitlines(), 1):
        if text.startswith("-- case: "):
            cases.append((text.removeprefix("-- case: ").strip(), []))
        elif not text.strip() or text.startswith("--"):
            continue
        elif not cases:
            schema.append(text)
        elif text.startswith("\\"):
            cases[-1][1].append(Line(number, text.strip()))
        else:
            sql, expected = STATEMENT.match(text.strip()).groups()
            cases[-1][1].append(Line(number, sql, expected))
    return "\n".join(schema), cases


def result(block: str) -> str:
    """A statement's printed output, in the `-- =>` form."""
    lines = block.splitlines()
    if len(lines) < 3 or not re.fullmatch(r"\(\d+ rows?\)", lines[-1]):
        return block
    rows = [", ".join(v.strip() for v in row.split("|")) for row in lines[2:-1]]
    return "; ".join(rows) or "(no rows)"


def matches(expected: str | None, actual: str) -> bool:
    if expected is None:
        return not actual.startswith("error:")
    if expected.startswith("error:"):
        return actual.startswith("error:") and expected.removeprefix("error:").strip() in actual
    return actual == expected


def run_case(schema: str, lines: list[Line]) -> list[str]:
    """Runs a case on a fresh database and returns its failures."""
    with tempfile.TemporaryDirectory() as tmp:
        blocks = cli.run(Path(tmp) / "test.db", "\n".join([schema] + [l.text for l in lines]))
    schema_blocks = len(blocks) - len(lines)
    failures = [f"schema: {b}" for b in blocks[:schema_blocks] if b.startswith("error:")]
    for line, block in zip(lines, blocks[schema_blocks:]):
        if line.text.startswith("\\session"):
            if not block.startswith('Session "'):
                return ["output out of step with the statements:\n" + "\n\n".join(blocks)]
            continue
        actual = result(block)
        if not matches(line.expected, actual):
            failures.append(
                f"line {line.number}: {line.text}\n"
                f"    expected: {line.expected or 'success'}\n    actual:   {actual}"
            )
    return failures


def run(args) -> tuple[int, int]:
    """Runs the cases whose names contain `args.k`. Returns how many ran and
    how many failed."""
    schema, cases = parse(FILE)
    cases = [(name, lines) for name, lines in cases if not args.k or args.k in name]
    if cases:
        print("\ntransactions\n")
    failed = 0
    for name, lines in cases:
        failures = run_case(schema, lines)
        print(f"  {'FAIL' if failures else 'ok  '}  {name}")
        for failure in failures:
            print("        " + failure.replace("\n", "\n        "))
        failed += bool(failures)
    return len(cases), failed
