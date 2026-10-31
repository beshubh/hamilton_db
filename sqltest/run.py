#!/usr/bin/env python3
"""Runs the SQL test files through hamilton-cli: checks each case, then times it.

A test file is a list of cases. Each starts with `-- case: <name>`, may name
the index its second query must use with `-- index: <name>`, and holds two
queries: one on the plain table, one on the indexed table.

For each case, run.py checks that
  - both queries return the same output,
  - the plain query's plan uses no index, and the indexed query's plan uses
    the named index,
then runs each query --warmup times untimed and --runs times timed, and
reports the median. Exits non-zero if any check fails.

    python3 sqltest/run.py [--runs N] [--warmup N] [FILE.sql ...]
"""
import argparse
import re
import statistics
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CLI = ROOT / "target" / "release" / "hamilton-cli"
TIME = re.compile(r"^Time: ([0-9.]+) ms$")


@dataclass
class Case:
    name: str
    index: str | None
    plain: str
    indexed: str


def parse_cases(path: Path) -> list[Case]:
    cases = []
    for block in re.split(r"^-- case: ", path.read_text(), flags=re.M)[1:]:
        name, _, body = block.partition("\n")
        index = re.search(r"^-- index: (\S+)", body, flags=re.M)
        sql = "\n".join(l for l in body.splitlines() if not l.startswith("--"))
        queries = [q.strip() + ";" for q in sql.split(";") if q.strip()]
        if len(queries) != 2:
            sys.exit(f"{path}: case '{name}' needs 2 queries, has {len(queries)}")
        cases.append(Case(name.strip(), index and index[1], *queries))
    return cases


def run_cli(db: Path, script: str) -> list[str]:
    """Runs `script` with timing on and returns each statement's output,
    without its timing line. Exits if a statement failed."""
    out = subprocess.run(
        [CLI, db], input="\\timing\n" + script, capture_output=True, text=True, check=True
    ).stdout
    for line in out.splitlines():
        if line.startswith("error:"):
            sys.exit(f"hamilton-cli failed: {line}")
    outputs, current = [], []
    for line in out.splitlines()[1:]:  # Skip "Timing is on."
        match = TIME.match(line)
        if match:
            outputs.append(("\n".join(current).strip("\n"), float(match[1])))
            current = []
        else:
            current.append(line)
    return outputs


def load(db: Path):
    data = subprocess.run(
        [sys.executable, HERE / "gen_data.py"], capture_output=True, text=True, check=True
    ).stdout
    run_cli(db, (HERE / "schema.sql").read_text() + data)


def check_plans(case: Case, plain_plan: str, indexed_plan: str) -> list[str]:
    problems = []
    if "IndexScan" in plain_plan:
        problems.append(f"plain query uses an index:\n{plain_plan}")
    if case.index and f"IndexScan using {case.index}" not in indexed_plan:
        problems.append(f"indexed query doesn't use {case.index}:\n{indexed_plan}")
    return problems


def run_case(db: Path, case: Case, warmup: int, runs: int):
    """Returns (plain ms, indexed ms, row count, problems)."""
    n = warmup + runs
    script = "\n".join(
        [f"EXPLAIN {case.plain}", f"EXPLAIN {case.indexed}"]
        + [case.plain] * n
        + [case.indexed] * n
    )
    outputs = run_cli(db, script)
    (plain_plan, _), (indexed_plan, _) = outputs[:2]
    plain, indexed = outputs[2 : 2 + n], outputs[2 + n :]

    problems = check_plans(case, plain_plan, indexed_plan)
    if plain[0][0] != indexed[0][0]:
        problems.append(f"results differ:\n--- plain\n{plain[0][0]}\n--- indexed\n{indexed[0][0]}")
    rows = re.search(r"\((\d+) rows?\)", plain[0][0])
    median = lambda results: statistics.median(ms for _, ms in results[warmup:])
    return median(plain), median(indexed), rows and int(rows[1]), problems


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("files", nargs="*", type=Path, default=[HERE / "index_storage.sql"])
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=5)
    args = parser.parse_args()

    subprocess.run(["cargo", "build", "--release", "-q", "-p", "hamilton-cli"], cwd=ROOT, check=True)
    failed = 0
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "sqltest.db"
        load(db)
        for path in args.files:
            print(f"\n{path.name}  (median of {args.runs} runs, ms)\n")
            print(f"{'case':40} {'rows':>6} {'no index':>10} {'index':>10} {'speedup':>8}")
            for case in parse_cases(path):
                plain, indexed, rows, problems = run_case(db, case, args.warmup, args.runs)
                status = "" if not problems else "  FAIL"
                print(
                    f"{case.name:40} {rows if rows is not None else '':>6} "
                    f"{plain:10.3f} {indexed:10.3f} {plain / indexed:7.1f}x{status}"
                )
                for problem in problems:
                    print("    " + problem.replace("\n", "\n    "))
                failed += bool(problems)
    if failed:
        sys.exit(f"\n{failed} case(s) failed")


if __name__ == "__main__":
    main()
