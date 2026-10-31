#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Runs the SQL test suites through hamilton-cli.

    uv run sqltest/run.py                          # every suite
    uv run sqltest/run.py transactions             # some suites
    uv run sqltest/run.py -k "lost update"         # cases whose name contains the text
    uv run sqltest/run.py index_storage --runs 50  # more timed runs per query

Builds the CLI first. Exits non-zero if any case fails.
"""
import argparse
import sys

import cli
import index_storage
import transactions

SUITES = {
    "index_storage": index_storage,
    "transactions": transactions,
}


def main():
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("suites", nargs="*", metavar="SUITE",
                        help=f"suites to run (default: all): {', '.join(SUITES)}")
    parser.add_argument("-k", metavar="TEXT", help="run only cases whose name contains TEXT")
    parser.add_argument("--runs", type=int, default=20, help="timed runs per query (index_storage)")
    parser.add_argument("--warmup", type=int, default=5, help="untimed runs first (index_storage)")
    args = parser.parse_args()
    if unknown := [s for s in args.suites if s not in SUITES]:
        parser.error(f"unknown suite {unknown[0]!r}; choose from {', '.join(SUITES)}")

    cli.build()
    ran = failed = 0
    for name in args.suites or SUITES:
        suite_ran, suite_failed = SUITES[name].run(args)
        ran, failed = ran + suite_ran, failed + suite_failed
    if not ran:
        sys.exit("no cases match")
    if failed:
        sys.exit(f"\n{failed} case(s) failed")


if __name__ == "__main__":
    main()
