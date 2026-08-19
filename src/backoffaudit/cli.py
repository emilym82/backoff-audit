"""Streaming CLI: reads one attempt timestamp per line and checks each one
against a RetryPolicy as it arrives, so a multi-gigabyte log (or a live
`tail -f`) never has to be held in memory at once."""

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, TextIO

from .policy import RetryPolicy


@dataclass
class AttemptReport:
    line: str
    is_violation: bool


def parse_timestamp(raw: str) -> float:
    raw = raw.strip()
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(raw).timestamp()
    except ValueError:
        raise ValueError(f"unrecognized timestamp: {raw!r}")


def read_timestamps(fp: TextIO) -> Iterator[float]:
    for line in fp:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        yield parse_timestamp(line)


def audit(policy: RetryPolicy, timestamps: Iterator[float]) -> Iterator[AttemptReport]:
    """Yields one report per attempt. Keeps only the previous timestamp and
    the attempt count in memory, regardless of how long the stream runs."""
    prev_ts = None
    attempt = 0

    for ts in timestamps:
        attempt += 1

        if attempt > policy.max_attempts:
            yield AttemptReport(
                f"attempt {attempt} at {ts:.3f}  "
                f"VIOLATION: exceeds max_attempts={policy.max_attempts}",
                is_violation=True,
            )
            prev_ts = ts
            continue

        if prev_ts is None:
            yield AttemptReport(
                f"attempt {attempt} at {ts:.3f}  ok (first attempt)",
                is_violation=False,
            )
            prev_ts = ts
            continue

        delay = ts - prev_ts
        lo, hi = policy.allowed_window(attempt - 1)
        if delay < lo:
            is_violation = True
            verdict = f"VIOLATION: too fast (expected [{lo:.3f}, {hi:.3f}])"
        elif delay > hi:
            is_violation = True
            verdict = f"VIOLATION: too slow (expected [{lo:.3f}, {hi:.3f}])"
        else:
            is_violation = False
            verdict = "ok"
        yield AttemptReport(
            f"attempt {attempt} at {ts:.3f}  delay={delay:.3f}s  {verdict}",
            is_violation=is_violation,
        )
        prev_ts = ts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backoff-audit",
        description=(
            "Check a stream of retry attempt timestamps (one per line, "
            "epoch seconds or ISO 8601) against a backoff policy."
        ),
    )
    parser.add_argument(
        "logfile",
        nargs="?",
        help="file of attempt timestamps, one per line; defaults to stdin",
    )
    parser.add_argument("--max-attempts", type=int, required=True)
    parser.add_argument("--base-delay", type=float, required=True, help="seconds")
    parser.add_argument("--multiplier", type=float, default=2.0)
    parser.add_argument("--max-delay", type=float, required=True, help="seconds")
    parser.add_argument(
        "--jitter", choices=["none", "full", "equal"], default="none"
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.05,
        help="slack fraction for --jitter none, to absorb clock imprecision",
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    policy = RetryPolicy(
        max_attempts=args.max_attempts,
        base_delay=args.base_delay,
        multiplier=args.multiplier,
        max_delay=args.max_delay,
        jitter=args.jitter,
        tolerance=args.tolerance,
    )

    source = open(args.logfile) if args.logfile else sys.stdin
    attempts = 0
    violations = 0
    try:
        for report in audit(policy, read_timestamps(source)):
            print(report.line)
            attempts += 1
            violations += report.is_violation
    finally:
        if source is not sys.stdin:
            source.close()

    print(f"--- {attempts} attempts, {violations} violation(s)")
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
