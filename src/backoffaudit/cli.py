"""Streaming CLI: reads one attempt timestamp per line and checks each one
against a RetryPolicy as it arrives, so a multi-gigabyte log (or a live
`tail -f`) never has to be held in memory at once. With --group-by-request,
lines carry a request id and attempts for different requests are checked
independently in the same pass."""

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Optional, TextIO

from .config import load_policy_config
from .policy import RetryPolicy

DEFAULTS = {"multiplier": 2.0, "jitter": "none", "tolerance": 0.05}
REQUIRED_KEYS = ("max_attempts", "base_delay", "max_delay")


@dataclass
class AttemptReport:
    attempt: int
    timestamp: float
    delay: Optional[float]
    window: Optional[tuple]
    is_violation: bool
    reason: str
    message: str
    request_id: Optional[str] = None

    def render_text(self) -> str:
        prefix = f"attempt {self.attempt} at {self.timestamp:.3f}"
        if self.request_id is not None:
            prefix = f"[{self.request_id}] {prefix}"
        if self.delay is None:
            return f"{prefix}  {self.message}"
        return f"{prefix}  delay={self.delay:.3f}s  {self.message}"

    def render_json(self) -> str:
        return json.dumps(
            {
                "request_id": self.request_id,
                "attempt": self.attempt,
                "timestamp": self.timestamp,
                "delay": self.delay,
                "window": list(self.window) if self.window is not None else None,
                "violation": self.is_violation,
                "reason": self.reason,
                "message": self.message,
            }
        )


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


def read_grouped_records(fp: TextIO) -> Iterator[tuple[str, float]]:
    """Like read_timestamps, but each line is '<request_id> <timestamp>' —
    the format --group-by-request expects so a log covering several
    concurrent requests can still be audited in one pass over the file."""
    for line in fp:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError(
                f"--group-by-request expects '<request_id> <timestamp>' per line, got: {line!r}"
            )
        request_id, raw_ts = parts
        yield request_id, parse_timestamp(raw_ts)


class _RequestState:
    """Everything audit needs to remember about one request's attempt
    stream: nothing but the previous timestamp and how many real attempts
    it's seen. Grouping by request id just means keeping one of these per
    id instead of one for the whole file."""

    __slots__ = ("prev_ts", "attempt")

    def __init__(self):
        self.prev_ts: Optional[float] = None
        self.attempt = 0


def _evaluate(policy: RetryPolicy, state: _RequestState, ts: float) -> AttemptReport:
    """Checks one timestamp against a request's running state, mutating
    that state to reflect the attempt (or non-attempt) it represents.

    A line whose timestamp doesn't advance past the previous one (a repeated
    line, or clock skew putting it earlier) isn't a genuine retry attempt, so
    it's flagged and skipped rather than being counted against max_attempts
    or thrown into the exponential wait-index math — one bad line shouldn't
    corrupt the delay computed for every real attempt that follows it.
    """
    prev_ts = state.prev_ts

    if prev_ts is not None and ts <= prev_ts:
        if ts == prev_ts:
            reason = "duplicate_timestamp"
            message = "VIOLATION: duplicate timestamp (same as the previous attempt)"
        else:
            reason = "out_of_order"
            message = "VIOLATION: out-of-order timestamp (earlier than the previous attempt)"
        return AttemptReport(
            attempt=state.attempt + 1,
            timestamp=ts,
            delay=ts - prev_ts,
            window=None,
            is_violation=True,
            reason=reason,
            message=message,
        )

    state.attempt += 1
    attempt = state.attempt

    if attempt > policy.max_attempts:
        state.prev_ts = ts
        return AttemptReport(
            attempt=attempt,
            timestamp=ts,
            delay=None,
            window=None,
            is_violation=True,
            reason="max_attempts_exceeded",
            message=f"VIOLATION: exceeds max_attempts={policy.max_attempts}",
        )

    if prev_ts is None:
        state.prev_ts = ts
        return AttemptReport(
            attempt=attempt,
            timestamp=ts,
            delay=None,
            window=None,
            is_violation=False,
            reason="first_attempt",
            message="ok (first attempt)",
        )

    delay = ts - prev_ts
    lo, hi = policy.allowed_window(attempt - 1)
    if delay < lo:
        is_violation = True
        reason = "too_fast"
        message = f"VIOLATION: too fast (expected [{lo:.3f}, {hi:.3f}])"
    elif delay > hi:
        is_violation = True
        reason = "too_slow"
        message = f"VIOLATION: too slow (expected [{lo:.3f}, {hi:.3f}])"
    else:
        is_violation = False
        reason = "ok"
        message = "ok"
    state.prev_ts = ts
    return AttemptReport(
        attempt=attempt,
        timestamp=ts,
        delay=delay,
        window=(lo, hi),
        is_violation=is_violation,
        reason=reason,
        message=message,
    )


def audit(policy: RetryPolicy, timestamps: Iterator[float]) -> Iterator[AttemptReport]:
    """Yields one report per attempt of a single request's stream. Keeps
    only the previous timestamp and the attempt count in memory, regardless
    of how long the stream runs."""
    state = _RequestState()
    for ts in timestamps:
        yield _evaluate(policy, state, ts)


def audit_grouped(
    policy: RetryPolicy, records: Iterator[tuple[str, float]]
) -> Iterator[AttemptReport]:
    """Same as audit(), but for a log interleaving several requests, each
    tagged with a request id. Attempt counts and delay windows are tracked
    independently per request id, in a single pass over the input — no
    request's line has to wait for another's to finish before it can be
    checked.

    Memory use is one _RequestState per distinct request id seen so far,
    not one per line, so this stays cheap for the normal case of many
    attempts against a bounded set of in-flight requests."""
    states: dict[str, _RequestState] = {}
    for request_id, ts in records:
        state = states.setdefault(request_id, _RequestState())
        report = _evaluate(policy, state, ts)
        report.request_id = request_id
        yield report


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
    parser.add_argument(
        "--config",
        help="JSON or TOML file supplying policy settings; flags below override it",
    )
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--base-delay", type=float, default=None, help="seconds")
    parser.add_argument("--multiplier", type=float, default=None)
    parser.add_argument("--max-delay", type=float, default=None, help="seconds")
    parser.add_argument(
        "--jitter", choices=["none", "full", "equal"], default=None
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=None,
        help="slack fraction for --jitter none, to absorb clock imprecision",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="'json' emits one JSON object per line (newline-delimited) plus a "
        "JSON summary object, for machine consumption",
    )
    parser.add_argument(
        "--group-by-request",
        action="store_true",
        help="each line is '<request_id> <timestamp>'; attempt counts and delay "
        "windows are tracked independently per request id, in one pass over "
        "a log that interleaves several requests",
    )
    return parser


def resolve_policy_settings(args, parser: argparse.ArgumentParser) -> dict:
    """Merges (in increasing precedence) built-in defaults, --config file
    contents, and any flags the user actually passed on the command line."""
    settings = dict(DEFAULTS)

    if args.config:
        try:
            settings.update(load_policy_config(args.config))
        except (OSError, ValueError) as exc:
            parser.error(str(exc))

    for key in ("max_attempts", "base_delay", "multiplier", "max_delay", "jitter", "tolerance"):
        value = getattr(args, key)
        if value is not None:
            settings[key] = value

    missing = [key for key in REQUIRED_KEYS if key not in settings]
    if missing:
        parser.error(
            f"missing required policy setting(s): {', '.join(missing)} "
            "(pass as flags or set them in --config)"
        )

    return settings


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = resolve_policy_settings(args, parser)

    policy = RetryPolicy(**settings)

    source = open(args.logfile) if args.logfile else sys.stdin
    attempts = 0
    violations = 0
    try:
        if args.group_by_request:
            reports = audit_grouped(policy, read_grouped_records(source))
        else:
            reports = audit(policy, read_timestamps(source))
        for report in reports:
            print(report.render_json() if args.format == "json" else report.render_text())
            attempts += 1
            violations += report.is_violation
    finally:
        if source is not sys.stdin:
            source.close()

    if args.format == "json":
        print(json.dumps({"attempts": attempts, "violations": violations}))
    else:
        print(f"--- {attempts} attempts, {violations} violation(s)")
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
