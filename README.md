# backoff-audit

You wrote a retry policy: exponential backoff, base 500ms, cap at 30s, five
attempts max. Somewhere downstream a client library implements that policy.
Six months later you're staring at a log full of retries and asking the only
question that matters: is the client actually doing what the policy says, or
is it hammering your service every 200ms because someone's HTTP client has
its own hardcoded backoff that ignores your config?

`backoff-audit` answers that one question. Feed it a policy and a stream of
attempt timestamps for a single request; it tells you, attempt by attempt,
whether the wait before each one falls inside the window the policy allows.

## Usage

```
$ cat attempts.log
1755680400.000
1755680400.510
1755680401.520
1755680404.480

$ backoff-audit attempts.log \
    --max-attempts 5 --base-delay 0.5 --multiplier 2 --max-delay 30 \
    --jitter none
attempt 1 at 1755680400.000  ok (first attempt)
attempt 2 at 1755680400.510  delay=0.510s  ok
attempt 3 at 1755680401.520  delay=1.010s  ok
attempt 4 at 1755680404.480  delay=2.960s  VIOLATION: too slow (expected [1.900, 2.100])
--- 4 attempts, 1 violation(s)
```

Exit code is `0` if every attempt complied, `1` if any attempt violated the
policy (wrong delay, or more attempts than `--max-attempts` allows). That
makes it usable as a check in a log-processing pipeline, not just for
one-off reading.

Timestamps can be epoch seconds or ISO 8601 (`2026-08-20T10:00:00`), one per
line. Blank lines and lines starting with `#` are skipped. If you omit the
file argument it reads from stdin, so it works on a live tail:

```
tail -f gateway.log | grep 'retrying request-id=abc123' | cut -d' ' -f1 \
  | backoff-audit --max-attempts 5 --base-delay 0.5 --multiplier 2 --max-delay 30
```

Or point `backoff-audit` at the file directly with `--follow` and it does
the tailing itself, watching for lines appended after EOF instead of
exiting once it's caught up:

```
backoff-audit gateway.log --follow \
  --max-attempts 5 --base-delay 0.5 --multiplier 2 --max-delay 30 --jitter none
```

`--follow` requires a logfile argument — stdin already blocks for more
input when it's the read end of a live pipe, so there's nothing extra to
do there. Output is flushed after every line rather than block-buffered,
so violations show up as soon as they're read rather than waiting for a
buffer to fill. Stop it with Ctrl-C; it prints the summary line for
whatever it audited before exiting.

Pass `--format json` to get newline-delimited JSON instead, one object per
attempt plus a summary object at the end, for feeding into something else
rather than reading with your eyes:

```
$ backoff-audit attempts.log \
    --max-attempts 5 --base-delay 0.5 --multiplier 2 --max-delay 30 \
    --jitter none --format json
{"attempt": 1, "timestamp": 1755680400.0, "delay": null, "window": null, "violation": false, "reason": "first_attempt", "message": "ok (first attempt)"}
{"attempt": 2, "timestamp": 1755680400.51, "delay": 0.51, "window": [0.475, 0.525], "violation": false, "reason": "ok", "message": "ok"}
{"attempt": 3, "timestamp": 1755680401.52, "delay": 1.01, "window": [0.95, 1.05], "violation": false, "reason": "ok", "message": "ok"}
{"attempt": 4, "timestamp": 1755680404.48, "delay": 2.96, "window": [1.9, 2.1], "violation": true, "reason": "too_slow", "message": "VIOLATION: too slow (expected [1.900, 2.100])"}
{"attempts": 4, "violations": 1}
```

## Config files

Typing out the same five flags for every invocation gets old, so a policy
can also live in a JSON or TOML file:

```
$ cat policy.json
{
  "max_attempts": 5,
  "base_delay": 0.5,
  "multiplier": 2,
  "max_delay": 30,
  "jitter": "none"
}

$ backoff-audit attempts.log --config policy.json
```

Any flag also given on the command line overrides the corresponding value
in the config file, so `--config policy.json --jitter full` runs the same
policy with a different jitter mode without editing the file. TOML configs
need Python 3.11+ (`tomllib`); JSON works everywhere.

## Jitter modes

The policy's `--jitter` flag controls the allowed window for each wait,
matching the common backoff implementations:

- `none` — delay must match the nominal exponential value within a small
  tolerance (`--tolerance`, default 5%, to absorb clock/log imprecision).
- `full` — delay may be anywhere from 0 up to the nominal value
  (`random.uniform(0, nominal)`, the "full jitter" strategy).
- `equal` — delay must be between half and all of the nominal value
  (`nominal/2 + random.uniform(0, nominal/2)`, "equal jitter").

## Out-of-order and duplicate lines

Logs get reordered: two writers on the same host, a shipper that retries a
batch, clock skew between processes. If a line's timestamp doesn't advance
past the one before it, `backoff-audit` doesn't treat it as a genuine retry
attempt — it flags the line (`duplicate_timestamp` if the timestamp repeats
exactly, `out_of_order` if it's earlier) and moves on without touching the
attempt count or the exponential wait index, so one bad line doesn't throw
off the delay window computed for every attempt that follows it.

## Multiple requests in one log

The examples above assume every line belongs to the same request. If your
log interleaves attempts from several requests — a gateway serving many
callers at once — pass `--group-by-request` and prefix each line with a
request id:

```
$ cat multi.log
req-1 1755680400.000
req-2 1755680400.200
req-1 1755680400.510
req-2 1755680401.220
req-1 1755680401.520

$ backoff-audit multi.log --group-by-request \
    --max-attempts 5 --base-delay 0.5 --multiplier 2 --max-delay 30 --jitter none
[req-1] attempt 1 at 1755680400.000  ok (first attempt)
[req-2] attempt 1 at 1755680400.200  ok (first attempt)
[req-1] attempt 2 at 1755680400.510  delay=0.510s  ok
[req-2] attempt 2 at 1755680401.220  delay=1.020s  ok
[req-1] attempt 3 at 1755680401.520  delay=1.010s  ok
--- 5 attempts, 0 violation(s)
```

Each request id gets its own attempt count and delay window, computed
independently, in a single pass over the file — a line for `req-2` never
affects the window checked for `req-1`'s next attempt, no matter how the
lines are interleaved. Memory use scales with the number of distinct
request ids in flight, not with the number of lines, so this is still cheap
for a log covering thousands of requests as long as they're not all open at
once.

## Why streaming matters here

Attempt logs are exactly the kind of file that grows without bound: a
misbehaving client retrying every 200ms for an hour produces tens of
thousands of lines before anyone notices. `backoff-audit` reads one line at
a time and keeps only the previous timestamp and a running count in memory
— it never buffers the file, so a multi-gigabyte log or an indefinite
`tail -f` costs the same handful of bytes as a ten-line one.

## Install

No dependencies beyond the standard library.

```
pip install -e .
```

## License

MIT, see LICENSE.
