import contextlib
import datetime
import io
import unittest

from backoffaudit.cli import (
    audit,
    audit_grouped,
    build_parser,
    follow_lines,
    main,
    parse_timestamp,
    read_grouped_records,
    read_timestamps,
    resolve_policy_settings,
)
from backoffaudit.policy import RetryPolicy


def make_policy(**overrides):
    settings = dict(
        max_attempts=3,
        base_delay=1.0,
        multiplier=2.0,
        max_delay=30.0,
        jitter="none",
        tolerance=0.05,
    )
    settings.update(overrides)
    return RetryPolicy(**settings)


class ParseTimestampTests(unittest.TestCase):
    def test_epoch_seconds(self):
        self.assertEqual(parse_timestamp("1755680400.5"), 1755680400.5)

    def test_iso8601(self):
        expected = datetime.datetime.fromisoformat("2026-08-20T10:00:00").timestamp()
        self.assertEqual(parse_timestamp("2026-08-20T10:00:00"), expected)

    def test_unrecognized_raises(self):
        with self.assertRaises(ValueError):
            parse_timestamp("not-a-timestamp")


class ReadTimestampsTests(unittest.TestCase):
    def test_skips_blank_and_comment_lines(self):
        fp = io.StringIO("1000\n\n# comment\n1001\n")
        self.assertEqual(list(read_timestamps(fp)), [1000.0, 1001.0])


class ReadGroupedRecordsTests(unittest.TestCase):
    def test_splits_request_id_from_timestamp(self):
        fp = io.StringIO("req-a 1000\nreq-b 1000.5\n\n# comment\nreq-a 1001\n")
        self.assertEqual(
            list(read_grouped_records(fp)),
            [("req-a", 1000.0), ("req-b", 1000.5), ("req-a", 1001.0)],
        )

    def test_missing_request_id_raises(self):
        fp = io.StringIO("1000\n")
        with self.assertRaises(ValueError):
            list(read_grouped_records(fp))


class _FakeFile:
    """Stands in for a real file handle in follow_lines() tests: each call
    to readline() returns the next scripted chunk, with "" standing for
    "nothing new yet" (what a real file returns at EOF)."""

    def __init__(self, chunks):
        self._chunks = iter(chunks)

    def readline(self):
        return next(self._chunks, "")


class FollowLinesTests(unittest.TestCase):
    def test_yields_complete_lines_and_polls_past_eof(self):
        # "" simulates hitting EOF before more data has been written.
        fp = _FakeFile(["1000\n", "", "1001\n"])
        gen = follow_lines(fp, poll_interval=0)
        self.assertEqual([next(gen), next(gen)], ["1000\n", "1001\n"])

    def test_buffers_a_partial_line_until_the_newline_arrives(self):
        # a writer that flushes mid-line shouldn't produce a truncated
        # line that fails to parse.
        fp = _FakeFile(["10", "", "01\n", "1002\n"])
        gen = follow_lines(fp, poll_interval=0)
        self.assertEqual([next(gen), next(gen)], ["1001\n", "1002\n"])


class FollowFlagTests(unittest.TestCase):
    def test_follow_without_logfile_is_rejected(self):
        parser = build_parser()
        args = parser.parse_args(
            ["--follow", "--max-attempts", "3", "--base-delay", "1", "--max-delay", "30"]
        )
        settings = resolve_policy_settings(args, parser)
        RetryPolicy(**settings)  # settings themselves are valid
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            main(
                [
                    "--follow",
                    "--max-attempts",
                    "3",
                    "--base-delay",
                    "1",
                    "--max-delay",
                    "30",
                ]
            )


class AuditTests(unittest.TestCase):
    def test_first_attempt_has_no_delay_or_window(self):
        policy = make_policy()
        reports = list(audit(policy, iter([1000.0])))
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0].reason, "first_attempt")
        self.assertIsNone(reports[0].delay)
        self.assertIsNone(reports[0].window)
        self.assertFalse(reports[0].is_violation)

    def test_ok_attempt_within_window(self):
        policy = make_policy(tolerance=0.1)
        reports = list(audit(policy, iter([1000.0, 1001.0])))
        self.assertFalse(reports[1].is_violation)
        self.assertEqual(reports[1].reason, "ok")

    def test_too_fast(self):
        policy = make_policy(tolerance=0.1)
        reports = list(audit(policy, iter([1000.0, 1000.5])))
        self.assertTrue(reports[1].is_violation)
        self.assertEqual(reports[1].reason, "too_fast")

    def test_too_slow(self):
        policy = make_policy(tolerance=0.1)
        reports = list(audit(policy, iter([1000.0, 1005.0])))
        self.assertTrue(reports[1].is_violation)
        self.assertEqual(reports[1].reason, "too_slow")

    def test_max_attempts_exceeded(self):
        policy = make_policy(max_attempts=2, tolerance=0.5)
        timestamps = [1000.0, 1001.0, 1003.0, 1010.0]
        reports = list(audit(policy, iter(timestamps)))
        self.assertEqual(reports[-1].reason, "max_attempts_exceeded")
        self.assertEqual(reports[-1].attempt, 4)
        self.assertTrue(all(r.is_violation for r in reports[2:]))

    def test_duplicate_timestamp_flagged_without_corrupting_later_attempts(self):
        policy = make_policy(tolerance=0.5)
        with_dup = list(audit(policy, iter([1000.0, 1001.0, 1001.0, 1003.0])))
        without_dup = list(audit(policy, iter([1000.0, 1001.0, 1003.0])))

        self.assertEqual(with_dup[2].reason, "duplicate_timestamp")
        self.assertTrue(with_dup[2].is_violation)

        real_attempts = [r for r in with_dup if r.reason != "duplicate_timestamp"]
        self.assertEqual([r.attempt for r in real_attempts], [r.attempt for r in without_dup])
        self.assertEqual([r.window for r in real_attempts], [r.window for r in without_dup])

    def test_out_of_order_timestamp_flagged_without_moving_prev_ts(self):
        policy = make_policy(tolerance=0.5)
        timestamps = [1000.0, 1005.0, 1002.0, 1009.0]
        reports = list(audit(policy, iter(timestamps)))

        self.assertEqual(reports[2].reason, "out_of_order")
        self.assertTrue(reports[2].is_violation)
        # the out-of-order line at 1002 doesn't become the new "previous
        # attempt": the next genuine attempt's delay is still measured
        # from 1005, the last timestamp that actually advanced.
        self.assertEqual(reports[3].delay, 1009.0 - 1005.0)


class AuditGroupedTests(unittest.TestCase):
    def test_interleaved_requests_tracked_independently(self):
        policy = make_policy(tolerance=0.1)
        records = [
            ("req-a", 1000.0),
            ("req-b", 1000.0),
            ("req-a", 1001.0),
            ("req-b", 1002.0),
        ]
        reports = list(audit_grouped(policy, iter(records)))

        by_a = [r for r in reports if r.request_id == "req-a"]
        by_b = [r for r in reports if r.request_id == "req-b"]
        self.assertEqual([r.attempt for r in by_a], [1, 2])
        self.assertEqual([r.attempt for r in by_b], [1, 2])
        # req-b's first attempt lands on the same timestamp as req-a's
        # first attempt, so it doesn't get flagged as a duplicate against
        # req-a's state — the two requests are tracked separately.
        self.assertEqual(by_b[0].reason, "first_attempt")
        self.assertFalse(by_b[0].is_violation)
        self.assertEqual(by_a[1].delay, 1.0)
        self.assertEqual(by_b[1].delay, 2.0)

    def test_matches_ungrouped_audit_for_a_single_request_id(self):
        policy = make_policy(tolerance=0.5)
        timestamps = [1000.0, 1001.0, 1003.0]
        grouped = list(audit_grouped(policy, iter((("req-a", ts) for ts in timestamps))))
        ungrouped = list(audit(policy, iter(timestamps)))

        self.assertEqual([r.attempt for r in grouped], [r.attempt for r in ungrouped])
        self.assertEqual([r.delay for r in grouped], [r.delay for r in ungrouped])
        self.assertEqual([r.reason for r in grouped], [r.reason for r in ungrouped])
        self.assertTrue(all(r.request_id == "req-a" for r in grouped))

    def test_reports_carry_request_id_in_text_and_json(self):
        policy = make_policy()
        report = next(iter(audit_grouped(policy, iter([("req-a", 1000.0)]))))
        self.assertTrue(report.render_text().startswith("[req-a] attempt 1"))
        self.assertIn('"request_id": "req-a"', report.render_json())


if __name__ == "__main__":
    unittest.main()
