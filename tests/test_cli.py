import datetime
import io
import unittest

from backoffaudit.cli import audit, parse_timestamp, read_timestamps
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


if __name__ == "__main__":
    unittest.main()
