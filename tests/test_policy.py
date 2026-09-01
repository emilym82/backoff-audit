import unittest

from backoffaudit.policy import RetryPolicy


def make_policy(**overrides):
    settings = dict(
        max_attempts=5,
        base_delay=0.5,
        multiplier=2.0,
        max_delay=30.0,
        jitter="none",
        tolerance=0.05,
    )
    settings.update(overrides)
    return RetryPolicy(**settings)


class NominalDelayTests(unittest.TestCase):
    def test_exponential_growth(self):
        policy = make_policy()
        self.assertEqual(policy.nominal_delay(1), 0.5)
        self.assertEqual(policy.nominal_delay(2), 1.0)
        self.assertEqual(policy.nominal_delay(3), 2.0)

    def test_capped_at_max_delay(self):
        policy = make_policy(max_delay=1.5)
        self.assertEqual(policy.nominal_delay(1), 0.5)
        self.assertEqual(policy.nominal_delay(2), 1.0)
        self.assertEqual(policy.nominal_delay(3), 1.5)
        self.assertEqual(policy.nominal_delay(10), 1.5)


class AllowedWindowTests(unittest.TestCase):
    def test_none_jitter_uses_tolerance(self):
        policy = make_policy(jitter="none", tolerance=0.1)
        lo, hi = policy.allowed_window(2)
        self.assertAlmostEqual(lo, 0.9)
        self.assertAlmostEqual(hi, 1.1)

    def test_full_jitter_window_starts_at_zero(self):
        policy = make_policy(jitter="full")
        lo, hi = policy.allowed_window(2)
        self.assertEqual(lo, 0.0)
        self.assertEqual(hi, 1.0)

    def test_equal_jitter_window_is_upper_half(self):
        policy = make_policy(jitter="equal")
        lo, hi = policy.allowed_window(2)
        self.assertEqual(lo, 0.5)
        self.assertEqual(hi, 1.0)

    def test_windows_respect_max_delay_cap(self):
        policy = make_policy(jitter="full", max_delay=1.5)
        lo, hi = policy.allowed_window(3)
        self.assertEqual(hi, 1.5)


class ValidationTests(unittest.TestCase):
    def test_rejects_zero_max_attempts(self):
        with self.assertRaises(ValueError):
            make_policy(max_attempts=0)

    def test_rejects_negative_base_delay(self):
        with self.assertRaises(ValueError):
            make_policy(base_delay=-1)

    def test_rejects_multiplier_below_one(self):
        with self.assertRaises(ValueError):
            make_policy(multiplier=0.5)

    def test_rejects_max_delay_below_base_delay(self):
        with self.assertRaises(ValueError):
            make_policy(base_delay=5, max_delay=1)

    def test_rejects_unknown_jitter_mode(self):
        with self.assertRaises(ValueError):
            make_policy(jitter="gaussian")


if __name__ == "__main__":
    unittest.main()
