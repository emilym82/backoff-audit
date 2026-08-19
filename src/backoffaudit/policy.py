"""Backoff policy math: given attempt N, what delay before it was allowed."""

from dataclasses import dataclass

JITTER_MODES = ("none", "full", "equal")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    base_delay: float
    multiplier: float
    max_delay: float
    jitter: str = "none"
    # clock/measurement slack for the "none" jitter case, as a fraction
    # of the expected delay (log timestamps are never perfectly exact).
    tolerance: float = 0.05

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must not be negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.jitter not in JITTER_MODES:
            raise ValueError(f"jitter must be one of {JITTER_MODES}")

    def nominal_delay(self, wait_index: int) -> float:
        """Delay the policy would produce with no jitter, before attempt
        wait_index + 1 (wait_index is 1 for the wait before the 2nd attempt)."""
        raw = self.base_delay * (self.multiplier ** (wait_index - 1))
        return min(raw, self.max_delay)

    def allowed_window(self, wait_index: int) -> tuple[float, float]:
        """(min, max) seconds a compliant client could have waited before
        the attempt that follows wait number `wait_index`."""
        nominal = self.nominal_delay(wait_index)
        if self.jitter == "none":
            return (nominal * (1 - self.tolerance), nominal * (1 + self.tolerance))
        if self.jitter == "full":
            return (0.0, nominal)
        if self.jitter == "equal":
            return (nominal / 2, nominal)
        raise AssertionError("unreachable: __post_init__ validates jitter")
