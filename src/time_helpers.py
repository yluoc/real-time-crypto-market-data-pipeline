"""Time helpers with explicit clock domain separation."""

import time


def now_epoch_ms() -> int:
    """Get current epoch time in milliseconds (wall clock, for exchange→recv latency)."""
    return int(time.time_ns() // 1_000_000)


def now_mono_ms() -> int:
    """Get current monotonic time in milliseconds (for rolling window bookkeeping)."""
    return int(time.monotonic_ns() // 1_000_000)

def now_mono_ns() -> int:
    """Get current monotonic time in nanoseconds (for high-precision internal stage latency)."""
    return time.monotonic_ns()
