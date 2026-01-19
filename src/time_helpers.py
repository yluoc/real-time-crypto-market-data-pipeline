"""Time helpers with explicit clock domain separation."""

import time

# Optional clock sanity check (only if DEBUG enabled)
_DEBUG = False  # Set to True to enable clock sanity logging
if _DEBUG:
    _clock_sanity_ns = time.monotonic_ns()
    if _clock_sanity_ns <= 0:
        raise RuntimeError("Monotonic clock returned non-positive value")


def now_epoch_ms() -> int:
    """Get current epoch time in milliseconds (wall clock, for exchange→recv latency)."""
    return int(time.time_ns() // 1_000_000)


def now_mono_ms() -> int:
    """Get current monotonic time in milliseconds (for rolling window bookkeeping)."""
    return int(time.monotonic_ns() // 1_000_000)

def now_mono_ns() -> int:
    """Get current monotonic time in nanoseconds (for high-precision internal stage latency)."""
    return time.monotonic_ns()
