"""The fake clock must behave like the two real ones, or tests prove nothing."""
from __future__ import annotations

import pytest

from server.common.clock import FakeClock, SystemClock


def test_advance_moves_both_clocks_together():
    clock = FakeClock(wall=100.0, mono=5.0)
    clock.advance(30)
    assert clock.time() == 130.0
    assert clock.monotonic() == 35.0


def test_a_wall_clock_jump_does_not_touch_the_monotonic_clock():
    """The reason liveness uses monotonic time (ARCHITECTURE.md Section 4.4)."""
    clock = FakeClock(wall=100.0, mono=5.0)
    clock.jump_wall(-3600)  # NTP steps the clock back an hour
    assert clock.time() == -3500.0
    assert clock.monotonic() == 5.0


def test_time_cannot_be_advanced_backwards():
    with pytest.raises(ValueError, match="jump_wall"):
        FakeClock().advance(-1)


def test_the_system_monotonic_clock_never_goes_backwards():
    clock = SystemClock()
    first = clock.monotonic()
    assert clock.monotonic() >= first


def test_the_system_wall_clock_is_a_plausible_epoch_time():
    assert SystemClock().time() > 1_700_000_000
