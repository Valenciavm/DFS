"""Injectable clocks.

``ARCHITECTURE.md`` Section 4.4 separates two kinds of time and forbids mixing
them:

* **monotonic** time drives liveness (heartbeats, timeouts, in-flight
  replications). It is per process and cannot jump, so a wall-clock correction
  can never kill a healthy DataNode.
* **wall** time is stored or compared across processes and restarts
  (``created_at``, ``expires_at``, leases, token expiry).

Code takes a `Clock` instead of calling ``time`` directly, so tests drive both
without sleeping.
"""
from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """What the NameNode needs to know about time."""

    def time(self) -> float:
        """Wall-clock UTC seconds since the epoch."""
        ...

    def monotonic(self) -> float:
        """Seconds from an arbitrary origin that never goes backwards."""
        ...


class SystemClock:
    """The real clocks."""

    def time(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()


class FakeClock:
    """A clock tests move by hand. Both readings start at fixed values."""

    def __init__(self, wall: float = 1_760_000_000.0, mono: float = 1_000.0) -> None:
        self._wall = wall
        self._mono = mono

    def time(self) -> float:
        return self._wall

    def monotonic(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        """Moves both clocks forward, as real elapsed time would."""
        if seconds < 0:
            raise ValueError("time only moves forward; use jump_wall for corrections")
        self._wall += seconds
        self._mono += seconds

    def jump_wall(self, seconds: float) -> None:
        """Simulates an NTP correction: only the wall clock moves, in either direction."""
        self._wall += seconds


__all__ = ["Clock", "FakeClock", "SystemClock"]
