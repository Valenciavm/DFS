from __future__ import annotations

from datetime import UTC, datetime

from server.nodes.namenode.store.postgres import LeaderLease, PostgresDatabase


class LeaseDatabaseDouble(PostgresDatabase):
    def __init__(self) -> None:
        self.leader_guard = None
        self.fencing_required = True
        self.acquire_calls = 0

    def release_leader(self, holder: str, term: int) -> bool:
        return True

    def current_leader(self) -> LeaderLease:
        return LeaderLease(
            holder=None,
            url=None,
            term=7,
            lease_expires_at=datetime.now(UTC),
        )

    def acquire_leader(
        self, holder: str, url: str, lease_s: int
    ) -> LeaderLease | None:
        self.acquire_calls += 1
        return None


async def test_stepdown_retries_lease_acquisition_after_handover_delay(nn, clock):
    database = LeaseDatabaseDouble()
    nn.database = database
    nn._ha_enabled = True
    nn._is_leader = True
    nn.term = 7

    nn.step_down()
    await nn.refresh_leadership()

    assert database.acquire_calls == 0

    clock.advance(nn.settings.lease_renew_interval_s)
    await nn.refresh_leadership()

    assert database.acquire_calls == 1
