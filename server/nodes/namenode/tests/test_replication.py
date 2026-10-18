"""Re-replication after failures (test plan T4 and T19)."""
from __future__ import annotations

import pytest

from server.common.tokens import SYSTEM_PRINCIPAL, BlockOp, verify
from server.nodes.namenode.core import IN_FLIGHT_TIMEOUT_S
from server.nodes.namenode.tests.conftest import SECRET
from server.nodes.namenode.tests.helpers import (
    GIB,
    MIB,
    add_nodes,
    checksum_of,
    confirm_all,
    upload,
)

HOME = "/users/ana"


@pytest.fixture
def cluster(nn):
    add_nodes(nn, 5)
    nn.store.mkdir(HOME, "ana", nn.clock.time())
    return nn


def kill(nn, clock, *victims: str) -> None:
    """Silences ``victims`` while everyone else keeps beating, then runs the liveness check."""
    clock.advance(31)
    for node_id in nn.cluster.datanodes:
        if node_id not in victims:
            nn.record_heartbeat(node_id, 100 * GIB, 0)
    assert sorted(nn.check_liveness()) == sorted(victims)


def revive(nn, node_id: str) -> None:
    nn.record_heartbeat(node_id, 100 * GIB, 0)


def replicate_commands(nn):
    return [
        (source, c)
        for source, commands in nn.cluster.pending_commands.items()
        for c in commands
        if c.cmd == "replicate"
    ]


def finish_copies(nn) -> None:
    """Plays the DataNodes completing every replicate command that was queued."""
    for source, command in replicate_commands(nn):
        block = nn.store.get_block(command.block_id)
        for target in command.targets:
            nn.record_block_received(target, command.block_id, block.size_bytes,
                                     checksum_of(command.block_id))
    nn.cluster.pending_commands.clear()


class TestHealthyCluster:
    def test_a_healthy_cluster_needs_nothing(self, cluster):
        upload(cluster, f"{HOME}/f", 12 * MIB)
        scan = cluster.scan_replication()
        assert (scan.under_replicated, scan.scheduled, scan.deleted) == (0, 0, 0)
        assert replicate_commands(cluster) == []

    def test_an_empty_cluster_needs_nothing(self, cluster):
        assert cluster.scan_replication().under_replicated == 0


class TestRepair:
    def test_a_dead_node_makes_its_blocks_under_replicated_and_repair_is_scheduled(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        victim = plan.blocks[0].pipeline[0].node_id
        kill(cluster, clock, victim)

        scan = cluster.scan_replication()

        assert scan.under_replicated == 1
        assert scan.scheduled == 1
        assert scan.unrecoverable == 0

    def test_the_command_goes_to_a_live_holder_and_targets_a_node_without_the_block(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block_id = plan.blocks[0].block.block_id
        holders = {n.node_id for n in plan.blocks[0].pipeline}
        victim = plan.blocks[0].pipeline[0].node_id
        kill(cluster, clock, victim)

        cluster.scan_replication()
        (source, command), = replicate_commands(cluster)

        assert source in holders - {victim}, "the source must hold a live copy"
        assert command.block_id == block_id
        assert len(command.targets) == 1
        assert command.targets[0] not in holders, "never copy onto a node that has it"
        assert command.targets[0] != victim

    def test_the_command_carries_a_system_write_token_for_that_block(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block_id = plan.blocks[0].block.block_id
        kill(cluster, clock, plan.blocks[0].pipeline[0].node_id)
        cluster.scan_replication()
        (_, command), = replicate_commands(cluster)

        claims = verify(command.token, SECRET, block_id=block_id, op=BlockOp.WRITE, now=clock.time())
        assert claims.user == SYSTEM_PRINCIPAL

    def test_a_copy_in_flight_is_not_scheduled_again(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        kill(cluster, clock, plan.blocks[0].pipeline[0].node_id)

        first = cluster.scan_replication()
        second = cluster.scan_replication()

        assert (first.scheduled, second.scheduled) == (1, 0)
        assert len(replicate_commands(cluster)) == 1

    def test_the_target_space_is_reserved_while_the_copy_is_in_flight(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        kill(cluster, clock, plan.blocks[0].pipeline[0].node_id)
        cluster.scan_replication()
        (_, command), = replicate_commands(cluster)
        assert cluster.cluster.reserved_bytes == {command.targets[0]: 4 * MIB}

    def test_once_the_copy_is_confirmed_replication_is_restored(self, cluster, clock):
        """T4: under_replicated_blocks rises, then returns to 0."""
        plan = upload(cluster, f"{HOME}/f", 12 * MIB)
        kill(cluster, clock, plan.blocks[0].pipeline[0].node_id)
        assert cluster.scan_replication().under_replicated >= 1

        finish_copies(cluster)

        assert cluster.scan_replication().under_replicated == 0
        assert cluster.cluster.reserved_bytes == {}
        assert cluster.cluster.in_flight_replications == {}

    def test_every_block_of_the_dead_node_is_repaired(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 60 * MIB)
        counts = {}
        for p in plan.blocks:
            for n in p.pipeline:
                counts[n.node_id] = counts.get(n.node_id, 0) + 1
        victim = max(counts, key=counts.get)
        kill(cluster, clock, victim)

        scan = cluster.scan_replication()

        assert scan.under_replicated == counts[victim]
        assert scan.scheduled == counts[victim]
        finish_copies(cluster)
        assert cluster.scan_replication().under_replicated == 0

    def test_a_file_replicated_below_the_default_is_held_to_its_own_target(self, cluster):
        """Each file carries the replication factor it was written with."""
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        cluster.store.get_file(f"{HOME}/f").replication = 2
        cluster.cluster.replicas[plan.blocks[0].block.block_id].pop()  # now 2 live copies
        assert cluster.scan_replication().under_replicated == 0


class TestPriority:
    def test_the_most_endangered_block_is_repaired_first(self, cluster):
        """A block with 1 live replica goes before one with 2."""
        a = upload(cluster, f"{HOME}/a", 4 * MIB)
        b = upload(cluster, f"{HOME}/b", 4 * MIB)
        block_a, block_b = a.blocks[0].block.block_id, b.blocks[0].block.block_id
        cluster.cluster.replicas[block_a] = set(list(cluster.cluster.replicas[block_a])[:2])  # 2 live
        cluster.cluster.replicas[block_b] = set(list(cluster.cluster.replicas[block_b])[:1])  # 1 live

        cluster.scan_replication()

        assert list(cluster.cluster.in_flight_replications) == [block_b, block_a]


class TestTimeouts:
    def test_a_lost_copy_is_given_up_on_and_retried(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        kill(cluster, clock, plan.blocks[0].pipeline[0].node_id)
        cluster.scan_replication()
        cluster.cluster.pending_commands.clear()  # the command was lost

        clock.advance(IN_FLIGHT_TIMEOUT_S + 1)
        for node_id in cluster.cluster.datanodes:
            if cluster.cluster.datanodes[node_id].is_alive:
                revive(cluster, node_id)
        scan = cluster.scan_replication()

        assert scan.scheduled == 1, "the block must be retried instead of staying blocked"

    def test_giving_up_releases_the_reserved_space(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        kill(cluster, clock, plan.blocks[0].pipeline[0].node_id)
        cluster.scan_replication()
        clock.advance(IN_FLIGHT_TIMEOUT_S + 1)
        for node in cluster.cluster.datanodes.values():
            if node.is_alive:
                revive(cluster, node.node_id)
        cluster.scan_replication()  # expires the old attempt, schedules a new one
        assert sum(cluster.cluster.reserved_bytes.values()) == 4 * MIB, "only the new attempt holds space"

    def test_a_target_that_dies_mid_copy_is_replaced(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        kill(cluster, clock, plan.blocks[0].pipeline[0].node_id)
        cluster.scan_replication()
        (_, command), = replicate_commands(cluster)
        doomed = command.targets[0]

        cluster.cluster.pending_commands.clear()
        # silence only the target; kill() cannot be used because the first victim is
        # already dead and must stay that way
        clock.advance(31)
        for node in cluster.cluster.datanodes.values():
            if node.node_id != doomed and node.is_alive:
                revive(cluster, node.node_id)
        assert cluster.check_liveness() == [doomed]

        scan = cluster.scan_replication()
        assert scan.scheduled == 1
        (_, retry), = replicate_commands(cluster)
        assert retry.targets[0] != doomed


class TestUnrecoverable:
    def test_a_block_with_no_live_replica_is_counted_and_cannot_be_copied(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        kill(cluster, clock, *[n.node_id for n in plan.blocks[0].pipeline])

        scan = cluster.scan_replication()

        assert scan.under_replicated == 1
        assert scan.unrecoverable == 1
        assert scan.scheduled == 0
        assert replicate_commands(cluster) == []

    def test_the_block_comes_back_when_a_holder_returns(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        holders = [n.node_id for n in plan.blocks[0].pipeline]
        kill(cluster, clock, *holders)
        assert cluster.scan_replication().unrecoverable == 1

        revive(cluster, holders[0])
        scan = cluster.scan_replication()

        assert scan.unrecoverable == 0
        assert scan.scheduled == 1


class TestClusterTooSmall:
    def test_with_exactly_replication_factor_nodes_a_death_can_never_be_repaired(self, nn, clock):
        """The finding behind decision 11 and the five node minimum: repair needs a live
        node that does not already hold the block, and with 3 nodes and factor 3 none exists,
        so under_replicated_blocks can never return to 0 (T4 could not pass)."""
        add_nodes(nn, 3)
        nn.store.mkdir(HOME, "ana", clock.time())
        plan = upload(nn, f"{HOME}/f", 4 * MIB)
        kill(nn, clock, plan.blocks[0].pipeline[0].node_id)

        scan = nn.scan_replication()

        assert scan.under_replicated == 1
        assert scan.scheduled == 0, "there is nowhere to copy to"
        assert replicate_commands(nn) == []

    def test_a_node_without_room_is_not_a_repair_target(self, nn, clock):
        add_nodes(nn, 3)                                  # dn1..dn3 hold the block
        add_nodes(nn, 1, capacity=1 * MIB, start=4)       # dn4 is alive but too small for 4 MiB
        nn.store.mkdir(HOME, "ana", clock.time())
        plan = upload(nn, f"{HOME}/f", 4 * MIB)
        assert {n.node_id for n in plan.blocks[0].pipeline} == {"dn1", "dn2", "dn3"}

        clock.advance(31)  # dn1 goes silent; the others beat with their real free space
        nn.record_heartbeat("dn2", 100 * GIB, 0)
        nn.record_heartbeat("dn3", 100 * GIB, 0)
        nn.record_heartbeat("dn4", 1 * MIB, 0)  # kill() would have inflated this to 100 GiB
        assert nn.check_liveness() == ["dn1"]

        scan = nn.scan_replication()

        assert scan.under_replicated == 1
        assert scan.scheduled == 0, "dn4 is alive but cannot hold the block"


class TestPendingFilesAreIgnored:
    def test_an_upload_in_progress_is_not_repaired(self, cluster):
        """T19: monitor_replication must ignore PENDING files. They are short of replicas
        by design while being written, and copying them would race the client."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan, replicas=1)  # one replica each, well short of 3

        scan = cluster.scan_replication()

        assert (scan.under_replicated, scan.scheduled) == (0, 0)
        assert replicate_commands(cluster) == []

    def test_an_aborted_upload_is_not_repaired_either(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan, replicas=1)
        cluster.abort_file_upload(plan.file.file_id)
        assert cluster.scan_replication().under_replicated == 0


class TestSurplus:
    def test_a_returning_node_makes_a_surplus_that_is_trimmed(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block_id = plan.blocks[0].block.block_id
        returning = plan.blocks[0].pipeline[0].node_id
        kill(cluster, clock, returning)
        cluster.scan_replication()
        finish_copies(cluster)  # repaired onto another node: 3 live copies again

        revive(cluster, returning)  # the old holder is back, so 4 live copies
        scan = cluster.scan_replication()

        assert scan.deleted == 1
        assert len(cluster.cluster.replicas[block_id]) == 3
        deletes = [c for cmds in cluster.cluster.pending_commands.values() for c in cmds if c.cmd == "delete"]
        assert [c.block_id for c in deletes] == [block_id]

    def test_the_fullest_node_is_the_one_asked_to_drop_its_copy(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        holders = [n.node_id for n in plan.blocks[0].pipeline]
        extra = next(n for n in cluster.cluster.datanodes if n not in holders)
        cluster.cluster.replicas[plan.blocks[0].block.block_id].add(extra)  # now 4 copies
        for node_id in holders + [extra]:
            used = 90 * GIB if node_id == holders[1] else 10 * GIB
            cluster.record_heartbeat(node_id, 100 * GIB - used, used)

        cluster.scan_replication()

        (target, (command,)), = [
            (n, cmds) for n, cmds in cluster.cluster.pending_commands.items()
        ]
        assert target == holders[1] and command.cmd == "delete"
