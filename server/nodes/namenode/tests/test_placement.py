"""Block placement: eligibility, distinctness, weighting and partitioning."""
from __future__ import annotations

import random
from collections import Counter

import pytest

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.cluster_state import DataNodeInfo
from server.nodes.namenode.placement import (
    choose_pipeline,
    eligible_nodes,
    usable_space,
    weighted_sample,
)

GIB = 1 << 30
MIB = 1 << 20


def node(node_id: str, free_gib: int = 100, alive: bool = True) -> DataNodeInfo:
    return DataNodeInfo(
        node_id=node_id,
        internal_addr=f"{node_id}:9000",
        public_addr=f"{node_id}.example.com:9000",
        capacity_bytes=free_gib * GIB,
        used_bytes=0,
        is_alive=alive,
    )


def cluster(count: int, **kwargs) -> list[DataNodeInfo]:
    return [node(f"dn{i}", **kwargs) for i in range(1, count + 1)]


class TestEligibility:
    def test_dead_nodes_are_never_candidates(self):
        nodes = [node("dn1"), node("dn2", alive=False)]
        assert [n.node_id for n in eligible_nodes(nodes, {}, MIB)] == ["dn1"]

    def test_a_node_without_room_is_excluded(self):
        nodes = [node("dn1", free_gib=100), node("dn2", free_gib=0)]
        assert [n.node_id for n in eligible_nodes(nodes, {}, MIB)] == ["dn1"]

    def test_reserved_space_counts_against_a_node(self):
        """Space promised to uploads in flight is not free (Section 6.2, step 1)."""
        nodes = [node("dn1", free_gib=1)]
        assert eligible_nodes(nodes, {}, 512 * MIB) != []
        assert eligible_nodes(nodes, {"dn1": 900 * MIB}, 512 * MIB) == []

    def test_a_node_with_exactly_enough_room_qualifies(self):
        nodes = [node("dn1", free_gib=1)]
        assert eligible_nodes(nodes, {}, GIB) == nodes

    def test_excluded_ids_are_skipped(self):
        nodes = cluster(3)
        assert [n.node_id for n in eligible_nodes(nodes, {}, MIB, exclude={"dn2"})] == ["dn1", "dn3"]

    def test_usable_space_never_goes_negative(self):
        assert usable_space(node("dn1", free_gib=1), {"dn1": 5 * GIB}) == 0


class TestPipeline:
    def test_a_pipeline_has_the_requested_number_of_distinct_nodes(self):
        rng = random.Random(1)
        for _ in range(200):
            pipeline = choose_pipeline(cluster(6), {}, MIB, 3, rng)
            assert len(pipeline) == 3
            assert len({n.node_id for n in pipeline}) == 3, "a node must not repeat in one block"

    def test_too_few_nodes_is_not_enough_datanodes(self):
        with pytest.raises(DfshaError) as error:
            choose_pipeline(cluster(2), {}, MIB, 3, random.Random(1))
        assert error.value.code is ErrorCode.NOT_ENOUGH_DATANODES
        assert error.value.http_status == 503

    def test_nodes_that_are_full_do_not_count_towards_the_minimum(self):
        nodes = [node("dn1"), node("dn2"), node("dn3", free_gib=0)]
        with pytest.raises(DfshaError, match="found 2"):
            choose_pipeline(nodes, {}, MIB, 3, random.Random(1))

    def test_the_node_with_most_space_goes_first(self):
        """It receives from the client, so it carries the heaviest load."""
        nodes = [node("small", 10), node("big", 900), node("mid", 100)]
        pipeline = choose_pipeline(nodes, {}, MIB, 3, random.Random(1))
        assert [n.node_id for n in pipeline] == ["big", "mid", "small"]

    def test_a_seeded_run_is_reproducible(self):
        a = choose_pipeline(cluster(8), {}, MIB, 3, random.Random(42))
        b = choose_pipeline(cluster(8), {}, MIB, 3, random.Random(42))
        assert [n.node_id for n in a] == [n.node_id for n in b]

    def test_an_empty_block_is_still_placed(self):
        """An empty file has one block of 0 bytes (Section 6.1)."""
        assert len(choose_pipeline(cluster(3), {}, 0, 3, random.Random(1))) == 3


class TestWeighting:
    def test_emptier_nodes_are_chosen_more_often(self):
        """Weighted by space: the node with 9x the room should win far more picks."""
        nodes = [node("roomy", 900), node("tight", 100), node("other", 100)]
        rng = random.Random(3)
        picks = Counter()
        for _ in range(3000):
            picks[weighted_sample(nodes, 1, rng, {})[0].node_id] += 1
        assert picks["roomy"] > 4 * picks["tight"]
        assert picks["roomy"] > 4 * picks["other"]

    def test_equal_nodes_are_chosen_about_equally(self):
        nodes = cluster(4)
        rng = random.Random(5)
        picks = Counter()
        for _ in range(4000):
            picks[weighted_sample(nodes, 1, rng, {})[0].node_id] += 1
        for node_id in ("dn1", "dn2", "dn3", "dn4"):
            assert 800 < picks[node_id] < 1200, picks

    def test_huge_free_space_does_not_collapse_the_weights(self):
        """With the naive u**(1/w) every key rounds to 1.0 at this scale and all tie."""
        nodes = [node("a", 900_000), node("b", 100_000)]  # ~1 PiB and ~100 TiB
        rng = random.Random(9)
        picks = Counter(weighted_sample(nodes, 1, rng, {})[0].node_id for _ in range(2000))
        assert picks["a"] > picks["b"] > 0, picks

    def test_a_full_but_eligible_node_has_a_nonzero_weight(self):
        """A zero-size block can land on a node with exactly zero usable space."""
        nodes = [node("a", 0), node("b", 0)]
        assert len(weighted_sample(nodes, 2, random.Random(1), {})) == 2

    def test_asking_for_more_than_available_returns_what_exists(self):
        assert len(weighted_sample(cluster(2), 5, random.Random(1), {})) == 2


class TestPartitioning:
    """The point of Section 6.2: the same node set is NOT reused for every block."""

    def test_with_exactly_replication_factor_nodes_every_block_uses_all_of_them(self):
        """The measured problem: 3 nodes and factor 3 leave no freedom, so it mirrors."""
        rng = random.Random(1)
        placements = {
            frozenset(n.node_id for n in choose_pipeline(cluster(3), {}, MIB, 3, rng))
            for _ in range(50)
        }
        assert len(placements) == 1

    def test_with_five_nodes_blocks_land_on_different_sets(self):
        """Decision 11: more nodes than the factor is what makes partitioning real."""
        rng = random.Random(1)
        nodes = cluster(5)
        placements = {
            frozenset(n.node_id for n in choose_pipeline(nodes, {}, MIB, 3, rng))
            for _ in range(60)
        }
        assert len(placements) > 3

    def test_with_five_nodes_no_node_holds_every_block_of_a_file(self):
        """Partitioning in the sense of the statement: no node has the whole file."""
        rng = random.Random(11)
        nodes = cluster(5)
        held: Counter[str] = Counter()
        blocks = 12
        for _ in range(blocks):
            for n in choose_pipeline(nodes, {}, MIB, 3, rng):
                held[n.node_id] += 1
        assert max(held.values()) < blocks, held

    def test_a_heavily_reserved_node_is_picked_far_less_often(self):
        """Space already promised to other uploads steers new blocks away from a node.

        The sampling is probabilistic, so this asserts the tendency, not an exact balance.
        """
        nodes = [node(f"dn{i}", free_gib=1) for i in range(1, 6)]
        reserved = {"dn5": 900 * MIB}  # dn5 has ~124 MiB usable, the rest 1 GiB
        rng = random.Random(2)
        picks: Counter[str] = Counter()
        for _ in range(2000):
            for n in choose_pipeline(nodes, reserved, 64 * MIB, 3, rng):
                picks[n.node_id] += 1
        assert picks["dn5"] * 3 < min(picks["dn1"], picks["dn2"], picks["dn3"], picks["dn4"])
