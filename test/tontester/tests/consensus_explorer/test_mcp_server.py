# pyright: reportAny=false
# CallToolResult.structured_content is `Any` in the SDK; every read of it is
# funnelled through the TypeAdapters below, which is where it gets typed.

from collections.abc import Sequence
from typing import final, override

import pytest
from consensus_explorer.leader_stats import LeaderStatsAnalyzer
from consensus_explorer.mcp_server import (
    GroupHealth,
    GroupStatsOut,
    GroupSummary,
    GroupTimeline,
    LeaderStatsOut,
    SlotDetail,
    SlotTimings,
    TimingEntry,
    ValidationTiming,
    build_server,
)
from consensus_explorer.models import ConsensusData, EventData, GroupData, GroupInfo, SlotData
from consensus_explorer.parser.parser_base import GroupParser
from consensus_explorer.validator_set_info import ValidatorSetInfoProvider
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_types import CallToolResult, InputRequiredResult, TextContent
from pydantic import TypeAdapter

GROUP_A = GroupInfo(
    valgroup_hash=b"a",
    catchain_seqno=7,
    workchain=0,
    shard=0x8000000000000000,
    group_start_est=1000.0,
)
GROUP_B = GroupInfo(
    valgroup_hash=b"b",
    catchain_seqno=8,
    workchain=-1,
    shard=0x8000000000000000,
    group_start_est=2000.0,
)


@final
class FakeParser(GroupParser):
    """Two validators alternating every 2 slots; slot 3 is skipped over."""

    def __init__(self, groups: list[GroupData], data: dict[str, ConsensusData] | None = None):
        self._groups = groups
        self._data = data or {}

    @override
    def list_groups(self) -> list[GroupData]:
        return list(self._groups)

    @override
    def parse_group(self, valgroup_name: str) -> ConsensusData:
        return self._data.get(valgroup_name, ConsensusData(groups=[], slots=[], events=[]))


COLLATION_TIME_STATS = [("total_time", 0.0064), ("wt_real:do_collate", 0.005), ("ssc:hit", 12.0)]
VALIDATION_TIME_STATS = {1: [("total_time", 0.002)], 0: [("total_time", 0.003)]}


def _slot(
    name: str,
    slot: int,
    collator: int,
    parent: str | None,
    time_stats: list[tuple[str, float]] | None = None,
    validation_time_stats: dict[int, list[tuple[str, float]]] | None = None,
) -> SlotData:
    return SlotData(
        valgroup_id=name,
        slot=slot,
        is_empty=False,
        slot_start_est_ms=float(slot) * 1000,
        block_id_ext=f"(0,8000000000000000,{slot}):hash",
        candidate_id=None,
        parent_block=parent,
        collator=collator,
        time_stats=time_stats,
        validation_time_stats=validation_time_stats,
    )


def _group_a_data() -> ConsensusData:
    name = GROUP_A.valgroup_name
    slots = [
        _slot(name, 0, 0, "genesis"),
        _slot(name, 1, 0, "{0, aaa}", COLLATION_TIME_STATS, VALIDATION_TIME_STATS),
        _slot(name, 2, 1, "{1, bbb}"),
        _slot(name, 3, 1, "{2, ccc}"),
        _slot(name, 4, 0, "{2, ccc}"),
    ]
    events = [
        EventData(valgroup_id=name, slot=4, label="finalize_reached", kind="reached", t_ms=4000.0),
        # A span event: the duration is the point of it.
        EventData(
            valgroup_id=name, slot=1, label="collate", kind="phase", t_ms=1000.0, t1_ms=1150.0
        ),
        # Per-validator activity on slot 1: validator 0 leads and collates,
        # validator 1 only votes.
        EventData(
            valgroup_id=name,
            slot=1,
            label="candidate_received",
            kind="local",
            t_ms=1000.0,
            validator=0,
        ),
        EventData(
            valgroup_id=name,
            slot=1,
            label="collation",
            kind="local",
            t_ms=1010.0,
            t1_ms=1160.0,
            validator=0,
        ),
        EventData(
            valgroup_id=name,
            slot=1,
            label="finalize_vote",
            kind="local",
            t_ms=1200.0,
            validator=1,
        ),
        EventData(
            valgroup_id=name,
            slot=4,
            label="candidate_received",
            kind="local",
            t_ms=3900.0,
            validator=0,
        ),
        EventData(
            valgroup_id=name,
            slot=4,
            label="mc_ref_finalized",
            kind="crosslink",
            t_ms=4100.0,
            source_valgroup_id="-1,8000000000000000.8",
            source_slot=11,
            source_block_id="(-1,8000000000000000,99)",
        ),
    ]
    return ConsensusData(groups=[GROUP_A], slots=slots, events=events)


def _build(vset_provider: ValidatorSetInfoProvider | None = None) -> MCPServer:
    parser = FakeParser([GROUP_A, GROUP_B], {GROUP_A.valgroup_name: _group_a_data()})
    return build_server(parser, LeaderStatsAnalyzer(parser, vset_provider))


type _ToolResult = CallToolResult | InputRequiredResult

_GROUP_LIST = TypeAdapter(list[GroupSummary])
_GROUP_STATS = TypeAdapter(GroupStatsOut)
_RANGE_STATS = TypeAdapter(LeaderStatsOut)
_TIMELINE = TypeAdapter(GroupTimeline)


def _payload(result: _ToolResult) -> CallToolResult:
    assert isinstance(result, CallToolResult), result
    assert not result.is_error, _text(result)
    assert result.structured_content is not None
    return result


def _text(result: CallToolResult) -> str:
    return "".join(c.text for c in result.content if isinstance(c, TextContent))


def _groups(result: _ToolResult) -> list[GroupSummary]:
    # Validating against the declared output type is the point: it fails if the
    # tool's structured content ever drifts from its published schema.
    return _GROUP_LIST.validate_python(_payload(result).structured_content["result"])


def _stats(result: _ToolResult) -> GroupStatsOut:
    return _GROUP_STATS.validate_python(_payload(result).structured_content)


def _range(result: _ToolResult) -> LeaderStatsOut:
    return _RANGE_STATS.validate_python(_payload(result).structured_content)


def _timeline(result: _ToolResult) -> GroupTimeline:
    return _TIMELINE.validate_python(_payload(result).structured_content)


@pytest.mark.asyncio
async def test_tools_are_registered_with_stable_names_and_descriptions():
    tools = await _build().list_tools()

    assert {t.name for t in tools} == {
        "list_groups",
        "group_leader_stats",
        "leader_stats_range",
        "group_timeline",
        "slot_detail",
        "slot_timings",
        "group_health",
    }
    assert all(t.description for t in tools)


@pytest.mark.asyncio
async def test_list_groups_returns_most_recent_first_with_decoded_shard():
    result = await _build().call_tool("list_groups", {})

    assert _groups(result) == [
        GroupSummary(
            valgroup_name="-1,8000000000000000.8",
            group_start_est=2000.0,
            group_start_utc="1970-01-01T00:33:20+00:00",
            workchain=-1,
            shard="8000000000000000",
            catchain_seqno=8,
        ),
        GroupSummary(
            valgroup_name="0,8000000000000000.7",
            group_start_est=1000.0,
            group_start_utc="1970-01-01T00:16:40+00:00",
            workchain=0,
            shard="8000000000000000",
            catchain_seqno=7,
        ),
    ]


@pytest.mark.asyncio
async def test_list_groups_honours_time_window_and_limit():
    server = _build()

    windowed = _groups(await server.call_tool("list_groups", {"time_from": 1500.0}))
    limited = _groups(await server.call_tool("list_groups", {"limit": 1}))

    assert [g.valgroup_name for g in windowed] == ["-1,8000000000000000.8"]
    assert [g.valgroup_name for g in limited] == ["-1,8000000000000000.8"]


@pytest.mark.asyncio
async def test_group_leader_stats_reports_slot_fates_per_leader():
    result = await _build().call_tool(
        "group_leader_stats", {"valgroup_name": GROUP_A.valgroup_name}
    )

    stats = _stats(result)
    assert stats.total_validators == 2
    assert stats.slots_per_leader_window == 2
    assert (stats.first_slot, stats.last_slot) == (0, 4)
    # Slot 3 was skipped over: slot 4's parent is slot 2.
    by_idx = {v.validator.idx: v for v in stats.validators}
    assert by_idx[0].finalized == 3  # slots 0, 1, 4
    assert by_idx[1].finalized == 1  # slot 2
    assert by_idx[1].skipped == 1  # slot 3
    assert by_idx[0].produced_pct == 100.0
    assert by_idx[1].produced_pct == 50.0


@pytest.mark.asyncio
async def test_group_leader_stats_errors_when_the_group_cannot_be_analyzed():
    # The session layer turns this into an is_error result for the client.
    with pytest.raises(ToolError, match="nope"):
        _ = await _build().call_tool("group_leader_stats", {"valgroup_name": "nope"})


@pytest.mark.asyncio
async def test_leader_stats_range_skips_unanalyzable_groups():
    result = await _build().call_tool("leader_stats_range", {"time_from": 0.0})

    stats = _range(result)
    # GROUP_B has no slot data, so it drops out rather than failing the call.
    assert [g.valgroup_id for g in stats.groups] == [GROUP_A.valgroup_name]
    # Aggregation is keyed by ADNL, which needs validator set lookup.
    assert stats.aggregate == []


@pytest.mark.asyncio
async def test_leader_stats_range_requires_a_bounded_window():
    with pytest.raises(ToolError, match="time_from"):
        _ = await _build().call_tool("leader_stats_range", {})


@pytest.mark.asyncio
async def test_group_timeline_filters_by_slot_and_flags_truncation():
    server = _build()

    windowed = _timeline(
        await server.call_tool(
            "group_timeline",
            {"valgroup_name": GROUP_A.valgroup_name, "slot_from": 1, "slot_to": 2},
        )
    )
    truncated = _timeline(
        await server.call_tool(
            "group_timeline", {"valgroup_name": GROUP_A.valgroup_name, "limit": 1}
        )
    )

    assert [s.slot for s in windowed.slots] == [1, 2]
    assert sorted(e.label for e in windowed.events) == [
        "candidate_received",
        "collate",
        "collation",
        "finalize_vote",
    ]
    assert windowed.truncated is False
    assert truncated.slot_count == 5
    assert len(truncated.slots) == 1
    assert truncated.truncated is True


@pytest.mark.asyncio
async def test_tool_output_schemas_describe_the_returned_structures():
    tools = {t.name: t for t in await _build().list_tools()}

    # The client validates structured content against these, so a drifting
    # dataclass must not silently produce a schema-less tool.
    for tool in tools.values():
        assert tool.output_schema is not None
    assert "valgroup_name" in str(tools["list_groups"].output_schema)
    assert "produced_pct" in str(tools["group_leader_stats"].output_schema)


@pytest.mark.asyncio
async def test_group_timeline_exposes_span_durations():
    timeline = _timeline(
        await _build().call_tool("group_timeline", {"valgroup_name": GROUP_A.valgroup_name})
    )

    by_label = {e.label: e for e in timeline.events}
    # The phase cost is the whole reason these synthetic span events exist.
    assert by_label["collate"].t1_ms == 1150.0
    assert by_label["collate"].duration_ms == 150.0
    # Point events have no span, and must not fake one.
    assert by_label["finalize_reached"].t1_ms is None
    assert by_label["finalize_reached"].duration_ms is None


@pytest.mark.asyncio
async def test_group_timeline_exposes_crosslink_sources():
    timeline = _timeline(
        await _build().call_tool("group_timeline", {"valgroup_name": GROUP_A.valgroup_name})
    )

    crosslink = next(e for e in timeline.events if e.kind == "crosslink")
    assert crosslink.source_valgroup_id == "-1,8000000000000000.8"
    assert crosslink.source_slot == 11
    assert crosslink.source_block_id == "(-1,8000000000000000,99)"
    # A non-crosslink event carries no provenance.
    collate = next(e for e in timeline.events if e.label == "collate")
    assert (collate.source_valgroup_id, collate.source_slot, collate.source_block_id) == (
        None,
        None,
        None,
    )


@pytest.mark.asyncio
async def test_group_timeline_omits_timing_breakdowns_by_default():
    # They run to kilobytes per slot, so they must not ride along unasked.
    timeline = _timeline(
        await _build().call_tool("group_timeline", {"valgroup_name": GROUP_A.valgroup_name})
    )

    assert all(s.time_stats is None for s in timeline.slots)
    assert all(s.validation_time_stats is None for s in timeline.slots)


@pytest.mark.asyncio
async def test_group_timeline_includes_timing_breakdowns_on_request():
    timeline = _timeline(
        await _build().call_tool(
            "group_timeline",
            {"valgroup_name": GROUP_A.valgroup_name, "include_timings": True},
        )
    )

    slot1 = next(s for s in timeline.slots if s.slot == 1)
    assert slot1.time_stats == [
        TimingEntry(name="total_time", value=0.0064),
        TimingEntry(name="wt_real:do_collate", value=0.005),
        TimingEntry(name="ssc:hit", value=12.0),
    ]
    assert slot1.validation_time_stats == [
        ValidationTiming(validator=0, entries=[TimingEntry(name="total_time", value=0.003)]),
        ValidationTiming(validator=1, entries=[TimingEntry(name="total_time", value=0.002)]),
    ]
    # Slots without breakdowns stay null rather than becoming empty lists.
    slot0 = next(s for s in timeline.slots if s.slot == 0)
    assert slot0.time_stats is None
    assert slot0.validation_time_stats is None


VSET_TABLE = "\n".join(
    [
        "idx | adnl | pub_key_hash | name",
        f"0 | {'a' * 64} | {'f' * 64} | ton-tval-01",
        f"1 | {'b' * 64} | {'e' * 64} | ton-tval-02",
    ]
)


def _stub_vset(monkeypatch: pytest.MonkeyPatch) -> ValidatorSetInfoProvider:
    """A real provider with only the network-facing step replaced."""
    provider = ValidatorSetInfoProvider()

    def fake_text(_valgroup_id: str, _slots: Sequence[SlotData]) -> str:
        return VSET_TABLE

    monkeypatch.setattr(provider, "get_validator_set_text", fake_text)
    return provider


@pytest.mark.asyncio
async def test_validator_identity_is_absent_without_validator_set_lookup():
    stats = _stats(
        await _build().call_tool("group_leader_stats", {"valgroup_name": GROUP_A.valgroup_name})
    )

    # The index is always known; the rest needs the vset provider.
    assert [v.validator.idx for v in stats.validators] == [0, 1]
    assert all(v.validator.adnl == "" and v.validator.name == "" for v in stats.validators)


@pytest.mark.asyncio
async def test_group_leader_stats_carries_resolved_validator_identity(
    monkeypatch: pytest.MonkeyPatch,
):
    server = _build(_stub_vset(monkeypatch))

    stats = _stats(
        await server.call_tool("group_leader_stats", {"valgroup_name": GROUP_A.valgroup_name})
    )

    by_idx = {v.validator.idx: v for v in stats.validators}
    assert by_idx[0].validator.adnl == "a" * 64
    assert by_idx[0].validator.name == "ton-tval-01"
    assert by_idx[1].validator.name == "ton-tval-02"
    # Identity rides alongside the numbers, not instead of them.
    assert by_idx[1].produced_pct == 50.0


@pytest.mark.asyncio
async def test_aggregate_identifies_validators_by_adnl_not_index(
    monkeypatch: pytest.MonkeyPatch,
):
    server = _build(_stub_vset(monkeypatch))

    stats = _range(await server.call_tool("leader_stats_range", {"time_from": 0.0}))

    # Indices are group local, so the aggregate cannot carry a meaningful one.
    assert [(v.validator.name, v.validator.idx) for v in stats.aggregate] == [
        ("ton-tval-02", -1),
        ("ton-tval-01", -1),
    ]


_SLOT_DETAIL = TypeAdapter(SlotDetail)
_SLOT_TIMINGS = TypeAdapter(SlotTimings)
_GROUP_HEALTH = TypeAdapter(GroupHealth)


def _detail(result: _ToolResult) -> SlotDetail:
    return _SLOT_DETAIL.validate_python(_payload(result).structured_content)


def _timings_out(result: _ToolResult) -> SlotTimings:
    return _SLOT_TIMINGS.validate_python(_payload(result).structured_content)


def _health(result: _ToolResult) -> GroupHealth:
    return _GROUP_HEALTH.validate_python(_payload(result).structured_content)


@pytest.mark.asyncio
async def test_slot_detail_splits_events_by_validator_and_marks_the_leader(
    monkeypatch: pytest.MonkeyPatch,
):
    detail = _detail(
        await _build(_stub_vset(monkeypatch)).call_tool(
            "slot_detail", {"valgroup_name": GROUP_A.valgroup_name, "slot": 1}
        )
    )

    assert detail.status == "finalized"
    assert detail.leader is not None and detail.leader.name == "ton-tval-01"
    by_idx = {v.validator.idx: v for v in detail.validators}
    # Slot 1 is in validator 0's leader window.
    assert by_idx[0].is_leader is True
    assert by_idx[1].is_leader is False
    # Ordered by time, so the row reads as what that validator did.
    assert [e.label for e in by_idx[0].events] == ["candidate_received", "collation"]
    assert by_idx[0].events[1].duration_ms == 150.0
    assert [e.label for e in by_idx[1].events] == ["finalize_vote"]
    # Group level events keep their own bucket.
    assert [e.label for e in detail.group_events] == ["collate"]


@pytest.mark.asyncio
async def test_slot_detail_attaches_each_validators_own_validation_breakdown(
    monkeypatch: pytest.MonkeyPatch,
):
    detail = _detail(
        await _build(_stub_vset(monkeypatch)).call_tool(
            "slot_detail", {"valgroup_name": GROUP_A.valgroup_name, "slot": 1}
        )
    )

    by_idx = {v.validator.idx: v for v in detail.validators}
    assert by_idx[0].validation_time_stats == [TimingEntry(name="total_time", value=0.003)]
    assert by_idx[1].validation_time_stats == [TimingEntry(name="total_time", value=0.002)]
    # The collation breakdown belongs to the slot, not to a validator, and is
    # not duplicated into the per-validator rows.
    assert detail.slot.time_stats is not None
    assert detail.slot.validation_time_stats is None


@pytest.mark.asyncio
async def test_slot_detail_reports_validators_that_said_nothing(monkeypatch: pytest.MonkeyPatch):
    detail = _detail(
        await _build(_stub_vset(monkeypatch)).call_tool(
            "slot_detail", {"valgroup_name": GROUP_A.valgroup_name, "slot": 3}
        )
    )

    # Nobody logged anything for slot 3; both set members are silent.
    assert detail.validators == []
    assert detail.silent_validators == [0, 1]


@pytest.mark.asyncio
async def test_slot_detail_rejects_an_unknown_slot_with_the_known_range():
    with pytest.raises(ToolError, match=r"0\.\.4"):
        _ = await _build().call_tool(
            "slot_detail", {"valgroup_name": GROUP_A.valgroup_name, "slot": 99}
        )


@pytest.mark.asyncio
async def test_slot_timings_reports_phase_durations_per_slot():
    timings = _timings_out(
        await _build().call_tool("slot_timings", {"valgroup_name": GROUP_A.valgroup_name})
    )

    by_slot = {r.slot: r for r in timings.slots}
    assert timings.slot_count == 5
    assert by_slot[1].collate_ms == 150.0
    assert by_slot[1].leader_idx == 0
    assert by_slot[1].status == "finalized"
    # Slot 4 saw a candidate at 3900 and finalize quorum at 4000.
    assert by_slot[4].candidate_to_finalize_ms == 100.0
    # Unobserved phases stay null rather than being reported as zero.
    assert by_slot[2].collate_ms is None


@pytest.mark.asyncio
async def test_slot_timings_percentiles_cover_only_observed_values():
    timings = _timings_out(
        await _build().call_tool("slot_timings", {"valgroup_name": GROUP_A.valgroup_name})
    )

    by_metric = {p.metric: p for p in timings.percentiles}
    # One slot has a collate duration, so every percentile is that value.
    assert by_metric["collate_ms"].count == 1
    assert by_metric["collate_ms"].p50_ms == 150.0
    assert by_metric["collate_ms"].max_ms == 150.0
    # Metrics nothing was observed for are omitted, not reported as zero.
    assert "notarize_ms" not in by_metric


@pytest.mark.asyncio
async def test_slot_timings_honours_the_slot_window():
    timings = _timings_out(
        await _build().call_tool(
            "slot_timings",
            {"valgroup_name": GROUP_A.valgroup_name, "slot_from": 2, "slot_to": 3},
        )
    )

    assert [r.slot for r in timings.slots] == [2, 3]
    assert timings.truncated is False


@pytest.mark.asyncio
async def test_group_health_counts_reconcile_with_group_leader_stats():
    server = _build()

    health = _health(
        await server.call_tool("group_health", {"valgroup_name": GROUP_A.valgroup_name})
    )
    stats = _stats(
        await server.call_tool("group_leader_stats", {"valgroup_name": GROUP_A.valgroup_name})
    )

    # Same reconstruction on both sides: per-validator totals must sum to the
    # group totals, or one of the tools is lying about what "finalized" means.
    assert health.finalized == sum(v.finalized for v in stats.validators)
    assert health.skipped == sum(v.skipped for v in stats.validators)
    assert health.empty == sum(v.empty for v in stats.validators)
    assert health.unknown == sum(v.unknown for v in stats.validators)


@pytest.mark.asyncio
async def test_group_health_separates_chain_progress_from_log_coverage():
    health = _health(
        await _build().call_tool("group_health", {"valgroup_name": GROUP_A.valgroup_name})
    )

    # Only slot 4 carries a certificate, but the parent walk finalizes its
    # ancestors too. The gap is coverage, not a stalled chain.
    assert health.directly_certified == 1
    assert health.finalized == 4
    assert health.skipped_slots == [3]
    assert (health.first_slot, health.last_slot) == (0, 4)


@pytest.mark.asyncio
async def test_group_health_measures_the_gap_between_finalized_blocks():
    health = _health(
        await _build().call_tool("group_health", {"valgroup_name": GROUP_A.valgroup_name})
    )

    assert health.block_interval is not None
    # Candidates at slot 1 (1000) and slot 4 (3900): one gap of 2900ms.
    assert health.block_interval.count == 1
    assert health.block_interval.min_ms == 2900.0
    assert health.block_interval.max_between == [1, 4]


@pytest.mark.asyncio
async def test_group_health_caps_slot_lists_but_not_counts():
    health = _health(
        await _build().call_tool(
            "group_health", {"valgroup_name": GROUP_A.valgroup_name, "max_slot_list": 0}
        )
    )

    assert health.skipped_slots == []
    assert health.slot_lists_truncated is True
    assert health.skipped == 1
