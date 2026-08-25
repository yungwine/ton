"""MCP server exposing consensus explorer data over stdio.

Serves the same session-stats index the Dash explorer and the leader-stats web
app read, as MCP tools: group discovery, per-validator leader finalization
stats, and the raw slot/event timeline of a single group.

A pure consumer: with ``--stats-dir`` the index must already exist and is only
ever read, polled for updates from whichever process maintains it.
"""

import argparse
import logging
import math
import os
import time
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast, final

from mcp.server import MCPServer
from pydantic import TypeAdapter, ValidationError

from .leader_stats import (
    GroupLeaderStats,
    LeaderStatsAnalyzer,
    SlotStatus,
    ValidatorLeaderStats,
    walk_finalized_chains,
)
from .models import ConsensusData, EventData, GroupData, GroupInfo, SlotData
from .parser import GroupParser
from .validator_set_info import ValidatorRef, ValidatorSetInfoProvider

logger = logging.getLogger(__name__)

DEFAULT_NETWORK = "default"


@dataclass(frozen=True)
class NetworkConfig:
    stats_dir: str
    db: str = ""
    block_explorer_url: str = ""
    validator_names_json: str = ""
    cache_dir: str = ""


_NETWORKS_ADAPTER = TypeAdapter(dict[str, NetworkConfig])

_INSTRUCTIONS = """\
Read-only access to TON consensus session stats collected from validators.

A "valgroup" is one validator group session, named `<workchain>,<shard_hex>.<catchain_seqno>`
(e.g. `-1,8000000000000000.42`). Groups are short-lived, so start from `list_groups`
to find the ones covering the time window you care about, then drill in.

Timestamps are unix seconds. Pass `last_minutes` instead of `time_from` when you
want a window relative to now.

{networks}
"""

_ONE_NETWORK = """\
Every result carries the `network` it came from. Only `{name}` is configured here,
so the `network` argument can be omitted."""

_MANY_NETWORKS = """\
This server serves several separate networks: {names}. They have distinct
validator sets and their valgroup names are NOT interchangeable, so pass
`network` on every call, and carry the `network` from a `list_groups` result
into the calls that follow. `list_groups` with no `network` searches all of
them."""


@dataclass(frozen=True)
class GroupSummary:
    network: str
    valgroup_name: str
    group_start_est: float
    group_start_utc: str
    workchain: int | None
    shard: str | None
    catchain_seqno: int | None


@dataclass(frozen=True)
class ValidatorStatsOut:
    validator: ValidatorRef
    total_leader_slots: int
    finalized: int
    empty: int
    skipped: int
    unknown: int
    produced_pct: float | None


@dataclass(frozen=True)
class GroupStatsOut:
    network: str
    valgroup_id: str
    group_start_est: float
    group_start_utc: str
    total_validators: int
    slots_per_leader_window: int
    first_slot: int
    last_slot: int
    validators: list[ValidatorStatsOut]


@dataclass(frozen=True)
class LeaderStatsOut:
    network: str
    groups: list[GroupStatsOut]
    aggregate: list[ValidatorStatsOut]


@dataclass(frozen=True)
class TimingEntry:
    """One entry of a collation or validation breakdown.

    Durations are in seconds -- the plain names and the `wt_real:`/`wt_cpu:`
    work-time families. The `ssc:` storage-stat-cache entries are counts.
    """

    name: str
    value: float


@dataclass(frozen=True)
class ValidationTiming:
    validator: int
    entries: list[TimingEntry]


@dataclass(frozen=True)
class SlotOut:
    slot: int
    is_empty: bool
    slot_start_est_ms: float
    # {slot, hash} of the candidate this slot carried. Present for every slot
    # any node reported on; block_id needs the collating node's own report and
    # is therefore much sparser.
    candidate_id: str | None
    block_id: str | None
    parent_block: str | None
    collator: int | str | None
    collate_target_slot: int | None
    time_stats: list[TimingEntry] | None
    validation_time_stats: list[ValidationTiming] | None


@dataclass(frozen=True)
class EventOut:
    slot: int
    label: str
    kind: str
    t_ms: float
    validator: int | str | None
    # Span events (collation, block_validation, notarize, finalize, ...) carry
    # their end in t1_ms; for point events both are absent.
    t1_ms: float | None
    duration_ms: float | None
    # Crosslink events point back at the block on the other chain.
    source_valgroup_id: str | None
    source_slot: int | None
    source_block_id: str | None


@dataclass(frozen=True)
class GroupTimeline:
    network: str
    valgroup_id: str
    slot_count: int
    event_count: int
    slots: list[SlotOut]
    events: list[EventOut]
    truncated: bool


@dataclass(frozen=True)
class ValidatorSlotDetail:
    validator: ValidatorRef
    is_leader: bool
    events: list[EventOut]
    validation_time_stats: list[TimingEntry] | None


@dataclass(frozen=True)
class ObserverSlotDetail:
    host: str
    events: list[EventOut]


@dataclass(frozen=True)
class SlotDetail:
    network: str
    valgroup_id: str
    slot: SlotOut
    status: str | None
    leader: ValidatorRef | None
    # Events with no validator of their own: the quorum-reached points, the
    # group level phases, crosslinks.
    group_events: list[EventOut]
    validators: list[ValidatorSlotDetail]
    # Nodes following the group without being in its validator set, keyed by
    # host. They never collate or vote, so they only carry observed events.
    observers: list[ObserverSlotDetail]
    # In the validator set but silent for this slot -- no votes, no candidate,
    # nothing. Either they were down or their logs are not collected.
    silent_validators: list[int]


@dataclass(frozen=True)
class SlotTiming:
    slot: int
    leader_idx: int | None
    status: str | None
    collate_ms: float | None
    notarize_ms: float | None
    finalize_ms: float | None
    candidate_to_finalize_ms: float | None


@dataclass(frozen=True)
class TimingPercentiles:
    metric: str
    count: int
    p50_ms: float
    p90_ms: float
    p99_ms: float
    max_ms: float


@dataclass(frozen=True)
class SlotTimings:
    network: str
    valgroup_id: str
    slot_count: int
    slots: list[SlotTiming]
    percentiles: list[TimingPercentiles]
    truncated: bool


@dataclass(frozen=True)
class BlockInterval:
    """Gap between consecutive finalized blocks, by their finalization certificates.

    Two blocks count as consecutive only when every slot between them is
    certified as having produced nothing. Certificates reach every node, so
    that holds for the whole group, whereas a block id or a parent link is
    written only by the node that collated the block.
    """

    count: int
    min_ms: float
    avg_ms: float
    max_ms: float
    min_between: list[int]
    max_between: list[int]


@dataclass(frozen=True)
class GroupHealth:
    network: str
    valgroup_id: str
    group_start_est: float
    group_start_utc: str
    first_slot: int
    last_slot: int
    finalized: int
    empty: int
    skipped: int
    unknown: int
    # Slots we saw a certificate for directly. The gap to `finalized` is log
    # coverage, not chain behaviour.
    directly_certified: int
    skipped_slots: list[int]
    empty_block_slots: list[int]
    skip_observed_slots: list[int]
    slot_lists_truncated: bool
    block_interval: BlockInterval | None
    # Consecutive observed finalized blocks that the chain does not link
    # directly, so the gap between them spans blocks we never saw and cannot be
    # timed. Large next to block_interval.count means a thin sample; nonzero
    # with block_interval null means nothing was measurable at all.
    unmeasured_block_gaps: int


def _utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def _resolve_window(
    time_from: float | None, time_until: float | None, last_minutes: float | None
) -> tuple[float | None, float | None]:
    if last_minutes is not None:
        return time.time() - last_minutes * 60, time_until
    return time_from, time_until


def _summarize(network: str, group: GroupData) -> GroupSummary:
    if isinstance(group, GroupInfo):
        return GroupSummary(
            network=network,
            valgroup_name=group.valgroup_name,
            group_start_est=group.group_start_est,
            group_start_utc=_utc(group.group_start_est),
            workchain=group.workchain,
            shard=f"{group.shard & 0xFFFFFFFFFFFFFFFF:016x}",
            catchain_seqno=group.catchain_seqno,
        )
    return GroupSummary(
        network=network,
        valgroup_name=group.valgroup_name,
        group_start_est=group.group_start_est,
        group_start_utc=_utc(group.group_start_est),
        workchain=None,
        shard=None,
        catchain_seqno=None,
    )


def _timings(entries: Sequence[tuple[str, float]]) -> list[TimingEntry]:
    return [TimingEntry(name=name, value=value) for name, value in entries]


def _validation_timings(
    stats: Mapping[int, Sequence[tuple[str, float]]],
) -> list[ValidationTiming]:
    return [
        ValidationTiming(validator=validator, entries=_timings(entries))
        for validator, entries in sorted(stats.items())
    ]


def _slot_out(s: SlotData, *, time_stats: bool, validation_time_stats: bool) -> SlotOut:
    return SlotOut(
        slot=s.slot,
        is_empty=s.is_empty,
        slot_start_est_ms=s.slot_start_est_ms,
        candidate_id=s.candidate_id,
        block_id=s.block_id(),
        parent_block=s.parent_block,
        collator=s.collator,
        collate_target_slot=s.collate_target_slot,
        time_stats=_timings(s.time_stats) if time_stats and s.time_stats else None,
        validation_time_stats=(
            _validation_timings(s.validation_time_stats)
            if validation_time_stats and s.validation_time_stats
            else None
        ),
    )


def _event_out(e: EventData) -> EventOut:
    return EventOut(
        slot=e.slot,
        label=e.label,
        kind=e.kind,
        t_ms=e.t_ms,
        validator=e.validator,
        t1_ms=e.t1_ms,
        duration_ms=None if e.t1_ms is None else e.t1_ms - e.t_ms,
        source_valgroup_id=e.source_valgroup_id,
        source_slot=e.source_slot,
        source_block_id=e.source_block_id,
    )


def _produced_pct(v: ValidatorLeaderStats) -> float | None:
    """Share of leader slots that reached a block, empty or not.

    Unknown slots are excluded: no certificate was observed for them, which says
    more about log coverage than about the leader.
    """
    known = v.finalized + v.empty + v.skipped
    if known == 0:
        return None
    return round((v.finalized + v.empty) / known * 100, 2)


def _validator_ref(v: ValidatorLeaderStats) -> ValidatorRef:
    return ValidatorRef(
        idx=v.validator_idx,
        adnl=v.adnl,
        pub_key_hash=v.pub_key_hash,
        name=v.name,
    )


def _validator_out(v: ValidatorLeaderStats) -> ValidatorStatsOut:
    return ValidatorStatsOut(
        validator=_validator_ref(v),
        total_leader_slots=v.total_leader_slots,
        finalized=v.finalized,
        empty=v.empty,
        skipped=v.skipped,
        unknown=v.unknown,
        produced_pct=_produced_pct(v),
    )


def _group_stats_out(network: str, gs: GroupLeaderStats) -> GroupStatsOut:
    return GroupStatsOut(
        network=network,
        valgroup_id=gs.valgroup_id,
        group_start_est=gs.group_start_est,
        group_start_utc=_utc(gs.group_start_est),
        total_validators=gs.total_validators,
        slots_per_leader_window=gs.slots_per_leader_window,
        first_slot=gs.observed_slot_range[0],
        last_slot=gs.observed_slot_range[1],
        validators=[_validator_out(v) for v in gs.validators],
    )


def _stats_out(network: str, all_stats: list[GroupLeaderStats]) -> LeaderStatsOut:
    agg = LeaderStatsAnalyzer.aggregate_by_validator(all_stats)
    aggregate = [_validator_out(v) for v in agg.values()]
    # Worst producers first: that is what anyone reading these numbers is after.
    aggregate.sort(key=lambda v: v.produced_pct if v.produced_pct is not None else 101.0)
    return LeaderStatsOut(
        network=network,
        groups=[_group_stats_out(network, gs) for gs in all_stats],
        aggregate=aggregate,
    )


@final
class _Roster:
    """Who the validators of a group are, and who leads a given slot."""

    def __init__(self, stats: GroupLeaderStats):
        self.by_idx = {v.validator_idx: _validator_ref(v) for v in stats.validators}
        self.total_validators = stats.total_validators
        self.slots_per_leader_window = stats.slots_per_leader_window

    def leader_idx(self, slot: int) -> int:
        return slot // self.slots_per_leader_window % self.total_validators

    def leader(self, slot: int) -> ValidatorRef | None:
        return self.by_idx.get(self.leader_idx(slot))


@final
class _GroupView:
    """One group's parsed data, indexed the way the tools need it.

    Every derived notion lives here once -- slot status, per-slot events, the
    roster -- so the tools stay projections and cannot drift apart on what
    "finalized" or "leader" means.
    """

    def __init__(self, valgroup_name: str, data: ConsensusData, roster: _Roster | None):
        self.valgroup_name = valgroup_name
        self.roster = roster
        self.slots = {s.slot: s for s in data.slots if s.valgroup_id == valgroup_name}
        self.events = [e for e in data.events if e.valgroup_id == valgroup_name]
        group = next((g for g in data.groups if g.valgroup_name == valgroup_name), None)
        self.group_start_est = group.group_start_est if group is not None else None

        self.events_by_slot: dict[int, list[EventData]] = {}
        self.directly_certified: set[int] = set()
        # Slots known to have produced no block: a skip certificate, or a
        # candidate that was itself empty. Both are direct evidence, unlike the
        # SKIPPED classification, which is derived from parent links.
        self.no_block: set[int] = {
            s.slot for s in data.slots if s.is_empty or s.block_id_ext == "empty"
        }
        for e in self.events:
            self.events_by_slot.setdefault(e.slot, []).append(e)
            if e.label == "finalize_reached":
                self.directly_certified.add(e.slot)
            elif e.label == "skip_observed":
                self.no_block.add(e.slot)

        # Rule of record for slot fate, shared with group_leader_stats: a
        # certificate finalizes every ancestor it builds on, so walk back.
        self.status = walk_finalized_chains(self.directly_certified, self.slots)

    def consecutive_blocks(self, earlier: int, later: int) -> bool:
        """Whether two finalized blocks follow one another in the chain.

        Decided from certificates rather than from parent links: if every slot
        in between is certified as having produced nothing, the two are
        consecutive. A slot whose fate we do not know makes it undecidable, and
        the pair is then not counted rather than guessed at.
        """
        return all(s in self.no_block for s in range(earlier + 1, later))

    def slot_events(self, slot: int) -> list[EventData]:
        return sorted(self.events_by_slot.get(slot, []), key=lambda e: e.t_ms)

    def status_of(self, slot: int) -> str | None:
        status = self.status.get(slot)
        return status.value if status is not None else None

    def span_ms(self, slot: int, label: str) -> float | None:
        for e in self.events_by_slot.get(slot, []):
            if e.label == label and e.t1_ms is not None:
                return round(e.t1_ms - e.t_ms, 3)
        return None

    def first_candidate_ms(self, slot: int) -> float | None:
        times = [
            e.t_ms for e in self.events_by_slot.get(slot, []) if e.label == "candidate_received"
        ]
        return min(times) if times else None

    def reached_ms(self, slot: int, label: str) -> float | None:
        for e in self.events_by_slot.get(slot, []):
            if e.label == label:
                return e.t_ms
        return None


def _percentiles(metric: str, values: list[float]) -> TimingPercentiles | None:
    if not values:
        return None
    ordered = sorted(values)

    def at(q: float) -> float:
        # Nearest-rank: with a handful of slots, interpolation invents numbers
        # that no slot actually took.
        return ordered[min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))]

    return TimingPercentiles(
        metric=metric,
        count=len(ordered),
        p50_ms=at(0.5),
        p90_ms=at(0.9),
        p99_ms=at(0.99),
        max_ms=ordered[-1],
    )


@final
class Network:
    """One network's data: its own index, parser and analyzer."""

    def __init__(self, name: str, parser: GroupParser, analyzer: LeaderStatsAnalyzer):
        self.name = name
        self.parser = parser
        self.analyzer = analyzer


def build_server(networks: Mapping[str, Network]) -> MCPServer:
    if not networks:
        raise ValueError("At least one network must be configured.")
    names = sorted(networks)
    blurb = (
        _ONE_NETWORK.format(name=names[0])
        if len(names) == 1
        else _MANY_NETWORKS.format(names=", ".join(f"`{n}`" for n in names))
    )
    server = MCPServer(
        name="consensus-explorer",
        version="0.1.0",
        instructions=_INSTRUCTIONS.format(networks=blurb),
    )

    def pick(network: str | None) -> Network:
        if network is not None:
            chosen = networks.get(network)
            if chosen is None:
                raise ValueError(f"Unknown network {network!r}. Configured: {', '.join(names)}.")
            return chosen
        if len(names) == 1:
            return networks[names[0]]
        raise ValueError(f"Specify network: one of {', '.join(names)}.")

    @server.tool(name="list_groups")
    def _(
        network: str | None = None,
        time_from: float | None = None,
        time_until: float | None = None,
        last_minutes: float | None = None,
        limit: int = 100,
    ) -> list[GroupSummary]:
        """List indexed validator group sessions, most recent first.

        Filters on the group's estimated start time (unix seconds). `last_minutes`
        overrides `time_from` with a window relative to now.

        Omitting `network` searches every configured network. Each result carries
        the `network` it came from; pass that back with its `valgroup_name` to the
        other tools, since the same name can exist on more than one network.
        """
        start, until = _resolve_window(time_from, time_until, last_minutes)
        chosen = list(networks.values()) if network is None else [pick(network)]
        found: list[GroupSummary] = []
        for net in chosen:
            groups = net.analyzer.list_groups(start, until)
            groups.sort(key=lambda g: g.group_start_est, reverse=True)
            found.extend(_summarize(net.name, g) for g in groups[:limit])
        found.sort(key=lambda g: g.group_start_est, reverse=True)
        return found[:limit]

    @server.tool(name="group_leader_stats")
    def _(valgroup_name: str, network: str | None = None) -> GroupStatsOut:
        """Per-validator leader slot finalization stats for one group.

        For every slot in the observed range the leader is derived from the round
        robin schedule, and the slot's fate is reconstructed by walking parent
        links back from slots that reached a finalization certificate:
        `finalized` (block), `empty` (leader produced an empty slot), `skipped`
        (leader produced nothing and the chain moved on), `unknown` (no
        certificate covers it -- usually missing log coverage, not a fault).

        Each row carries a `validator` identity: group local `idx`, the stable
        `adnl`, and `name` -- the label the validator names json maps that adnl
        to. `adnl` and `name` are empty unless the server was started with
        validator set lookup configured (block explorer url plus
        show-validator-set binary).
        """
        net = pick(network)
        stats = net.analyzer.analyze_group(valgroup_name)
        if stats is None:
            raise ValueError(
                (
                    f"No leader stats for group {valgroup_name!r} on {net.name}: "
                    "unknown group, or too few slots to infer the leader schedule."
                )
            )
        return _group_stats_out(net.name, stats)

    @server.tool(name="leader_stats_range")
    def _(
        network: str | None = None,
        time_from: float | None = None,
        time_until: float | None = None,
        last_minutes: float | None = None,
    ) -> LeaderStatsOut:
        """Leader stats for every group in a time window, plus a cross-group aggregate.

        The aggregate is keyed by ADNL and sorted worst-producer first; it is
        empty unless validator set lookup is configured, since validator indices
        are group local and cannot be matched across groups without it. For the
        same reason aggregate rows carry `validator.idx = -1`; identify them by
        `validator.adnl` or `validator.name`.

        Every group in the window is parsed, so keep windows to minutes rather
        than hours on a busy stats directory.
        """
        net = pick(network)
        start, until = _resolve_window(time_from, time_until, last_minutes)
        if start is None and until is None:
            raise ValueError("Specify time_from, time_until or last_minutes to bound the window.")
        return _stats_out(net.name, net.analyzer.analyze_time_range(start, until))

    @server.tool(name="group_timeline")
    def _(
        valgroup_name: str,
        network: str | None = None,
        slot_from: int | None = None,
        slot_to: int | None = None,
        limit: int = 200,
        include_timings: bool = False,
    ) -> GroupTimeline:
        """Raw slots and consensus events of one group, for drilling into a specific slot.

        Events carry a label (`collate`, `notarize_reached`, `finalize_reached`,
        `skip_observed`, ...), a kind (`local`, `observed`, `phase`, `reached`,
        `estimate`, `crosslink`) and a millisecond timestamp. `limit` caps slots
        and events separately -- narrow with `slot_from`/`slot_to` rather than
        raising it.

        Span events -- `collation`, `block_validation`, `finalization`,
        `collate`, `notarize`, `finalize` -- also carry `t1_ms` and the
        `duration_ms` derived from it; that duration is the phase cost and is
        what makes a slow slot legible. Crosslink events carry the `source_*`
        fields identifying the block on the other chain.

        `include_timings` adds the per-slot collation breakdown and the
        per-validator validation breakdown. They run to kilobytes per slot and
        scale with validator count, so turn them on only for a narrow slot range.
        """
        net = pick(network)
        data = net.parser.parse_group(valgroup_name)
        slots = [
            s
            for s in data.slots
            if s.valgroup_id == valgroup_name
            and (slot_from is None or s.slot >= slot_from)
            and (slot_to is None or s.slot <= slot_to)
        ]
        events = [
            e
            for e in data.events
            if e.valgroup_id == valgroup_name
            and (slot_from is None or e.slot >= slot_from)
            and (slot_to is None or e.slot <= slot_to)
        ]
        slots.sort(key=lambda s: s.slot)
        events.sort(key=lambda e: (e.slot, e.t_ms))
        return GroupTimeline(
            network=net.name,
            valgroup_id=valgroup_name,
            slot_count=len(slots),
            event_count=len(events),
            slots=[
                _slot_out(s, time_stats=include_timings, validation_time_stats=include_timings)
                for s in slots[:limit]
            ],
            events=[_event_out(e) for e in events[:limit]],
            truncated=len(slots) > limit or len(events) > limit,
        )

    def view(net: Network, valgroup_name: str) -> _GroupView:
        stats = net.analyzer.analyze_group(valgroup_name)
        return _GroupView(
            valgroup_name,
            net.parser.parse_group(valgroup_name),
            _Roster(stats) if stats is not None else None,
        )

    @server.tool(name="slot_detail")
    def _(valgroup_name: str, slot: int, network: str | None = None) -> SlotDetail:
        """Everything known about one slot, broken down per validator.

        This is the drill-down for "slot N was slow or did not finalize": what
        each validator did and when, the leader's collation breakdown, each
        validator's validation breakdown, and the quorum-reached points.

        Per-validator events include `candidate_received`, the `collation`,
        `block_validation` and `finalization` spans (with `duration_ms`), the
        `notarize_vote` / `finalize_vote` / `skip_vote` moments and
        `skip_observed`. `silent_validators` lists set members with no events at
        all here -- which means either they were down or their logs are not
        collected, and those two look identical from the outside.

        `observers` holds nodes that follow the group without being in its
        validator set, keyed by host. They never collate or vote, so they carry
        only `skip_observed`, `block_accepted` and the `finalization` span --
        but they often have the widest certificate coverage of any node logged.
        """
        net = pick(network)
        v = view(net, valgroup_name)
        slot_data = v.slots.get(slot)
        if slot_data is None:
            known = sorted(v.slots)
            hint = f" Known slots run {known[0]}..{known[-1]}." if known else ""
            raise ValueError(f"No slot {slot} in group {valgroup_name!r} on {net.name}.{hint}")

        by_validator: dict[int, list[EventData]] = {}
        by_observer: dict[str, list[EventData]] = {}
        group_events: list[EventData] = []
        for e in v.slot_events(slot):
            if e.validator is None:
                group_events.append(e)
            elif isinstance(e.validator, int):
                by_validator.setdefault(e.validator, []).append(e)
            else:
                by_observer.setdefault(e.validator, []).append(e)

        validation_stats = slot_data.validation_time_stats or {}
        leader_idx = v.roster.leader_idx(slot) if v.roster else None
        validators = [
            ValidatorSlotDetail(
                validator=(v.roster.by_idx.get(idx) if v.roster else None)
                or ValidatorRef(idx=idx, adnl="", pub_key_hash="", name=""),
                is_leader=idx == leader_idx,
                events=[_event_out(e) for e in events],
                validation_time_stats=(
                    _timings(validation_stats[idx]) if idx in validation_stats else None
                ),
            )
            for idx, events in sorted(by_validator.items())
        ]

        silent = (
            sorted(idx for idx in v.roster.by_idx if idx not in by_validator) if v.roster else []
        )
        return SlotDetail(
            network=net.name,
            valgroup_id=valgroup_name,
            slot=_slot_out(slot_data, time_stats=True, validation_time_stats=False),
            status=v.status_of(slot),
            leader=v.roster.leader(slot) if v.roster else None,
            group_events=[_event_out(e) for e in group_events],
            validators=validators,
            observers=[
                ObserverSlotDetail(host=host, events=[_event_out(e) for e in events])
                for host, events in sorted(by_observer.items())
            ],
            silent_validators=silent,
        )

    @server.tool(name="slot_timings")
    def _(
        valgroup_name: str,
        network: str | None = None,
        slot_from: int | None = None,
        slot_to: int | None = None,
        limit: int = 500,
    ) -> SlotTimings:
        """Per-slot phase durations plus percentiles: which slots were slow, and where.

        One compact row per slot instead of hundreds of events. Durations in ms:

        - `collate_ms` -- the leader building the block
        - `notarize_ms` -- end of collation to notarize quorum
        - `finalize_ms` -- notarize quorum to finalize quorum
        - `candidate_to_finalize_ms` -- first candidate seen anywhere to
          finalize quorum, i.e. the slot's end-to-end cost

        A null means the phase was not observed for that slot, which is normal
        for skipped slots and for slots at the edge of log coverage. Percentiles
        are nearest-rank over every non-null value in the range, including slots
        past `limit` -- only the per-slot rows are capped.
        """
        net = pick(network)
        v = view(net, valgroup_name)
        wanted = sorted(
            s
            for s in v.slots
            if (slot_from is None or s >= slot_from) and (slot_to is None or s <= slot_to)
        )

        rows: list[SlotTiming] = []
        for s in wanted:
            finalize_at = v.reached_ms(s, "finalize_reached")
            first_candidate = v.first_candidate_ms(s)
            rows.append(
                SlotTiming(
                    slot=s,
                    leader_idx=v.roster.leader_idx(s) if v.roster else None,
                    status=v.status_of(s),
                    collate_ms=v.span_ms(s, "collate"),
                    notarize_ms=v.span_ms(s, "notarize"),
                    finalize_ms=v.span_ms(s, "finalize"),
                    candidate_to_finalize_ms=(
                        round(finalize_at - first_candidate, 3)
                        if finalize_at is not None and first_candidate is not None
                        else None
                    ),
                )
            )

        metrics = {
            "collate_ms": [r.collate_ms for r in rows],
            "notarize_ms": [r.notarize_ms for r in rows],
            "finalize_ms": [r.finalize_ms for r in rows],
            "candidate_to_finalize_ms": [r.candidate_to_finalize_ms for r in rows],
        }
        percentiles = [
            p
            for metric, values in metrics.items()
            if (p := _percentiles(metric, [x for x in values if x is not None])) is not None
        ]
        return SlotTimings(
            network=net.name,
            valgroup_id=valgroup_name,
            slot_count=len(wanted),
            # Percentiles stay over the whole range; only the rows are capped,
            # so a truncated response still summarises what was asked for.
            slots=rows[:limit],
            percentiles=percentiles,
            truncated=len(wanted) > limit,
        )

    @server.tool(name="group_health")
    def _(valgroup_name: str, network: str | None = None, max_slot_list: int = 50) -> GroupHealth:
        """Fastest read on whether a group is healthy.

        Counts every slot in the observed range by fate -- `finalized`, `empty`,
        `skipped`, `unknown` -- using the same reconstruction as
        `group_leader_stats`, so the numbers reconcile between the two tools.

        `directly_certified` counts only slots whose own finalization
        certificate we actually saw. The gap between it and `finalized` measures
        log coverage, not chain behaviour: a healthy group with logs from a
        third of its validators has a large gap.

        `block_interval` is the wall-clock gap between consecutive finalized
        blocks, timed from their finalization certificates. Two blocks count as
        consecutive only when every slot between them is certified as having
        produced nothing, so a block that existed but was not observed cannot
        be mistaken for a stall. `unmeasured_block_gaps` counts pairs where a
        slot in between had no certificate either way, leaving it undecidable.
        Slot lists are capped by `max_slot_list`; the counts are always
        complete.
        """
        net = pick(network)
        v = view(net, valgroup_name)
        if not v.slots:
            raise ValueError(
                f"No slots for group {valgroup_name!r} on {net.name}; unknown or not yet indexed."
            )

        first_slot, last_slot = min(v.slots), max(v.slots)

        counts = {status: 0 for status in SlotStatus}
        unknown = 0
        for s in range(first_slot, last_slot + 1):
            status = v.status.get(s)
            if status is None:
                unknown += 1
            else:
                counts[status] += 1

        # The walk reaches back past the observed range -- a genesis parent marks
        # every slot below it skipped -- so bound the lists the same way the
        # counts are bounded, or they contradict each other.
        def in_range(status: SlotStatus) -> list[int]:
            return sorted(
                s for s, st in v.status.items() if st == status and first_slot <= s <= last_slot
            )

        finalized_slots = in_range(SlotStatus.FINALIZED)
        skipped_slots = in_range(SlotStatus.SKIPPED)
        empty_block_slots = sorted(
            s for s, sd in v.slots.items() if sd.block_id_ext == "empty" or sd.is_empty
        )
        skip_observed_slots = sorted(
            {e.slot for e in v.events if e.label == "skip_observed"} & set(v.slots)
        )

        # Gap between consecutive finalized blocks, timed from their
        # finalization certificates. Adjacent in this list is not adjacent in
        # the chain, so every slot in between must be certified as having
        # produced nothing before the pair is timed.
        stamped = [
            (s, t)
            for s in finalized_slots
            if (t := v.reached_ms(s, "finalize_reached")) is not None
        ]
        deltas: list[tuple[int, int, float]] = []
        unlinked = 0
        for (prev_slot, prev_t), (slot, t) in zip(stamped, stamped[1:]):
            if not v.consecutive_blocks(prev_slot, slot):
                unlinked += 1
                continue
            deltas.append((prev_slot, slot, t - prev_t))
        interval = None
        if deltas:
            shortest = min(deltas, key=lambda d: d[2])
            longest = max(deltas, key=lambda d: d[2])
            interval = BlockInterval(
                count=len(deltas),
                min_ms=round(shortest[2], 3),
                avg_ms=round(sum(d[2] for d in deltas) / len(deltas), 3),
                max_ms=round(longest[2], 3),
                min_between=[shortest[0], shortest[1]],
                max_between=[longest[0], longest[1]],
            )

        # Falling back to the earliest slot keeps this usable for groups whose
        # id event never made it into the logs.
        group_start = (
            v.group_start_est
            if v.group_start_est is not None
            else v.slots[first_slot].slot_start_est_ms / 1000
        )
        return GroupHealth(
            network=net.name,
            valgroup_id=valgroup_name,
            group_start_est=group_start,
            group_start_utc=_utc(group_start),
            first_slot=first_slot,
            last_slot=last_slot,
            finalized=counts[SlotStatus.FINALIZED],
            empty=counts[SlotStatus.EMPTY],
            skipped=counts[SlotStatus.SKIPPED],
            unknown=unknown,
            directly_certified=len(v.directly_certified),
            skipped_slots=skipped_slots[:max_slot_list],
            empty_block_slots=empty_block_slots[:max_slot_list],
            skip_observed_slots=skip_observed_slots[:max_slot_list],
            slot_lists_truncated=(
                len(skipped_slots) > max_slot_list
                or len(empty_block_slots) > max_slot_list
                or len(skip_observed_slots) > max_slot_list
            ),
            block_interval=interval,
            unmeasured_block_gaps=unlinked,
        )

    return server


def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        force=True,
    )

    ap = argparse.ArgumentParser(
        description="MCP server (stdio) for consensus session stats",
    )
    source = ap.add_mutually_exclusive_group(required=True)
    _ = source.add_argument("--logs", nargs="+", help="Paths to log files or directory")
    _ = source.add_argument(
        "--networks",
        help=(
            "Path to a JSON file mapping network name to its stats_dir, db, "
            "block_explorer_url, validator_names_json and cache_dir. Serves every "
            "network from one process; replaces the single-network flags."
        ),
    )
    _ = source.add_argument(
        "--stats-dir",
        help=(
            "Directory with gzipped session stats files, indexed by a consensus_explorer "
            "or leader_stats process (this server only reads that index)"
        ),
    )
    _ = ap.add_argument(
        "--db",
        help="Path to the existing SQLite index (default: <stats-dir>/index.db)",
    )
    _ = ap.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Seconds between checks for index updates by its owner (default: 5)",
    )
    _ = ap.add_argument(
        "--hostname-regex",
        default=r"^(.*)$",
        help="Regex with capture group to extract hostname from filename",
    )
    _ = ap.add_argument(
        "--block-explorer-url",
        default=os.getenv("CONSENSUS_EXPLORER_URL", ""),
        help="Block explorer base url (for validator set lookup)",
    )
    _ = ap.add_argument(
        "--show-validator-set-bin",
        default=os.getenv("CONSENSUS_EXPLORER_SHOW_VALIDATOR_SET_BIN", ""),
        help="Path to show-validator-set binary",
    )
    _ = ap.add_argument(
        "--validator-names-json",
        default=os.getenv("CONSENSUS_EXPLORER_VALIDATOR_NAMES_JSON", ""),
        help='Path to json map {"adnl": "name"} for validator names',
    )
    _ = ap.add_argument(
        "--cache-dir",
        default=os.getenv("CONSENSUS_EXPLORER_CACHE_DIR", ""),
        help="Directory to cache downloaded key blocks",
    )
    _ = ap.add_argument(
        "--sudo-helper",
        default="",
        help="Path to helper script for reading files via sudo on permission denied",
    )

    raw = ap.parse_args()

    logs = cast(list[str] | None, raw.logs)
    stats_dir_str = cast(str | None, raw.stats_dir)
    db_str = cast(str | None, raw.db)
    hostname_regex = cast(str, raw.hostname_regex)
    block_explorer_url = cast(str, raw.block_explorer_url)
    show_validator_set_bin = cast(str, raw.show_validator_set_bin)
    validator_names_json = cast(str, raw.validator_names_json)
    cache_dir = cast(str, raw.cache_dir)
    sudo_helper = cast(str, raw.sudo_helper)
    poll_interval = cast(float, raw.poll_interval)

    networks_str = cast(str | None, raw.networks)

    def make_vset(
        explorer_url: str, names_json: str, cache: str
    ) -> ValidatorSetInfoProvider | None:
        if not (explorer_url and show_validator_set_bin):
            return None
        if not Path(show_validator_set_bin).exists():
            ap.error(f"show-validator-set binary not found at {show_validator_set_bin}")
        return ValidatorSetInfoProvider(
            explorer_url, show_validator_set_bin, names_json, cache_dir=cache or None
        )

    def indexed_network(
        name: str, stats_dir: Path, db_path: Path, vset: ValidatorSetInfoProvider | None
    ) -> tuple[Network, "FileIndex"]:
        if not db_path.exists():
            ap.error(
                (
                    f"No index database at {db_path} for network {name!r}. This server only "
                    "reads an index; run consensus_explorer or leader_stats to build one."
                )
            )
        # Read-only always: the index belongs to whichever process maintains it,
        # and a second indexer over the same directory is pure duplicated work.
        file_index = FileIndex(
            stats_dir, db_path, read_only=True, poll_interval_seconds=poll_interval
        )
        parser = CachedGroupParser(file_index, hostname_regex, sudo_helper=sudo_helper or None)
        file_index.install_callback(parser)
        return Network(name, parser, LeaderStatsAnalyzer(parser, vset)), file_index

    if networks_str:
        from .cached_parser import CachedGroupParser
        from .file_index import FileIndex

        config_path = Path(networks_str)
        if not config_path.exists():
            ap.error(f"Networks config not found at {config_path}")
        try:
            configs = _NETWORKS_ADAPTER.validate_json(config_path.read_text(encoding="utf-8"))
        except ValidationError as exc:
            ap.error(f"Invalid networks config {config_path}: {exc}")
        if not configs:
            ap.error(f"Networks config {config_path} defines no networks")

        built: dict[str, Network] = {}
        indexes: list[FileIndex] = []
        for name, cfg in configs.items():
            stats_dir = Path(cfg.stats_dir)
            db_path = Path(cfg.db) if cfg.db else stats_dir / "index.db"
            net, index = indexed_network(
                name,
                stats_dir,
                db_path,
                make_vset(cfg.block_explorer_url, cfg.validator_names_json, cfg.cache_dir),
            )
            built[name] = net
            indexes.append(index)

        with ExitStack() as stack:
            for index in indexes:
                _ = stack.enter_context(index)
            build_server(built).run(transport="stdio")
        return

    vset_provider = make_vset(block_explorer_url, validator_names_json, cache_dir)

    if stats_dir_str:
        from .cached_parser import CachedGroupParser
        from .file_index import FileIndex

        stats_dir = Path(stats_dir_str)
        db_path = Path(db_str) if db_str else stats_dir / "index.db"
        net, file_index = indexed_network(DEFAULT_NETWORK, stats_dir, db_path, vset_provider)
        with file_index:
            build_server({DEFAULT_NETWORK: net}).run(transport="stdio")
    else:
        from .parser.parser_session_stats import ParserSessionStats

        assert logs is not None
        log_paths: list[Path] = []
        for log in logs:
            p = Path(log)
            if p.is_dir():
                log_paths.extend(p.iterdir())
            else:
                log_paths.append(p)

        parser = ParserSessionStats(log_paths, hostname_regex)
        server = build_server(
            {
                DEFAULT_NETWORK: Network(
                    DEFAULT_NETWORK, parser, LeaderStatsAnalyzer(parser, vset_provider)
                )
            }
        )
        server.run(transport="stdio")


if __name__ == "__main__":
    _main()
