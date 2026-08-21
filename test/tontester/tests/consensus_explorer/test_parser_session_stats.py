# pyright: reportPrivateUsage=false

import gzip
import shutil
from pathlib import Path

from consensus_explorer.cached_parser import CachedGroupParser
from consensus_explorer.file_index import FileIndex
from consensus_explorer.parser.parser_session_stats import (
    ParserSessionStats,
    format_candidate_id,
)
from consensus_explorer.visualizer.figure_builder import FigureBuilder
from tonapi.ton_api import (
    Consensus_candidateId,
    Consensus_simplex_notarizeVote,
    Consensus_simplex_skipVote,
    Consensus_simplex_stats_certObserved,
    Consensus_stats_blockAccepted,
    Consensus_stats_collateFinished,
    Consensus_stats_collateStarted,
    Consensus_stats_events,
    Consensus_stats_id,
    Consensus_stats_timestampedEvent,
    Consensus_stats_validationFinished,
    Consensus_stats_validationStarted,
)

# Vendored session-stats logs from a 2-node local network (see test_basic.py).
# Shard valgroup "0,8000000000000000.0" carries masterchain crosslink markers.
_DATA_DIR = Path(__file__).resolve().parent / "data"
_SHARD_VALGROUP = "0,8000000000000000.0"


def _stats_logs() -> list[Path]:
    return sorted(_DATA_DIR.glob("session-logs*"))


def test_parser_adds_shard_markers_from_masterchain_shard_configuration():
    parser = ParserSessionStats(_stats_logs(), r"^(.*)$", with_cache=False)

    data = parser.parse()

    mc_finalize_markers = [e for e in data.events if e.label == "mc_ref_finalized"]
    mc_collate_markers = [e for e in data.events if e.label == "mc_ref_collate_started"]
    assert mc_finalize_markers
    assert mc_collate_markers

    source_finalize_events = {
        (e.valgroup_id, e.slot, e.t_ms) for e in data.events if e.label == "finalize_reached"
    }
    source_collate_events = {
        (e.valgroup_id, e.slot, e.t_ms) for e in data.events if e.label == "collate"
    }

    for marker in mc_finalize_markers:
        assert marker.valgroup_id.startswith("0,")
        assert marker.source_valgroup_id is not None
        assert marker.source_valgroup_id.startswith("-1,")
        assert marker.source_slot is not None
        assert marker.source_block_id is not None
        assert (
            marker.source_valgroup_id,
            marker.source_slot,
            marker.t_ms,
        ) in source_finalize_events

    for marker in mc_collate_markers:
        assert marker.valgroup_id.startswith("0,")
        assert marker.source_valgroup_id is not None
        assert marker.source_valgroup_id.startswith("-1,")
        assert marker.source_slot is not None
        assert marker.source_block_id is not None
        assert (
            marker.source_valgroup_id,
            marker.source_slot,
            marker.t_ms,
        ) in source_collate_events

    assert {(e.valgroup_id, e.slot) for e in mc_finalize_markers} & {
        (e.valgroup_id, e.slot) for e in mc_collate_markers
    }


def test_figure_builder_includes_crosslink_markers():
    parser = ParserSessionStats(_stats_logs(), r"^(.*)$", with_cache=False)
    data = parser.parse()
    builder = FigureBuilder(data)

    summary = builder.build_summary(_SHARD_VALGROUP, 0, 50, False)

    # plotly ships no type stubs, so Figure.data traces are untyped (see figure_builder.py)
    summary_names = {  # pyright: ignore[reportUnknownVariableType]
        trace.name  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
        for trace in summary.data  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    }

    assert "mc_ref_collate_started" in summary_names
    assert "mc_ref_finalized" in summary_names


def test_cached_parser_keeps_crosslink_markers_in_stats_dir_mode(tmp_path: Path):
    stats_dir = tmp_path / "stats"
    stats_dir.mkdir()
    for src in _DATA_DIR.glob("session-logs*"):
        _ = shutil.copy(src, stats_dir / src.name)

    file_index = FileIndex(stats_dir, stats_dir / "index.db")
    conn = file_index._connect()
    try:
        file_index._initial_scan(conn)
    finally:
        conn.close()

    parser = CachedGroupParser(file_index, r"^(.*)$")
    data = parser.parse_group(_SHARD_VALGROUP)

    labels = {e.label for e in data.events}
    assert "mc_ref_collate_started" in labels
    assert "mc_ref_finalized" in labels


def test_collation_attached_to_finished_slot_when_slots_differ():
    """When collation starts at slot x but finishes at slot y,
    both collate_started and collate_finished should be attached to slot y."""
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)
    v_group = "test_group"
    v_id = 0
    target_slot = 10  # slot x: where collation was initiated
    finished_slot = 11  # slot y: where collation actually finished

    # Simulate collateStarted for target_slot
    parser._parse_stats_event(
        Consensus_stats_collateStarted(target_slot=target_slot),
        t_ms=1000.0,
        v_group=v_group,
        v_id=v_id,
    )

    # collate_started should initially be under target_slot
    assert (v_group, target_slot) in parser._slot_events
    assert "collate_started" in parser._slot_events[(v_group, target_slot)][v_id]
    assert (v_group, target_slot) in parser._collated
    assert "collate_started" in parser._collated[(v_group, target_slot)]

    # Simulate collateFinished at a different slot
    parser._parse_stats_event(
        Consensus_stats_collateFinished(
            target_slot=target_slot,
            id=Consensus_candidateId(slot=finished_slot, hash=b"\x00" * 32),
        ),
        t_ms=2000.0,
        v_group=v_group,
        v_id=v_id,
    )

    finished_slot_id = (v_group, finished_slot)

    # Both events should now be under finished_slot
    assert "collate_started" in parser._slot_events[finished_slot_id][v_id]
    assert "collate_finished" in parser._slot_events[finished_slot_id][v_id]
    assert parser._slot_events[finished_slot_id][v_id]["collate_started"].slot == finished_slot
    assert parser._slot_events[finished_slot_id][v_id]["collate_finished"].slot == finished_slot

    assert "collate_started" in parser._collated[finished_slot_id]
    assert "collate_finished" in parser._collated[finished_slot_id]
    assert parser._collated[finished_slot_id]["collate_started"].slot == finished_slot

    # collate_started should be removed from target_slot
    target_slot_id = (v_group, target_slot)
    assert "collate_started" not in parser._slot_events.get(target_slot_id, {}).get(v_id, {})
    assert "collate_started" not in parser._collated.get(target_slot_id, {})

    # Collator should be set on the finished slot
    assert parser._slots[finished_slot_id].collator == v_id


def test_collation_same_slot_unchanged():
    """When collation starts and finishes at the same slot, no relocation needed."""
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)
    v_group = "test_group"
    v_id = 0
    slot = 10

    parser._parse_stats_event(
        Consensus_stats_collateStarted(target_slot=slot),
        t_ms=1000.0,
        v_group=v_group,
        v_id=v_id,
    )
    parser._parse_stats_event(
        Consensus_stats_collateFinished(
            target_slot=slot,
            id=Consensus_candidateId(slot=slot, hash=b"\x00" * 32),
        ),
        t_ms=2000.0,
        v_group=v_group,
        v_id=v_id,
    )

    slot_id = (v_group, slot)
    assert "collate_started" in parser._slot_events[slot_id][v_id]
    assert "collate_finished" in parser._slot_events[slot_id][v_id]
    assert parser._slot_events[slot_id][v_id]["collate_started"].slot == slot
    assert parser._slot_events[slot_id][v_id]["collate_finished"].slot == slot


def test_parser_emits_block_accepted_events():
    """consensus.stats.blockAccepted is an observer's only timing signal.

    Nodes that follow a group without being in its validator set emit nothing
    but certObserved and blockAccepted, so dropping the latter left them with
    no measurable moment of their own.
    """
    raw = 0
    for path in _stats_logs():
        with gzip.open(path, "rt") as fh:
            for line in fh:
                if not line.startswith('{"@type":"consensus.stats.events"'):
                    continue
                for entry in Consensus_stats_events.from_json(line).events:
                    if isinstance(entry.event, Consensus_stats_blockAccepted):
                        raw += 1
    assert raw > 0, "fixture no longer contains blockAccepted records"

    data = ParserSessionStats(_stats_logs(), r"^(.*)$", with_cache=False).parse()

    accepted = [e for e in data.events if e.label == "block_accepted"]
    assert len(accepted) == raw
    # A point in time for the node that accepted it, not an inferred span.
    assert all(e.t1_ms is None and e.validator is not None for e in accepted)
    assert {e.kind for e in accepted} == {"local"}


def _observer_batch(idx: int, slots: list[int]) -> list[Consensus_stats_timestampedEvent]:
    events = [
        Consensus_stats_timestampedEvent(
            ts=1.0,
            event=Consensus_stats_id(
                workchain=0,
                shard=0,
                cc_seqno=1,
                idx=idx,
                total_validators=2,
                weight=0 if idx < 0 else 5,
                total_weight=10,
                slots_per_leader_window=1,
            ),
        )
    ]
    for slot in slots:
        events.append(
            Consensus_stats_timestampedEvent(
                ts=2.0 + slot,
                event=Consensus_simplex_stats_certObserved(
                    vote=Consensus_simplex_skipVote(slot=slot)
                ),
            )
        )
    return events


def test_observers_are_keyed_by_host_not_merged_under_index_minus_one():
    """Nodes outside the validator set all report idx -1.

    Keying their events by index collapsed every such node into one bucket,
    where each overwrote the last in _slot_events.
    """
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)

    _ = parser._process_group_events(b"g", _observer_batch(-1, [7]), "host-a")
    _ = parser._process_group_events(b"g", _observer_batch(-1, [7]), "host-b")
    _ = parser._process_group_events(b"g", _observer_batch(0, [7]), "host-c")

    per_validator = parser._slot_events[("0,0000000000000000.1", 7)]
    assert set(per_validator) == {"host-a", "host-b", 0}
    assert all("skip_observed" in labels for labels in per_validator.values())


def test_observers_do_not_count_towards_validator_coverage():
    """total_validators counts set members, so observers must not inflate it."""
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)

    _ = parser._process_group_events(b"g", _observer_batch(-1, [1]), "host-a")
    _ = parser._process_group_events(b"g", _observer_batch(-1, [1]), "host-b")
    _ = parser._process_group_events(b"g", _observer_batch(1, [1]), "host-c")

    # Three hosts reported, but only one is in the set of two.
    assert parser._seen_validators["0,0000000000000000.1"] == {1}


def _id_event(idx: int, total_validators: int, window: int) -> Consensus_stats_timestampedEvent:
    return Consensus_stats_timestampedEvent(
        ts=1.0,
        event=Consensus_stats_id(
            workchain=0,
            shard=0,
            cc_seqno=1,
            idx=idx,
            total_validators=total_validators,
            weight=0 if idx < 0 else 5,
            total_weight=100,
            slots_per_leader_window=window,
        ),
    )


def test_parse_carries_the_group_reported_params():
    """The group states its own size; the parser must not drop it.

    Deriving it downstream from observed collators undercounts whenever log
    coverage is partial, which silently reassigns every leader.
    """
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)

    group = parser._process_group_events(b"g", [_id_event(-1, 23, 4)], "ton-coll-01")

    assert parser._total_validators[group.valgroup_name] == 23
    assert parser._slots_per_leader_window[group.valgroup_name] == 4


def test_a_collator_outside_the_validator_set_is_not_recorded_as_collator():
    """ton-coll-* nodes collate without being in the set (idx -1).

    Writing their host into SlotData.collator both loses the scheduled leader
    index and poisons any inference that reads the field.
    """
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)
    v_group = "0,0000000000000000.1"
    parser._total_validators[v_group] = 23
    parser._slots_per_leader_window[v_group] = 4

    parser._parse_stats_event(
        Consensus_stats_collateStarted(target_slot=52),
        t_ms=1000.0,
        v_group=v_group,
        v_id="ton-coll-01",
    )

    # Slot 52 is led by 52 // 4 % 23 == 13, and that must survive.
    assert parser._slots[(v_group, 52)].collator == 13


def test_candidate_id_comes_from_any_event_that_names_one():
    """Only the collating node emits candidateReceived.

    Every other event naming a candidate carries the same {slot, hash}, so
    taking the id only from candidateReceived leaves it blank for most slots.
    """
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)
    v_group = "test_group"
    other = Consensus_candidateId(slot=7, hash=b"\xaa" * 32)

    parser._parse_stats_event(
        Consensus_stats_blockAccepted(id=other), t_ms=1000.0, v_group=v_group, v_id="ton-coll-01"
    )

    assert parser._slots[(v_group, 7)].candidate_id == format_candidate_id(other)


def test_the_accepted_candidate_wins_over_a_merely_seen_one():
    """A candidate can be proposed and never finalized; blockAccepted names the
    one that actually became the block."""
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)
    v_group = "test_group"
    proposed = Consensus_candidateId(slot=9, hash=b"\x11" * 32)
    accepted = Consensus_candidateId(slot=9, hash=b"\x22" * 32)

    parser._parse_stats_event(
        Consensus_stats_validationStarted(id=proposed), t_ms=1000.0, v_group=v_group, v_id=0
    )
    assert parser._slots[(v_group, 9)].candidate_id == format_candidate_id(proposed)

    parser._parse_stats_event(
        Consensus_stats_blockAccepted(id=accepted), t_ms=2000.0, v_group=v_group, v_id=0
    )
    assert parser._slots[(v_group, 9)].candidate_id == format_candidate_id(accepted)

    # And a later non-authoritative sighting must not undo it.
    parser._parse_stats_event(
        Consensus_stats_validationFinished(id=proposed), t_ms=3000.0, v_group=v_group, v_id=0
    )
    assert parser._slots[(v_group, 9)].candidate_id == format_candidate_id(accepted)


def test_an_observed_certificate_creates_and_timestamps_its_slot():
    """A notarize/finalize certificate proves a block existed in that slot.

    Only skip votes used to create the slot, so slots seen purely through an
    observed certificate were missing entirely -- and creating one without
    stamping its time leaves slot_start_est_ms at infinity, which poisons
    anything that sorts by time.
    """
    parser = ParserSessionStats([], r"^(.*)$", with_cache=False)
    v_group = "test_group"
    candidate = Consensus_candidateId(slot=12, hash=b"\x33" * 32)
    vote = Consensus_simplex_notarizeVote(id=candidate)

    parser._parse_cert_observed(
        Consensus_simplex_stats_certObserved(vote=vote),
        t_ms=5000.0,
        v_group=v_group,
        v_id="ton-coll-01",
        get_slot_leader=lambda _s: 0,
    )

    slot_data = parser._slots[(v_group, 12)]
    assert slot_data.slot_start_est_ms == 5000.0
    assert slot_data.candidate_id == format_candidate_id(candidate)
