import base64
import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class GroupInfo:
    valgroup_hash: bytes
    catchain_seqno: int
    workchain: int
    shard: int
    group_start_est: float

    @property
    def valgroup_name(self) -> str:
        shard_hex = f"{self.shard & 0xFFFFFFFFFFFFFFFF:016x}"
        return f"{self.workchain},{shard_hex}.{self.catchain_seqno}"


@dataclass(frozen=True)
class UnnamedGroupInfo:
    valgroup_hash: bytes
    group_start_est: float

    @property
    def valgroup_name(self) -> str:
        return base64.b64encode(self.valgroup_hash).decode()


type GroupData = UnnamedGroupInfo | GroupInfo


@dataclass
class SlotData:
    valgroup_id: str
    slot: int
    is_empty: bool
    slot_start_est_ms: float
    block_id_ext: str | None
    candidate_id: str | None
    parent_block: str | None
    collator: int | str | None
    collate_target_slot: int | None = None
    time_stats: list[tuple[str, float]] | None = None
    validation_time_stats: dict[int, list[tuple[str, float]]] | None = None

    def block_id(self) -> str | None:
        return self.block_id_ext.split(":")[0] if self.block_id_ext else None

    def parent_slot(self) -> int | None:
        """Slot of the block this one builds on.

        None at genesis, and None when the parent was never recorded -- which
        is not the same as "no parent": it means the link is unknown, so two
        slots either side of it are not necessarily consecutive blocks.
        """
        if self.parent_block is None or self.parent_block == "genesis":
            return None
        m = re.match(r"\{(\d+),", self.parent_block)
        return int(m.group(1)) if m else None


@dataclass
class EventData:
    valgroup_id: str
    slot: int
    label: str
    kind: str
    t_ms: float
    validator: int | str | None = None
    t1_ms: float | None = None
    source_valgroup_id: str | None = None
    source_slot: int | None = None
    source_block_id: str | None = None

    def get_color(self) -> str | None:
        from .visualizer.style import COLOR_MAP

        return COLOR_MAP.get(self.label)

    def get_symbol(self) -> str:
        from .visualizer.style import SYMBOL_MAP

        return SYMBOL_MAP.get(self.kind, "circle")


@dataclass(frozen=True)
class GroupParams:
    """What a group reports about itself in its consensus.stats.id event."""

    total_validators: int
    slots_per_leader_window: int


@dataclass
class ConsensusData:
    groups: list[GroupData]
    slots: list[SlotData]
    events: list[EventData]
    # valgroup_name -> the group's own reported parameters. Authoritative:
    # deriving them from observed collators undercounts whenever log coverage
    # is partial, and silently mis-assigns every leader.
    group_params: dict[str, GroupParams] = field(default_factory=dict)
