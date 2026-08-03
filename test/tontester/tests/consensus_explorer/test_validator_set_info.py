from collections.abc import Sequence

import pytest
from consensus_explorer.models import SlotData
from consensus_explorer.validator_set_info import ValidatorRef, ValidatorSetInfoProvider

ADNL_A = "a" * 64
ADNL_B = "b" * 64
PKH_A = "f" * 64
PKH_B = "e" * 64

VSET_TABLE = "\n".join(
    [
        "blockid = (-1,8000000000000000,42)",
        "roothash = deadbeef",
        "",
        "idx | adnl | pub_key_hash | name",
        f"0 | {ADNL_A} | {PKH_A} | ton-tval-01",
        f"1 | {ADNL_B} | {PKH_B} | ton-tval-02",
    ]
)

UNCONFIGURED = "validator set info: explorer url is not configured"


def test_get_validators_parses_the_table_and_ignores_prose_lines(
    monkeypatch: pytest.MonkeyPatch,
):
    provider = ValidatorSetInfoProvider()

    def fake_text(_valgroup_id: str, _slots: Sequence[SlotData]) -> str:
        return VSET_TABLE

    monkeypatch.setattr(provider, "get_validator_set_text", fake_text)

    assert provider.get_validators("0,8000000000000000.1", []) == {
        0: ValidatorRef(idx=0, adnl=ADNL_A, pub_key_hash=PKH_A, name="ton-tval-01"),
        1: ValidatorRef(idx=1, adnl=ADNL_B, pub_key_hash=PKH_B, name="ton-tval-02"),
    }


def test_get_validators_skips_rows_without_a_full_length_adnl(monkeypatch: pytest.MonkeyPatch):
    def fake_text(_valgroup_id: str, _slots: Sequence[SlotData]) -> str:
        return f"0 | truncated | {PKH_A} | ton-tval-01\n1 | {ADNL_B} | {PKH_B} | ton-tval-02"

    provider = ValidatorSetInfoProvider()
    monkeypatch.setattr(provider, "get_validator_set_text", fake_text)

    assert list(provider.get_validators("g", [])) == [1]


def test_get_validators_is_empty_when_lookup_is_not_configured():
    # No explorer url: the text form returns prose, which must not parse into
    # a validator that does not exist.
    assert ValidatorSetInfoProvider().get_validators("0,8000000000000000.1", []) == {}


def test_get_validators_caches_successes_but_retries_failures(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    def fake_text(valgroup_id: str, _slots: Sequence[SlotData]) -> str:
        calls.append(valgroup_id)
        # First call fails, as a transient explorer outage would.
        return UNCONFIGURED if len(calls) == 1 else VSET_TABLE

    provider = ValidatorSetInfoProvider()
    monkeypatch.setattr(provider, "get_validator_set_text", fake_text)

    assert provider.get_validators("g", []) == {}
    assert len(provider.get_validators("g", [])) == 2
    assert len(provider.get_validators("g", [])) == 2
    # Two calls, not three: the failure was retried, the success was cached.
    assert calls == ["g", "g"]
