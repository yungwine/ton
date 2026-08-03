from decimal import Decimal

import pytest

from contract import ton


def test_ton_converts_decimal_friendly_float_without_truncation():
    assert ton(0.000000015).grams == 15


def test_ton_accepts_exact_string_and_decimal_inputs():
    assert ton("1.000000001").grams == 1_000_000_001
    assert ton(Decimal("2.5")).grams == 2_500_000_000


def test_ton_rejects_amounts_that_do_not_land_on_whole_nanotons():
    with pytest.raises(ValueError, match="whole number of nanotons"):
        ton(0.0000000001)

    with pytest.raises(ValueError, match="whole number of nanotons"):
        ton(0.1 + 0.2)
