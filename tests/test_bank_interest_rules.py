from datetime import datetime, timezone
import time

import pytest

from nonebot_plugin_xiuxian_2.features.bank.interest_rules import calculate_interest


@pytest.mark.parametrize("saved_at,settled_at", [
    ("2026-10-07 10:00:00", datetime(2026, 10, 7, 12)),
    ("2026-10-07T10:00:00.000000", datetime(2026, 10, 7, 12)),
    ("2026-10-07T10:00:00+08:00", datetime(2026, 10, 7, 4, tzinfo=timezone.utc)),
    ("2026-10-07T02:00:00Z", datetime(2026, 10, 7, 4, tzinfo=timezone.utc)),
])
def test_interest_accepts_legacy_and_offset_timestamps(saved_at, settled_at):
    assert calculate_interest(saved_stone=1000, saved_at=saved_at,
                              settled_at=settled_at, rate=0.002) == (4, 2.0)


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="process timezone switching is unavailable")
def test_legacy_naive_timestamp_is_local_when_runtime_clock_is_utc(monkeypatch):
    with monkeypatch.context() as context:
        context.setenv("TZ", "Asia/Shanghai")
        time.tzset()
        try:
            assert calculate_interest(
                saved_stone=1000, saved_at="2026-10-07 10:00:00",
                settled_at=datetime(2026, 10, 7, 4, tzinfo=timezone.utc), rate=0.002,
            ) == (4, 2.0)
        finally:
            context.undo()
            time.tzset()


@pytest.mark.parametrize("rate", [-0.001, float("nan"), float("inf")])
def test_invalid_rate_is_rejected(rate):
    with pytest.raises(ValueError, match="values are invalid"):
        calculate_interest(saved_stone=1000, saved_at="2026-10-07 10:00:00",
                           settled_at=datetime(2026, 10, 7, 12), rate=rate)


@pytest.mark.parametrize("saved_at", ["2026-10-07 12:00:01", "2026-10-08 12:00:00"])
def test_future_account_timestamp_is_rejected_before_rounding(saved_at):
    with pytest.raises(ValueError, match="precedes account time"):
        calculate_interest(saved_stone=1000, saved_at=saved_at,
                           settled_at=datetime(2026, 10, 7, 12), rate=0.002)


def test_interest_retains_existing_centihour_rounding():
    assert calculate_interest(saved_stone=10000, saved_at="2026-10-07 10:00:00",
                              settled_at=datetime(2026, 10, 7, 11, 14, 59), rate=0.002) == (25, 1.25)
