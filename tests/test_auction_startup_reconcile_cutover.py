import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


TRADE_FACADE = Path(
    "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
)


def test_startup_reconcile_default_path_uses_feature_application():
    source = TRADE_FACADE.read_text(encoding="utf-8")
    assert "reconcile_auction_after_restart" not in source
    assert "def _reconcile_auction_with_application" in source
    assert "_end_auction_with_application(f\"auction-finish:{session['session_id']}\")" in source
    assert "_reconcile_auction_with_application," in source


def test_startup_reconcile_settles_expired_session_with_stable_operation_id():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import (
        _reconcile_auction_with_application,
    )

    now = datetime(2026, 9, 26, 12, tzinfo=timezone.utc)
    query = SimpleNamespace(get_current_auction=lambda: [{"id": "a-1"}])
    session_service = SimpleNamespace(
        get_active_session=lambda: {
            "session_id": "session-1",
            "end_time": (now - timedelta(minutes=1)).timestamp(),
        }
    )
    settle = AsyncMock()
    with (
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_query_application",
            return_value=query,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_session_service",
            return_value=session_service,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.runtime_clock",
            SimpleNamespace(now=lambda: now),
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._end_auction_with_application",
            settle,
        ),
    ):
        asyncio.run(_reconcile_auction_with_application())

    settle.assert_awaited_once_with("auction-finish:session-1")


def test_startup_reconcile_keeps_future_session_open():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import (
        _reconcile_auction_with_application,
    )

    now = datetime(2026, 9, 26, 12, tzinfo=timezone.utc)
    query = SimpleNamespace(get_current_auction=lambda: [{"id": "a-1"}])
    session_service = SimpleNamespace(
        get_active_session=lambda: {
            "session_id": "session-1",
            "end_time": (now + timedelta(minutes=20)).timestamp(),
        }
    )
    settle = AsyncMock()
    with (
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_query_application",
            return_value=query,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_session_service",
            return_value=session_service,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.runtime_clock",
            SimpleNamespace(now=lambda: now),
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._end_auction_with_application",
            settle,
        ),
    ):
        asyncio.run(_reconcile_auction_with_application())

    settle.assert_not_awaited()
