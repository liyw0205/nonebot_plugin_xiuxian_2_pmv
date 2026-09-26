from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()


def test_default_auction_settlement_paths_use_feature_application():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
    ).read_text(encoding="utf-8")
    assert "end_auction_process" not in source
    assert "def _end_auction_with_application" in source
    assert "_auction_settlement_application().settle_active(" in source
    assert "results = await _end_auction_with_application(" in source
    assert "await _end_auction_with_application(" in source


def test_settlement_adapter_passes_item_types_and_returns_records():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import (
        _end_auction_with_application,
    )

    query = SimpleNamespace(
        get_current_auction=lambda: [{"item_id": 7}],
    )
    session = SimpleNamespace(get_active_session=lambda: {"session_id": "s-1"})
    settlement = SimpleNamespace(
        settle_active=lambda **kwargs: SimpleNamespace(
            ok=True,
            code="settled",
            data={"results": [{"auction_id": "a-1", "status": "成交"}]},
        )
    )
    with (
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_query_application",
            return_value=query,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_session_service",
            return_value=session,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_settlement_application",
            return_value=settlement,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.items",
            SimpleNamespace(get_data_by_item_id=lambda item_id: {"type": "装备"}),
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_config.get_auction_rules",
            return_value={"fee_rate": 0.1},
        ),
    ):
        result = __import__(
            "asyncio"
        ).run(_end_auction_with_application("finish-1"))

    assert result == [{"auction_id": "a-1", "status": "成交"}]
