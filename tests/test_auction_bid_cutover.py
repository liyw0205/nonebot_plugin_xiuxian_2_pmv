from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()


def test_default_auction_bid_handler_uses_feature_application():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
    ).read_text(encoding="utf-8")
    assert "    place_auction_bid," not in source
    assert "await place_auction_bid(" not in source
    assert "def _place_auction_bid_with_application" in source
    assert "_auction_bid_application().place_bid(" in source
    assert "success, result_msg = await _place_auction_bid_with_application(" in source


def test_bid_adapter_passes_snapshot_to_application():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import (
        _place_auction_bid_with_application,
    )

    query = SimpleNamespace(
        get_current_auction=lambda _auction_id: {
            "id": "a-1",
            "name": "法器",
            "seller_id": "seller",
            "start_price": 100,
            "current_price": 100,
            "bids": {},
        }
    )
    application = SimpleNamespace(
        place_bid=lambda **kwargs: SimpleNamespace(
            ok=True,
            status="applied",
            code="bid",
            replayed=False,
            data={"status": "bid"},
        )
    )
    with (
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.get_auction_status",
            return_value={"active": True},
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_query_application",
            return_value=query,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_bid_application",
            return_value=application,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._sql_message",
            return_value=SimpleNamespace(
                get_user_info_with_id=lambda _user_id: {"stone": 1000, "user_name": "玩家"}
            ),
        ),
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.auction_config.get_auction_rules",
            return_value={"min_bid_increment": 10, "min_increment_percent": 0.1},
        ),
    ):
        import asyncio

        success, message = asyncio.run(
            _place_auction_bid_with_application("buyer", "a-1", 200)
        )

    assert success is True
    assert "法器" in message
