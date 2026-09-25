from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()


def test_default_auction_start_path_uses_session_application():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
    ).read_text(encoding="utf-8")
    assert "start_auction_process" not in source
    assert "def _start_auction_with_application" in source
    assert "_auction_session_start_application().start(" in source
    assert "success = _start_auction_with_application(" in source


def test_start_adapter_preserves_schedule_and_success_contract():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade import (
        _start_auction_with_application,
        auction_config,
    )

    application = SimpleNamespace(
        start=lambda operation_id, *, system_items_config, duration_hours: SimpleNamespace(
            succeeded=True, status="started", items_count=2
        )
    )
    with (
        patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade._auction_session_start_application",
            return_value=application,
        ),
        patch.object(
            auction_config,
            "get_system_items",
            return_value={"丹药": {"id": 1, "start_price": 2}},
        ),
        patch.object(
            auction_config,
            "get_auction_schedule",
            return_value={"duration_hours": 2},
        ),
        patch.object(auction_config, "set_auction_config_value") as set_config,
    ):
        assert _start_auction_with_application("auction-start:test") is True

    set_config.assert_called_once()
    assert set_config.call_args.args[0] == "schedule"
    assert set_config.call_args.args[2] == "last_auto_start_date"
