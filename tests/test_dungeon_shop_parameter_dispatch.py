from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


def test_real_shop_catalogue_is_readonly():
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", "dungeon-shop-catalogue-readonly", "--report", str(report),
             "--base-sha", "a79428053b9325ecc650445bf5cf5d26d53d62f6"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=120,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


def test_shop_handler_catalogue_and_buttons_survive_adapter_routes():
    import nonebot

    nonebot.init()
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    event = SimpleNamespace(message_id=997099, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    sent = AsyncMock()
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "handle_send", sent)):
        with pytest.raises(FinishedException):
            asyncio.run(module.handle_dungeon_shop(qq, event))
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    expected = [f"{item_id} | {module.items.get_data_by_item_id(item_id)['name']} | {module.number_to(item['cost'])}灵石"
                for item_id, item in module.DUNGEON_SHOP.items()]
    assert reply.splitlines() == ["【副本商店】", *expected, "使用：副本兑换 物品ID 数量"]
    assert options == {"md_type": "副本", "k1": "兑换", "v1": "副本兑换", "k2": "探索", "v2": "探索副本",
                       "k3": "状态", "v3": "我的副本状态"}
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False),
              patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply, **options))
        plain.assert_awaited_once()
        assert reply in str(plain.await_args.kwargs["message"])
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown_type", markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    markdown.assert_awaited_once()
    assert markdown.await_args.args[2:10] == (reply, "副本", "兑换", "副本兑换", "探索", "探索副本", "状态", "我的副本状态")
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown_type", AsyncMock(side_effect=RuntimeError("isolated shop markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)
