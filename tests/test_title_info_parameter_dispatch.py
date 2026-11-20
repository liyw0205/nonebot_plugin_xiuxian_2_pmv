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


def test_real_title_info_definitions_and_readonly_contract():
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", "title-info-definitions-readonly", "--report", str(report),
             "--base-sha", "70b6777b3e2aeb4f23b017458404cf0ed0e1e2fc"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=120,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


@pytest.mark.parametrize("argument", ["30001", "30101", "", "不存在的诊断称号"])
def test_title_info_handler_content_and_buttons_survive_adapters(argument):
    import nonebot

    nonebot.init()
    from nonebot.adapters.onebot.v11 import Message
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title import title_data
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    definitions = json.loads(title_data.TITLE_JSONPATH.read_text(encoding="utf-8"))
    definition = definitions.get(argument)
    expected = (f"【称号详情】\n名称：{definition['name']}\n描述：{definition['desc']}\n获取条件：{definition['condition']}"
                if definition else "未找到该称号！" if argument else "请输入要查看的称号名称或ID！\n例如：称号详情 持之以恒")
    event = SimpleNamespace(message_id=999099, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    sent = AsyncMock()
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "handle_send", sent)):
        with pytest.raises(FinishedException):
            asyncio.run(module.title_info_(qq, event, Message(argument)))
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    assert reply == expected
    assert options == ({"md_type": "修仙", "k1": "称号", "v1": "我的称号", "k2": "装备",
                        "v2": f"装备称号 {definition['name']}", "k3": "帮助", "v3": "称号帮助"} if definition else {})
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False), patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply, **options))
        plain.assert_awaited_once()
        assert expected in str(plain.await_args.kwargs["message"]).replace("\r", "\n")
    route = "handle_send_markdown_type" if definition else "handle_send_markdown"
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True), patch.object(utils, route, markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    markdown.assert_awaited_once()
    assert markdown.await_args.args[2] == expected
    if definition:
        assert markdown.await_args.args[3:10] == ("修仙", "称号", "我的称号", "装备", f"装备称号 {definition['name']}", "帮助", "称号帮助")
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, route, AsyncMock(side_effect=RuntimeError("isolated title info markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, expected)
