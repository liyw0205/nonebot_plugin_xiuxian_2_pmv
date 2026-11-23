from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


def test_real_published_dungeon_queries():
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-info-parameters", "--report", str(report),
             "--base-sha", "96028f85495e2bc66166821efae0bdbd9c4f67c8"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=150,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 2}, details


def test_info_handler_content_and_actions_survive_adapters():
    import nonebot

    nonebot.init()
    import pytest
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    event = SimpleNamespace(message_id=979799, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    manager = SimpleNamespace(get_dungeon_progress=Mock(return_value={
        "name": "Snapshot Trial", "description": "First line\nSecond line", "type": "puzzle", "total_layers": 7,
        "date": "2026-11-20",
    }))
    sent = AsyncMock()
    with (patch.object(module, "dungeon_manager", manager),
          patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "handle_send", sent)):
        with pytest.raises(FinishedException):
            asyncio.run(module.handle_dungeon_info(qq, event))
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    for phrase in ("名称：Snapshot Trial", "简介：First line\nSecond line", "副本类型：puzzle", "总层数：7层", "副本日期：2026-11-20"):
        assert phrase in reply
    assert options == {"md_type": "副本", "k1": "探索副本", "v1": "探索副本", "k2": "副本状态",
                       "v2": "我的副本状态", "k3": "副本帮助", "v3": "副本帮助"}
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
    assert markdown.await_args.args[2:10] == (reply, "副本", "探索副本", "探索副本", "副本状态", "我的副本状态", "副本帮助", "副本帮助")
    failed = AsyncMock(side_effect=RuntimeError("isolated dungeon information markdown failure"))
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown_type", failed),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)
