from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest


@pytest.mark.parametrize("case_key", ["dungeon-history-actual-settlement", "dungeon-history-window-summary",
                                     "dungeon-history-invalid-time"])
def test_real_history_query(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key, "--report", str(report),
             "--base-sha", "c45ae11f21e9a44cdf5a40ec54e098f3a5a17ec0"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=120,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


@pytest.mark.parametrize("has_history", [True, False])
def test_history_handler_content_survives_adapter_routes(has_history):
    import nonebot

    nonebot.init()
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    summary = {"ok": has_history, "notice": "近7天没有副本探索记录。", "total": 1, "cleared": 1,
               "best_layer": 5, "battles": 2, "recent": [{"updated_at": "2026-11-19 12:00:00",
               "current_layer": 5, "dungeon_status": "completed", "message": "获得灵石100\n通关", "battles": 2}]}
    application = SimpleNamespace(summary=Mock(return_value=summary))
    event = SimpleNamespace(message_id=979899, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    sent = AsyncMock()
    with (patch.object(module, "dungeon_explore_history_application", application),
          patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "handle_send", sent)):
        with pytest.raises(FinishedException):
            asyncio.run(module.dungeon_explore_history_(qq, event))
    application.summary.assert_called_once_with("900001")
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    phrases = ("近7天副本探索记录", "探索：1 次（通关 1）", "最高层：5 层", "战斗 2 场", "获得灵石100 / 通关") if has_history else ("近7天没有副本探索记录。",)
    for phrase in phrases:
        assert phrase in reply
    assert options == {"md_type": "副本"}
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False),
              patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply, **options))
        plain.assert_awaited_once()
        for phrase in phrases:
            assert phrase in str(plain.await_args.kwargs["message"])
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown_type", markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    markdown.assert_awaited_once()
    assert markdown.await_args.args[2:4] == (reply, "副本")
    failed = AsyncMock(side_effect=RuntimeError("isolated dungeon history markdown failure"))
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown_type", failed),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)
