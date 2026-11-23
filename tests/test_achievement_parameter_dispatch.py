from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest


@pytest.mark.parametrize("case_key", ["achievement-real-signin-progress", "achievement-source-read-recovery",
                                     "achievement-filters-pages-readonly", "achievement-compound-progress"])
def test_real_achievement(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key, "--report", str(report),
             "--base-sha", "5f879c8eb7741b67cb577447d6e528e5dd5e6dbb"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=150,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


@pytest.mark.parametrize("state", ["progress", "empty-filter", "new-unlock", "source-failure"])
def test_actual_achievement_handler_and_helper_survive_adapter_routes(state):
    import nonebot

    nonebot.init()
    from nonebot.adapters.onebot.v11 import Message
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    event = SimpleNamespace(message_id=999499, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    sent = AsyncMock()
    snapshot = SimpleNamespace(profile={"level": "江湖好手"}, statistics={"修仙签到": 10 if state == "new-unlock" else 9}, tower={})
    condition_read = Mock(side_effect=sqlite3.OperationalError("isolated achievement statistics source failure")) if state == "source-failure" else Mock(return_value=snapshot)
    action = Mock(return_value=SimpleNamespace(status="applied", succeeded=True))
    argument = "不存在的诊断成就" if state == "empty-filter" else "初心不改"
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "get_user_unlocked_titles", return_value=[]),
          patch.object(module, "get_title_condition_snapshot", condition_read),
          patch.object(module, "_run_title_action", action),
          patch.object(module, "handle_send", sent), patch.object(utils, "handle_send", sent)):
        if state == "source-failure":
            asyncio.run(module.achievement_list_(qq, event, Message(argument)))
        else:
            with pytest.raises(FinishedException):
                asyncio.run(module.achievement_list_(qq, event, Message(argument)))
    condition_read.assert_called_once_with("900001")
    if state == "new-unlock":
        assert action.call_args.args == ("unlock_batch", "title-unlock:999499:900001", "900001")
        assert action.call_args.kwargs == {"expected_unlocked": [], "title_ids": ["30001"]}
    else:
        action.assert_not_called()
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    if state == "source-failure":
        assert "内部异常" in reply and "定位号：" in reply
        assert "【我的成就】" not in reply and "修仙签到：0 / 10" not in reply
        assert options == {}
    else:
        assert options == {"md_type": "修仙", "k1": "检查", "v1": "检查成就", "k2": "称号", "v2": "我的称号",
                           "k3": "帮助", "v3": "称号帮助"}
        if state == "new-unlock":
            assert "本次新解锁" in reply and "【初心不改】已完成 100%" in reply and "修仙签到：10 / 10" in reply
        elif state == "empty-filter":
            assert "筛选：不存在的诊断成就" in reply and "暂无符合条件的成就" in reply
        else:
            assert "【初心不改】未完成 90%" in reply and "修仙签到：9 / 10" in reply
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False), patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply, **options))
        plain.assert_awaited_once()
        assert reply in str(plain.await_args.kwargs["message"]).replace("\r", "\n")
    route = "handle_send_markdown" if state == "source-failure" else "handle_send_markdown_type"
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True), patch.object(utils, route, markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    markdown.assert_awaited_once()
    assert markdown.await_args.args[2] == reply
    if state != "source-failure":
        assert markdown.await_args.args[3:10] == ("修仙", "检查", "检查成就", "称号", "我的称号", "帮助", "称号帮助")
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, route, AsyncMock(side_effect=RuntimeError("isolated achievement markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)
