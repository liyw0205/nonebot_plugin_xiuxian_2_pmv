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


def test_real_achievement_check_counts_and_unlock():
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", "achievement-check-counts-unlock", "--report", str(report),
             "--base-sha", "b880d795ab68d48d508a895ee7025994148d8aea"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=120,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


@pytest.mark.parametrize("new_unlock", [False, True])
def test_actual_achievement_check_helper_and_summary_survive_adapters(new_unlock):
    import nonebot

    nonebot.init()
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    definitions = module.get_all_titles()
    total = sum(bool(value["condition"]) for value in definitions.values())
    event = SimpleNamespace(message_id=999599, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    snapshot = SimpleNamespace(profile={"level": "江湖好手"}, statistics={"修仙签到": 10 if new_unlock else 9}, tower={})
    sent = AsyncMock()
    action = Mock(return_value=SimpleNamespace(succeeded=True, status="applied"))
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "get_user_unlocked_titles", return_value=["30101", "legacy-missing"]),
          patch.object(module, "get_title_condition_snapshot", return_value=snapshot),
          patch.object(module, "_run_title_action", action), patch.object(module, "handle_send", sent)):
        with pytest.raises(FinishedException):
            asyncio.run(module.achievement_check_(qq, event))
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    if new_unlock:
        title = definitions["30001"]
        assert reply == f"检查完成，解锁新成就：\n【{title['name']}】{title['desc']}"
        assert action.call_args.args == ("unlock_batch", "title-unlock:999599:900001", "900001")
    else:
        assert reply == f"检查完成，当前已完成0/{total}个成就，暂无新成就解锁。"
        action.assert_not_called()
    assert options == {"md_type": "修仙", "k1": "成就", "v1": "我的成就", "k2": "称号", "v2": "我的称号",
                       "k3": "帮助", "v3": "称号帮助"}
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
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True), patch.object(utils, "handle_send_markdown_type", markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    markdown.assert_awaited_once()
    assert markdown.await_args.args[2:10] == (reply, "修仙", "成就", "我的成就", "称号", "我的称号", "帮助", "称号帮助")
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown_type", AsyncMock(side_effect=RuntimeError("isolated achievement check markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)
