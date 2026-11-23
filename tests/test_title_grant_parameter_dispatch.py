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


@pytest.mark.parametrize("case_key", [
    "title-grant-write-recovery", "title-grant-abort-recovery",
    "title-grant-permission-contract", "title-grant-self-mention",
    "title-grant-all-target-recovery", "title-grant-all-empty",
    "title-grant-all-startup-failure", "title-grant-all-interrupted-recovery",
    "title-grant-all-target-abort-recovery",
    "title-grant-all-read-error-before-first-target",
])
def test_real_title_grant(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key, "--report", str(report),
             "--base-sha", "2842027c40d3598477e64542265216302d0ab2ef"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert evidence["summary"] == {"pass": 1}, details


@pytest.mark.parametrize("reason", [
    "title_grant_state_write_rejected", "title_grant_state_projection_mismatch",
    "title_grant_receipt_write_rejected", "title_grant_receipt_projection_mismatch",
    "title_grant_ledger_projection_mismatch",
])
def test_grant_failure_reason_and_subject_survive_actual_helper_and_adapters(reason):
    import nonebot

    nonebot.init()
    from nonebot.adapters.onebot.v11 import Message
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    title = module.get_title_by_id("30101")
    result = SimpleNamespace(status="failed", reason=reason, subject="30101", title_id="30101")
    event = SimpleNamespace(message_id=999799, get_user_id=lambda: "900003")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    sent = AsyncMock()
    sql_message = Mock()
    sql_message.get_user_info_with_name.return_value = {"user_id": "900001", "user_name": "诊断道友"}
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900003"))),
          patch.object(module, "find_title_id_by_name_or_id", return_value="30101"),
          patch.object(module, "get_title_by_id", return_value=title),
          patch.object(module, "get_at_user_id", return_value=None),
          patch.object(module, "_sql_message", return_value=sql_message),
          patch.object(module, "get_user_unlocked_titles", return_value=[]),
          patch.object(module, "_run_title_action", Mock(return_value=result)),
          patch.object(module, "handle_send", sent), patch.object(utils, "handle_send", sent)):
        asyncio.run(module.title_grant_(qq, event, Message("初入仙途 诊断道友")))
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    assert reason_text(reason) in reply and "称号【初入仙途】" in reply and "成功给" not in reply
    assert options == {}
    batch_reply = (
        "全服赠送称号【初入仙途】处理结束：已处理3/3，成功2，重复0，失败1\n"
        "失败明细：\n玩家900002 / 称号30101：赠送称号操作回执保存失败（reason=title_grant_receipt_write_rejected）"
    )
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False), patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply, **options))
            asyncio.run(utils.handle_send(bot, event, batch_reply, **options))
        assert plain.await_count == 2
        assert batch_reply in str(plain.await_args.kwargs["message"])
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown", markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
        asyncio.run(utils.handle_send(qq, event, batch_reply, **options))
    assert markdown.await_count == 2
    assert markdown.await_args.args == (qq, event, batch_reply)
    assert markdown.await_args.kwargs == {"title": None, "button_id": None}
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown", AsyncMock(side_effect=RuntimeError("isolated title grant markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
        asyncio.run(utils.handle_send(qq, event, batch_reply, **options))
    assert fallback.await_count == 2
    fallback.assert_awaited_with(qq, event, batch_reply)
