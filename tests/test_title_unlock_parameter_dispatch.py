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
    "title-unlock-receipt-recovery", "title-unlock-write-recovery", "title-unlock-abort-recovery",
    "title-unlock-normal-boundaries", "title-unlock-shared-command-failures", "title-unlock-conflict-reasons",
])
def test_real_title_unlock(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key, "--report", str(report),
             "--base-sha", "bf1f9e22cb74b907e1501ee3ab78889b3e683d87"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=150,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


def context():
    import nonebot

    nonebot.init()
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    event = SimpleNamespace(message_id=998999, get_user_id=lambda: "900001")
    return module, bot, event


@pytest.mark.parametrize("result_kind", [
    "title_batch_unlock_state_write_rejected", "title_batch_unlock_state_projection_mismatch",
    "title_batch_unlock_receipt_write_rejected", "title_batch_unlock_receipt_projection_mismatch",
    "title_batch_unlock_ledger_projection_mismatch", "title_unlocked_changed", "operation_conflict", "applied", "empty",
])
def test_actual_helper_reasons_and_handler_content_survive_adapters(result_kind):
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    module, qq, event = context()
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    title = {**module.get_title_by_id("30001"), "id": "30001"}
    failed = result_kind not in {"applied", "empty"}
    # Operation conflicts have no DTO title_id/reason; preserve that historical shape.
    result = (SimpleNamespace(status="operation_conflict", succeeded=False) if result_kind == "operation_conflict" else
              SimpleNamespace(status="failed" if failed else "applied", reason=result_kind if failed else "",
                              subject="30001", title_id="30001", succeeded=not failed))
    sent = AsyncMock()
    run = Mock(return_value=result)
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "get_user_unlocked_titles", return_value=[]),
          patch.object(module, "get_title_condition_snapshot", return_value=object()),
          patch.object(module, "find_unlockable_titles", return_value=[] if result_kind == "empty" else [title]),
          patch.object(module, "_run_title_action", run),
          patch.object(module, "handle_send", sent), patch.object(utils, "handle_send", sent)):
        if failed:
            asyncio.run(module.title_check_(qq, event))
        else:
            with pytest.raises(FinishedException):
                asyncio.run(module.title_check_(qq, event))
    if result_kind == "empty":
        run.assert_not_called()
    else:
        assert run.call_count == 1 and run.call_args.args[:3] == ("unlock_batch", "title-unlock:998999:900001", "900001")
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    if failed:
        phrases = [reason_text(result_kind)]
        if result_kind != "operation_conflict":
            phrases.append("称号【30001】")
        assert "暂无新称号" not in reply and "恭喜解锁" not in reply
    else:
        phrases = ["当前已解锁0个称号，暂无新称号解锁"] if result_kind == "empty" else [title["name"], title["desc"], "恭喜解锁"]
    for phrase in phrases:
        assert phrase and phrase in reply
    assert options == ({} if failed else {"md_type": "修仙", "k1": "称号", "v1": "我的称号",
                                        "k2": "帮助", "v2": "称号帮助", "k3": "存档", "v3": "我的修仙信息"})
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False), patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply, **options))
        plain.assert_awaited_once()
        for phrase in phrases:
            assert phrase in str(plain.await_args.kwargs["message"])
    route = "handle_send_markdown" if failed else "handle_send_markdown_type"
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True), patch.object(utils, route, markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    markdown.assert_awaited_once()
    assert markdown.await_args.args[2] == reply
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, route, AsyncMock(side_effect=RuntimeError("isolated title unlock markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)


@pytest.mark.parametrize("exception_name", ["FinishedException", "StopPropagation"])
def test_shared_unlock_handler_preserves_nonebot_control_flow(exception_name):
    from nonebot import exception

    module, bot, event = context()
    control = getattr(exception, exception_name)()
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(bot, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "_unlock_titles_from_event", return_value=([], [], None)),
          patch.object(module, "handle_send", AsyncMock(side_effect=control))):
        with pytest.raises(type(control)) as caught:
            asyncio.run(module.title_check_(bot, event))
    assert caught.value is control
