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
    "dungeon-explore-receipt-recovery", "dungeon-explore-stage-recovery", "dungeon-explore-item-recovery",
    "dungeon-explore-manager-isolation", "dungeon-explore-team-recovery", "dungeon-explore-rejection-contracts",
    "dungeon-explore-abort-reply", "dungeon-explore-capacity", "dungeon-explore-real-battles",
    "dungeon-explore-intent-recovery", "dungeon-explore-intent-inventory-recovery",
    "dungeon-explore-intent-team-recovery", "dungeon-explore-nothing-encounter", "dungeon-explore-battle-win",
])
def test_real_exploration_dispatch(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key, "--report", str(report),
             "--base-sha", "0a741b5474bd4c9f7afe781f1c3d2c9d1c019fc7"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=150,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


def module_and_context():
    import nonebot

    nonebot.init()
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

    event = SimpleNamespace(message_id=996999, get_user_id=lambda: "900001")
    bot = SimpleNamespace(self_id="900000", adapter=SimpleNamespace(get_name=lambda: "QQ"))
    return module, bot, event


@pytest.mark.parametrize("exception_name", ["FinishedException", "StopPropagation"])
def test_battle_report_preserves_nonebot_flow_control(exception_name):
    from nonebot import exception

    module, bot, event = module_and_context()
    application = SimpleNamespace(replay=Mock(return_value={"status": "duplicate", "phase": "completed",
        "response": {"message": "已保存的战斗结算", "battle_messages": ["已保存的完整战报"]}}))
    control = getattr(exception, exception_name)()
    summary = AsyncMock()
    with (patch.object(module, "dungeon_application", application),
          patch.object(module, "assign_bot", AsyncMock(return_value=(bot, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "send_msg_handler", AsyncMock(side_effect=control)),
          patch.object(module, "handle_send", summary)):
        with pytest.raises(type(control)) as caught:
            asyncio.run(module.handle_explore_dungeon(bot, event))
    assert caught.value is control
    summary.assert_not_awaited()


def test_battle_report_delivery_failure_keeps_committed_summary_available():
    module, bot, event = module_and_context()
    response = {"message": "已保存的战斗结算", "battle_messages": ["已保存的完整战报"]}
    summary = AsyncMock()
    with (patch.object(module, "send_msg_handler", AsyncMock(side_effect=RuntimeError("isolated battle transport failure"))),
          patch.object(module, "handle_send", summary), patch.object(module.logger, "exception") as logged):
        asyncio.run(module._send_explore_response(bot, event, response))
    logged.assert_called_once()
    assert summary.await_args.args[2] == response["message"]
    assert response["battle_messages"] == ["已保存的完整战报"]


def rejection_codes():
    return [f"dungeon_explore_{step}_{failure}" for step in ("intent", "prepared", "resources", "inventory", "receipt")
            for failure in ("write_rejected", "projection_mismatch")] + [
                "dungeon_progress_write_rejected", "dungeon_progress_projection_mismatch"]


@pytest.mark.parametrize("reason", [*rejection_codes(), "applied", "replayed"])
def test_explore_handler_reply_survives_adapter_routes(reason):
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    module, qq, event = module_and_context()
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    response = {"message": "击败敌人，获得灵石100\n当前：第2层", "battle_messages": ["完整战报"]}
    application = SimpleNamespace(replay=Mock(return_value={
        "status": "duplicate" if reason == "replayed" else "prepared", "phase": "completed" if reason == "replayed" else "prepared",
        "response": response}), settle=Mock(return_value={"phase": "completed", "response": response}))
    failed = reason not in {"applied", "replayed"}
    subject = "900002:9004" if "inventory" in reason else "900002"
    if failed:
        application.replay.side_effect = DomainError(reason, reason, {"reason": reason, "subject": subject})
    sent = AsyncMock()
    with (patch.object(module, "dungeon_application", application),
          patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "send_msg_handler", AsyncMock()),
          patch.object(module, "handle_send", sent), patch.object(utils, "handle_send", sent)):
        if failed:
            asyncio.run(module.handle_explore_dungeon(qq, event))
        else:
            with pytest.raises(FinishedException):
                asyncio.run(module.handle_explore_dungeon(qq, event))
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    phrases = [reason_text(reason), "玩家【900002】"] if failed else [response["message"]]
    if "inventory" in reason:
        phrases.append("物品【9004】")
    for phrase in phrases:
        assert phrase and phrase in reply
    assert options == ({} if failed else {"md_type": "副本", "k1": "探索副本", "v1": "探索副本",
        "k2": "副本状态", "v2": "我的副本状态", "k3": "副本帮助", "v3": "副本帮助"})
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
          patch.object(utils, route, AsyncMock(side_effect=RuntimeError("isolated exploration markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)
