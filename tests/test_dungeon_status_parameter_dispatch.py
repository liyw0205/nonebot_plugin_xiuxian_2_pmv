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


@pytest.mark.parametrize("case_key", ["dungeon-status-phases-readonly", "dungeon-status-initialization-recovery",
                                     "dungeon-status-abort-reply-failure"])
def test_real_status_query(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key, "--report", str(report),
             "--base-sha", "ce070497d23318250eed9709d88d1888ebb8fc3c"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=120,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


@pytest.mark.parametrize("reason,phrase", [
    ("write_rejected", "副本状态初始化保存失败"),
    ("projection_mismatch", "副本状态初始化结果与当前副本不一致"),
])
def test_status_handler_reasons_survive_adapters(reason, phrase):
    import nonebot

    nonebot.init()
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    event = SimpleNamespace(message_id=979599, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
    error = DomainError("dungeon_status_initialization_" + reason, "isolated cause", {"subject": "900001"})
    manager = SimpleNamespace(get_player_status=Mock(side_effect=error))
    sent = AsyncMock()
    with (patch.object(module, "dungeon_manager", manager),
          patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(utils, "handle_send", sent)):
        asyncio.run(module.handle_dungeon_status(qq, event))
    sent.assert_awaited_once()
    reply = sent.await_args.args[2]
    assert phrase in reply and "【副本信息】" not in reply and "isolated cause" not in reply
    assert error.details["subject"] == "900001"
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False),
              patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply))
        plain.assert_awaited_once()
        assert phrase in str(plain.await_args.kwargs["message"])
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown", markdown)):
        asyncio.run(utils.handle_send(qq, event, reply))
    markdown.assert_awaited_once()
    assert reply == markdown.await_args.args[2]
    failed = AsyncMock(side_effect=RuntimeError("isolated status markdown failure"))
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown", failed),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply))
    fallback.assert_awaited_once_with(qq, event, reply)

    manager.get_player_status = Mock(return_value={"dungeon_name": "Trial", "dungeon_status": "exploring", "current_layer": 2, "total_layers": 5})
    with (patch.object(module, "dungeon_manager", manager),
          patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "handle_send", AsyncMock())):
        with pytest.raises(FinishedException):
            asyncio.run(module.handle_dungeon_status(qq, event))
