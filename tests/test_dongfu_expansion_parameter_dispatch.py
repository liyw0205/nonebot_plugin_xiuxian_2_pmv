from __future__ import annotations

import json
import asyncio
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_real_expansion_parameters_reach_isolated_writes():
    with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-expansion-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--group", "dongfu-expansion", "--report", str(report)],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        assert payload["summary"] == {"pass": 19}, result.stdout


def test_expansion_failure_reason_survives_onebot_and_qq_formatters():
    from nonebot.adapters.onebot.v11 import Message

    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("洞府扩建 1", "900001", 972699)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    plain_config = SimpleNamespace(markdown_status=False, merge_forward_send=1, reference_reply=False,
                                    message_optimization=False, img=False, at_sender=False)
    markdown_config = SimpleNamespace(markdown_status=True, merge_forward_send=4, markdown_id="",
                                      markdown_button_status=False, reference_reply=False,
                                      message_optimization=False, img=False, at_sender=False)
    reason = "dongfu_expansion_deed_write_rejected"
    capture = AsyncMock()
    handler = module.dongfu_expand.handlers[0].call
    with (
        patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
        patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
        patch.object(module, "_has_dongfu", return_value=(True, {"built": 1, "plot_count": 3})),
        patch.object(module.dongfu_application, "expand", return_value=SimpleNamespace(
            status="state_changed", succeeded=False, reason=reason, subject="item:21008",
        )),
        patch.object(module, "handle_send", capture),
    ):
        asyncio.run(handler(bot=onebot, event=event, args=Message("1")))
    message = capture.await_args.args[2]
    assert "洞府扩建：洞府地契" in message and reason_text(reason) in message, message

    for bot, selected_config, use_markdown, fallback in (
        (onebot, plain_config, False, False),
        (qq, plain_config, False, False),
        (qq, markdown_config, True, False),
        (qq, markdown_config, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated expansion Markdown failure")
        with (
            patch.object(utils, "XiuConfig", return_value=selected_config),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(bot, event, "洞府扩建", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        assert reason_text(reason) in delivered.await_args.args[2]
        assert "洞府地契" in delivered.await_args.args[2]
