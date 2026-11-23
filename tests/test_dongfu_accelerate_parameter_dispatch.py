from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_real_accelerate_parameters_writes_time_boundaries_recovery_and_replay():
    with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-accelerate-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--group", "dongfu-accelerate", "--report", str(report)],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        assert payload["summary"] == {"pass": 14}, result.stdout


def test_accelerate_known_rejections_keep_specific_reasons_in_message_adapters():
    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("洞府催熟 1", "900001", 970699)
    handler = module.dongfu_accelerate.handlers[0].call
    plain_config = SimpleNamespace(markdown_status=False, merge_forward_send=1,
                                   reference_reply=False, message_optimization=False, img=False, at_sender=False)
    markdown_config = SimpleNamespace(markdown_status=True, merge_forward_send=4, markdown_id="",
                                      markdown_button_status=False, reference_reply=False,
                                      message_optimization=False, img=False, at_sender=False)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    for part, subject, label in (("inventory", "item:21005", "灵息露"),
                                 ("state", "plot:1", "1号灵田"), ("receipt", "receipt", "催熟回执")):
        for suffix in ("write_rejected", "projection_mismatch"):
            reason = f"dongfu_accelerate_{part}_{suffix}"
            capture = AsyncMock()
            with (patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
                  patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
                  patch.object(module.dongfu_application, "operation_receipt",
                               side_effect=DomainError(reason, reason, {"reason": reason, "subject": subject})),
                  patch.object(utils, "handle_send", capture)):
                asyncio.run(handler(bot=onebot, event=event))
            capture.assert_awaited_once()
            message = capture.await_args.args[2]
            assert reason_text(reason) in message and label in message
            for bot, config, markdown, fallback in (
                (onebot, plain_config, False, False), (qq, plain_config, False, False),
                (qq, markdown_config, True, False), (qq, markdown_config, True, True),
            ):
                plain, md = AsyncMock(), AsyncMock()
                if fallback:
                    md.side_effect = RuntimeError("isolated acceleration Markdown failure")
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "handle_send2", plain),
                      patch.object(utils, "handle_send_markdown", md)):
                    asyncio.run(utils.send_msg_handler(bot, event, "洞府催熟", "900000", [message]))
                delivered = md if markdown and not fallback else plain
                delivered.assert_awaited_once()
                assert reason_text(reason) in delivered.await_args.args[2]
                assert label in delivered.await_args.args[2]
