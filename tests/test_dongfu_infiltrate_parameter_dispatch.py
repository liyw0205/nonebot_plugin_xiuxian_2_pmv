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


def test_real_infiltration_parameters_reach_settlement_and_isolated_databases():
    with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-infiltrate-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--group", "dongfu-infiltrate", "--report", str(report)],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        assert payload["summary"] == {"pass": 12}, result.stdout
        unexpected = []
        for row in payload["results"]:
            for call in row.get("calls", []):
                for error in call.get("errors", []):
                    expected_abort = (
                        row.get("key") == "dongfu-infiltrate-sqlite-abort-recovery"
                        and "Traceback" in error
                        and "sqlite3.IntegrityError: infiltrate settlement refused" in error
                    )
                    if not expected_abort:
                        unexpected.append(error)
                for caught in call.get("caught_exceptions", []):
                    trace = caught.get("traceback", "")
                    expected_format_fallback = (
                        caught.get("function") == "_parse_dt"
                        and "ValueError: time data" in trace
                        and "does not match format '%Y-%m-%d %H:%M:%S.%f'" in trace
                    )
                    if not expected_format_fallback:
                        unexpected.append(caught)
        assert unexpected == [], json.dumps(unexpected, ensure_ascii=False, indent=2)

def test_known_infiltration_rejections_keep_reasons_in_onebot_qq_messages():
    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from nonebot.adapters.onebot.v11 import Message
    from tests.command_probe import make_event

    event = make_event("潜入洞府 诊断对手", "900001", 970913)
    handler = module.infiltrate_dongfu.handlers[0].call
    plain_config = SimpleNamespace(markdown_status=False, merge_forward_send=1,
                                   reference_reply=False, message_optimization=False, img=False, at_sender=False)
    markdown_config = SimpleNamespace(markdown_status=True, merge_forward_send=4, markdown_id="",
                                      markdown_button_status=False, reference_reply=False,
                                      message_optimization=False, img=False, at_sender=False)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    reasons = (
        ("dongfu_infiltrate_plan_write_rejected", "plan", "潜入结算计划"),
        ("dongfu_infiltrate_success_player_write_rejected", "actor", "发起方"),
        ("dongfu_infiltrate_success_inventory_write_rejected", "item:21005", "背包物品"),
        ("dongfu_infiltrate_failure_stone_projection_mismatch", "stone_wallet", "灵石钱包"),
        ("dongfu_infiltrate_success_receipt_write_rejected", "receipt", "操作回执"),
    )
    for reason, subject, label in reasons:
        capture = AsyncMock()
        error = DomainError(reason, reason, {"reason": reason, "subject": subject})
        with (patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
              patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
              patch.object(module.dongfu_application, "infiltration_plan", side_effect=error),
              patch.object(utils, "handle_send", capture)):
            asyncio.run(handler(bot=onebot, event=event, args=Message("诊断对手")))
        capture.assert_awaited_once()
        message = capture.await_args.args[2]
        assert reason_text(reason) in message and label in message, message
        for bot, config, markdown, fallback in (
            (onebot, plain_config, False, False), (qq, plain_config, False, False),
            (qq, markdown_config, True, False), (qq, markdown_config, True, True),
        ):
            plain, md = AsyncMock(), AsyncMock()
            if fallback:
                md.side_effect = RuntimeError("isolated infiltration Markdown failure")
            with (patch.object(utils, "XiuConfig", return_value=config),
                  patch.object(utils, "handle_send2", plain),
                  patch.object(utils, "handle_send_markdown", md)):
                asyncio.run(utils.send_msg_handler(bot, event, "潜入洞府", "900000", [message]))
            delivered = md if markdown and not fallback else plain
            delivered.assert_awaited_once()
            assert reason_text(reason) in delivered.await_args.args[2]
            assert label in delivered.await_args.args[2]
