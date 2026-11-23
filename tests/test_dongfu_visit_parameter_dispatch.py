from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_real_visit_parameters_reach_reward_and_receipt_writes():
    with tempfile.TemporaryDirectory(prefix="xiuxian-dongfu-visit-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "dongfu-visit",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=120,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(
            failures, ensure_ascii=False, indent=2
        )
        assert payload["summary"] == {"pass": 11}, result.stdout


def test_known_visit_write_rejections_keep_reason_and_subject_in_onebot_qq_messages():
    import asyncio

    from nonebot.adapters.onebot.v11 import Message

    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("拜访道友 诊断对手", "900001", 972299)
    handler = module.visit_friend.handlers[0].call
    plain_config = SimpleNamespace(markdown_status=False, merge_forward_send=1,
                                   reference_reply=False, message_optimization=False, img=False, at_sender=False)
    markdown_config = SimpleNamespace(markdown_status=True, merge_forward_send=4, markdown_id="",
                                      markdown_button_status=False, reference_reply=False,
                                      message_optimization=False, img=False, at_sender=False)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    cases = (
        ("dongfu_visit_stone_write_rejected", "stone_wallet", "灵石钱包"),
        ("dongfu_visit_stone_projection_mismatch", "stone_wallet", "灵石钱包"),
        ("dongfu_visit_receipt_write_rejected", "receipt", "拜访奖励回执"),
        ("dongfu_visit_receipt_projection_mismatch", "receipt", "拜访奖励回执"),
    )
    for reason, subject, label in cases:
        capture = AsyncMock()
        error = DomainError(reason, reason, {"reason": reason, "subject": subject})
        with (patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
              patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
              patch.object(module.player_profile_application, "get_user_profile_by_name",
                           return_value={"user_id": "900002"}),
              patch.object(module, "_get_dongfu", return_value={"built": 1, "node_name": "矿脉"}),
              patch.object(module.dongfu_application, "visit_reward", side_effect=error),
              patch.object(utils, "handle_send", capture)):
            asyncio.run(handler(bot=onebot, event=event, args=Message("诊断对手")))
        capture.assert_awaited_once()
        message = capture.await_args.args[2]
        assert reason_text(reason) in message and label in message, message
        assert "体力不退还" in message

        for bot, config, markdown, fallback in (
            (onebot, plain_config, False, False), (qq, plain_config, False, False),
            (qq, markdown_config, True, False), (qq, markdown_config, True, True),
        ):
            plain, md = AsyncMock(), AsyncMock()
            if fallback:
                md.side_effect = RuntimeError("isolated visit Markdown failure")
            with (patch.object(utils, "XiuConfig", return_value=config),
                  patch.object(utils, "handle_send2", plain),
                  patch.object(utils, "handle_send_markdown", md)):
                asyncio.run(utils.send_msg_handler(bot, event, "拜访道友", "900000", [message]))
            delivered = md if markdown and not fallback else plain
            delivered.assert_awaited_once()
            assert reason_text(reason) in delivered.await_args.args[2]
            assert label in delivered.await_args.args[2]
