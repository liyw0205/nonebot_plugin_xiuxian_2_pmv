from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
from tests.command_probe import make_event


def test_real_settlement_matcher_windows_bonus_write_recovery_and_replay():
    with tempfile.TemporaryDirectory(prefix="xiuxian-tianti-settlement-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "tianti-settlement-parameters",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=240,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        details = json.dumps(payload.get("results", []), ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert payload.get("summary") == {"pass": 9}, details
        assert payload.get("outcomes") == {"recovery": 7, "success": 2}, details


def test_settlement_reasons_survive_onebot_qq_markdown_and_fallback():
    rejection_specs = (
        ("tianti_settlement_profile_write_rejected", "player_profile"),
        ("tianti_settlement_profile_projection_mismatch", "player_profile"),
        ("tianti_settlement_receipt_write_rejected", "receipt"),
        ("tianti_settlement_receipt_projection_mismatch", "receipt"),
    )
    content = [
        (
            code,
            subject,
            result_failure(
                "炼体结算", {"status": "state_changed", "reason": code, "subject": subject}
            ),
        )
        for code, subject in rejection_specs
    ]
    event = make_event("炼体结算", "900001", 979_899)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    plain_config = SimpleNamespace(
        markdown_status=False,
        merge_forward_send=1,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    markdown_config = SimpleNamespace(
        markdown_status=True,
        merge_forward_send=4,
        markdown_id="",
        markdown_button_status=False,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )

    for code, subject, message in content:
        expected_subject = "玩家档案" if subject == "player_profile" else "操作回执"
        plain = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=plain_config),
            patch.object(utils, "handle_send2", plain),
        ):
            asyncio.run(utils.send_msg_handler(onebot, event, "炼体结算", "900000", [message]))
        plain.assert_awaited_once()
        plain_text = plain.await_args.args[2]
        assert reason_text(code) in plain_text and expected_subject in plain_text, plain_text

        markdown = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=markdown_config),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(qq, event, "炼体结算", "900000", [message]))
        markdown.assert_awaited_once()
        markdown_text = markdown.await_args.args[2]
        assert reason_text(code) in markdown_text and expected_subject in markdown_text, markdown_text

        fallback = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=markdown_config),
            patch.object(
                utils,
                "handle_send_markdown",
                AsyncMock(side_effect=RuntimeError("isolated QQ Markdown failure")),
            ),
            patch.object(utils, "handle_send2", fallback),
        ):
            asyncio.run(utils.send_msg_handler(qq, event, "炼体结算", "900000", [message]))
        fallback.assert_awaited_once()
        fallback_text = fallback.await_args.args[2]
        assert reason_text(code) in fallback_text and expected_subject in fallback_text, fallback_text
