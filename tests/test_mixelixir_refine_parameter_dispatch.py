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


def test_registered_mixelixir_refine_parameters_and_same_message_replay():
    with tempfile.TemporaryDirectory(prefix="xiuxian-mixelixir-refine-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "mixelixir-refine",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=180,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, (
            result.stdout + result.stderr
            + json.dumps(failures, ensure_ascii=False, indent=2)
        )
        assert payload.get("summary", {}).get("exception", 0) == 0, result.stdout
        assert payload.get("summary", {}).get("pass") == len(payload.get("results", [])), result.stdout
        assert payload["outcomes"].get("success", 0) >= 1
        assert payload["outcomes"].get("recovery", 0) >= 1


def test_refine_failure_reasons_survive_onebot_qq_markdown_and_fallback():
    import asyncio

    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("配方", "900001", 970601)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    plain_config = SimpleNamespace(
        markdown_status=False,
        merge_forward_send=1,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
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
    failures = (
        ("mixelixir_refine_cost_material_write_rejected", "item:3001", "3001"),
        ("mixelixir_refine_cost_receipt_write_rejected", "receipt", "操作回执"),
        ("mixelixir_refine_reward_player_write_rejected", "player_mixelixir", "玩家炼丹记录"),
        ("mixelixir_refine_reward_receipt_projection_mismatch", "receipt", "操作回执"),
    )
    for reason, subject, label in failures:
        message = result_failure(
            "炼丹",
            SimpleNamespace(status="write_rejected", reason=reason, subject=subject),
        )
        assert reason_text(reason) in message and label in message, message
        for bot, config, use_markdown, fail_markdown in (
            (onebot, plain_config, False, False),
            (qq, plain_config, False, False),
            (qq, markdown_config, True, False),
            (qq, markdown_config, True, True),
        ):
            plain, markdown = AsyncMock(), AsyncMock()
            if fail_markdown:
                markdown.side_effect = RuntimeError("isolated QQ Markdown delivery failure")
            with (
                patch.object(utils, "XiuConfig", return_value=config),
                patch.object(utils, "handle_send2", plain),
                patch.object(utils, "handle_send_markdown", markdown),
            ):
                asyncio.run(utils.send_msg_handler(bot, event, "炼丹", "bot", [message]))
            delivered = markdown if use_markdown and not fail_markdown else plain
            delivered.assert_awaited_once()
            text = str(delivered.await_args.args[2])
            assert reason_text(reason) in text and label in text, text
