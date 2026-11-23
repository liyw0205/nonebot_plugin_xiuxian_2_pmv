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


def test_registered_explore_settlement_parameters_and_sqlite_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-explore-settlement-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable, "-m", "tests.parameter_probe", "--minimal-map",
                "--group", "map-explore-settle", "--report", str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=360,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        assert payload.get("summary") == {"pass": 16}, result.stdout
        assert payload["outcomes"] == {"success": 1, "rejection": 1, "recovery": 14}


def test_explore_settlement_reason_survives_onebot_and_qq_formatters():
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    reason = "map_explore_settlement_receipt_write_rejected"
    message = result_failure(
        "探索结算",
        {"reason": reason, "subject": "receipt"},
        subjects={"receipt": "探索结算回执"},
    )
    event = make_event("探索结算", "900001", 974299)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))

    for bot, use_markdown, fallback in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated exploration settlement Markdown failure")
        config = SimpleNamespace(
            markdown_status=use_markdown, merge_forward_send=4 if use_markdown else 1,
            markdown_id="", markdown_id2="", markdown_button_status=False,
            reference_reply=False, message_optimization=False, img=False, at_sender=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            import asyncio

            asyncio.run(utils.send_msg_handler(bot, event, "探索结算", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        rendered = str(delivered.await_args.args[2]).replace("\r", "")
        assert "探索结算回执" in rendered and reason_text(reason) in rendered, rendered
