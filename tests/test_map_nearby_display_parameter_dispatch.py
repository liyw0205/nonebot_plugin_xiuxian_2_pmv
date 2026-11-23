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


def test_registered_nearby_display_parameters_and_sqlite_read_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-nearby-display-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--minimal-map",
                "--group",
                "map-nearby-display",
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
        assert payload.get("summary") == {"pass": 3}, result.stdout
        assert payload["outcomes"] == {"success": 1, "rejection": 1, "recovery": 1}


def test_nearby_display_read_failure_reason_reaches_onebot_qq_plain_markdown_and_fallback():
    import asyncio

    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("附近道友", "900001", 975799)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    cases = (
        ("map_nearby_position_read_failed", "map_status", "本人地图位置"),
        ("map_nearby_display_read_failed", "nearby_candidates", "附近道友列表"),
    )
    for reason, subject, label in cases:
        message = result_failure(
            "附近道友",
            {"reason": reason, "subject": subject},
            subjects={"map_status": "本人地图位置", "nearby_candidates": "附近道友列表"},
        )
        for bot, markdown_enabled, fail_markdown in (
            (onebot, False, False),
            (qq, False, False),
            (qq, True, False),
            (qq, True, True),
        ):
            plain, markdown = AsyncMock(), AsyncMock()
            if fail_markdown:
                markdown.side_effect = RuntimeError("isolated nearby display Markdown failure")
            config = SimpleNamespace(
                markdown_status=markdown_enabled,
                markdown_id="",
                markdown_id2="",
                markdown_button_status=False,
                message_optimization=False,
                img=False,
                at_sender=False,
                reference_reply=False,
            )
            with (
                patch.object(utils, "XiuConfig", return_value=config),
                patch.object(utils, "_allow_native_markdown", return_value=True),
                patch.object(utils, "handle_send2", plain),
                patch.object(utils, "handle_send_markdown", markdown),
            ):
                asyncio.run(utils.handle_send(bot, event, message))
            delivered = markdown if markdown_enabled and not fail_markdown else plain
            delivered.assert_awaited_once()
            rendered = str(delivered.await_args.args[2]).replace("\r", "")
            assert f"附近道友：{label}" in rendered, rendered
            assert reason_text(reason) in rendered, rendered
