from __future__ import annotations

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


def test_real_chongqiao_matcher_writes_recover_and_respect_boundaries():
    with tempfile.TemporaryDirectory(prefix="xiuxian-tianti-qiaoxue-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--group",
                "tianti-qiaoxue-parameters",
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
        assert payload.get("summary") == {"pass": 7}, details
        assert payload.get("outcomes") == {"recovery": 6, "success": 1}, details


def test_chongqiao_failure_reasons_survive_onebot_qq_markdown_and_fallback():
    rejection_specs = (
        ("tianti_qiaoxue_profile_write_rejected", "player_profile"),
        ("tianti_qiaoxue_profile_projection_mismatch", "player_profile"),
        ("tianti_qiaoxue_receipt_write_rejected", "receipt"),
        ("tianti_qiaoxue_receipt_projection_mismatch", "receipt"),
    )
    content = [
        result_failure(
            "冲窍", {"status": "state_changed", "reason": code, "subject": subject}
        )
        for code, subject in rejection_specs
    ]
    event = make_event("冲窍", "900001", 979_999)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    plain_config = SimpleNamespace(
        markdown_status=False,
        merge_forward_send=1,
        reference_reply=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    plain = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=plain_config),
        patch.object(utils, "handle_send2", plain),
    ):
        import asyncio

        asyncio.run(utils.send_msg_handler(onebot, event, "冲窍", "900000", content))
    plain.assert_awaited_once()
    plain_text = plain.await_args.args[2]
    assert all(reason_text(code) in plain_text for code, _ in rejection_specs)

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
    markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=markdown_config),
        patch.object(utils, "handle_send_markdown", markdown),
    ):
        asyncio.run(utils.send_msg_handler(qq, event, "冲窍", "900000", content))
    markdown.assert_awaited_once()
    markdown_text = markdown.await_args.args[2]
    assert all(reason_text(code) in markdown_text for code, _ in rejection_specs)

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
        asyncio.run(utils.send_msg_handler(qq, event, "冲窍", "900000", content))
    fallback.assert_awaited_once()
    fallback_text = fallback.await_args.args[2]
    assert all(reason_text(code) in fallback_text for code, _ in rejection_specs)
