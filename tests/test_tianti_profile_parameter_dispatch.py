from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.command_probe import make_event
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def test_my_tianti_status_uses_real_matcher_and_reports_profile_read_failure():
    with tempfile.TemporaryDirectory(prefix="xiuxian-tianti-profile-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--case",
                "tianti-profile-read",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=180,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        details = json.dumps(payload.get("results", []), ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert payload.get("summary") == {"pass": 1}, details
        evidence = payload["results"][0]["evidence"]
        assert evidence["active_profile"]["bath_remaining_minutes"] == 60
        assert evidence["cold_profile_not_created"] and evidence["full_database_zero_writes"]
        assert "定位号：" in evidence["sqlite_authorizer_read_failure_reply"]


def test_my_tianti_status_message_survives_onebot_qq_markdown_and_fallback():
    event = make_event("我的炼体", "900001", 979_807)
    content = [
        "【我的炼体】",
        "境界：初境",
        "炼体气血：1.23万",
        "药浴：有效药浴，结算效果125%（有效至13:00，剩余 60 分钟）",
    ]
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
        asyncio.run(
            utils.send_msg_handler(onebot, event, "我的炼体", "900000", content)
        )
    plain.assert_awaited_once()
    plain_text = plain.await_args.args[2]
    assert all(value in plain_text for value in (content[0], content[1], content[2], content[3]))

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
        asyncio.run(
            utils.send_msg_handler(qq, event, "我的炼体", "900000", content)
        )
    markdown.assert_awaited_once()
    markdown_text = markdown.await_args.args[2]
    assert all(value in markdown_text for value in (content[0], content[1], content[2], content[3]))

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
        asyncio.run(
            utils.send_msg_handler(qq, event, "我的炼体", "900000", content)
        )
    fallback.assert_awaited_once()
    fallback_text = fallback.await_args.args[2]
    assert all(value in fallback_text for value in (content[0], content[1], content[2], content[3]))
