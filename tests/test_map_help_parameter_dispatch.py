from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from tests.parameter_cases_map_help import USAGES


@pytest.fixture(scope="module")
def map_help_probe(tmp_path_factory):
    report = tmp_path_factory.mktemp("xiuxian-map-help") / "parameters.json"
    result = subprocess.run(
        [sys.executable, "-m", "tests.parameter_probe", "--group", "map-help", "--report", str(report)],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        timeout=180,
    )
    payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
    details = json.dumps(payload.get("results", []), ensure_ascii=False, indent=2)
    assert result.returncode == 0, result.stdout + result.stderr + details
    assert payload.get("summary") == {"pass": 1}, details
    return payload["results"][0]["evidence"]


def test_real_map_help_registered_usage_and_read_only_contract(map_help_probe):
    assert "地图帮助" in map_help_probe["catalog"]
    assert map_help_probe["business_databases_unchanged"]
    assert map_help_probe["message_delta"] == {"messages": 6, "user_nicknames": 1}


def test_map_help_survives_qq_markdown_and_plain_fallback(map_help_probe):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    event = SimpleNamespace(message_id=974610, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    options = {"k1": "地图", "v1": "地图", "k2": "位置", "v2": "我的位置", "k3": "洞府", "v3": "洞府帮助"}

    for mode, bot in (("plain", onebot), ("markdown", qq), ("fallback", qq)):
        config = SimpleNamespace(
            markdown_status=mode != "plain",
            markdown_id="",
            markdown_id2="",
            markdown_button_status=False,
            message_optimization=False,
            img=False,
            at_sender=False,
        )
        plain, markdown, fallback = AsyncMock(), AsyncMock(), AsyncMock()
        if mode == "fallback":
            markdown.side_effect = RuntimeError("isolated map help Markdown failure")
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "is_group_event", return_value=True),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "_send_event_message", plain),
            patch.object(utils, "_send_markdown_or_keyboard", markdown),
            patch.object(utils, "handle_send2", fallback) if mode == "fallback" else nullcontext(),
        ):
            asyncio.run(utils.send_help_message(bot, event, map_help_probe["catalog"], **options))

        if mode == "plain":
            plain.assert_awaited_once()
            markdown.assert_not_awaited()
            fallback.assert_not_awaited()
            delivered = str(plain.await_args.kwargs["message"])
        elif mode == "markdown":
            markdown.assert_awaited_once()
            plain.assert_not_awaited()
            fallback.assert_not_awaited()
            delivered = markdown.await_args.args[2]
            assert "mqqapi://aio/inlinecmd" in delivered
        else:
            markdown.assert_awaited_once()
            fallback.assert_awaited_once()
            plain.assert_not_awaited()
            delivered = fallback.await_args.args[2]

        if mode in ("markdown", "fallback"):
            from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import strip_md_command_links

            delivered = strip_md_command_links(delivered)
        comparable = delivered.replace("【", "[").replace("】", "]")
        assert all(form in comparable for form in USAGES), delivered
        assert all(name in delivered for name in ("收杆", "落镐", "采收", "洞府帮助")), delivered
