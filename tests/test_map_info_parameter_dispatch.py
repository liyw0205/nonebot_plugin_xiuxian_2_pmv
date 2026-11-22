from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_registered_map_info_queries_and_read_failure_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-info-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "tests.parameter_probe",
                "--minimal-map",
                "--group",
                "map-info",
                "--report",
                str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=150,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, (
            result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        )
        assert payload.get("summary") == {"pass": 4}, result.stdout
        assert payload["outcomes"] == {"success": 2, "rejection": 1, "recovery": 1}
        assert payload["results"][0]["evidence"]["realms"] == 3
        assert payload["results"][0]["evidence"]["heavens"] == 19


def test_map_info_known_and_unexpected_read_failures_reach_adapter():
    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    event = make_event("地图", "900001", 976101)
    realm = "隔离界"
    map_data = {realm: {"heavens": {"隔离天": []}}}
    args = SimpleNamespace(extract_plain_text=lambda: realm)
    handler = module.map_info.handlers[0].call
    config = SimpleNamespace(
        markdown_status=False,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        message_optimization=False,
        img=False,
        at_sender=False,
        reference_reply=False,
    )

    for bot, error, expected in (
        (
            onebot,
            DomainError(
                "schema_missing",
                "missing map state",
                {"reason": "map_status_schema_missing", "subject": "map_status"},
            ),
            reason_text("map_status_schema_missing"),
        ),
        (qq, sqlite3.DatabaseError("isolated map read failure"), "定位号"),
    ):
        delivered = AsyncMock()
        with (
            patch.object(module, "assign_bot", AsyncMock(return_value=(bot, "900000"))),
            patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
            patch.object(module, "_load_map_data", return_value=map_data),
            patch.object(module.map_application, "map_status", side_effect=error),
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_send_event_message", delivered),
        ):
            asyncio.run(handler(bot=bot, event=event, args=args))

        delivered.assert_awaited_once()
        message = str(delivered.await_args.kwargs["message"])
        assert "【地图 -" not in message
        assert expected in message
        if bot is onebot:
            assert "地图位置" in message


def test_map_info_realm_view_uses_onebot_plain_and_qq_markdown_fallback():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    data = {
        "隔离界": {
            "heavens": {
                "隔离天一": [{"id": "n1", "name": "节点甲", "type": "城镇"}],
                "隔离天二": [{"id": "n2", "name": "节点乙", "type": "秘境"}],
            }
        }
    }
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    event = make_event("地图", "900001", 976102)
    args = SimpleNamespace(extract_plain_text=lambda: "隔离界")
    handler = module.map_info.handlers[0].call
    config = SimpleNamespace(
        markdown_status=True,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        message_optimization=False,
        img=False,
        at_sender=False,
        reference_reply=False,
    )
    delivered = {}

    async def send_event_message(bot, event, message, **kwargs):
        delivered["plain"] = str(message)

    async def send_markdown(bot, event, message, **kwargs):
        delivered["markdown"] = message

    for mode, bot in (("plain", onebot), ("markdown", qq), ("fallback", qq)):
        delivered.clear()
        markdown = AsyncMock(side_effect=RuntimeError("isolated QQ Markdown failure")
                             if mode == "fallback" else None)
        with (
            patch.object(module, "assign_bot", AsyncMock(return_value=(bot, "900000"))),
            patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
            patch.object(module, "_load_map_data", return_value=data),
            patch.object(module.map_application, "map_status", return_value={
                "realm": "隔离界", "heaven": "隔离天一", "node_id": "n1", "visited_nodes": ["n1"]
            }),
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "_send_event_message", side_effect=send_event_message),
            patch.object(utils, "_send_markdown_or_keyboard", side_effect=send_markdown if mode == "markdown" else markdown),
        ):
            asyncio.run(handler(bot=bot, event=event, args=args))

        if mode == "markdown":
            assert "mqqapi://aio/inlinecmd" in delivered["markdown"]
            assert "隔离天一" in delivered["markdown"] and "隔离天二" in delivered["markdown"]
        else:
            assert "【地图 - 隔离界】" in delivered["plain"]
            assert "▫ 隔离天二" in delivered["plain"]
            assert "mqqapi://aio/inlinecmd" not in delivered["plain"]
