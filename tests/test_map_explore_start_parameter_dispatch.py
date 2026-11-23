from __future__ import annotations

import json
import asyncio
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_registered_explore_start_parameters_and_sqlite_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-explore-start-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--minimal-map", "--group",
             "map-explore-start", "--report", str(report)],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=360,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        assert payload.get("summary") == {"pass": 13}, result.stdout
        assert payload["outcomes"] == {"success": 1, "rejection": 1, "recovery": 11}


def test_explore_start_known_write_reason_survives_onebot_and_qq_formatters():
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    from nonebot_plugin_xiuxian_2.paths import get_paths

    module.MAP_FILE = get_paths().data / "地图.json"
    module.map_data_provider.path = module.MAP_FILE
    data = module._load_map_data()
    node = next(
        {**candidate, "realm": realm, "heaven": heaven}
        for realm in module._all_realms(data)
        for heaven in module._heaven_names(data, realm)
        for candidate in module._nodes(data, realm, heaven)
        if candidate["type"] in module.EXPLORE_NODE_TYPES
    )
    event = make_event("开始探索", "900001", 974199)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    result = SimpleNamespace(
        ok=False,
        code="state_changed",
        data={
            "status": "state_changed",
            "reason": "map_explore_start_receipt_write_rejected",
            "subject": "receipt",
        },
    )
    capture = AsyncMock()
    handler = module.start_explore_cmd.handlers[0].call

    async def invoke(bot):
        with (
            patch.object(module, "assign_bot", AsyncMock(return_value=(bot, "900000"))),
            patch.object(module, "check_user", return_value=(True, {"user_id": "900001", "user_stamina": 100}, "")),
            patch.object(module, "_check_daily_cap", return_value=(True, 0, 3)),
            patch.object(module, "_get_cd", return_value=None),
            patch.object(module, "get_player_current_node", return_value=node),
            patch.object(module, "get_player_current_position", return_value={"realm": node["realm"], "heaven": node["heaven"], "node_id": node["id"]}),
            patch.object(module, "_get_explore_status", return_value={"running": 0}),
            patch.object(module, "_get_daily_limit", return_value={"date": "2026-11-22", "explore_count": 0, "resource_total_count": 0}),
            patch.object(module.map_application, "cooldown_until", return_value=None),
            patch.object(module.map_application, "explore_start", return_value=result),
            patch.object(module, "handle_send", capture),
            patch.object(utils, "handle_send", capture),
        ):
            await handler(bot=bot, event=event, args=SimpleNamespace(extract_plain_text=lambda: "20"))

    asyncio.run(invoke(onebot))
    message = capture.await_args.args[2]
    assert "探索永久回执" in message and reason_text("map_explore_start_receipt_write_rejected") in message

    for bot, use_markdown, fallback in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated exploration Markdown failure")
        config = SimpleNamespace(
            markdown_status=use_markdown, merge_forward_send=4 if use_markdown else 1,
            markdown_id="", markdown_id2="", markdown_button_status=False,
            reference_reply=False, message_optimization=False, img=False,
            at_sender=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(bot, event, "开始探索", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        rendered = re.sub(r"\r", "", str(delivered.await_args.args[2]))
        assert "探索永久回执" in rendered
        assert reason_text("map_explore_start_receipt_write_rejected") in rendered
