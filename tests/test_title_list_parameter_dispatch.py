from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.parametrize("case_key", ["title-list-pages-readonly", "title-list-real-unlock"])
def test_real_title_list(case_key):
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "parameters.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--case", case_key, "--report", str(report),
             "--base-sha", "151eb188c6b5be7470f1665f4c87f70525c0611e"],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=120,
        )
        evidence = json.loads(report.read_text(encoding="utf-8"))
        details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
        assert result.returncode == 0, result.stdout + result.stderr + details
        assert evidence["summary"] == {"pass": 1}, details


@pytest.mark.parametrize("state", ["empty", "page-two", "new-unlock"])
def test_title_list_handler_and_buttons_survive_adapter_routes(state):
    import nonebot

    nonebot.init()
    from nonebot.adapters.onebot.v11 import Message
    from nonebot.exception import FinishedException
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    definitions = module.get_all_titles()
    title_ids = list(definitions)[:12] if state == "page-two" else ["30001"] if state == "new-unlock" else []
    newly_unlocked = [{**definitions["30001"], "id": "30001"}] if state == "new-unlock" else []
    event = SimpleNamespace(message_id=999299, get_user_id=lambda: "900001")
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    sent = AsyncMock()
    with (patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
          patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
          patch.object(module, "_unlock_titles_from_event", return_value=(newly_unlocked, title_ids, None)),
          patch.object(module, "get_user_equipped_title", return_value=title_ids[-1] if state == "page-two" else None),
          patch.object(module, "handle_send", sent)):
        with pytest.raises(FinishedException):
            asyncio.run(module.title_list_(qq, event, Message("2" if state == "page-two" else "1")))
    sent.assert_awaited_once()
    reply, options = sent.await_args.args[2], sent.await_args.kwargs
    if state == "empty":
        assert "你还没有解锁任何称号" in reply
        assert options == {"md_type": "修仙", "k1": "帮助", "v1": "称号帮助", "k2": "存档", "v2": "我的修仙信息",
                           "k3": "帮助", "v3": "修仙帮助"}
    else:
        assert options == {"md_type": "修仙", "k1": "装备", "v1": "装备称号", "k2": "帮助", "v2": "称号帮助",
                           "k3": "存档", "v3": "我的修仙信息"}
        if state == "page-two":
            assert "共12个（第2/2页）" in reply and "⭐已装备" in reply
            shown = title_ids[10:]
        else:
            assert "恭喜解锁新称号" in reply and "共1个（第1/1页）" in reply
            shown = title_ids
        for title_id in shown:
            assert definitions[title_id]["name"] in reply and definitions[title_id]["desc"] in reply
    config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="", markdown_button_status=False,
                             message_optimization=False, img=False, at_sender=False)
    for bot in (onebot, qq):
        plain = AsyncMock()
        with (patch.object(utils, "XiuConfig", return_value=config),
              patch.object(utils, "is_group_event", return_value=True),
              patch.object(utils, "_allow_native_markdown", return_value=False), patch.object(utils, "_send_event_message", plain)):
            asyncio.run(utils.handle_send(bot, event, reply, **options))
        plain.assert_awaited_once()
        assert reply in str(plain.await_args.kwargs["message"]).replace("\r", "\n")
    markdown = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True), patch.object(utils, "handle_send_markdown_type", markdown)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    markdown.assert_awaited_once()
    assert markdown.await_args.args[2:10] == (reply, "修仙", options["k1"], options["v1"], options["k2"], options["v2"], options["k3"], options["v3"])
    fallback = AsyncMock()
    with (patch.object(utils, "XiuConfig", return_value=config),
          patch.object(utils, "_allow_native_markdown", return_value=True),
          patch.object(utils, "handle_send_markdown_type", AsyncMock(side_effect=RuntimeError("isolated title list markdown failure"))),
          patch.object(utils, "handle_send2", fallback)):
        asyncio.run(utils.handle_send(qq, event, reply, **options))
    fallback.assert_awaited_once_with(qq, event, reply)
