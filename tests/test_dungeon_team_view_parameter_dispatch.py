from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


class DungeonTeamViewParameterDispatchTests(unittest.TestCase):
    def test_real_team_and_invitation_queries_are_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-team-view-parameters",
                 "--report", str(report), "--base-sha", "bf7dde564c82d4936e50fab9baeff0b17eb3affa"],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=180,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 2}, evidence["summary"], details)

    def test_handler_team_and_invitation_content_survives_adapter_routes(self):
        import nonebot

        nonebot.init()
        from nonebot.exception import FinishedException
        from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import TeamInviteSnapshot
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        team = {"team_id": "team", "team_name": "Query Trial", "leader": "900001",
                "members": ["900001", "900002"], "create_time": "2026-11-20 12:00:00", "max_members": 4}
        users = {"900001": {"user_name": "Leader"}, "900002": {"user_name": "Member"}}
        event = SimpleNamespace(message_id=978099, get_user_id=lambda: "900002")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        config = SimpleNamespace(markdown_status=True, markdown_id="", markdown_id2="",
                                 markdown_button_status=False, message_optimization=False, img=False, at_sender=False)
        invite = TeamInviteSnapshot("invite", "team", "900001", "900002", "900100", 100, 160)
        for state in ("member", "invited", "missing"):
            with self.subTest(state=state):
                app = SimpleNamespace(
                    team_id_for_user=Mock(return_value="team" if state == "member" else None),
                    team_info=Mock(return_value=team),
                    pending_invite=Mock(return_value=invite if state == "invited" else None),
                )
                sent = AsyncMock()
                with (patch.object(module, "dungeon_team_application", app),
                      patch.object(module, "assign_bot", AsyncMock(return_value=(qq, "900100"))),
                      patch.object(module, "check_user", return_value=(True, {"user_id": "900002"}, "")),
                      patch.object(module, "_sql_message", return_value=SimpleNamespace(get_user_info_with_id=users.get)),
                      patch.object(module.time, "time", return_value=100),
                      patch.object(module, "handle_send", sent)):
                    with self.assertRaises(FinishedException):
                        asyncio.run(module.view_team_handler(qq, event))
                sent.assert_awaited_once()
                reply, options = sent.await_args.args[2], sent.await_args.kwargs
                phrases = {
                    "member": ("Query Trial", "成员：2/4", "👑 Leader", "👤 Member"),
                    "invited": ("待处理组队邀请", "Query Trial", "邀请人：Leader", "发起群：900100", "同意组队", "拒绝组队"),
                    "missing": ("你不在任何队伍中", "创建队伍"),
                }[state]
                for phrase in phrases:
                    self.assertIn(phrase, reply)
                for bot in (onebot, qq):
                    plain = AsyncMock()
                    with (patch.object(utils, "XiuConfig", return_value=config),
                          patch.object(utils, "is_group_event", return_value=True),
                          patch.object(utils, "_allow_native_markdown", return_value=False),
                          patch.object(utils, "_send_event_message", plain)):
                        asyncio.run(utils.handle_send(bot, event, reply, **options))
                    plain.assert_awaited_once()
                    for phrase in phrases:
                        self.assertIn(phrase, str(plain.await_args.kwargs["message"]))
                markdown = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown_type", markdown)):
                    asyncio.run(utils.handle_send(qq, event, reply, **options))
                markdown.assert_awaited_once()
                self.assertEqual(reply, markdown.await_args.args[2])
                self.assertEqual("team", markdown.await_args.args[3])
                failed = AsyncMock(side_effect=RuntimeError("isolated team query markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown_type", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply, **options))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
