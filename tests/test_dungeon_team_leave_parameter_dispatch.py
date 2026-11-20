from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class DungeonTeamLeaveParameterDispatchTests(unittest.TestCase):
    def test_real_leave_parameters_fault_recovery_and_reply_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-team-leave-parameters",
                 "--report", str(report), "--base-sha", "fbd3725469c4c2fc0d4a79ea87295cc03cb4649f"],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=240,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 3}, evidence["summary"], details)

    def test_specific_reasons_survive_onebot_qq_plain_markdown_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import TeamExitResult
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import _team_exit_message
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        reasons = (
            ("state_write_rejected", "离队状态保存失败", "team", "队伍【Trial】"),
            ("state_projection_mismatch", "离队状态与请求不一致", "team", "队伍【Trial】"),
            ("members_write_rejected", "离队成员索引保存失败", "team", "队伍【Trial】"),
            ("members_projection_mismatch", "离队成员索引与请求不一致", "team", "队伍【Trial】"),
            ("cooldown_write_rejected", "离队冷却保存失败", "member", "离队玩家"),
            ("cooldown_projection_mismatch", "离队冷却与原记录不一致", "member", "离队玩家"),
            ("invite_write_rejected", "离队关联邀请保存失败", "pending", "邀请【pending】"),
            ("invite_projection_mismatch", "离队关联邀请与原记录不一致", "pending", "邀请【pending】"),
            ("receipt_write_rejected", "离队操作回执保存失败", "team", "队伍【Trial】"),
            ("receipt_projection_mismatch", "离队操作回执与请求不一致", "team", "队伍【Trial】"),
        )
        config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
        event = SimpleNamespace(message_id=975599, get_user_id=lambda: "900002")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        for reason, phrase, subject, label in reasons:
            with self.subTest(reason=reason):
                result = TeamExitResult("failed", team_id="team", team_name="Trial", target_id="member",
                                        reason="dungeon_team_leave_" + reason, subject=subject)
                reply = _team_exit_message("leave", result)
                self.assertIn(phrase, reply)
                self.assertIn(label, reply)
                self.assertNotIn("你已离开队伍", reply)
                for bot in (onebot, qq):
                    plain = AsyncMock()
                    with (patch.object(utils, "XiuConfig", return_value=config),
                          patch.object(utils, "is_group_event", return_value=True),
                          patch.object(utils, "_allow_native_markdown", return_value=False),
                          patch.object(utils, "_send_event_message", plain)):
                        asyncio.run(utils.handle_send(bot, event, reply))
                    plain.assert_awaited_once()
                    self.assertIn(phrase, str(plain.call_args.kwargs["message"]))
                    self.assertIn(label, str(plain.call_args.kwargs["message"]))
                markdown = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", markdown)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                markdown.assert_awaited_once()
                self.assertIn(phrase, markdown.call_args.args[2])
                self.assertIn(label, markdown.call_args.args[2])
                failed = AsyncMock(side_effect=RuntimeError("isolated departure markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
