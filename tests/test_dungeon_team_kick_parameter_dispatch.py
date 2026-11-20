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


class DungeonTeamKickParameterDispatchTests(unittest.TestCase):
    def test_real_kick_parameters_fault_recovery_and_reply_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-team-kick-parameters",
                 "--report", str(report), "--base-sha", "672504fff0b4a2da8aa73ea23be8ebe1f123bca0"],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=240,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 4}, evidence["summary"], details)

    def test_specific_reasons_survive_onebot_qq_plain_markdown_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import TeamExitResult
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import _team_exit_message
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        reasons = (
            ("state_write_rejected", "移除队员状态保存失败", "team", "队伍【Trial】"),
            ("state_projection_mismatch", "移除队员状态与请求不一致", "team", "队伍【Trial】"),
            ("members_write_rejected", "移除队员索引保存失败", "team", "队伍【Trial】"),
            ("members_projection_mismatch", "移除队员索引与请求不一致", "team", "队伍【Trial】"),
            ("cooldown_write_rejected", "被移除队员冷却保存失败", "member", "被移除队员"),
            ("cooldown_projection_mismatch", "被移除队员冷却与原记录不一致", "member", "被移除队员"),
            ("invite_write_rejected", "移除队员关联邀请保存失败", "pending", "邀请【pending】"),
            ("invite_projection_mismatch", "移除队员关联邀请与原记录不一致", "pending", "邀请【pending】"),
            ("receipt_write_rejected", "移除队员操作回执保存失败", "team", "队伍【Trial】"),
            ("receipt_projection_mismatch", "移除队员操作回执与请求不一致", "team", "队伍【Trial】"),
        )
        config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
        event = SimpleNamespace(message_id=976599, get_user_id=lambda: "900001")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        for reason, phrase, subject, label in reasons:
            with self.subTest(reason=reason):
                result = TeamExitResult("failed", team_id="team", team_name="Trial", target_id="member",
                                        reason="dungeon_team_kick_" + reason, subject=subject)
                reply = _team_exit_message("kick", result)
                self.assertIn(phrase, reply)
                self.assertIn(label, reply)
                self.assertNotIn("已将成员", reply)
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
                failed = AsyncMock(side_effect=RuntimeError("isolated removal markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
