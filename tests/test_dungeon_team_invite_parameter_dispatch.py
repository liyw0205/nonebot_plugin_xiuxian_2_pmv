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


class DungeonTeamInviteParameterDispatchTests(unittest.TestCase):
    def test_real_invitation_parameters_faults_recovery_and_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-team-invite-parameters",
                 "--report", str(report), "--base-sha", "c11ac6d7982d70d7b5413927b407211531653d55"],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=240,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 3}, evidence["summary"], details)

    def test_specific_failure_reasons_survive_onebot_qq_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import TeamMutationResult
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import _team_mutation_message
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        reasons = (
            ("expiry_write_rejected", "旧组队邀请过期标记保存失败"),
            ("expiry_projection_mismatch", "旧组队邀请过期记录与预期不一致"),
            ("state_write_rejected", "组队邀请记录保存失败"),
            ("state_projection_mismatch", "组队邀请记录与请求不一致"),
            ("receipt_write_rejected", "组队邀请回执保存失败"),
            ("receipt_projection_mismatch", "组队邀请回执与请求不一致"),
        )
        config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
        event = SimpleNamespace(message_id=971099, get_user_id=lambda: "900001")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        for reason, phrase in reasons:
            with self.subTest(reason=reason):
                expiry = reason.startswith("expiry_")
                result = TeamMutationResult("failed", team_id="team", invite_id="new-invite", target_id="900002",
                                            reason="dungeon_team_invite_" + reason, subject="old-invite" if expiry else "900002")
                with patch.object(module, "_sql_message", return_value=SimpleNamespace(get_user_info_with_id=lambda _: {"user_name": "诊断对手"})):
                    reply = _team_mutation_message("invite", result)
                subject = "旧邀请【old-invite】" if expiry else "被邀请者【诊断对手】"
                self.assertIn(phrase, reply)
                self.assertIn(subject, reply)
                self.assertNotIn("发送组队邀请", reply)

                for bot in (onebot, qq):
                    plain = AsyncMock()
                    with (patch.object(utils, "XiuConfig", return_value=config),
                          patch.object(utils, "is_group_event", return_value=True),
                          patch.object(utils, "_allow_native_markdown", return_value=False),
                          patch.object(utils, "_send_event_message", plain)):
                        asyncio.run(utils.handle_send(bot, event, reply))
                    plain.assert_awaited_once()
                    self.assertIn(phrase, str(plain.call_args.kwargs["message"]))
                    self.assertIn(subject, str(plain.call_args.kwargs["message"]))

                markdown = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", markdown)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                markdown.assert_awaited_once()
                self.assertIn(phrase, markdown.call_args.args[2])
                self.assertIn(subject, markdown.call_args.args[2])

                failed = AsyncMock(side_effect=RuntimeError("isolated invitation markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
