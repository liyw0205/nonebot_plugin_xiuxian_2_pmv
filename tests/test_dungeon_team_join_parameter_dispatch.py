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


class DungeonTeamJoinParameterDispatchTests(unittest.TestCase):
    def test_real_join_parameters_faults_recovery_and_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-team-join-parameters",
                 "--report", str(report), "--base-sha", "c6e30654b95403acc3bb30a0e672ced200c1568d"],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=240,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 4}, evidence["summary"], details)

    def test_specific_reasons_survive_onebot_qq_plain_markdown_and_fallback(self):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import TeamMutationResult
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import _team_mutation_message
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        reasons = (
            ("state_write_rejected", "队伍入队状态保存失败", "team", "队伍【测试队】"),
            ("state_projection_mismatch", "队伍入队状态与请求不一致", "team", "队伍【测试队】"),
            ("members_write_rejected", "入队成员索引保存失败", "team", "队伍【测试队】"),
            ("members_projection_mismatch", "入队成员索引与请求不一致", "team", "队伍【测试队】"),
            ("cooldown_write_rejected", "入队首次加入标记保存失败", "900002", "入队玩家【诊断对手】"),
            ("cooldown_projection_mismatch", "入队首次加入记录与请求不一致", "900002", "入队玩家【诊断对手】"),
            ("invite_write_rejected", "入队邀请消费标记保存失败", "invite", "邀请【invite】"),
            ("invite_projection_mismatch", "入队邀请记录与请求不一致", "invite", "邀请【invite】"),
            ("receipt_write_rejected", "入队操作回执保存失败", "team", "队伍【测试队】"),
            ("receipt_projection_mismatch", "入队操作回执与请求不一致", "team", "队伍【测试队】"),
        )
        config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
        event = SimpleNamespace(message_id=973999, get_user_id=lambda: "900002")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        for reason, phrase, subject, label in reasons:
            with self.subTest(reason=reason):
                result = TeamMutationResult("failed", team_id="team", team_name="测试队", invite_id="invite", target_id="900002",
                                            reason="dungeon_team_join_" + reason, subject=subject)
                with patch.object(module, "_sql_message", return_value=SimpleNamespace(get_user_info_with_id=lambda _: {"user_name": "诊断对手"})):
                    reply = _team_mutation_message("join", result)
                self.assertIn(phrase, reply)
                self.assertIn(label, reply)
                self.assertNotIn("成功加入", reply)
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
                failed = AsyncMock(side_effect=RuntimeError("isolated join markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
