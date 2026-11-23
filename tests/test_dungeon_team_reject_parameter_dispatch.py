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


class DungeonTeamRejectParameterDispatchTests(unittest.TestCase):
    def test_real_reject_parameters_faults_recovery_and_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-team-reject-parameters",
                 "--report", str(report), "--base-sha", "c8419efd94a7e277f77678830fde298ce51c4ab8"],
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

        reasons = (
            ("state_write_rejected", "组队拒绝状态保存失败"),
            ("state_projection_mismatch", "组队拒绝状态与请求不一致"),
            ("receipt_write_rejected", "组队拒绝回执保存失败"),
            ("receipt_projection_mismatch", "组队拒绝回执与请求不一致"),
        )
        config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
        event = SimpleNamespace(message_id=974199, get_user_id=lambda: "900002")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        for reason, phrase in reasons:
            with self.subTest(reason=reason):
                result = TeamMutationResult("failed", invite_id="invite", target_id="900002",
                                            reason="dungeon_team_reject_" + reason, subject="invite")
                reply = _team_mutation_message("reject", result)
                self.assertIn(phrase, reply)
                self.assertIn("邀请【invite】", reply)
                self.assertNotIn("已拒绝组队邀请", reply)
                for bot in (onebot, qq):
                    plain = AsyncMock()
                    with (patch.object(utils, "XiuConfig", return_value=config),
                          patch.object(utils, "is_group_event", return_value=True),
                          patch.object(utils, "_allow_native_markdown", return_value=False),
                          patch.object(utils, "_send_event_message", plain)):
                        asyncio.run(utils.handle_send(bot, event, reply))
                    plain.assert_awaited_once()
                    self.assertIn(phrase, str(plain.call_args.kwargs["message"]))
                    self.assertIn("邀请【invite】", str(plain.call_args.kwargs["message"]))
                markdown = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", markdown)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                markdown.assert_awaited_once()
                self.assertIn(phrase, markdown.call_args.args[2])
                self.assertIn("邀请【invite】", markdown.call_args.args[2])
                failed = AsyncMock(side_effect=RuntimeError("isolated refusal markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
