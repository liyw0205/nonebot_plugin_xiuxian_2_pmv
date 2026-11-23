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


class DungeonTeamTransferParameterDispatchTests(unittest.TestCase):
    def test_real_transfer_parameters_fault_recovery_and_reply_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", "dungeon-team-transfer-parameters",
                 "--report", str(report), "--base-sha", "9beff8400b621e517c020acee074b71f0f2fa9ae"],
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
            ("state_write_rejected", "队长转移状态保存失败"),
            ("state_projection_mismatch", "队长转移状态与请求不一致"),
            ("members_write_rejected", "队伍成员索引更新失败"),
            ("members_projection_mismatch", "队伍成员索引与队长状态不一致"),
            ("receipt_write_rejected", "队长转移回执保存失败"),
            ("receipt_projection_mismatch", "队长转移回执与请求不一致"),
        )
        config = SimpleNamespace(markdown_status=True, markdown_id="", message_optimization=False, img=False, at_sender=False)
        event = SimpleNamespace(message_id=974599, get_user_id=lambda: "900001")
        onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
        qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        for reason, phrase in reasons:
            with self.subTest(reason=reason):
                result = TeamMutationResult("failed", team_id="team", team_name="Trial", target_id="member",
                                            reason="dungeon_team_transfer_" + reason, subject="team")
                reply = _team_mutation_message("transfer", result)
                self.assertIn(phrase, reply)
                self.assertIn("队伍【Trial】", reply)
                self.assertNotIn("队长已成功转移", reply)
                for bot in (onebot, qq):
                    plain = AsyncMock()
                    with (patch.object(utils, "XiuConfig", return_value=config),
                          patch.object(utils, "is_group_event", return_value=True),
                          patch.object(utils, "_allow_native_markdown", return_value=False),
                          patch.object(utils, "_send_event_message", plain)):
                        asyncio.run(utils.handle_send(bot, event, reply))
                    plain.assert_awaited_once()
                    self.assertIn(phrase, str(plain.call_args.kwargs["message"]))
                    self.assertIn("队伍【Trial】", str(plain.call_args.kwargs["message"]))
                markdown = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", markdown)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                markdown.assert_awaited_once()
                self.assertIn(phrase, markdown.call_args.args[2])
                self.assertIn("队伍【Trial】", markdown.call_args.args[2])
                failed = AsyncMock(side_effect=RuntimeError("isolated transfer markdown failure"))
                fallback = AsyncMock()
                with (patch.object(utils, "XiuConfig", return_value=config),
                      patch.object(utils, "_allow_native_markdown", return_value=True),
                      patch.object(utils, "handle_send_markdown", failed),
                      patch.object(utils, "handle_send2", fallback)):
                    asyncio.run(utils.handle_send(qq, event, reply))
                fallback.assert_awaited_once_with(qq, event, reply)


if __name__ == "__main__":
    unittest.main()
