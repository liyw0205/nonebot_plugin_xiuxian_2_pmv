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


class DungeonTeamCreateParameterDispatchTests(unittest.TestCase):
    def test_real_create_command_write_failures_and_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tests.parameter_probe",
                    "--case",
                    "dungeon-team-create-replay",
                    "--case",
                    "dungeon-team-create-write-recovery",
                    "--case",
                    "dungeon-team-create-parameters",
                    "--report",
                    str(report),
                    "--base-sha",
                    "81a3be76414b624649a0cdc9b2286faf41a4757e",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                timeout=240,
            )
            evidence = json.loads(report.read_text(encoding="utf-8"))
            details = json.dumps(evidence["results"], ensure_ascii=False, indent=2)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr + details)
            self.assertEqual({"pass": 3}, evidence["summary"], details)

    def test_failure_reasons_keep_team_subject_through_onebot_qq_and_fallback(self):
        reasons = (
            ("dungeon_team_create_state_write_rejected", "队伍主记录保存失败"),
            ("dungeon_team_create_state_projection_mismatch", "队伍主记录与创建请求不一致"),
            ("dungeon_team_create_members_write_rejected", "队伍成员索引保存失败"),
            ("dungeon_team_create_members_projection_mismatch", "队伍成员索引与创建请求不一致"),
            ("dungeon_team_create_receipt_write_rejected", "队伍创建回执保存失败"),
            ("dungeon_team_create_receipt_projection_mismatch", "队伍创建回执与请求不一致"),
        )
        for reason, phrase in reasons:
            with self.subTest(reason=reason):
                self.assert_failure_delivery(reason, phrase)

    def assert_failure_delivery(self, reason, phrase):
        import nonebot

        nonebot.init()
        from nonebot_plugin_xiuxian_2.features.dungeon.team_repository import TeamMutationResult
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon import _team_mutation_message
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        event = SimpleNamespace(message_id=970690, get_user_id=lambda: "900001")
        result = TeamMutationResult(
            "failed",
            team_id="900100_970010",
            team_name="红测队",
            reason=reason,
            subject="900100_970010",
        )
        reply = _team_mutation_message("create", result)
        self.assertIn(phrase, reply)
        self.assertIn("队伍【红测队】", reply)

        config = SimpleNamespace(
            markdown_status=True,
            markdown_id="",
            message_optimization=False,
            img=False,
            at_sender=False,
        )
        onebot_text = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "is_group_event", return_value=True),
            patch.object(utils, "_send_event_message", onebot_text),
        ):
            asyncio.run(
                utils.handle_send(
                    SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11")),
                    event,
                    reply,
                )
            )
        self.assertIn(phrase, str(onebot_text.call_args.kwargs["message"]))
        self.assertIn("队伍【红测队】", str(onebot_text.call_args.kwargs["message"]))

        markdown = AsyncMock()
        qq_bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.handle_send(qq_bot, event, reply))
        markdown.assert_awaited_once()
        self.assertIn(phrase, markdown.call_args.args[2])
        self.assertIn("队伍【红测队】", markdown.call_args.args[2])

        failed_markdown = AsyncMock(side_effect=RuntimeError("isolated team markdown failure"))
        fallback = AsyncMock()
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "_allow_native_markdown", return_value=True),
            patch.object(utils, "handle_send_markdown", failed_markdown),
            patch.object(utils, "handle_send2", fallback),
        ):
            asyncio.run(utils.handle_send(qq_bot, event, reply))
        fallback.assert_awaited_once_with(qq_bot, event, reply)


if __name__ == "__main__":
    unittest.main()
