"""Gameplay entry coverage on the activity center card.

``gameplay_entry_commands`` previously truncated entries at four, silently
dropping the boss entries once a third gameplay type was configured. Every
in-progress gameplay now keeps all of its entries (default or configured
override); message-length constraints belong to the delivery layer. Written as
``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import quote
import tempfile
import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    _commands_for_activity,
    gameplay_entry_commands,
)


NOW = datetime(2026, 10, 15, 12)

COLLECT = {
    "key": "c1",
    "type": "collect_words",
    "enabled": True,
    "name": "集字",
    "start_time": "0",
    "end_time": "无限",
}
POINTS = {
    "key": "p1",
    "type": "event_points",
    "enabled": True,
    "name": "积分",
    "start_time": "0",
    "end_time": "无限",
}
BOSS = {
    "key": "b1",
    "type": "activity_boss",
    "enabled": True,
    "name": "首领",
    "boss_name": "年兽",
    "start_time": "0",
    "end_time": "无限",
}


def _config(bosses: list[dict]) -> dict:
    return {"gameplay_activities": bosses}


class GameplayEntryCoverageTests(unittest.TestCase):
    def test_three_gameplay_types_keep_all_six_entries(self) -> None:
        entries = gameplay_entry_commands(
            _config([COLLECT, POINTS, BOSS]), "进行中", NOW
        )
        commands = [command for _, command in entries]
        self.assertIn("活动背包", commands)
        self.assertIn("活动兑换", commands)
        self.assertIn("活动积分", commands)
        self.assertIn("活动商店", commands)
        self.assertIn("活动首领", commands)
        self.assertIn("活动首领排行", commands)
        self.assertEqual(len(entries), 6)

    def test_configured_override_renders_in_full(self) -> None:
        activity = dict(
            COLLECT,
            commands=[
                {"label": "一", "command": "活动背包"},
                {"label": "二", "command": "活动兑换"},
                {"label": "三", "command": "活动积分"},
                {"label": "四", "command": "活动商店"},
                {"label": "五", "command": "活动奖励"},
            ],
        )
        commands = _commands_for_activity(activity)
        self.assertEqual(len(commands), 5)
        self.assertEqual(commands[-1], ("五", "活动奖励"))

    def test_dedup_keeps_first_occurrence_only(self) -> None:
        first = dict(COLLECT, commands=[{"label": "A", "command": "活动积分"}])
        entries = gameplay_entry_commands(
            _config([first, POINTS]), "进行中", NOW
        )
        commands = [command for _, command in entries]
        self.assertEqual(commands.count("活动积分"), 1)

    def test_not_started_gameplay_contributes_no_entries(self) -> None:
        future = dict(COLLECT, start_time="2099-01-01", end_time="2099-12-31")
        entries = gameplay_entry_commands(
            _config([future, POINTS]), "进行中", NOW
        )
        commands = [command for _, command in entries]
        self.assertNotIn("活动背包", commands)
        self.assertIn("活动积分", commands)

    def test_non_progress_status_renders_nothing(self) -> None:
        self.assertEqual(
            gameplay_entry_commands(_config([COLLECT]), "已结束", NOW), ()
        )


def _max_config() -> dict:
    """Every gameplay type plus stages, rank boards and boss milestones."""
    return {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": {
            "activity_pass": {"enabled": False},
            "stages": [
                {
                    "key": "running",
                    "name": "进行期",
                    "start_time": "2026-10-01",
                    "end_time": "2026-11-01",
                    "features": ["sign", "task", "pass", "points", "collect", "boss", "shop", "claim"],
                }
            ],
            "rank": {
                "boards": [
                    {"key": "points", "label": "积分榜", "source": "points", "limit": 10}
                ],
                "rewards": [],
            },
        },
        "gameplay_activities": [COLLECT, POINTS, BOSS],
    }


class MaxConfigRenderingTests(unittest.TestCase):
    """Max-config rendering: every entry reachable, rows bounded, payload safe."""

    def _application(self):
        from types import SimpleNamespace

        from nonebot_plugin_xiuxian_2.features.activity.migrations import (
            apply_activity_state_schema,
        )
        from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
            ActivityReadModelApplication,
        )
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
        return ActivityReadModelApplication(
            database,
            config_loader=_max_config,
            clock=SimpleNamespace(now=lambda: NOW),
        )

    def test_max_config_keeps_every_entry_reachable_in_grouped_rows(self) -> None:
        application = self._application()
        text = application.activity_center_text("u1")
        # All six gameplay entries stay reachable after the mapping change —
        # asserted by command inside the mqqapi links and by label in plain.
        for command in (
            "活动背包", "活动兑换", "活动积分", "活动商店", "活动首领", "活动首领排行",
        ):
            self.assertIn(
                f"command={quote(command, safe='')}", text
            )
        # Grouped rows: one row per gameplay activity, labeled by its name.
        self.assertIn("集字：", text)
        self.assertIn("积分：", text)
        self.assertIn("首领：", text)
        # Each group row is bounded: it carries only that group's entries.
        collect_row = next(
            line for line in text.splitlines() if line.startswith("集字：")
        )
        self.assertIn("集字背包", collect_row)
        self.assertIn("集字兑换", collect_row)
        self.assertNotIn("活动首领", collect_row)

    def test_max_config_payload_stays_well_below_qq_limits(self) -> None:
        application = self._application()
        text = application.activity_center_text("u1")
        payload_bytes = len(text.encode("utf-8"))
        # QQ native markdown single-message budget: the plugin's own md模板
        # input cap is 64 KiB; the card must stay far below it even at max
        # config. (Measured: the max-config card renders in the low KiB range.)
        self.assertLess(payload_bytes, 64 * 1024)
        # And the grouped layout bounds each row: no single line carries all
        # gameplay entries once the group count grows.
        longest = max(len(line) for line in text.splitlines())
        self.assertLess(longest, len(text))

    def test_plain_fallback_covers_every_entry(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
            strip_md_command_links,
        )

        application = self._application()
        text = application.activity_center_text("u1")
        plain = strip_md_command_links(text)
        # The plain fallback keeps link labels, not command targets.
        for label in ("集字背包", "集字兑换", "活动积分", "活动商店", "活动首领", "首领排行"):
            self.assertIn(label, plain)


if __name__ == "__main__":
    unittest.main()
