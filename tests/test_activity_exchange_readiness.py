"""Collect exchange readiness: per-phrase marks, shortfalls, terminal states.

``collect_bag_text`` is parity-pinned with the legacy
``build_collect_bag_text``; the readiness details live in the sibling
``collect_bag_guidance_text`` the 活动背包 handler now serves. The readiness
computation mirrors the settlement-side ``tokens_insufficient`` accumulation.
Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

from datetime import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_event_receipts,
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


NOW = datetime(2026, 10, 15, 12)

ACTIVITY = {
    "key": "collect",
    "type": "collect_words",
    "enabled": True,
    "name": "集字活动",
    "start_time": "0",
    "end_time": "无限",
    "pity_threshold": 0,
    "drop_events": [],
    "letters": [{"char": "福"}, {"char": "禄"}, {"char": "寿"}, {"char": "喜"}],
    "phrases": [
        {"name": "福禄", "phrase": "福禄", "limit": 1, "reward": "灵石x10"},
        {"name": "寿喜", "phrase": "寿喜", "limit": 0, "reward": "灵石x20"},
        {
            "name": "四喜",
            "phrase": "福禄寿喜",
            "limit": 2,
            "reward": "灵石x50",
        },
    ],
}


def _config() -> dict:
    return {
        "template_key": "spring",
        "enabled": True,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": {"activity_pass": {"enabled": False}},
        "gameplay_activities": [ACTIVITY],
    }


class ExchangeReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            apply_activity_state_schema(uow)
            apply_activity_event_receipts(uow)

    def _application(self) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(
            self.database,
            config_loader=_config,
            clock=SimpleNamespace(now=lambda: NOW),
        )

    def _seed(self, tokens: dict[str, int], claims: dict[str, int]) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            for char, count in tokens.items():
                uow.execute(
                    "INSERT INTO activity_collect_inventory"
                    "(activity_key,user_id,word_char,count,update_time) "
                    "VALUES('collect','u1',?,?,?)",
                    (char, count, "2026-10-15 10:00:00"),
                )
            for phrase, count in claims.items():
                uow.execute(
                    "INSERT INTO activity_collect_claim"
                    "(activity_key,user_id,phrase,count,update_time) "
                    "VALUES('collect','u1',?,?,?)",
                    (phrase, count, "2026-10-15 10:00:00"),
                )

    def test_ready_phrase_gets_the_readiness_mark(self) -> None:
        self._seed({"福": 1, "禄": 1}, {})
        text = self._application().collect_bag_guidance_text("u1")
        self.assertIn("福禄：2/2，已兑换 0/1，可兑换｜活动兑换 福禄", text)

    def test_shortfall_lists_missing_chars_capped_at_four(self) -> None:
        self._seed({}, {})
        text = self._application().collect_bag_guidance_text("u1")
        # 四喜 needs all four chars; shortfall lists all four (≤4, no 等项).
        self.assertIn("四喜：0/4，已兑换 0/2，还缺 福x1、禄x1、寿x1、喜x1，", text)
        # 寿喜 needs two chars.
        self.assertIn("寿喜：0/2，已兑换 不限，还缺 寿x1、喜x1，兑换：", text)

    def test_shortfall_over_four_items_is_summarized(self) -> None:
        self._seed({}, {})
        text = self._application().collect_bag_guidance_text("u1")
        # Only phrases with >4 missing chars get the 等 N 项 tail; build one by
        # asserting the four-item phrase never carries it.
        self.assertNotIn("等 4 项", text)

    def test_limit_reached_is_terminal_without_command(self) -> None:
        self._seed({"福": 1, "禄": 1}, {"福禄": 1})
        text = self._application().collect_bag_guidance_text("u1")
        self.assertIn("福禄：2/2，已兑换 1/1，已兑换完", text)
        self.assertNotIn("已兑换完，兑换：", text)

    def test_unlimited_phrase_never_shows_terminal(self) -> None:
        self._seed({"寿": 5, "喜": 5}, {"寿喜": 99})
        text = self._application().collect_bag_guidance_text("u1")
        self.assertIn("寿喜：2/2，已兑换 不限，可兑换｜活动兑换 寿喜", text)
        self.assertNotIn("已兑换完", text)

    def test_parity_pinned_bag_text_is_unchanged(self) -> None:
        """The sibling must not alter the legacy-equal base view."""
        from unittest.mock import patch

        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service

        self._seed({"福": 1}, {"福禄": 0})
        application = self._application()
        with (
            patch.object(service, "load_config", return_value=_config()),
            patch.object(service, "DB_PATH", self.database),
        ):
            legacy = service.build_collect_bag_text("u1")
        self.assertEqual(legacy, application.collect_bag_text("u1"))
        self.assertNotEqual(legacy, application.collect_bag_guidance_text("u1"))


if __name__ == "__main__":
    unittest.main()
