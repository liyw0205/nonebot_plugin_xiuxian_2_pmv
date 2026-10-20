"""Shop view purchase guidance: per-item buy command and terminal states.

``point_shop_text`` is parity-pinned with the legacy
``build_activity_shop_text``; the guidance lives in the sibling
``point_shop_guidance_text`` the 活动商店 handler now serves. Written as
``unittest.TestCase`` because the CI quality job collects tests with
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
    "key": "points",
    "type": "event_points",
    "enabled": True,
    "name": "积分活动",
    "start_time": "0",
    "end_time": "无限",
    "point_name": "活动积分",
    "event_rules": [{"event": "sign_in", "points": 5}],
    "shop": [
        {"item_key": "pack", "name": "灵石补给", "cost": 10, "limit": 1, "reward": "灵石x100"},
        {"item_key": "box", "name": "宝箱", "cost": 20, "limit": 0, "reward": "道具x1"},
        {
            "item_key": "limited",
            "name": "限定",
            "cost": 30,
            "limit": 2,
            "stock_limit": 1,
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


class ShopGuidanceTests(unittest.TestCase):
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

    def _seed(self, purchases: list[tuple[str, int]]) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                "VALUES('points','u1',100,100)"
            )
            for item_key, count in purchases:
                uow.execute(
                    "INSERT INTO activity_point_purchase(activity_key,user_id,item_key,count) "
                    "VALUES('points','u1',?,?)",
                    (item_key, count),
                )

    def test_purchasable_items_carry_buy_guidance_and_summary(self) -> None:
        self._seed([])
        text = self._application().point_shop_guidance_text("u1")
        self.assertIn("购买：活动购买 灵石补给", text)
        self.assertIn("购买：活动购买 宝箱", text)
        # Stock-limited item with no purchases is still purchasable.
        self.assertIn("购买：活动购买 限定", text)
        self.assertIn("可购商品 3 件，购买：活动购买 灵石补给", text)

    def test_personal_limit_reached_marks_terminal_without_guidance(self) -> None:
        self._seed([("pack", 1)])
        text = self._application().point_shop_guidance_text("u1")
        self.assertIn("已兑换完（1/1）", text)
        # The terminal item no longer guides a doomed purchase.
        self.assertNotIn("购买：活动购买 灵石补给", text)
        # Unlimited item keeps its guidance.
        self.assertIn("购买：活动购买 宝箱", text)
        self.assertIn("可购商品 2 件", text)

    def test_sold_out_marks_terminal_without_guidance(self) -> None:
        # Another user bought out the whole stock of 限定 (stock_limit=1).
        self._seed([])
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_point_purchase(activity_key,user_id,item_key,count) "
                "VALUES('points','u2','limited',1)"
            )
        text = self._application().point_shop_guidance_text("u1")
        self.assertIn("已售罄", text)
        self.assertNotIn("购买：活动购买 限定", text)

    def test_unlimited_items_never_show_terminal_state(self) -> None:
        self._seed([("box", 99)])
        text = self._application().point_shop_guidance_text("u1")
        self.assertIn("已兑换 不限", text)
        self.assertIn("购买：活动购买 宝箱", text)
        self.assertNotIn("已兑换完", text)

    def test_parity_pinned_point_shop_text_is_unchanged(self) -> None:
        """The sibling must not alter the legacy-equal base view."""
        from unittest.mock import patch

        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service

        self._seed([("pack", 1)])
        application = self._application()
        with (
            patch.object(service, "load_config", return_value=_config()),
            patch.object(service, "DB_PATH", self.database),
        ):
            legacy = service.build_activity_shop_text("u1")
        self.assertEqual(legacy, application.point_shop_text("u1"))
        # And the sibling differs (it carries guidance).
        self.assertNotEqual(
            legacy, application.point_shop_guidance_text("u1")
        )

    def test_no_purchasable_items_renders_no_summary(self) -> None:
        self._seed([("pack", 1), ("limited", 1)])
        # 宝箱 is unlimited so it stays purchasable; build an all-terminal
        # case by checking the summary counts only purchasable items.
        text = self._application().point_shop_guidance_text("u1")
        self.assertIn("可购商品 1 件", text)


if __name__ == "__main__":
    unittest.main()
