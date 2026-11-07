"""炼体药浴生命周期：时段表、剩余时间、三态状态与生效判定单一来源。"""

from __future__ import annotations

import unittest
from datetime import datetime

from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import (
    BathStatus,
    bath_remaining_text,
    bath_schedule,
    bath_slot_remaining,
    bath_status,
    medicine_bath_active,
)

# 与生产配置同形：一个跨零点时段 + 一个普通时段。
SLOTS = (
    {
        "name": "子卯阴息",
        "start": 23,
        "end": 5,
        "range": (1.85, 2.00),
        "herbs": {"夜交藤", "鬼臼草", "冥胎骨"},
    },
    {
        "name": "卯午生机",
        "start": 5,
        "end": 11,
        "range": (1.65, 1.85),
        "herbs": {"恒心草", "天青花", "九叶芝"},
    },
)


class BathSlotRemainingTests(unittest.TestCase):
    def test_plain_window_remaining_counts_to_end(self):
        slot = {"name": "卯午生机", "start": 5, "end": 11}
        self.assertEqual(bath_slot_remaining(slot, datetime(2026, 11, 8, 9, 15)), 105)

    def test_wrapping_window_remaining_spans_midnight(self):
        slot = {"name": "子卯阴息", "start": 23, "end": 5}
        # 23:30 -> 次日 05:00 = 330 分钟
        self.assertEqual(bath_slot_remaining(slot, datetime(2026, 11, 8, 23, 30)), 330)
        # 次日 01:00 -> 05:00 = 240 分钟（同一窗口的另一半）
        self.assertEqual(bath_slot_remaining(slot, datetime(2026, 11, 8, 1, 0)), 240)

    def test_outside_window_is_zero(self):
        slot = {"name": "卯午生机", "start": 5, "end": 11}
        self.assertEqual(bath_slot_remaining(slot, datetime(2026, 11, 8, 11, 0)), 0)
        self.assertEqual(bath_slot_remaining(slot, datetime(2026, 11, 8, 12, 0)), 0)
        self.assertEqual(bath_slot_remaining(slot, datetime(2026, 11, 8, 3, 0)), 0)

    def test_malformed_slot_is_zero_not_error(self):
        self.assertEqual(bath_slot_remaining({"start": "x", "end": 5}, datetime(2026, 11, 8, 1, 0)), 0)
        self.assertEqual(bath_slot_remaining({}, datetime(2026, 11, 8, 1, 0)), 0)


class BathScheduleTests(unittest.TestCase):
    def test_schedule_flags_only_current_slot(self):
        views = bath_schedule(SLOTS, datetime(2026, 11, 8, 9, 15))
        self.assertEqual([view.name for view in views], ["子卯阴息", "卯午生机"])
        self.assertFalse(views[0].current)
        self.assertTrue(views[1].current)
        self.assertEqual(views[1].remaining_minutes, 105)
        self.assertEqual(views[0].remaining_minutes, 0)

    def test_schedule_flags_wrapping_slot_after_midnight(self):
        views = bath_schedule(SLOTS, datetime(2026, 11, 8, 1, 0))
        self.assertTrue(views[0].current)
        self.assertFalse(views[1].current)
        self.assertEqual(views[0].remaining_minutes, 240)

    def test_herbs_sorted_and_capped(self):
        views = bath_schedule(SLOTS, datetime(2026, 11, 8, 9, 0), herb_limit=2)
        self.assertEqual(views[1].herbs, ("九叶芝", "天青花"))

    def test_bad_entries_skipped_without_raising(self):
        broken = [{"name": "", "start": 1, "end": 2}, "nope", {"name": "坏", "start": "x", "end": 3}, SLOTS[1]]
        views = bath_schedule(broken, datetime(2026, 11, 8, 9, 0))
        self.assertEqual([view.name for view in views], ["卯午生机"])

    def test_empty_config_yields_empty_schedule(self):
        self.assertEqual(bath_schedule((), datetime(2026, 11, 8, 9, 0)), ())
        self.assertEqual(bath_schedule(None, datetime(2026, 11, 8, 9, 0)), ())


class BathRemainingTextTests(unittest.TestCase):
    def test_active_window_text(self):
        self.assertEqual(
            bath_remaining_text("2026-11-08 15:00:00", datetime(2026, 11, 8, 14, 30, 30)),
            "剩余 29 分钟",
        )

    def test_expired_or_bad_window_is_empty(self):
        self.assertEqual(bath_remaining_text("2026-11-08 14:00:00", datetime(2026, 11, 8, 14, 30)), "")
        self.assertEqual(bath_remaining_text(None, datetime(2026, 11, 8, 14, 30)), "")
        self.assertEqual(bath_remaining_text("not-a-time", datetime(2026, 11, 8, 14, 30)), "")


class BathStatusTests(unittest.TestCase):
    NOW = datetime(2026, 11, 8, 12, 0)

    def test_active_state(self):
        data = {
            "medicine_end_time": "2026-11-08 15:00:00",
            "medicine_effect": 1.5,
            "medicine_name": "卯午生机药浴（恒心草x20）",
        }
        status = bath_status(data, self.NOW)
        self.assertEqual(status.state, "active")
        self.assertTrue(status)
        self.assertEqual(status.remaining_minutes, 180)
        self.assertEqual(status.effect, 1.5)

    def test_expired_state_is_distinct_from_none(self):
        data = {
            "medicine_end_time": "2026-11-08 09:00:00",
            "medicine_effect": 1.8,
            "medicine_name": "子卯阴息药浴（夜交藤x20）",
        }
        status = bath_status(data, self.NOW)
        self.assertEqual(status.state, "expired")
        self.assertFalse(status)
        self.assertEqual(status.name, "子卯阴息药浴（夜交藤x20）")
        self.assertEqual(status.effect, 1.8)

    def test_never_used_state(self):
        status = bath_status({"medicine_end_time": None, "medicine_effect": 0.0, "medicine_name": ""}, self.NOW)
        self.assertEqual(status.state, "none")
        self.assertEqual(status, BathStatus("none"))

    def test_low_effect_counts_as_not_in_effect(self):
        # 与 get_active_medicine_bath 同口径：effect <= 1 不算生效。
        data = {"medicine_end_time": "2026-11-08 15:00:00", "medicine_effect": 1.0}
        self.assertEqual(bath_status(data, self.NOW).state, "expired")

    def test_bad_payload_never_raises(self):
        self.assertEqual(bath_status(None, self.NOW).state, "none")
        self.assertEqual(
            bath_status({"medicine_end_time": "bad", "medicine_effect": "bad"}, self.NOW).state, "expired"
        )
        self.assertEqual(
            bath_status({"medicine_end_time": "2026-11-08 15:00:00", "medicine_effect": "bad"}, self.NOW).effect,
            0.0,
        )


class MedicineBathActiveSingleSourceTests(unittest.TestCase):
    """四处内联判定必须共用同一谓词（P5）。"""

    NOW = datetime(2026, 11, 8, 12, 0)

    def test_predicate_matches_get_active_medicine_bath(self):
        from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import get_active_medicine_bath

        for data in (
            {"medicine_end_time": "2026-11-08 15:00:00", "medicine_effect": 1.5, "medicine_name": "x"},
            {"medicine_end_time": "2026-11-08 09:00:00", "medicine_effect": 1.5, "medicine_name": "x"},
            {"medicine_end_time": None, "medicine_effect": 0.0},
            {"medicine_end_time": "bad", "medicine_effect": 2.0},
        ):
            with self.subTest(data=data):
                self.assertEqual(medicine_bath_active(data, self.NOW), get_active_medicine_bath(data, self.NOW))

    def test_repositories_use_shared_predicate(self):
        from pathlib import Path

        root = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
        targets = {
            "features/tianti_training/repository.py": None,
            "features/tianti_settlement/repository.py": None,
            "features/sect_fairyland/claim_repository.py": None,
        }
        for relative in targets:
            source = (root / relative).read_text(encoding="utf-8")
            self.assertIn(
                "medicine_bath_active",
                source,
                msg=f"{relative} 必须调用共享生效判定",
            )


if __name__ == "__main__":
    unittest.main()
