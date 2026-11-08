"""师徒解除清算：120% 权威传功修为的原子收取与防刷（P1–P4）。

覆盖：120% 向上取整的纯算术、权威账目按配对隔离、余额不足拒绝、
重放/冲突/并发二次解除、缺表缺列的 fail-closed（含**账目不可读时不得免费解除**）、
统计与历史写入，以及“拜师→传功→解除→再拜”整循环不产生净修为。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.buff.mentor_dissolution import (
    STATUS_INSUFFICIENT,
    STATUS_NO_CHARGE,
    STATUS_READY,
    dissolution_charge,
    dissolution_decision,
    render_dissolution_line,
)
from nonebot_plugin_xiuxian_2.features.buff.mentor_dissolution_repository import (
    LEDGER_UNAVAILABLE,
    OPERATION_TABLE,
    SCHEMA_MISSING,
    MentorDissolutionService,
)
from nonebot_plugin_xiuxian_2.features.buff.migrations import (
    apply_mentor_dissolution_operations,
    apply_mentor_dissolution_player_schema,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

MENTOR_DDL = (
    "CREATE TABLE mentor(user_id TEXT PRIMARY KEY,mentor_id TEXT,apprentice_ids TEXT,"
    "mentor_cd_until TEXT,apprentice_cd_until TEXT,mentor_rebind_cd TEXT,"
    "mentor_history TEXT,bind_time TEXT,breakthrough_reward_count INTEGER)"
)
STATISTICS_DDL = (
    "CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"
    '"师徒清退修为" INTEGER DEFAULT 0,'
    '"被逐出师门次数" INTEGER DEFAULT 0,'
    '"离开师门次数" INTEGER DEFAULT 0)'
)
USER_DDL = "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER)"
TRANSMISSION_DDL = (
    "CREATE TABLE mentor_transmission_operations ("
    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
    "reward_exp INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
)


def _transmission_payload(mentor_id: str, apprentice_id: str, reward: int) -> str:
    return (
        f'["{mentor_id}","{apprentice_id}",0,{reward},0,0,0,0,0,0,'
        '"d","d"]'
    )


class DissolutionChargeTests(unittest.TestCase):
    """P1：120% 向上取整的纯算术与判定。"""

    def test_charge_is_120_percent_rounded_up(self):
        self.assertEqual(dissolution_charge(0), 0)
        self.assertEqual(dissolution_charge(1), 2)       # 1.2 -> 2
        self.assertEqual(dissolution_charge(5), 6)       # 6.0 -> 6
        self.assertEqual(dissolution_charge(1000), 1200)
        self.assertEqual(dissolution_charge(3), 4)       # 3.6 -> 4
        self.assertEqual(dissolution_charge(10**12), 12 * 10**11)

    def test_charge_tolerates_bad_values(self):
        for bad in (None, "x", "", -5, 0.0, object()):
            with self.subTest(bad=bad):
                self.assertEqual(dissolution_charge(bad), 0)

    def test_decision_states(self):
        self.assertEqual(dissolution_decision(gained_exp=0, apprentice_exp=100).status, STATUS_NO_CHARGE)
        ready = dissolution_decision(gained_exp=1000, apprentice_exp=1200)
        self.assertEqual(ready.status, STATUS_READY)
        self.assertTrue(ready.payable and ready.charged)
        self.assertEqual(ready.shortfall, 0)
        poor = dissolution_decision(gained_exp=1000, apprentice_exp=1199)
        self.assertEqual(poor.status, STATUS_INSUFFICIENT)
        self.assertFalse(poor.payable)
        self.assertEqual(poor.shortfall, 1)

    def test_render_line_states(self):
        self.assertEqual(render_dissolution_line(dissolution_decision(gained_exp=0, apprentice_exp=9)), "")
        ready = render_dissolution_line(
            dissolution_decision(gained_exp=1000, apprentice_exp=1200), format_number=lambda v: f"{v:,}"
        )
        self.assertIn("1,200", ready)
        poor = render_dissolution_line(
            dissolution_decision(gained_exp=1000, apprentice_exp=10),
            format_number=lambda v: f"{v:,}",
            refusal_tail="师门未变。",
        )
        self.assertIn("还差 1,190", poor)
        self.assertIn("师门未变。", poor)


class DissolutionSettlementTests(unittest.TestCase):
    """P2–P3：原子清算、权威账目、幂等/并发与 fail-closed。"""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.game, self.player = root / "g.db", root / "p.db"
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(MENTOR_DDL)
            uow.execute(STATISTICS_DDL)
            uow.executemany(
                "INSERT INTO mentor(user_id,mentor_id,apprentice_ids,mentor_rebind_cd,"
                "mentor_history,bind_time,breakthrough_reward_count) VALUES (?,?,?,?,?,?,?)",
                [
                    ("m", None, '["a"]', "{}", "[]", None, 0),
                    ("a", "m", "[]", "{}", "[]", "old", 2),
                ],
            )
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(USER_DDL)
            uow.executemany(
                "INSERT INTO user_xiuxian VALUES (?,?,?)",
                [("m", "金丹境圆满", 5000), ("a", "感气境初期", 2000)],
            )
            uow.execute(TRANSMISSION_DDL)
        self.service = MentorDissolutionService(self.game, self.player)

    # -- helpers ---------------------------------------------------------
    def _migrate(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_mentor_dissolution_operations(uow)

    def _transmit(self, reward: int, mentor="m", apprentice="a", operation="t1"):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute(
                "INSERT INTO mentor_transmission_operations(operation_id,payload,reward_exp)"
                " VALUES (?,?,?)",
                (operation, _transmission_payload(mentor, apprentice, reward), reward),
            )

    def _exp(self, user="a"):
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            return uow.query_one("SELECT exp FROM user_xiuxian WHERE user_id=?", (user,))["exp"]

    def _stat(self, key: str, user="a"):
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            row = uow.query_one(
                f'SELECT "{key}" FROM statistics WHERE user_id=?', (user,)
            )
        return None if row is None else row[key]

    def _count_receipts(self) -> int:
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            return uow.query_one(f"SELECT COUNT(*) AS n FROM {OPERATION_TABLE}")["n"]

    def _mentor_row(self, user="a"):
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            return uow.query_one(
                "SELECT mentor_id,apprentice_ids,bind_time,mentor_cd_until,"
                "apprentice_cd_until, mentor_rebind_cd, mentor_history"
                " FROM mentor WHERE user_id=?",
                (user,),
            )

    def call(self, operation="d1", action="leave", level="感气境初期"):
        return self.service.settle(
            operation, "m", "a", action=action,
            expected_apprentice_level=level, occurred_at="2026-11-10 12:00:00",
            cooldown_field="apprentice_cd_until", cooldown_until="2026-11-17 12:00:00",
            pair_rebind_until="2026-12-10 12:00:00", history_limit=50,
            mentor_desc="逐出", apprentice_desc="被逐出",
            mentor_cd_until="2026-11-17 12:00:00",
        )

    # -- happy path ------------------------------------------------------
    def test_settlement_charges_120_percent_of_pair_receipts(self):
        self._migrate()
        self._transmit(1000, operation="t1")
        self._transmit(500, operation="t2")
        result = self.call()
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.gained, 1500)
        self.assertEqual(result.charge, 1800)          # 1500 * 1.2
        self.assertEqual(self._exp(), 200)             # 2000 - 1800
        row = self._mentor_row()
        self.assertIsNone(row["mentor_id"])
        self.assertIsNone(row["bind_time"])
        self.assertEqual("2026-11-17 12:00:00", row["apprentice_cd_until"])
        self.assertIn("m", row["mentor_rebind_cd"])
        mentor_row = self._mentor_row("m")
        self.assertEqual([], json.loads(mentor_row["apprentice_ids"]))
        self.assertEqual("2026-11-17 12:00:00", mentor_row["mentor_cd_until"])

    def test_statistics_and_history_are_written(self):
        self._migrate()
        self._transmit(1000, operation="t1")
        self.assertEqual(self.call(action="leave").status, "applied")

        self.assertEqual(1200, self._stat("师徒清退修为"))
        self.assertEqual(1, self._stat("离开师门次数"))
        self.assertEqual(0, self._stat("被逐出师门次数"))
        history = json.loads(self._mentor_row("a")["mentor_history"])
        self.assertEqual("leave", history[-1]["type"])
        self.assertEqual("m", history[-1]["related_id"])
        self.assertEqual(1, len(history))

    def test_only_this_pairs_receipts_count(self):
        self._migrate()
        self._transmit(1000, operation="mine")
        self._transmit(9999, mentor="m", apprentice="other", operation="others-apprentice")
        self._transmit(9999, mentor="other", apprentice="a", operation="other-mentor")
        result = self.call()
        self.assertEqual(result.gained, 1000)
        self.assertEqual(result.charge, 1200)

    def test_no_receipts_means_no_charge(self):
        self._migrate()
        result = self.call()
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.charge, 0)
        self.assertEqual(self._exp(), 2000)
        self.assertEqual(0, self._stat("师徒清退修为"))

    # -- refusal --------------------------------------------------------
    def test_insufficient_cultivation_keeps_relation_and_assets(self):
        self._migrate()
        self._transmit(10**6, operation="big")
        result = self.call()
        self.assertEqual(result.status, STATUS_INSUFFICIENT)
        self.assertEqual(result.charge, 1200000)
        self.assertEqual(result.shortfall, 1200000 - 2000)
        self.assertEqual(self._exp(), 2000)
        row = self._mentor_row("a")
        self.assertEqual("m", row["mentor_id"])
        self.assertEqual('["a"]', self._mentor_row("m")["apprentice_ids"])
        self.assertEqual(0, self._count_receipts())
        self.assertIsNone(self._stat("师徒清退修为"))

    def test_charge_never_goes_negative(self):
        self._migrate()
        self._transmit(2000, operation="exact")
        result = self.call()
        # 2000 * 1.2 = 2400 > 2000 → 拒绝，余额不变且不为负。
        self.assertEqual(result.status, STATUS_INSUFFICIENT)
        self.assertEqual(self._exp(), 2000)

    # -- idempotency / concurrency ---------------------------------------
    def test_replay_returns_duplicate_with_first_charge(self):
        self._migrate()
        self._transmit(1000, operation="t1")
        first = self.call()
        self.assertEqual(first.status, "applied")
        balance = self._exp()
        again = self.call()
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(again.charge, first.charge)
        self.assertEqual(self._exp(), balance)
        self.assertEqual(1, self._count_receipts())

    def test_conflicting_payload_is_rejected(self):
        self._migrate()
        self._transmit(1000, operation="t1")
        self.call()
        balance = self._exp()
        conflict = self.service.settle(
            "d1", "m", "a", action="expel",
            expected_apprentice_level="感气境初期", occurred_at="2026-11-10 12:00:00",
            cooldown_field="apprentice_cd_until", cooldown_until="2026-11-17 12:00:00",
            pair_rebind_until="2026-12-10 12:00:00", history_limit=50,
            mentor_desc="逐出", apprentice_desc="被逐出",
        )
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(self._exp(), balance)

    def test_second_dissolution_is_state_changed(self):
        self._migrate()
        self._transmit(1000, operation="t1")
        self.assertEqual(self.call("first").status, "applied")
        balance = self._exp()
        self.assertEqual(self.call("second").status, "state_changed")
        self.assertEqual(self._exp(), balance)

    def test_level_drift_is_state_changed(self):
        self._migrate()
        self._transmit(1000, operation="t1")
        self.assertEqual(self.call(level="金丹境圆满").status, "state_changed")
        self.assertEqual(self._exp(), 2000)

    def test_invalid_cooldown_field_is_rejected_before_any_write(self):
        self._migrate()
        with self.assertRaises(ValueError):
            self.service.settle(
                "bad", "m", "a", action="leave",
                expected_apprentice_level="感气境初期", occurred_at="t",
                cooldown_field="mentor_history", cooldown_until="t",
                pair_rebind_until="t", history_limit=50,
                mentor_desc="d", apprentice_desc="d",
            )
        self.assertEqual(0, self._count_receipts())

    # -- fail closed ------------------------------------------------------
    def test_missing_receipt_table_fails_closed(self):
        self._transmit(1000, operation="t1")
        result = self.call()
        self.assertEqual(result.status, SCHEMA_MISSING)
        self.assertEqual(self._exp(), 2000)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            table = uow.query_one(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (OPERATION_TABLE,)
            )
        self.assertIsNone(table)   # 请求路径不建表

    def test_missing_statistics_column_fails_closed(self):
        self._migrate()
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("ALTER TABLE statistics DROP COLUMN \"师徒清退修为\"")
        self._transmit(1000, operation="t1")

        result = self.call()

        self.assertEqual(result.status, SCHEMA_MISSING)
        self.assertEqual(self._exp(), 2000)
        self.assertEqual(0, self._count_receipts())

    def test_ledger_table_missing_never_gives_free_dissolution(self):
        """账目表缺失时不得按 0 处理：拒绝解除，关系与资产都不动。"""
        self._migrate()
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("DROP TABLE mentor_transmission_operations")

        result = self.call()

        self.assertEqual(result.status, LEDGER_UNAVAILABLE)
        self.assertEqual(0, result.charge)
        self.assertEqual(self._exp(), 2000)
        self.assertEqual("m", self._mentor_row("a")["mentor_id"])
        self.assertEqual(0, self._count_receipts())

    def test_corrupt_ledger_fails_closed(self):
        """账目表存在但读不出 reward_exp（坏 schema/坏 payload）时同样拒绝。"""
        self._migrate()
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("DROP TABLE mentor_transmission_operations")
            uow.execute("CREATE TABLE mentor_transmission_operations("
                        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")

        result = self.call()

        self.assertEqual(result.status, LEDGER_UNAVAILABLE)
        self.assertEqual(self._exp(), 2000)
        self.assertEqual(0, self._count_receipts())

    def test_player_schema_migration_adds_statistic_column(self):
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("ALTER TABLE statistics DROP COLUMN \"师徒清退修为\"")
            apply_mentor_dissolution_player_schema(uow)
            apply_mentor_dissolution_player_schema(uow)  # idempotent
            columns = {row["name"] for row in uow.query_all('PRAGMA table_info("statistics")')}
        self.assertIn("师徒清退修为", columns)

    def test_non_pair_is_state_changed(self):
        self._migrate()
        self._transmit(1000, operation="t1")
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("UPDATE mentor SET apprentice_ids='[]' WHERE user_id='m'")
        self.assertEqual(self.call().status, "state_changed")
        self.assertEqual(self._exp(), 2000)

    def test_farming_is_not_profitable_across_a_full_cycle(self):
        """拜师→传功→解除→再拜：每轮先付 120%，净修为不增。"""
        self._migrate()
        self._transmit(1000, operation="cycle1")
        first = self.call("cycle-1")
        self.assertEqual(first.status, "applied")
        after_first = self._exp()          # 2000 - 1200 = 800
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("UPDATE mentor SET apprentice_ids='[\"a\"]' WHERE user_id='m'")
            uow.execute("UPDATE mentor SET mentor_id='m',bind_time='new' WHERE user_id='a'")
        self._transmit(0, operation="cycle2")
        second = self.call("cycle-2")
        self.assertEqual(second.status, "applied")
        self.assertEqual(second.charge, 0)               # 无新增权威传功 → 不再收费
        self.assertLessEqual(self._exp(), after_first)   # 净修为不因循环而增加


if __name__ == "__main__":
    unittest.main()
