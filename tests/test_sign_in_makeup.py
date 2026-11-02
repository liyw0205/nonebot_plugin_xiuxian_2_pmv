"""签到补签（`修仙补签`）的持久化、失败与幂等边界。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.sign_in.application import SignInApplication
from nonebot_plugin_xiuxian_2.features.sign_in.migrations import (
    apply_sign_in_daily,
    apply_sign_in_makeup_source,
    apply_sign_in_statistics,
)
from nonebot_plugin_xiuxian_2.features.sign_in.repository import SignInRepository
from nonebot_plugin_xiuxian_2.features.sign_in.statistics import SignInStatisticsRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
MAKEUP_SRC = (PACKAGE / "features" / "sign_in" / "application.py").read_text(encoding="utf-8")
ADAPTER_SRC = (PACKAGE / "adapters" / "nonebot" / "commands.py").read_text(encoding="utf-8")
MANIFEST_SRC = (PACKAGE / "features" / "sign_in" / "manifest.py").read_text(encoding="utf-8")


class _Clock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


def _create_game_db(path: Path, *, with_daily: bool = True, with_source: bool = True) -> None:
    with DatabaseUnitOfWork(path) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian (user_id TEXT, user_name TEXT, "
            "is_sign INTEGER DEFAULT 0, stone INTEGER DEFAULT 0)"
        )
        uow.execute(
            "INSERT INTO user_xiuxian(user_id, user_name, is_sign, stone) VALUES (?,?,?,?)",
            ("u1", "道友甲", 0, 1_000_000),
        )
        uow.execute(
            "INSERT INTO user_xiuxian(user_id, user_name, is_sign, stone) VALUES (?,?,?,?)",
            ("u2", "道友乙", 0, 50_000),
        )
        uow.execute(
            "INSERT INTO user_xiuxian(user_id, user_name, is_sign, stone) VALUES (?,?,?,?)",
            ("u3", "道友丙", 1, 1_000_000),
        )
        OperationLedger().ensure_schema(uow)
        from nonebot_plugin_xiuxian_2.features.sign_in.migrations import apply_sign_in

        apply_sign_in(uow)
        if with_daily:
            apply_sign_in_daily(uow)
            if with_source:
                apply_sign_in_makeup_source(uow)


class MakeupApplicationTests(unittest.TestCase):
    TODAY = datetime(2026, 11, 2, 9, 0)

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "game.db"
        _create_game_db(self.database)
        self.clock = _Clock(self.TODAY)
        self.application = SignInApplication(
            self.database, clock=self.clock, makeup_cost=100_000
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _makeup(self, business_date: str, *, user_id: str = "u1", operation_id: str = "mk-1"):
        return self.application.makeup(
            user_id=user_id,
            operation_id=operation_id,
            business_date=business_date,
        )

    def _receipt(self, user_id: str, business_date: str):
        with DatabaseUnitOfWork(self.database) as uow:
            return uow.query_one(
                "SELECT stone, operation_id, source FROM sign_in_daily "
                "WHERE user_id=? AND business_date=?",
                (user_id, business_date),
            )

    def _stone(self, user_id: str) -> int:
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT CAST(COALESCE(stone,0) AS REAL) AS stone FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
        return int(float(row["stone"]))

    def test_success_charges_cost_and_writes_makeup_receipt(self) -> None:
        outcome = self._makeup("2026-11-01")
        self.assertTrue(outcome.ok, outcome.message)
        makeup = outcome.data["makeup"]
        self.assertEqual(makeup["business_date"], "2026-11-01")
        self.assertEqual(makeup["cost"], 100_000)
        self.assertEqual(makeup["stone_before"], 1_000_000)
        self.assertEqual(makeup["stone_after"], 900_000)
        self.assertEqual(self._stone("u1"), 900_000)
        receipt = self._receipt("u1", "2026-11-01")
        self.assertEqual((receipt["stone"], receipt["source"]), (0, "makeup"))
        self.assertEqual(receipt["operation_id"], "mk-1")
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one("SELECT is_sign FROM user_xiuxian WHERE user_id='u1'")
        self.assertEqual(int(row["is_sign"]), 0, "补签不得改写今日签到标记")

    def test_calendar_and_streak_include_makeup_day(self) -> None:
        outcome = self._makeup("2026-11-01")
        calendar = outcome.data["calendar"]
        text = "\n".join(calendar)
        self.assertIn("【签到日历 2026-11】", text)
        self.assertIn("本月已签 1 天", text)
        self.assertIn("连续 1 天", text)
        self.assertIn(" 1✓", text)

    def test_same_operation_replay_does_not_charge_twice(self) -> None:
        first = self._makeup("2026-11-01")
        replay = self._makeup("2026-11-01")
        self.assertTrue(first.ok)
        self.assertEqual(replay.status, "replayed")
        self.assertEqual(self._stone("u1"), 900_000)
        with DatabaseUnitOfWork(self.database) as uow:
            count = uow.query_one(
                "SELECT COUNT(*) AS count FROM sign_in_daily WHERE user_id='u1'"
            )["count"]
        self.assertEqual(count, 1)

    def test_different_operation_for_same_date_is_rejected(self) -> None:
        self._makeup("2026-11-01", operation_id="mk-a")
        second = self._makeup("2026-11-01", operation_id="mk-b")
        self.assertFalse(second.ok)
        self.assertEqual(second.code, "already_signed")
        self.assertEqual(self._stone("u1"), 900_000)

    def test_today_future_and_other_month_are_rejected(self) -> None:
        self.assertEqual(self._makeup("2026-11-02", operation_id="mk-today").code, "today_not_allowed")
        self.assertEqual(self._makeup("2026-11-03", operation_id="mk-future").code, "future_date")
        self.assertEqual(self._makeup("2026-10-31", operation_id="mk-oct").code, "out_of_month")
        self.assertEqual(self._stone("u1"), 1_000_000)

    def test_invalid_date_is_rejected(self) -> None:
        outcome = self._makeup("2026-02-30")
        self.assertEqual(outcome.code, "invalid_date")
        self.assertEqual(self._stone("u1"), 1_000_000)

    def test_insufficient_stone_is_rejected_without_receipt(self) -> None:
        outcome = self._makeup("2026-11-01", user_id="u2", operation_id="mk-poor")
        self.assertEqual(outcome.code, "stone_insufficient")
        self.assertEqual(self._stone("u2"), 50_000)
        self.assertIsNone(self._receipt("u2", "2026-11-01"))

    def test_exact_balance_is_allowed(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id='u1'", (100_000,))
        outcome = self._makeup("2026-11-01")
        self.assertTrue(outcome.ok)
        self.assertEqual(self._stone("u1"), 0)

    def test_missing_user_is_rejected(self) -> None:
        outcome = self._makeup("2026-11-01", user_id="missing", operation_id="mk-none")
        self.assertEqual(outcome.code, "user_missing")

    def test_already_signed_day_is_rejected(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO sign_in_daily(user_id,business_date,stone,operation_id,source) "
                "VALUES('u1','2026-11-01',100000,'claim-1','claim')"
            )
        outcome = self._makeup("2026-11-01")
        self.assertEqual(outcome.code, "already_signed")
        self.assertEqual(self._stone("u1"), 1_000_000)

    def test_same_day_claim_is_protected(self) -> None:
        # 补签页签过的日期不能被当成“今日已签”，今天的签到必须仍可进行。
        self._makeup("2026-11-01")
        claim = self.application.claim(
            user_id="u1", operation_id="claim-1", lower_limit=10, upper_limit=10
        )
        self.assertTrue(claim.ok, claim.message)
        self.assertEqual(self._stone("u1"), 900_010)


class MakeupSchemaTests(unittest.TestCase):
    TODAY = datetime(2026, 11, 2, 9, 0)

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "game.db"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _tables(self) -> set[str]:
        with DatabaseUnitOfWork(self.database) as uow:
            rows = uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        return {str(row["name"]) for row in rows}

    def test_missing_daily_table_fails_closed_without_ddl(self) -> None:
        _create_game_db(self.database, with_daily=False)
        application = SignInApplication(
            self.database, clock=_Clock(self.TODAY), makeup_cost=100_000
        )
        outcome = application.makeup(
            user_id="u1", operation_id="mk-noschema", business_date="2026-11-01"
        )
        self.assertEqual(outcome.code, "schema_missing")
        self.assertNotIn("sign_in_daily", self._tables())

    def test_receipt_table_without_source_column_fails_closed(self) -> None:
        _create_game_db(self.database, with_daily=True, with_source=False)
        application = SignInApplication(
            self.database, clock=_Clock(self.TODAY), makeup_cost=100_000
        )
        outcome = application.makeup(
            user_id="u1", operation_id="mk-old", business_date="2026-11-01"
        )
        self.assertEqual(outcome.code, "schema_missing")
        with DatabaseUnitOfWork(self.database) as uow:
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(sign_in_daily)")}
        self.assertNotIn("source", columns)


class MakeupEffectsTests(unittest.TestCase):
    TODAY = datetime(2026, 11, 2, 9, 0)

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "game.db"
        _create_game_db(self.database)
        with DatabaseUnitOfWork(self.database) as uow:
            apply_sign_in_statistics(uow)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_statistics_event_uses_target_day_not_today(self) -> None:
        from nonebot_plugin_xiuxian_2.features.sign_in.application_effects import (
            SignInApplicationEffects,
        )

        class _Lottery:
            def settle(self, **kwargs):  # pragma: no cover - makeup must not call it
                raise AssertionError("补签不得参与鸿运")

        class _Tasks:
            def record(self, **kwargs):  # pragma: no cover - makeup must not call it
                raise AssertionError("补签不得触发签到任务")

        effects = SignInApplicationEffects(
            self.database,
            lottery=_Lottery(),
            clock=_Clock(self.TODAY),
            statistics=SignInStatisticsRepository(self.database),
            tasks=_Tasks(),
        )
        application = SignInApplication(
            self.database, clock=_Clock(self.TODAY), makeup_cost=100_000, effects=effects
        )
        outcome = application.makeup(
            user_id="u1", operation_id="mk-stats", business_date="2026-11-01"
        )
        self.assertTrue(outcome.ok, outcome.message)
        repository = SignInStatisticsRepository(self.database)
        self.assertEqual(repository.value(user_id="u1", event_key="修仙签到"), 1)
        with DatabaseUnitOfWork(self.database) as uow:
            row = uow.query_one(
                "SELECT created_at FROM sign_in_statistics_events WHERE user_id='u1'"
            )
        self.assertTrue(str(row["created_at"]).startswith("2026-11-01"))


class MakeupContractTests(unittest.TestCase):
    def test_adapter_registers_command_with_aliases(self) -> None:
        self.assertIn('on_command(\n        "修仙补签", aliases={"补签", "签到补签"}, priority=1, block=True\n    )', ADAPTER_SRC)
        self.assertIn('_message_id(context.raw_event, context.user_id, "sign-makeup")', ADAPTER_SRC)

    def test_manifest_declares_command_config_and_migration(self) -> None:
        self.assertIn('CommandSpec("修仙补签", aliases=("补签", "签到补签"), permission="user")', MANIFEST_SRC)
        self.assertIn('ConfigSpec("sign_in_makeup_cost", "int", default=100000', MANIFEST_SRC)
        self.assertIn('migration_version="sign_in.005"', MANIFEST_SRC)

    def test_application_persists_source_and_never_signs_today(self) -> None:
        self.assertIn('"sign_in.makeup"', MAKEUP_SRC)
        self.assertIn("record_makeup_daily", MAKEUP_SRC)
        self.assertIn("charge_stone", MAKEUP_SRC)
        self.assertIn("makeup_schema_ready", MAKEUP_SRC)
        self.assertNotIn("is_sign = 1", MAKEUP_SRC)


if __name__ == "__main__":
    unittest.main()
