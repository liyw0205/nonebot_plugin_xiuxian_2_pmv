"""灵田收取域定向合同：规则、只读快照、启动迁移与入口委派。

本切片把 `灵田收取` 里三处内联逻辑收敛到 `features/mixelixir`：

* 成熟窗口（23 小时 / 0.05 加速基数）与「收取时间」解析，原先在命令里硬编码并裸
  ``strptime``，缺行/脏值直接抛异常；
* 药材数量与抽取去重（含候选为空时的「恒心草」兜底）；
* 四张炼丹回执表的请求期 ``CREATE TABLE``（与仓库级「请求路径不执行 DDL」合同冲突）。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.mixelixir import harvest_rules as rules
from nonebot_plugin_xiuxian_2.features.mixelixir.harvest_repository import (
    MixelixirHarvestSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.mixelixir.harvest_view import (
    MixelixirHarvestFieldApplication,
)
from nonebot_plugin_xiuxian_2.features.mixelixir.migrations import (
    OPERATIONS_TABLE_COLUMNS,
    apply_mixelixir_operations,
)
from nonebot_plugin_xiuxian_2.features.mixelixir.settlement_repository import (
    MixelixirSettlementSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

ROOT = Path(__file__).resolve().parents[1]
FACADE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_mixelixir" / "__init__.py"
FEATURE = ROOT / "nonebot_plugin_xiuxian_2" / "features" / "mixelixir"


class StubRng:
    """Deterministic stand-in for SystemRandom on the harvest path."""

    def __init__(self, picks):
        self.picks = list(picks)
        self.index = 0

    def choice(self, seq):
        rows = list(seq)
        if not rows:
            raise IndexError("empty candidate list")
        value = self.picks[self.index % len(self.picks)]
        self.index += 1
        return value if value in rows else rows[0]


class StubClock:
    def __init__(self, moment: datetime):
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


class MaturityRuleTests(unittest.TestCase):
    def test_full_window_matches_the_legacy_constant(self) -> None:
        self.assertEqual(rules.maturity_interval_hours(), 23.0)
        self.assertEqual(rules.maturity_interval_hours(herb_speed=2), 20.7)
        self.assertEqual(rules.maturity_interval_hours(herb_speed="bad"), 23.0)

    def test_never_harvested_shapes(self) -> None:
        for value in (None, "", 0, "0", "0.0", "None"):
            self.assertTrue(rules.is_never_harvested(value), value)
        for value in ("2026-01-01 00:00:00", 1, "1"):
            self.assertFalse(rules.is_never_harvested(value), value)

    def test_dirty_time_is_invalid_not_an_exception(self) -> None:
        now = datetime(2026, 1, 2, 0, 0)
        for value in (None, "", "0", "not-a-time", "2026-13-40 99:99:99"):
            self.assertEqual(rules.maturity_state(value, now)[0], rules.INVALID)
        self.assertIsNone(rules.parse_harvest_time("not-a-time"))

    def test_ready_and_waiting_boundaries(self) -> None:
        last = "2026-01-01 00:00:00"
        self.assertEqual(
            rules.maturity_state(last, datetime(2026, 1, 1, 23, 0))[0], rules.READY
        )
        state, remaining = rules.maturity_state(last, datetime(2026, 1, 1, 22, 0))
        self.assertEqual((state, remaining), (rules.WAITING, 1.0))
        # 药材速度会缩短窗口：2 点速度 → 20.7 小时即成熟
        self.assertEqual(
            rules.maturity_state(last, datetime(2026, 1, 1, 20, 45), herb_speed=2)[0],
            rules.READY,
        )

    def test_aware_now_is_normalized_to_naive_utc(self) -> None:
        last = "2026-01-01 00:00:00"
        aware = datetime(2026, 1, 1, 23, 30, tzinfo=timezone.utc)
        self.assertEqual(rules.maturity_state(last, aware)[0], rules.READY)
        self.assertEqual(
            rules.maturity_state(last, aware + timedelta(hours=8))[0], rules.READY
        )

    def test_remaining_text_matches_legacy_message(self) -> None:
        self.assertEqual(rules.remaining_text(1.5), "1小时30分钟")
        self.assertEqual(rules.remaining_text(0.25), "0小时15分钟")
        self.assertEqual(rules.remaining_text(0.0), "0小时0分钟")


class HarvestCountTests(unittest.TestCase):
    def test_count_sums_all_bonuses(self) -> None:
        self.assertEqual(rules.harvest_count(1, 0, 0, 0), 1)
        self.assertEqual(rules.harvest_count(1, 2, 3, 4), 10)
        self.assertEqual(rules.harvest_count("3", "1", "0", "2"), 6)
        self.assertEqual(rules.harvest_count(-5, -5, 0, 0), 0)
        self.assertEqual(rules.harvest_count(None, None, None, None), 0)


class RollRewardsTests(unittest.TestCase):
    def names(self, goods_id):
        return {1: "一号药材", 2: "二号药材", 3001: "恒心草"}.get(goods_id, "未知")

    def test_empty_candidates_fall_back_to_hengxincao(self) -> None:
        self.assertEqual(
            rules.roll_rewards([], 3, name_of=self.names, rng=StubRng([])),
            ((3001, "恒心草", 3),),
        )
        self.assertEqual(rules.roll_rewards([], 0, name_of=self.names, rng=StubRng([])), ())

    def test_duplicates_are_merged_and_order_is_stable(self) -> None:
        rolled = rules.roll_rewards(
            [1, 2], 4, name_of=self.names, rng=StubRng([2, 1, 2, 1])
        )
        self.assertEqual(rolled, ((2, "二号药材", 2), (1, "一号药材", 2)))

    def test_bad_candidate_ids_are_dropped(self) -> None:
        self.assertEqual(
            rules.roll_rewards(["bad", 0, -1], 2, name_of=self.names, rng=StubRng([])),
            ((3001, "恒心草", 2),),
        )

    def test_normalize_rewards_accepts_tuples_and_mappings(self) -> None:
        self.assertEqual(
            rules.normalize_rewards([(1, "一号", 2), {"item_id": 2, "name": "二号", "quantity": 3}]),
            ((1, "一号", 2), (2, "二号", 3)),
        )
        self.assertEqual(
            rules.normalize_rewards([{"goods_id": "3", "goods_name": "三号", "amount": "4"}]),
            ((3, "三号", 4),),
        )


class FieldSnapshotTests(unittest.TestCase):
    def build(self, temp: str, *, row: tuple | None, columns: str | None = None):
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with db_backend.transaction(game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u')")
        with db_backend.transaction(player) as conn:
            definitions = columns or (
                '"收取时间" TEXT,"收取等级" TEXT,"灵田数量" TEXT,"药材速度" TEXT'
            )
            conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{definitions})")
            if row is not None:
                conn.execute("INSERT INTO mix_elixir_info VALUES('u',?,?,?,?)", row)
        return game, player

    def snapshot(self, game, player, moment: datetime):
        return MixelixirHarvestFieldApplication(
            game, player, clock=StubClock(moment)
        ).snapshot("u")

    def test_waiting_reports_remaining_hours(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(
                temp, row=("2026-01-01 00:00:00", "1", "2", "0")
            )
            snap = self.snapshot(game, player, datetime(2026, 1, 1, 21, 0))
        self.assertTrue(snap.ok)
        self.assertEqual(snap.state, rules.WAITING)
        self.assertEqual(snap.remaining_hours, 2.0)
        self.assertEqual((snap.field_count, snap.harvest_level, snap.herb_speed), (2, 1, 0))
        self.assertFalse(snap.ready)

    def test_ready_after_the_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(
                temp, row=("2026-01-01 00:00:00", "0", "1", "4")
            )
            snap = self.snapshot(game, player, datetime(2026, 1, 1, 19, 0))
        self.assertEqual(snap.state, rules.READY)
        self.assertTrue(snap.ready)
        self.assertEqual(snap.remaining_hours, 0.0)

    def test_new_player_row_is_invalid_not_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, row=None)
            snap = self.snapshot(game, player, datetime(2026, 1, 1, 12, 0))
        self.assertTrue(snap.ok)
        self.assertEqual(snap.state, rules.INVALID)
        self.assertEqual(snap.last_time, "")

    def test_dirty_timestamp_degrades_instead_of_raising(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, row=("garbage", "0", "1", "0"))
            snap = self.snapshot(game, player, datetime(2026, 1, 1, 12, 0))
        self.assertTrue(snap.ok)
        self.assertEqual(snap.state, rules.INVALID)
        self.assertEqual(snap.last_time, "garbage")

    def test_missing_player_table_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u')")
            snap = MixelixirHarvestFieldApplication(
                game, player, clock=StubClock(datetime(2026, 1, 1))
            ).snapshot("u")
        self.assertFalse(snap.ok)
        self.assertIsNotNone(snap.notice)
        self.assertFalse(snap.ready)

    def test_missing_optional_columns_still_reads(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(
                temp,
                row=("2026-01-01 00:00:00", "0", "1", "0"),
                columns='"收取时间" TEXT,"收取等级" TEXT,"灵田数量" TEXT,"药材速度" TEXT',
            )
            # 只保留权威列集的一部分时仍然可读，缺失的加成按 0 计算
            with db_backend.transaction(player) as conn:
                conn.execute("ALTER TABLE mix_elixir_info RENAME TO legacy")
                conn.execute(
                    "CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,\"收取时间\" TEXT)"
                )
                conn.execute("INSERT INTO mix_elixir_info VALUES('u','2026-01-01 00:00:00')")
            snap = self.snapshot(game, player, datetime(2026, 1, 2, 0, 0))
        self.assertTrue(snap.ok)
        self.assertEqual(snap.state, rules.READY)
        self.assertEqual(snap.field_count, 1, "missing 灵田数量 falls back to 1")


class RequestPathNoDdlTests(unittest.TestCase):
    def build(self, temp: str):
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with db_backend.transaction(game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',0)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',1,'灵草','药材',5,0)")
        with db_backend.transaction(player) as conn:
            conn.execute('CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,"收取时间" TEXT)')
            conn.execute("INSERT INTO mix_elixir_info VALUES('u','2026-01-01 00:00:00')")
        return game, player

    def test_harvest_fails_closed_without_the_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            repo = MixelixirHarvestSqlRepository(game, player)
            result = repo.harvest(
                "h1", "u", "2026-01-01 00:00:00", "2026-01-02 00:00:00",
                ({"item_id": 1, "name": "灵草", "quantity": 2},), max_goods_num=99,
            )
            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(game) as conn:
                tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertNotIn("mixelixir_harvest_operations", tables)
            with db_backend.connection(game) as conn:
                self.assertEqual(
                    int(conn.execute("SELECT goods_num FROM back WHERE goods_id=1").fetchone()[0]),
                    5,
                    "no herbs are granted when the ledger is missing",
                )

    def test_settlement_fails_closed_without_the_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, _player = self.build(temp)
            repo = MixelixirSettlementSqlRepository(game)
            result = repo.settle("s1", "u", {1: 2}, 200, "丹药", 1, max_goods_num=99)
            self.assertEqual(result.status, "schema_missing")
            with db_backend.connection(game) as conn:
                tables = {
                    str(row[0])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertNotIn("mixelixir_settlement_operations", tables)

    def test_startup_migration_creates_all_four_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, _player = self.build(temp)
            with DatabaseUnitOfWork(game, immediate=True) as uow:
                apply_mixelixir_operations(uow)
            with db_backend.connection(game) as conn:
                for table in OPERATIONS_TABLE_COLUMNS:
                    columns = {
                        str(row[1])
                        for row in conn.execute(f'PRAGMA table_info("{table}")')
                    }
                    self.assertTrue(
                        OPERATIONS_TABLE_COLUMNS[table].issubset(columns), table
                    )
                version = conn.execute(
                    "SELECT version FROM mixelixir_feature_migrations"
                ).fetchall()
            self.assertIn(("mixelixir.004",), [tuple(row) for row in version])

    def test_legacy_lazy_tables_are_reused_not_replaced(self) -> None:
        """Old databases already carry the lazily created tables; rows must survive."""
        with tempfile.TemporaryDirectory() as temp:
            game, _player = self.build(temp)
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TABLE mixelixir_harvest_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,harvested_at TEXT NOT NULL,"
                    "rewards_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO mixelixir_harvest_operations VALUES('old','[\"u\"]','2026-01-01','[]','2026-01-01')"
                )
            with DatabaseUnitOfWork(game, immediate=True) as uow:
                apply_mixelixir_operations(uow)
            with db_backend.connection(game) as conn:
                rows = conn.execute(
                    "SELECT operation_id FROM mixelixir_harvest_operations"
                ).fetchall()
            self.assertEqual([str(row[0]) for row in rows], ["old"])


class FacadeContractTests(unittest.TestCase):
    def source(self) -> str:
        return FACADE.read_text(encoding="utf-8")

    def handler(self) -> str:
        source = self.source()
        start = source.index("@yaocai_get.handle")
        return source[start:source.index("@my_mix_elixir_info.handle", start)]

    def test_handler_delegatesthe_maturity_window_to_the_feature(self) -> None:
        handler = self.handler()
        self.assertIn("harvest_field_application.snapshot(", handler)
        self.assertIn("harvest_rules.WAITING", handler)
        self.assertIn("harvest_rules.remaining_text(", handler)
        self.assertIn("harvest_rules.harvest_count(", handler)
        self.assertIn("harvest_rules.roll_rewards(", handler)
        self.assertNotIn('"time_cost": 23', handler)
        self.assertNotIn('"加速基数": 0.05', handler)
        self.assertNotIn("datetime.strptime(last_time", handler)
        self.assertNotIn("get_player_info(user_id, \"mix_elixir_info\")", handler)

    def test_handler_keeps_the_cross_database_settlement(self) -> None:
        handler = self.handler()
        self.assertIn("mixelixir_application.harvest(", handler)
        self.assertNotIn("sql_message.send_back(", handler)
        for status in ("state_changed", "user_missing", "schema_missing"):
            self.assertIn(f'"{status}"', handler)

    def test_feature_modules_stay_pure(self) -> None:
        for name in ("harvest_rules.py", "harvest_view.py"):
            text = (FEATURE / name).read_text(encoding="utf-8")
            for banned in ("import nonebot", "import flask", "sqlite3.connect"):
                self.assertNotIn(banned, text, f"{name} imports {banned}")

    def test_repositories_no_longer_create_tables(self) -> None:
        for name in (
            "harvest_repository.py",
            "settlement_repository.py",
            "harvest_level_upgrade_repository.py",
            "fire_control_upgrade_repository.py",
        ):
            text = (FEATURE / name).read_text(encoding="utf-8")
            self.assertNotIn("CREATE TABLE", text, name)
            self.assertIn("_operations_ready", text, name)
            self.assertIn("schema_missing", text, name)


if __name__ == "__main__":
    unittest.main()