"""Production-shaped evidence for 炼丹升级 (升级收取等级 / 升级丹药控火).

Both defects this suite pins down were only reachable once the REAL player
schema (``mix_elixir_info`` has ``丹药控火`` and NOT ``控火等级``), the REAL
application and a real operation ledger were combined:

* ``MixelixirFireControlUpgradeSqlRepository`` read and wrote a nonexistent
  ``控火等级`` column, so every ``升级丹药控火`` raised ``OperationalError``.
  SQLite resolves an unknown identifier in a SELECT list to a string literal,
  which is why the read looked harmless and the write failed.
* ``MixelixirApplication.harvest_level_upgrade`` hard-coded
  ``expected_stone=0``, and the repository treats that as a wallet concurrency
  snapshot, so any player whose wallet was not exactly 0 got ``state_changed``
  and could never upgrade.

The pre-existing tests used a hand-built table that carried a ``控火等级``
column and a wallet value that happened to equal the hard-coded snapshot, so
both defects stayed green locally.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.mixelixir.application import MixelixirApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
    OperationLedger,
)
from tests.test_db_backend import db_backend

# The authoritative column list, from xiuxian2_handle.mix_elixir_infoconfigkey.
REAL_PLAYER_FIELDS = (
    "收取时间",
    "收取等级",
    "灵田数量",
    "药材速度",
    "灵田傀儡",
    "丹药控火",
    "丹药耐药性",
    "炼丹记录",
    "炼丹经验",
)


def real_player_schema(database: Path) -> None:
    columns = ", ".join(f'"{field}" TEXT' for field in REAL_PLAYER_FIELDS)
    with db_backend.transaction(database) as conn:
        conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{columns})")


class UpgradeProductionPathTests(unittest.TestCase):
    """Drive both upgrades the way the bot handlers drive them."""

    def build(
        self,
        temp: str,
        *,
        stone: int = 5000,
        harvest_level: str = "0",
        fire_control: str = "0",
        experience: str = "90000",
    ) -> tuple[Path, Path]:
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with DatabaseUnitOfWork(game, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,stone INTEGER)"
            )
            uow.execute(
                "INSERT INTO user_xiuxian VALUES('u1','化神境初期',?)", (stone,)
            )
            OperationLedger().ensure_schema(uow)
        real_player_schema(player)
        with db_backend.transaction(player) as conn:
            conn.execute(
                'INSERT INTO mix_elixir_info(user_id,"收取等级","丹药控火","炼丹经验") '
                "VALUES(?,?,?,?)",
                ("u1", harvest_level, fire_control, experience),
            )
        return game, player

    @staticmethod
    def player_state(player: Path) -> tuple[int, int, int]:
        with db_backend.connection(player) as conn:
            row = conn.execute(
                'SELECT CAST("收取等级" AS INTEGER),CAST("丹药控火" AS INTEGER),'
                'CAST("炼丹经验" AS INTEGER) FROM mix_elixir_info WHERE user_id=%s',
                ("u1",),
            ).fetchone()
        return int(row[0]), int(row[1]), int(row[2])

    @staticmethod
    def wallet(game: Path) -> int:
        with db_backend.connection(game) as conn:
            return int(
                conn.execute(
                    "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u1",)
                ).fetchone()[0]
            )

    # ---------------------------------------------------------------- 丹药控火

    def test_fire_control_upgrade_succeeds_on_the_real_schema(self) -> None:
        """Regression: the old SQL read and wrote a nonexistent 控火等级 column."""
        for level in ("0", "1", "2"):
            with self.subTest(丹药控火=level), tempfile.TemporaryDirectory() as temp:
                game, player = self.build(temp, fire_control=level)
                outcome = MixelixirApplication(game, player).fire_control_upgrade(
                    operation_id=f"fc-{level}",
                    user_id="u1",
                    current_level=int(level),
                    experience=90000,
                    expected_stone=5000,
                    next_level=int(level) + 1,
                    cost=1000,
                )
                self.assertEqual(outcome.status, "applied", outcome)
                harvest, fire, experience = self.player_state(player)
                self.assertEqual(fire, int(level) + 1)
                self.assertEqual(experience, 89000)
                self.assertEqual(self.wallet(game), 5000, "fire control is paid in 炼丹经验")

    def test_fire_control_duplicate_replays_without_double_spending(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            application = MixelixirApplication(game, player)
            first = application.fire_control_upgrade(
                operation_id="fc", user_id="u1", current_level=0, experience=90000,
                expected_stone=5000, next_level=1, cost=1000,
            )
            again = application.fire_control_upgrade(
                operation_id="fc", user_id="u1", current_level=0, experience=90000,
                expected_stone=5000, next_level=1, cost=1000,
            )
            self.assertEqual(first.status, "applied")
            self.assertTrue(again.replayed, again)
            self.assertEqual(again.data["status"], "applied")
            self.assertEqual(self.player_state(player), (0, 1, 89000))

    def test_fire_control_conflicting_payload_is_rejected(self) -> None:
        """Reusing an operation id for a different payload must not silently succeed."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            application = MixelixirApplication(game, player)
            application.fire_control_upgrade(
                operation_id="fc", user_id="u1", current_level=0, experience=90000,
                expected_stone=5000, next_level=1, cost=1000,
            )
            with self.assertRaises(OperationConflictError):
                application.fire_control_upgrade(
                    operation_id="fc", user_id="u1", current_level=0, experience=90000,
                    expected_stone=5000, next_level=1, cost=2000,
                )
            self.assertEqual(self.player_state(player), (0, 1, 89000))

    def test_fire_control_insufficient_experience_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, experience="500")
            outcome = MixelixirApplication(game, player).fire_control_upgrade(
                operation_id="fc", user_id="u1", current_level=0, experience=500,
                expected_stone=5000, next_level=1, cost=1000,
            )
            self.assertEqual(outcome.code, "experience_insufficient")
            self.assertFalse(outcome.ok)
            self.assertEqual(self.player_state(player), (0, 0, 500))

    def test_fire_control_stale_wallet_snapshot_is_state_changed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=5000)
            outcome = MixelixirApplication(game, player).fire_control_upgrade(
                operation_id="fc", user_id="u1", current_level=0, experience=90000,
                expected_stone=4999, next_level=1, cost=1000,
            )
            self.assertEqual(outcome.code, "state_changed")
            self.assertEqual(self.player_state(player), (0, 0, 90000))

    # -------------------------------------------------------------- 收取等级

    def test_harvest_level_upgrade_applies_for_every_wallet_balance(self) -> None:
        """Regression: expected_stone was hard-coded to 0, so any nonzero wallet failed."""
        for stone in (0, 1, 5000, 999_999_999):
            with self.subTest(stone=stone), tempfile.TemporaryDirectory() as temp:
                game, player = self.build(temp, stone=stone)
                outcome = MixelixirApplication(game, player).harvest_level_upgrade(
                    operation_id=f"hl-{stone}",
                    user_id="u1",
                    current_level=0,
                    experience=90000,
                    expected_stone=stone,
                    next_level=1,
                    cost=1500,
                )
                self.assertEqual(outcome.status, "applied", (stone, outcome))
                harvest, fire, experience = self.player_state(player)
                self.assertEqual(harvest, 1)
                self.assertEqual(experience, 88500)
                self.assertEqual(self.wallet(game), stone, "harvest level is paid in 炼丹经验")

    def test_harvest_level_upgrade_skips_wallet_check_when_snapshot_is_none(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=5000)
            outcome = MixelixirApplication(game, player).harvest_level_upgrade(
                operation_id="hl", user_id="u1", current_level=0, experience=90000,
                expected_stone=None, next_level=1, cost=1500,
            )
            self.assertEqual(outcome.status, "applied")
            self.assertEqual(self.player_state(player)[0], 1)

    def test_harvest_level_upgrade_stale_wallet_snapshot_is_state_changed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=5000)
            outcome = MixelixirApplication(game, player).harvest_level_upgrade(
                operation_id="hl", user_id="u1", current_level=0, experience=90000,
                expected_stone=4999, next_level=1, cost=1500,
            )
            self.assertEqual(outcome.code, "state_changed")
            self.assertEqual(self.player_state(player), (0, 0, 90000))

    def test_harvest_level_upgrade_duplicate_and_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            application = MixelixirApplication(game, player)
            first = application.harvest_level_upgrade(
                operation_id="hl", user_id="u1", current_level=0, experience=90000,
                expected_stone=5000, next_level=1, cost=1500,
            )
            again = application.harvest_level_upgrade(
                operation_id="hl", user_id="u1", current_level=0, experience=90000,
                expected_stone=5000, next_level=1, cost=1500,
            )
            self.assertEqual(first.status, "applied")
            self.assertTrue(again.replayed, again)
            self.assertEqual(self.player_state(player), (1, 0, 88500))
            with self.assertRaises(OperationConflictError):
                application.harvest_level_upgrade(
                    operation_id="hl", user_id="u1", current_level=0, experience=90000,
                    expected_stone=5000, next_level=1, cost=2000,
                )
            self.assertEqual(self.player_state(player), (1, 0, 88500))

    # --------------------------------------------------------- failure isolation

    def test_missing_player_row_does_not_create_a_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with db_backend.transaction(player) as conn:
                conn.execute("DELETE FROM mix_elixir_info")
            outcome = MixelixirApplication(game, player).fire_control_upgrade(
                operation_id="fc", user_id="u1", current_level=0, experience=90000,
                expected_stone=5000, next_level=1, cost=1000,
            )
            self.assertEqual(outcome.code, "state_changed")
            with db_backend.connection(player) as conn:
                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            self.assertEqual([row[0] for row in rows], ["mix_elixir_info"])

    def test_missing_player_row_is_reported_not_silently_created(self) -> None:
        """A player row absent from user_xiuxian must fail closed."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with db_backend.transaction(game) as conn:
                conn.execute("DELETE FROM user_xiuxian")
            outcome = MixelixirApplication(game, player).harvest_level_upgrade(
                operation_id="hl", user_id="u1", current_level=0, experience=90000,
                expected_stone=0, next_level=1, cost=1500,
            )
            self.assertEqual(outcome.code, "state_changed")
            self.assertEqual(self.player_state(player), (0, 0, 90000))

    def test_operation_write_failure_rolls_back_level_and_experience(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TABLE mixelixir_fire_control_upgrade_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
                    "wallet_stone INTEGER NOT NULL,level INTEGER NOT NULL,"
                    "experience INTEGER NOT NULL,created_at TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TRIGGER fail_fire_control BEFORE INSERT ON "
                    "mixelixir_fire_control_upgrade_operations "
                    "BEGIN SELECT RAISE(ABORT, 'failed'); END"
                )
            with self.assertRaises(db_backend.IntegrityError):
                MixelixirApplication(game, player).fire_control_upgrade(
                    operation_id="fc", user_id="u1", current_level=0, experience=90000,
                    expected_stone=5000, next_level=1, cost=1000,
                )
            self.assertEqual(self.player_state(player), (0, 0, 90000))

    def test_repository_uses_the_authoritative_column_names(self) -> None:
        """A quoted 控火等级 in SQL is the defect returning; the column is 丹药控火."""
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "mixelixir"
            / "fire_control_upgrade_repository.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn('\\"控火等级\\"', source, "the nonexistent column is back in SQL")
        self.assertIn('_LEVEL_FIELD = "丹药控火"', source)
        self.assertIn("CAST({level_field} AS INTEGER)", source)


class UpgradeHandlerContractTests(unittest.TestCase):
    """Both upgrade handlers must send the wallet snapshot they observed."""

    def handler_source(self) -> str:
        return (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_mixelixir"
            / "__init__.py"
        ).read_text(encoding="utf-8")

    def test_harvest_level_handler_passes_the_wallet_snapshot(self) -> None:
        source = self.handler_source()
        handler = source[
            source.index("@mix_elixir_sqdj_up.handle"):source.index("@mix_elixir_dykh_up.handle")
        ]
        self.assertIn("mixelixir_application.harvest_level_upgrade(", handler)
        self.assertIn("expected_stone=user_info['stone']", handler)

    def test_fire_control_handler_passes_the_wallet_snapshot(self) -> None:
        source = self.handler_source()
        handler = source[source.index("@mix_elixir_dykh_up.handle"):]
        self.assertIn("mixelixir_application.fire_control_upgrade(", handler)
        self.assertIn("expected_stone=user_info['stone']", handler)

    def test_application_no_longer_hard_codes_the_wallet_snapshot(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "mixelixir"
            / "application.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("expected_stone=0, next_level", source)


if __name__ == "__main__":
    unittest.main()
