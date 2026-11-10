import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..upgrade_repository import BlessedSpotUpgradeSqlRepository
from ..migrations import apply_blessed_spot_operations
from tests.test_db_backend import db_backend
from ....infrastructure.database import DatabaseUnitOfWork
from ....core.errors import DomainError


class BlessedSpotUpgradeRepositoryTests(unittest.TestCase):
    def test_each_write_rejection_rolls_back_both_databases_and_receipt(self):
        for target, table, reason in (
            ("game", "user_xiuxian", "stone_write_rejected"),
            ("player", "mix_elixir_info", "blessed_field_write_rejected"),
        ):
            with self.subTest(step=target), tempfile.TemporaryDirectory() as temp:
                game, player = Path(temp) / "game.db", Path(temp) / "player.db"
                with db_backend.transaction(game) as conn:
                    conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,blessed_spot_flag INTEGER)")
                    conn.execute("INSERT INTO user_xiuxian VALUES('u',500,1)")
                with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                    apply_blessed_spot_operations(uow)
                with db_backend.transaction(player) as conn:
                    conn.execute("CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,灵田数量 TEXT)")
                    conn.execute("INSERT INTO mix_elixir_info VALUES('u','1')")
                with db_backend.transaction(game if target == "game" else player) as conn:
                    conn.execute(f"CREATE TRIGGER skip_write BEFORE UPDATE ON {table} BEGIN SELECT RAISE(IGNORE); END")

                def snapshot():
                    with sqlite3.connect(game) as game_conn, sqlite3.connect(player) as player_conn:
                        return tuple(game_conn.iterdump()), tuple(player_conn.iterdump())

                before = snapshot()
                with self.assertRaises(DomainError) as caught:
                    BlessedSpotUpgradeSqlRepository(game, player).upgrade("blocked", "u", 1, 200, 10)
                self.assertEqual(caught.exception.code, reason)
                self.assertEqual(snapshot(), before)

    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,blessed_spot_flag INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',500,1)")
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                apply_blessed_spot_operations(uow)
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,灵田数量 TEXT)")
                conn.execute("INSERT INTO mix_elixir_info VALUES('u','1')")
            repository = BlessedSpotUpgradeSqlRepository(game, player)
            first = repository.upgrade("u1", "u", 1, 200, 10)
            duplicate = repository.upgrade("u1", "u", 1, 200, 10)
            stale = repository.upgrade("u2", "u", 1, 200, 10)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
