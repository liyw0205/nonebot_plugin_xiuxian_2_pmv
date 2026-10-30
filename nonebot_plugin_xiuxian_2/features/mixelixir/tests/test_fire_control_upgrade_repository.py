import tempfile
import unittest
from pathlib import Path

from ..fire_control_upgrade_repository import MixelixirFireControlUpgradeSqlRepository
from tests.test_db_backend import db_backend

# The authoritative player column set, from xiuxian2_handle.mix_elixir_infoconfigkey:
# the fire-control level lives in 丹药控火 and there is no 控火等级 column.  Building
# the fixture from this list is what keeps the repository's SQL honest.
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


class MixelixirFireControlUpgradeRepositoryTests(unittest.TestCase):
    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            with db_backend.transaction(player) as conn:
                columns = ", ".join(f'"{field}" TEXT' for field in REAL_PLAYER_FIELDS)
                conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{columns})")
                conn.execute(
                    'INSERT INTO mix_elixir_info(user_id,"丹药控火","炼丹经验") VALUES(?,?,?)',
                    ("u", "1", "100"),
                )
            repo = MixelixirFireControlUpgradeSqlRepository(game, player)
            first = repo.upgrade("u1", "u", 1, 100, 100, 2, 50)
            duplicate = repo.upgrade("u1", "u", 1, 100, 100, 2, 50)
            stale = repo.upgrade("u2", "u", 1, 100, 100, 2, 50)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))

    def test_upgrade_writes_the_fire_control_column(self):
        """The write must land in 丹药控火; 炼丹经验 pays for it and stones are untouched."""
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            player = Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            with db_backend.transaction(player) as conn:
                columns = ", ".join(f'"{field}" TEXT' for field in REAL_PLAYER_FIELDS)
                conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{columns})")
                conn.execute(
                    'INSERT INTO mix_elixir_info(user_id,"丹药控火","炼丹经验") VALUES(?,?,?)',
                    ("u", "0", "900"),
                )
            result = MixelixirFireControlUpgradeSqlRepository(game, player).upgrade(
                "u3", "u", 0, 900, 100, 1, 400
            )
            self.assertEqual((result.status, result.level, result.experience), ("applied", 1, 500))
            with db_backend.connection(player) as conn:
                row = conn.execute(
                    'SELECT "丹药控火","炼丹经验" FROM mix_elixir_info WHERE user_id=%s',
                    ("u",),
                ).fetchone()
            self.assertEqual((int(row[0]), int(row[1])), (1, 500))
            with db_backend.connection(game) as conn:
                self.assertEqual(
                    int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0]),
                    100,
                )


if __name__ == "__main__":
    unittest.main()
