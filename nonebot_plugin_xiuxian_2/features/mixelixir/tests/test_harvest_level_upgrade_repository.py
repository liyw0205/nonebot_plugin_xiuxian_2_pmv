import tempfile
import unittest
from pathlib import Path

from ..harvest_level_upgrade_repository import MixelixirHarvestLevelUpgradeSqlRepository
from tests.test_db_backend import db_backend

# The authoritative player column set, from xiuxian2_handle.mix_elixir_infoconfigkey.
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


class MixelixirHarvestLevelUpgradeRepositoryTests(unittest.TestCase):
    def build(self, temp: str, *, stone: int = 100, level: str = "1", experience: str = "100"):
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with db_backend.transaction(game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',%s)", (stone,))
        with db_backend.transaction(player) as conn:
            columns = ", ".join(f'"{field}" TEXT' for field in REAL_PLAYER_FIELDS)
            conn.execute(f"CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,{columns})")
            conn.execute(
                'INSERT INTO mix_elixir_info(user_id,"收取等级","炼丹经验") VALUES(?,?,?)',
                ("u", level, experience),
            )
        return game, player

    def test_applied_duplicate_and_state_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            repo = MixelixirHarvestLevelUpgradeSqlRepository(game, player)
            first = repo.upgrade("u1", "u", 1, 100, 100, 2, 50)
            duplicate = repo.upgrade("u1", "u", 1, 100, 100, 2, 50)
            stale = repo.upgrade("u2", "u", 1, 100, 100, 2, 50)
            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))

    def test_wallet_snapshot_must_match_the_real_balance(self):
        """The snapshot is a concurrency guard: a stale wallet must refuse."""
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=5000)
            repo = MixelixirHarvestLevelUpgradeSqlRepository(game, player)
            stale = repo.upgrade("u1", "u", 1, 100, 4999, 2, 50)
            self.assertEqual(stale.status, "state_changed")
            applied = repo.upgrade("u2", "u", 1, 100, 5000, 2, 50)
            self.assertEqual((applied.status, applied.level, applied.experience), ("applied", 2, 50))

    def test_none_snapshot_skips_the_wallet_check(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=12345)
            repo = MixelixirHarvestLevelUpgradeSqlRepository(game, player)
            applied = repo.upgrade("u1", "u", 1, 100, None, 2, 50)
            self.assertEqual(applied.status, "applied")
            self.assertEqual(applied.wallet_stone, 12345)


if __name__ == "__main__":
    unittest.main()
