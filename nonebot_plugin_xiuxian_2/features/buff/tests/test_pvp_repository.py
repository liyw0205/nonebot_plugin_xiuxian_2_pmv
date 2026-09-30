from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..pvp_repository import NormalPvpSqlRepository


class NormalPvpRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as connection:
            connection.execute(
                "CREATE TABLE normal_pvp_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,user_stamina INTEGER,exp INTEGER)"
            )
            connection.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,?,?,?)",
                [("challenger", 100, 80, 3, 500), ("opponent", 120, 90, 4, 600)],
            )
        with sqlite3.connect(self.player) as connection:
            connection.execute(
                'CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"切磋胜利" INTEGER DEFAULT 0,"切磋失败" INTEGER DEFAULT 0)'
            )
        self.repository = NormalPvpSqlRepository(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _settle(self, operation_id: str = "pvp-op", **changes):
        request = {
            "expected_challenger_hp": 100,
            "expected_challenger_mp": 80,
            "expected_challenger_stamina": 3,
            "expected_challenger_exp": 500,
            "expected_opponent_hp": 120,
            "expected_opponent_mp": 90,
            "expected_opponent_stamina": 4,
            "expected_opponent_exp": 600,
            "challenger_final_hp": 55,
            "challenger_final_mp": 30,
            "opponent_final_hp": 70,
            "opponent_final_mp": 40,
            "winner_id": "challenger",
            "winner_name": "challenger-name",
            "battle_messages": ["battle finished"],
        }
        request.update(changes)
        return self.repository.settle(operation_id, "challenger", "opponent", **request)

    def test_settles_atomically_and_replays(self) -> None:
        first = self._settle()
        duplicate = self._settle()
        self.assertEqual((first["status"], duplicate["status"]), ("applied", "duplicate"))
        with sqlite3.connect(self.game) as connection:
            self.assertEqual(
                [("challenger", 55, 30, 2), ("opponent", 70, 40, 4)],
                connection.execute(
                    "SELECT user_id,hp,mp,user_stamina FROM user_xiuxian ORDER BY user_id"
                ).fetchall(),
            )
        with sqlite3.connect(self.player) as connection:
            self.assertEqual(
                [("challenger", 1, 0), ("opponent", 0, 1)],
                connection.execute(
                    'SELECT user_id,"切磋胜利","切磋失败" FROM statistics ORDER BY user_id'
                ).fetchall(),
            )

    def test_missing_schema_fails_closed_without_request_ddl(self) -> None:
        missing = Path(self.temp_dir.name) / "missing.db"
        repository = NormalPvpSqlRepository(missing, self.player)
        result = repository.settle(
            "missing", "challenger", "opponent",
            expected_challenger_hp=100, expected_challenger_mp=80,
            expected_challenger_stamina=3, expected_challenger_exp=500,
            expected_opponent_hp=120, expected_opponent_mp=90,
            expected_opponent_stamina=4, expected_opponent_exp=600,
            challenger_final_hp=55, challenger_final_mp=30,
            opponent_final_hp=70, opponent_final_mp=40,
        )
        self.assertEqual("schema_missing", result["status"])
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
