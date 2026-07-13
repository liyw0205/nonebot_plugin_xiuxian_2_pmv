from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.travel_claim_service import PetTravelClaimService
from tests.test_db_backend import db_backend


class PetTravelClaimServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        self.travel = {"pet_uid": "pet-1", "start_at": 1, "end_at": 2, "duration_hours": 1}
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s, %s)", ("user", 100, 200))
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, "
                "goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))"
            )
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("CREATE TABLE player_pet_item (user_id TEXT, uid TEXT, total_exp INTEGER)")
            conn.execute("INSERT INTO player_pet_item VALUES (%s,%s,%s)", ("user", "pet-1", 0))
            conn.execute("CREATE TABLE player_pet (user_id TEXT PRIMARY KEY, travel TEXT)")
            conn.execute("INSERT INTO player_pet VALUES (%s, %s)", ("user", json.dumps(self.travel, ensure_ascii=False)))
        self.service = PetTravelClaimService(self.game_database, self.player_database)
        self.rewards = [{"id": 1, "name": "item", "type": "type", "amount": 2}]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def claim(self, operation_id="claim", **overrides):
        values = dict(travel=self.travel, stone=10, exp=20, items=self.rewards, max_goods=99)
        values.update(overrides)
        return self.service.claim(operation_id, "user", values["travel"], values["stone"], values["exp"], values["items"], values["max_goods"])

    def state(self):
        with db_backend.connection(self.game_database) as conn:
            user = conn.execute("SELECT stone, exp FROM user_xiuxian WHERE user_id=%s", ("user",)).fetchone()
            item = conn.execute("SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("user", 1)).fetchone()
        with db_backend.connection(self.player_database) as conn:
            travel = conn.execute("SELECT travel FROM player_pet WHERE user_id=%s", ("user",)).fetchone()[0]
        return tuple(map(int, user)), tuple(map(int, item)) if item else None, travel

    def test_success_consumes_travel_and_grants_every_reward(self) -> None:
        result = self.claim()
        self.assertEqual((result.status, result.stone, result.exp, result.items), ("applied", 10, 20, ((1, 2),)))
        self.assertEqual(self.state(), ((110, 220), (2, 2), None))

    def test_inventory_full_preserves_travel_and_assets(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("user", 1, "item", "type", 99, "", "", 99))
        self.assertEqual(self.claim("full").status, "inventory_full")
        self.assertEqual(self.state(), ((100, 200), (99, 99), json.dumps(self.travel, ensure_ascii=False)))

    def test_missing_travel_pet_preserves_state(self) -> None:
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("DELETE FROM player_pet_item WHERE user_id=%s", ("user",))
        self.assertEqual(self.claim("missing-pet").status, "pet_missing")
        self.assertEqual(self.state(), ((100, 200), None, json.dumps(self.travel, ensure_ascii=False)))

    def test_stale_travel_changes_nothing(self) -> None:
        self.assertEqual(self.claim(travel={**self.travel, "end_at": 3}).status, "state_changed")
        self.assertEqual(self.state(), ((100, 200), None, json.dumps(self.travel, ensure_ascii=False)))

    def test_duplicate_reuses_result_and_conflict_is_rejected(self) -> None:
        first = self.claim("repeat")
        duplicate = self.claim("repeat")
        conflict = self.claim("repeat", stone=11)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual(self.state(), ((110, 220), (2, 2), None))

    def test_operation_failure_rolls_back_travel_and_rewards(self) -> None:
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("CREATE TABLE pet_travel_claim_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_claim BEFORE INSERT ON pet_travel_claim_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.claim("rollback")
        self.assertEqual(self.state(), ((100, 200), None, json.dumps(self.travel, ensure_ascii=False)))


if __name__ == "__main__":
    unittest.main()
