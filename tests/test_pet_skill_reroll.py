from __future__ import annotations

import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import (  # noqa: E402
    PetSkillRerollService,
)
from tests.test_db_backend import db_backend  # noqa: E402


class PetSkillRerollTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)")
            conn.execute("INSERT INTO back VALUES ('u',20032,1)")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE player_pet_item("
                "user_id TEXT,uid TEXT,pet_id TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER,"
                "skill_id TEXT,is_active INTEGER,updated_at INTEGER)"
            )
            conn.execute("INSERT INTO player_pet_item VALUES ('u','pet','fox',5,2,50,'old',1,0)")
        self.service = PetSkillRerollService(self.game, self.player)
        self.snapshot = ("pet", "fox", 5, 2, 50, "old", 1)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_reroll_consumes_item_and_replays_fixed_result(self) -> None:
        result = self.service.reroll("op", "u", self.snapshot, "new", 20032)
        self.assertEqual((result.status, result.skill_id), ("applied", "new"))
        duplicate = self.service.reroll("op", "u", self.snapshot, "new", 20032)
        self.assertEqual((duplicate.status, duplicate.skill_id), ("duplicate", "new"))
        with closing(db_backend.connect(self.game)) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 0)
        with closing(db_backend.connect(self.player)) as conn:
            self.assertEqual(conn.execute("SELECT skill_id FROM player_pet_item").fetchone()[0], "new")

    def test_missing_item_snapshot_change_and_payload_conflict(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET goods_num=0")
        self.assertEqual(self.service.reroll("missing", "u", self.snapshot, "new", 20032).status, "item_missing")
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET goods_num=1")
        stale = (*self.snapshot[:5], "other", self.snapshot[6])
        self.assertEqual(self.service.reroll("stale", "u", stale, "new", 20032).status, "state_changed")
        self.assertEqual(self.service.reroll("op", "u", self.snapshot, "new", 20032).status, "applied")
        self.assertEqual(self.service.reroll("op", "u", self.snapshot, "different", 20032).status, "state_changed")

    def test_operation_failure_rolls_back_skill_and_item(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE pet_skill_reroll_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,skill_id TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_reroll_operation BEFORE INSERT ON pet_skill_reroll_operations "
                "BEGIN SELECT RAISE(ABORT,'boom'); END"
            )
        with self.assertRaises(db_backend.Error):
            self.service.reroll("rollback", "u", self.snapshot, "new", 20032)
        with closing(db_backend.connect(self.game)) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back").fetchone()[0], 1)
        with closing(db_backend.connect(self.player)) as conn:
            self.assertEqual(conn.execute("SELECT skill_id FROM player_pet_item").fetchone()[0], "old")


if __name__ == "__main__":
    unittest.main()
