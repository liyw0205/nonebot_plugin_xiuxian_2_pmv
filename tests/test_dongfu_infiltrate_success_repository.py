from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
from nonebot_plugin_xiuxian_2.features.dongfu.infiltrate_success_repository import (
    DongfuInfiltrateSuccessSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.dongfu.migrations import (
    apply_dongfu_infiltrate_success,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class DongfuInfiltrateSuccessSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        self.slots = [{"slot": 1, "seed_id": 21001, "plant_finish": "2026-07-13 12:00:00"}]
        self.expected_slots = json.dumps(self.slots)
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)", ("u", 100))
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
                "UNIQUE(user_id,goods_id))"
            )
            apply_dongfu_infiltrate_success(uow)
        with DatabaseUnitOfWork(self.player_database) as uow:
            uow.execute(
                "CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,infiltrate_date TEXT,"
                "infiltrate_active_count INTEGER,infiltrate_random_count INTEGER,intrude_date TEXT,"
                "intrude_count INTEGER,patrol_guard INTEGER,plant_slots TEXT)"
            )
            uow.executemany(
                "INSERT INTO dongfu_status VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    ("u", 1, "", 0, 0, "", 0, 0, "[]"),
                    ("t", 1, "", 0, 0, "", 0, 1, self.expected_slots),
                ),
            )
        self.repository = DongfuInfiltrateSuccessSqlRepository(
            self.game_database, self.player_database
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def settle(self, operation_id: str = "op", **overrides: object):
        values: dict[str, object] = {
            "expected_slots": self.expected_slots,
            "new_finish": "2026-07-13 13:00:00",
            "rewards": ((3001, "药材", "药材", 2),),
            "stone": 500,
            "max_goods_num": 99,
        }
        values.update(overrides)
        return self.repository.settle(
            operation_id,
            "u",
            "t",
            "2026-07-13",
            "infiltrate_active_count",
            3,
            3,
            str(values["expected_slots"]),
            1,
            str(values["new_finish"]),
            values["rewards"],
            int(values["stone"]),
            True,
            int(values["max_goods_num"]),
        )

    def state(self) -> tuple[int, int, int, int, int, str]:
        with DatabaseUnitOfWork(self.game_database) as uow:
            stone = int(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("u",))["stone"])
            item = uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", ("u", 3001))
        with DatabaseUnitOfWork(self.player_database) as uow:
            visitor = uow.query_one(
                "SELECT infiltrate_active_count FROM dongfu_status WHERE user_id=?", ("u",)
            )
            target = uow.query_one(
                "SELECT intrude_count,patrol_guard,plant_slots FROM dongfu_status WHERE user_id=?", ("t",)
            )
        return (
            stone,
            0 if item is None else int(item["goods_num"]),
            int(visitor["infiltrate_active_count"]),
            int(target["intrude_count"]),
            int(target["patrol_guard"]),
            str(json.loads(str(target["plant_slots"]))[0]["plant_finish"]),
        )

    def test_settles_and_replays_once(self) -> None:
        self.assertEqual(self.settle("same").status, "settled")
        self.assertEqual(self.settle("same").status, "duplicate")
        self.assertEqual(self.state(), (600, 2, 1, 1, 0, "2026-07-13 13:00:00"))

    def test_capacity_and_snapshot_rejections_do_not_mutate(self) -> None:
        self.assertEqual(self.settle("full", max_goods_num=1).status, "inventory_full")
        self.assertEqual(self.settle("stale", expected_slots="[]").status, "state_changed")
        self.assertEqual(self.state(), (100, 0, 0, 0, 1, "2026-07-13 12:00:00"))

    def test_operation_insert_failure_rolls_back_both_databases(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_infiltrate BEFORE INSERT ON dongfu_infiltrate_success_operations "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), (100, 0, 0, 0, 1, "2026-07-13 12:00:00"))

    def test_application_default_uses_feature_repository(self) -> None:
        result = DongfuApplication(
            self.game_database, self.player_database
        ).infiltrate_success(
            operation_id="application",
            visitor_id="u",
            target_id="t",
            day="2026-07-13",
            mode_field="infiltrate_active_count",
            mode_limit=3,
            target_limit=3,
            expected_slots=self.expected_slots,
            slot_no=1,
            new_finish="2026-07-13 13:00:00",
            rewards=((3001, "药材", "药材", 2),),
            stone=500,
            consume_guard=True,
            max_goods_num=99,
        )
        self.assertEqual(result.status, "settled")
        self.assertEqual(self.state(), (600, 2, 1, 1, 0, "2026-07-13 13:00:00"))


if __name__ == "__main__":
    unittest.main()
